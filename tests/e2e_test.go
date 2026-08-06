package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"tournament-engine/internal/checkin"
	"tournament-engine/internal/gateway"
	pairingservice "tournament-engine/internal/pairing"
	"tournament-engine/pkg/database"
	pairingv1 "tournament-engine/pkg/proto/pairing/v1"
	"tournament-engine/pkg/testutil"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"gorm.io/gorm"
)

const (
	e2eTournamentID = "t-e2e"
	e2ePlayerCount  = 10
	e2eTimeout      = 15 * time.Second
)

func TestTournamentPipelineEndToEnd(t *testing.T) {
	testCtx, cancelTest := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancelTest()

	environment, err := testutil.SetupTestContainers(testCtx)
	if err != nil {
		t.Fatalf("SetupTestContainers() error = %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := environment.Terminate(cleanupCtx); err != nil {
			t.Errorf("terminate test containers: %v", err)
		}
	})

	postgresDB := openPostgres(t, testCtx, environment.PostgresDSN)
	redisClient := openRedis(t, testCtx, environment.RedisAddr)
	repository, err := checkin.NewRepository(postgresDB)
	if err != nil {
		t.Fatalf("create check-in repository: %v", err)
	}

	consumerConfig := checkin.DefaultConsumerConfig()
	consumerConfig.ConsumerName = "e2e-consumer"
	consumerConfig.Block = 25 * time.Millisecond
	consumer, err := checkin.NewConsumer(redisClient, consumerConfig, repository)
	if err != nil {
		t.Fatalf("create check-in consumer: %v", err)
	}
	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- consumer.Run(consumerCtx)
	}()
	t.Cleanup(func() {
		cancelConsumer()
		select {
		case err := <-consumerDone:
			if err != nil {
				t.Errorf("check-in consumer stopped with error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("check-in consumer did not stop")
		}
	})
	waitForConsumerGroup(t, redisClient, consumerConfig)

	pairingAddress, pairingClient := startPairingService(t)
	t.Logf("pairing gRPC service listening on %s", pairingAddress)

	httpAddress := startGatewayServer(
		t,
		checkin.NewCheckinManager(redisClient),
		pairingClient,
	)
	t.Logf("gateway HTTP server listening on %s", httpAddress)

	playerIDs := make([]string, e2ePlayerCount)
	for index := range e2ePlayerCount {
		playerIDs[index] = fmt.Sprintf("player-%02d", index+1)
	}

	issueConcurrentCheckins(t, httpAddress, playerIDs, http.StatusOK)
	assertRedisLocks(t, redisClient, playerIDs)

	// Simultaneous duplicate requests exercise the same Lua-protected key. They
	// must all observe the existing lock and must not publish extra events.
	duplicateIDs := make([]string, 12)
	for index := range duplicateIDs {
		duplicateIDs[index] = playerIDs[0]
	}
	issueConcurrentCheckins(t, httpAddress, duplicateIDs, http.StatusConflict)

	streamLength, err := redisClient.XLen(context.Background(), checkin.CheckinEventStream).Result()
	if err != nil {
		t.Fatalf("read check-in event stream length: %v", err)
	}
	if streamLength != e2ePlayerCount {
		t.Fatalf("check-in event count = %d, want %d", streamLength, e2ePlayerCount)
	}

	waitForPersistedCheckins(t, postgresDB, e2ePlayerCount)
	assertPersistedPlayers(t, postgresDB, playerIDs)
	assertPairings(t, httpAddress, playerIDs)
}

func openPostgres(t *testing.T, ctx context.Context, dsn string) *gorm.DB {
	t.Helper()
	db, err := database.OpenGORM(ctx, dsn)
	if err != nil {
		t.Fatalf("open ephemeral Postgres: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get Postgres SQL connection: %v", err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close Postgres connection: %v", err)
		}
	})
	return db
}

func openRedis(t *testing.T, ctx context.Context, address string) *redis.Client {
	t.Helper()
	client, err := database.OpenRedis(ctx, database.RedisConfig{
		Address:  address,
		Database: 0,
		PoolSize: 32,
	})
	if err != nil {
		t.Fatalf("open ephemeral Redis: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close Redis connection: %v", err)
		}
	})
	return client
}

func waitForConsumerGroup(t *testing.T, client *redis.Client, config checkin.ConsumerConfig) {
	t.Helper()
	eventually(t, 5*time.Second, func() (bool, error) {
		groups, err := client.XInfoGroups(context.Background(), config.Stream).Result()
		if err != nil {
			return false, nil
		}
		for _, group := range groups {
			if group.Name == config.Group {
				return true, nil
			}
		}
		return false, nil
	})
}

func startPairingService(t *testing.T) (string, *gateway.PairingGRPCClient) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for pairing gRPC: %v", err)
	}

	workerPool, err := pairingservice.NewWorkerPool(4)
	if err != nil {
		_ = listener.Close()
		t.Fatalf("create pairing worker pool: %v", err)
	}
	t.Cleanup(workerPool.Close)

	server := grpc.NewServer()
	pairingv1.RegisterPairingServiceServer(server, pairingservice.NewService(workerPool))
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.GracefulStop()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("pairing gRPC server stopped with error: %v", err)
			}
		case <-time.After(5 * time.Second):
			server.Stop()
			t.Error("pairing gRPC server did not stop")
		}
	})

	client, err := gateway.NewPairingGRPCClient(listener.Addr().String())
	if err != nil {
		t.Fatalf("create gateway pairing client: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close gateway pairing client: %v", err)
		}
	})
	return listener.Addr().String(), client
}

func startGatewayServer(
	t *testing.T,
	checkins gateway.CheckinService,
	pairings gateway.PairingClient,
) string {
	t.Helper()
	httpGateway, err := gateway.New(checkins, pairings)
	if err != nil {
		t.Fatalf("create HTTP gateway: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for HTTP gateway: %v", err)
	}
	server := &http.Server{
		Handler:           httpGateway.Handler(),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       10 * time.Second,
	}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			t.Errorf("shut down HTTP gateway: %v", err)
		}
		select {
		case err := <-serveDone:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("HTTP gateway stopped with error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("HTTP gateway did not stop")
		}
	})
	return "http://" + listener.Addr().String()
}

func issueConcurrentCheckins(
	t *testing.T,
	gatewayAddress string,
	playerIDs []string,
	wantStatus int,
) {
	t.Helper()
	type result struct {
		playerID string
		status   int
		body     string
		err      error
	}

	client := &http.Client{Timeout: e2eTimeout}
	start := make(chan struct{})
	results := make(chan result, len(playerIDs))
	var requests sync.WaitGroup
	requests.Add(len(playerIDs))
	for _, playerID := range playerIDs {
		go func() {
			defer requests.Done()
			<-start
			statusCode, responseBody, err := postJSON(
				client,
				gatewayAddress+"/api/v1/tournaments/"+e2eTournamentID+"/checkin",
				map[string]any{"player_id": playerID, "ttl_seconds": 60},
			)
			results <- result{playerID: playerID, status: statusCode, body: responseBody, err: err}
		}()
	}
	close(start)
	requests.Wait()
	close(results)

	for result := range results {
		if result.err != nil {
			t.Errorf("POST check-in for %s: %v", result.playerID, result.err)
			continue
		}
		if result.status != wantStatus {
			t.Errorf(
				"POST check-in for %s status = %d, want %d; body=%s",
				result.playerID,
				result.status,
				wantStatus,
				result.body,
			)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
}

func assertRedisLocks(t *testing.T, client *redis.Client, playerIDs []string) {
	t.Helper()
	ctx := context.Background()
	for _, playerID := range playerIDs {
		key := fmt.Sprintf("checkin:%s:%s", e2eTournamentID, playerID)
		value, err := client.Get(ctx, key).Result()
		if err != nil {
			t.Fatalf("read Redis lock %s: %v", key, err)
		}
		if value != "1" {
			t.Errorf("Redis lock %s value = %q, want 1", key, value)
		}
		ttl, err := client.PTTL(ctx, key).Result()
		if err != nil {
			t.Fatalf("read Redis lock TTL %s: %v", key, err)
		}
		if ttl <= 0 || ttl > time.Minute {
			t.Errorf("Redis lock %s TTL = %s, want > 0 and <= 1m", key, ttl)
		}
	}
}

func waitForPersistedCheckins(t *testing.T, db *gorm.DB, want int64) {
	t.Helper()
	eventually(t, e2eTimeout, func() (bool, error) {
		var count int64
		err := db.WithContext(context.Background()).
			Model(&checkin.CheckinRecord{}).
			Where("tournament_id = ?", e2eTournamentID).
			Count(&count).Error
		return count == want, err
	})
}

func assertPersistedPlayers(t *testing.T, db *gorm.DB, playerIDs []string) {
	t.Helper()
	var records []checkin.CheckinRecord
	if err := db.WithContext(context.Background()).
		Where("tournament_id = ?", e2eTournamentID).
		Order("player_id ASC").
		Find(&records).Error; err != nil {
		t.Fatalf("query persisted E2E check-ins: %v", err)
	}
	if len(records) != len(playerIDs) {
		t.Fatalf("persisted E2E check-in count = %d, want %d", len(records), len(playerIDs))
	}
	for index, record := range records {
		if record.PlayerID != playerIDs[index] {
			t.Errorf("persisted player at index %d = %q, want %q", index, record.PlayerID, playerIDs[index])
		}
		if record.CheckedInAt.IsZero() {
			t.Errorf("persisted player %s has a zero check-in timestamp", record.PlayerID)
		}
	}
}

type pairingHTTPResponse struct {
	Success bool `json:"success"`
	Matches []struct {
		MatchID       string `json:"match_id"`
		WhitePlayerID string `json:"white_player_id"`
		BlackPlayerID string `json:"black_player_id"`
		BoardNumber   int32  `json:"board_number"`
	} `json:"matches"`
	ByePlayerID string `json:"bye_player_id"`
}

func assertPairings(t *testing.T, gatewayAddress string, playerIDs []string) {
	t.Helper()
	players := make([]map[string]any, 0, len(playerIDs))
	for index, playerID := range playerIDs {
		players = append(players, map[string]any{
			"player_id": playerID,
			"score":     len(playerIDs) - index,
		})
	}
	statusCode, responseBody, err := postJSON(
		&http.Client{Timeout: e2eTimeout},
		gatewayAddress+"/api/v1/tournaments/"+e2eTournamentID+"/pairings",
		map[string]any{"round": 1, "players": players},
	)
	if err != nil {
		t.Fatalf("POST pairings: %v", err)
	}
	if statusCode != http.StatusOK {
		t.Fatalf("POST pairings status = %d, want 200; body=%s", statusCode, responseBody)
	}

	var response pairingHTTPResponse
	if err := json.Unmarshal([]byte(responseBody), &response); err != nil {
		t.Fatalf("decode pairing response: %v; body=%s", err, responseBody)
	}
	if !response.Success {
		t.Fatalf("pairing response success = false; body=%s", responseBody)
	}
	if response.ByePlayerID != "" {
		t.Errorf("pairing response bye_player_id = %q, want empty for even player count", response.ByePlayerID)
	}
	if len(response.Matches) != len(playerIDs)/2 {
		t.Fatalf("generated match count = %d, want %d", len(response.Matches), len(playerIDs)/2)
	}

	seenPlayers := make(map[string]bool, len(playerIDs))
	for index, match := range response.Matches {
		wantBoard := int32(index + 1)
		if match.BoardNumber != wantBoard {
			t.Errorf("match %d board = %d, want %d", index, match.BoardNumber, wantBoard)
		}
		wantMatchID := fmt.Sprintf("%s-r1-b%d", e2eTournamentID, wantBoard)
		if match.MatchID != wantMatchID {
			t.Errorf("match %d ID = %q, want %q", index, match.MatchID, wantMatchID)
		}
		for _, playerID := range []string{match.WhitePlayerID, match.BlackPlayerID} {
			if seenPlayers[playerID] {
				t.Errorf("player %q appears in more than one pairing", playerID)
			}
			seenPlayers[playerID] = true
		}
	}
	for _, playerID := range playerIDs {
		if !seenPlayers[playerID] {
			t.Errorf("player %q is missing from generated pairings", playerID)
		}
	}
}

func postJSON(client *http.Client, url string, payload any) (int, string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, "", fmt.Errorf("encode request: %w", err)
	}
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, "", fmt.Errorf("create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return 0, "", fmt.Errorf("execute request: %w", err)
	}
	responseBody, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return 0, "", fmt.Errorf("read response: %w", readErr)
	}
	if closeErr != nil {
		return 0, "", fmt.Errorf("close response body: %w", closeErr)
	}
	return response.StatusCode, string(responseBody), nil
}

func eventually(t *testing.T, timeout time.Duration, condition func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ready, err := condition()
		if err != nil {
			t.Fatalf("eventual condition error: %v", err)
		}
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("eventual condition was not met within %s", timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
