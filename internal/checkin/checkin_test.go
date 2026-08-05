package checkin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultRedisAddress  = "localhost:6379"
	defaultRedisPassword = "redis123"
	testCheckinTTL       = 30 * time.Second
)

var testIDCounter atomic.Uint64

func TestTryCheckInSingleSuccess(t *testing.T) {
	manager, client := newTestManager(t)
	tournamentID, playerID := uniqueCheckinIDs(t)
	key := checkinKey(tournamentID, playerID)
	t.Cleanup(func() { deleteTestCheckinData(t, client, key, tournamentID, playerID) })

	succeeded, err := manager.TryCheckIn(
		context.Background(),
		tournamentID,
		playerID,
		testCheckinTTL,
	)
	if err != nil {
		t.Fatalf("TryCheckIn() error = %v", err)
	}
	if !succeeded {
		t.Fatal("TryCheckIn() = false, want true for a new check-in")
	}

	value, err := client.Get(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("read check-in key: %v", err)
	}
	if value != "1" {
		t.Fatalf("check-in value = %q, want %q", value, "1")
	}

	ttl, err := client.PTTL(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("read check-in TTL: %v", err)
	}
	if ttl <= 0 || ttl > testCheckinTTL {
		t.Fatalf("check-in TTL = %s, want > 0 and <= %s", ttl, testCheckinTTL)
	}
}

func TestTryCheckInRejectsDuplicate(t *testing.T) {
	manager, client := newTestManager(t)
	tournamentID, playerID := uniqueCheckinIDs(t)
	key := checkinKey(tournamentID, playerID)
	t.Cleanup(func() { deleteTestCheckinData(t, client, key, tournamentID, playerID) })

	first, err := manager.TryCheckIn(
		context.Background(),
		tournamentID,
		playerID,
		testCheckinTTL,
	)
	if err != nil {
		t.Fatalf("first TryCheckIn() error = %v", err)
	}
	if !first {
		t.Fatal("first TryCheckIn() = false, want true")
	}

	second, err := manager.TryCheckIn(
		context.Background(),
		tournamentID,
		playerID,
		testCheckinTTL,
	)
	if err != nil {
		t.Fatalf("duplicate TryCheckIn() error = %v", err)
	}
	if second {
		t.Fatal("duplicate TryCheckIn() = true, want false")
	}
}

func TestTryCheckInConcurrentExactlyOneSuccess(t *testing.T) {
	const goroutineCount = 64

	manager, client := newTestManager(t)
	tournamentID, playerID := uniqueCheckinIDs(t)
	key := checkinKey(tournamentID, playerID)
	t.Cleanup(func() { deleteTestCheckinData(t, client, key, tournamentID, playerID) })

	type result struct {
		succeeded bool
		err       error
	}

	start := make(chan struct{})
	results := make(chan result, goroutineCount)
	var workers sync.WaitGroup
	workers.Add(goroutineCount)

	for range goroutineCount {
		go func() {
			defer workers.Done()
			<-start

			succeeded, err := manager.TryCheckIn(
				context.Background(),
				tournamentID,
				playerID,
				testCheckinTTL,
			)
			results <- result{succeeded: succeeded, err: err}
		}()
	}

	close(start)
	workers.Wait()
	close(results)

	successCount := 0
	for result := range results {
		if result.err != nil {
			t.Errorf("TryCheckIn() error = %v", result.err)
			continue
		}
		if result.succeeded {
			successCount++
		}
	}

	if successCount != 1 {
		t.Fatalf("successful concurrent check-ins = %d, want exactly 1", successCount)
	}
}

func newTestManager(t *testing.T) (*CheckinManager, *redis.Client) {
	t.Helper()
	client := newTestRedisClient(t)
	return NewCheckinManager(client), client
}

func newTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	address := os.Getenv("REDIS_ADDR")
	if address == "" {
		address = defaultRedisAddress
	}
	password := os.Getenv("REDIS_PASSWORD")
	if password == "" {
		password = defaultRedisPassword
	}

	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       0,
	})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close Redis client: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect to Redis at %s: %v", address, err)
	}

	return client
}

func uniqueCheckinIDs(t *testing.T) (string, string) {
	t.Helper()

	id := testIDCounter.Add(1)
	return fmt.Sprintf("test-%d-%d", time.Now().UnixNano(), id), "player"
}

func deleteTestCheckinData(
	t *testing.T,
	client *redis.Client,
	key, tournamentID, playerID string,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Del(ctx, key).Err(); err != nil {
		t.Errorf("delete test key %q: %v", key, err)
	}

	messages, err := client.XRange(ctx, CheckinEventStream, "-", "+").Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		t.Errorf("read test check-in events: %v", err)
		return
	}

	messageIDs := make([]string, 0, 1)
	for _, message := range messages {
		if streamValue(message.Values, "tournament_id") == tournamentID &&
			streamValue(message.Values, "player_id") == playerID {
			messageIDs = append(messageIDs, message.ID)
		}
	}
	if len(messageIDs) > 0 {
		if err := client.XDel(ctx, CheckinEventStream, messageIDs...).Err(); err != nil {
			t.Errorf("delete test check-in events: %v", err)
		}
	}
}
