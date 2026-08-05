package checkin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"tournament-engine/pkg/database"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const defaultTestPostgresDSN = "host=localhost user=root password=secret dbname=chess_db port=5432 sslmode=disable TimeZone=UTC"

func TestSuccessfulCheckInPublishesEvent(t *testing.T) {
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
		t.Fatal("TryCheckIn() = false, want true")
	}

	messages, err := client.XRange(context.Background(), CheckinEventStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("read check-in event stream: %v", err)
	}

	matchingEvents := 0
	for _, message := range messages {
		if streamValue(message.Values, "tournament_id") != tournamentID ||
			streamValue(message.Values, "player_id") != playerID {
			continue
		}
		matchingEvents++

		timestamp := streamValue(message.Values, "timestamp")
		if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
			t.Errorf("event timestamp %q is not RFC3339Nano: %v", timestamp, err)
		}
	}

	if matchingEvents != 1 {
		t.Fatalf("published matching events = %d, want exactly 1", matchingEvents)
	}
}

func TestConsumerPersistsAndAcknowledgesMessage(t *testing.T) {
	client := newTestRedisClient(t)
	config := testConsumerConfig(t)
	cleanupConsumerKeys(t, client, config)

	repository, postgresDB := newTestRepository(t)
	tournamentID := fmt.Sprintf("tournament-persistence-%d", testIDCounter.Add(1))
	playerID := "player-persistence"
	checkedInAt := time.Now().UTC().Truncate(time.Microsecond)
	cleanupCheckinRecord(t, postgresDB, tournamentID, playerID)

	addTestCheckinEvent(t, client, config.Stream, tournamentID, playerID, checkedInAt)
	consumer, err := NewConsumer(
		client,
		config,
		repository,
	)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	cancel, consumerDone := startTestConsumer(t, consumer)
	defer cancel()

	eventually(t, 3*time.Second, func() (bool, error) {
		var count int64
		err := postgresDB.WithContext(context.Background()).
			Model(&CheckinRecord{}).
			Where("tournament_id = ? AND player_id = ?", tournamentID, playerID).
			Count(&count).Error
		return err == nil && count == 1, err
	})

	var record CheckinRecord
	if err := postgresDB.WithContext(context.Background()).
		Where("tournament_id = ? AND player_id = ?", tournamentID, playerID).
		First(&record).Error; err != nil {
		t.Fatalf("read persisted check-in record: %v", err)
	}
	if !record.CheckedInAt.Equal(checkedInAt) {
		t.Errorf("persisted checked_in_at = %s, want %s", record.CheckedInAt, checkedInAt)
	}
	t.Logf(
		"persisted check-in row id=%d tournament_id=%s player_id=%s",
		record.ID,
		record.TournamentID,
		record.PlayerID,
	)

	eventually(t, 2*time.Second, func() (bool, error) {
		pending, err := client.XPending(context.Background(), config.Stream, config.Group).Result()
		return err == nil && pending.Count == 0, err
	})

	cancel()
	assertConsumerStopped(t, consumerDone)
}

func TestRepositorySaveCheckinIsIdempotent(t *testing.T) {
	repository, postgresDB := newTestRepository(t)
	tournamentID := fmt.Sprintf("tournament-idempotency-%d", testIDCounter.Add(1))
	playerID := "player-idempotency"
	firstCheckin := time.Now().UTC().Truncate(time.Microsecond)
	cleanupCheckinRecord(t, postgresDB, tournamentID, playerID)

	if err := repository.SaveCheckin(
		context.Background(),
		tournamentID,
		playerID,
		firstCheckin,
	); err != nil {
		t.Fatalf("first SaveCheckin() error = %v", err)
	}
	if err := repository.SaveCheckin(
		context.Background(),
		tournamentID,
		playerID,
		firstCheckin.Add(time.Minute),
	); err != nil {
		t.Fatalf("duplicate SaveCheckin() error = %v", err)
	}

	var records []CheckinRecord
	if err := postgresDB.WithContext(context.Background()).
		Where("tournament_id = ? AND player_id = ?", tournamentID, playerID).
		Find(&records).Error; err != nil {
		t.Fatalf("read idempotent check-in records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("persisted duplicate record count = %d, want 1", len(records))
	}
	if !records[0].CheckedInAt.Equal(firstCheckin) {
		t.Errorf("duplicate save changed checked_in_at to %s, want %s", records[0].CheckedInAt, firstCheckin)
	}
}

func TestConsumerRetriesThenRoutesToDLQ(t *testing.T) {
	client := newTestRedisClient(t)
	config := testConsumerConfig(t)
	config.WorkerCount = 1
	config.InitialBackoff = 5 * time.Millisecond
	cleanupConsumerKeys(t, client, config)

	messageID := addTestCheckinEvent(
		t,
		client,
		config.Stream,
		"tournament-failure",
		"player-failure",
		time.Now().UTC(),
	)
	var processingAttempts atomic.Int32
	consumer, err := NewConsumer(
		client,
		config,
		checkinRepositoryFunc(func(context.Context, string, string, time.Time) error {
			processingAttempts.Add(1)
			return errors.New("simulated processing failure")
		}),
	)
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	cancel, consumerDone := startTestConsumer(t, consumer)
	defer cancel()

	eventually(t, 3*time.Second, func() (bool, error) {
		length, err := client.XLen(context.Background(), config.DLQStream).Result()
		return err == nil && length == 1, err
	})

	if got, want := processingAttempts.Load(), int32(config.MaxRetries+1); got != want {
		t.Fatalf("processing attempts = %d, want %d", got, want)
	}

	dlqMessages, err := client.XRange(context.Background(), config.DLQStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("read DLQ stream: %v", err)
	}
	if len(dlqMessages) != 1 {
		t.Fatalf("DLQ message count = %d, want 1", len(dlqMessages))
	}
	dlqValues := dlqMessages[0].Values
	if got := streamValue(dlqValues, "original_message_id"); got != messageID {
		t.Errorf("DLQ original_message_id = %q, want %q", got, messageID)
	}
	if got := streamValue(dlqValues, "retry_count"); got != "4" {
		t.Errorf("DLQ retry_count = %q, want %q", got, "4")
	}
	if got := streamValue(dlqValues, "error_message"); got != "simulated processing failure" {
		t.Errorf("DLQ error_message = %q, want simulated failure", got)
	}

	pending, err := client.XPending(context.Background(), config.Stream, config.Group).Result()
	if err != nil {
		t.Fatalf("read pending messages: %v", err)
	}
	if pending.Count != 0 {
		t.Errorf("pending message count = %d, want 0", pending.Count)
	}

	retryTracked, err := client.HExists(
		context.Background(),
		config.RetryCountHash,
		config.Stream+"|"+messageID,
	).Result()
	if err != nil {
		t.Fatalf("read retry tracking: %v", err)
	}
	if retryTracked {
		t.Error("retry counter still exists after DLQ routing")
	}

	cancel()
	assertConsumerStopped(t, consumerDone)
}

func testConsumerConfig(t *testing.T) ConsumerConfig {
	t.Helper()

	suffix := fmt.Sprintf("%d-%d", time.Now().UnixNano(), testIDCounter.Add(1))
	config := DefaultConsumerConfig()
	config.Stream = "stream:test:checkin_events:" + suffix
	config.DLQStream = "stream:test:checkin_dlq:" + suffix
	config.RetryCountHash = "checkin:test:retry_counts:" + suffix
	config.WorkerCount = 2
	config.BatchSize = 4
	config.Block = 20 * time.Millisecond
	return config
}

func cleanupConsumerKeys(t *testing.T, client *redis.Client, config ConsumerConfig) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := client.Del(ctx, config.Stream, config.DLQStream, config.RetryCountHash).Err(); err != nil {
			t.Errorf("delete consumer test keys: %v", err)
		}
	})
}

func addTestCheckinEvent(
	t *testing.T,
	client *redis.Client,
	stream, tournamentID, playerID string,
	checkedInAt time.Time,
) string {
	t.Helper()

	messageID, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"tournament_id": tournamentID,
			"player_id":     playerID,
			"timestamp":     checkedInAt.UTC().Format(time.RFC3339Nano),
		},
	}).Result()
	if err != nil {
		t.Fatalf("add test check-in event: %v", err)
	}
	return messageID
}

type checkinRepositoryFunc func(context.Context, string, string, time.Time) error

func (function checkinRepositoryFunc) SaveCheckin(
	ctx context.Context,
	tournamentID, playerID string,
	checkedInAt time.Time,
) error {
	return function(ctx, tournamentID, playerID, checkedInAt)
}

func newTestRepository(t *testing.T) (*Repository, *gorm.DB) {
	t.Helper()

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = defaultTestPostgresDSN
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	postgresDB, err := database.OpenGORM(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to test Postgres: %v", err)
	}
	sqlDB, err := postgresDB.DB()
	if err != nil {
		t.Fatalf("get test SQL connection: %v", err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close test Postgres connection: %v", err)
		}
	})

	repository, err := NewRepository(postgresDB)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	if err := repository.Migrate(ctx); err != nil {
		t.Fatalf("migrate test check-in records: %v", err)
	}
	return repository, postgresDB
}

func cleanupCheckinRecord(
	t *testing.T,
	postgresDB *gorm.DB,
	tournamentID, playerID string,
) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := postgresDB.WithContext(ctx).
			Where("tournament_id = ? AND player_id = ?", tournamentID, playerID).
			Delete(&CheckinRecord{}).Error; err != nil {
			t.Errorf("delete test check-in record: %v", err)
		}
	})
}

func startTestConsumer(t *testing.T, consumer *Consumer) (context.CancelFunc, <-chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()
	return cancel, done
}

func assertConsumerStopped(t *testing.T, consumerDone <-chan error) {
	t.Helper()

	select {
	case err := <-consumerDone:
		if err != nil {
			t.Errorf("Consumer.Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("consumer did not stop after context cancellation")
	}
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
			t.Fatal("eventual condition was not met before timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
