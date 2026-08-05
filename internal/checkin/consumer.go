package checkin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultConsumerWorkerCount = 4
	defaultConsumerBatchSize   = 16
	defaultConsumerBlock       = time.Second
	defaultMaxRetries          = int64(3)
	defaultInitialBackoff      = 100 * time.Millisecond
)

// MessageProcessor performs the application-specific work for one check-in
// event. Returning an error activates the durable retry and DLQ policy.
type MessageProcessor func(context.Context, CheckinEvent) error

type ConsumerConfig struct {
	Stream         string
	Group          string
	ConsumerName   string
	DLQStream      string
	RetryCountHash string
	WorkerCount    int
	BatchSize      int64
	Block          time.Duration
	MaxRetries     int64
	InitialBackoff time.Duration
}

func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		Stream:         CheckinEventStream,
		Group:          CheckinConsumerGroup,
		ConsumerName:   DefaultConsumerName,
		DLQStream:      CheckinDLQStream,
		RetryCountHash: defaultRetryCountHash,
		WorkerCount:    defaultConsumerWorkerCount,
		BatchSize:      defaultConsumerBatchSize,
		Block:          defaultConsumerBlock,
		MaxRetries:     defaultMaxRetries,
		InitialBackoff: defaultInitialBackoff,
	}
}

// Consumer reads a Redis Consumer Group into a bounded Goroutine worker pool.
type Consumer struct {
	client    redis.Cmdable
	config    ConsumerConfig
	processor MessageProcessor
}

func NewConsumer(
	client redis.Cmdable,
	config ConsumerConfig,
	processor MessageProcessor,
) (*Consumer, error) {
	if client == nil {
		return nil, errors.New("Redis client is required")
	}
	if processor == nil {
		return nil, errors.New("message processor is required")
	}
	if err := validateConsumerConfig(config); err != nil {
		return nil, err
	}

	return &Consumer{
		client:    client,
		config:    config,
		processor: processor,
	}, nil
}

// Run creates the Consumer Group when needed, recovers pending messages for
// this consumer, and then continuously dispatches new messages until ctx ends.
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.ensureGroup(ctx); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan redis.XMessage, c.config.WorkerCount*int(c.config.BatchSize))
	workerErrors := make(chan error, 1)
	var workers sync.WaitGroup
	workers.Add(c.config.WorkerCount)

	for range c.config.WorkerCount {
		go func() {
			defer workers.Done()
			c.runWorker(runCtx, cancel, jobs, workerErrors)
		}()
	}

	readErr := c.readMessages(runCtx, jobs)
	cancel()
	close(jobs)
	workers.Wait()

	select {
	case err := <-workerErrors:
		return err
	default:
	}

	if ctx.Err() != nil {
		return nil
	}
	if readErr != nil {
		return readErr
	}
	return errors.New("check-in consumer stopped unexpectedly")
}

func validateConsumerConfig(config ConsumerConfig) error {
	if config.Stream == "" {
		return errors.New("consumer stream is required")
	}
	if config.Group == "" {
		return errors.New("consumer group is required")
	}
	if config.ConsumerName == "" {
		return errors.New("consumer name is required")
	}
	if config.DLQStream == "" {
		return errors.New("DLQ stream is required")
	}
	if config.RetryCountHash == "" {
		return errors.New("retry count hash is required")
	}
	if config.WorkerCount <= 0 {
		return fmt.Errorf("consumer worker count must be positive: %d", config.WorkerCount)
	}
	if config.BatchSize <= 0 {
		return fmt.Errorf("consumer batch size must be positive: %d", config.BatchSize)
	}
	if config.Block <= 0 {
		return fmt.Errorf("consumer block duration must be positive: %s", config.Block)
	}
	if config.MaxRetries < 0 {
		return fmt.Errorf("maximum retries must not be negative: %d", config.MaxRetries)
	}
	if config.InitialBackoff <= 0 {
		return fmt.Errorf("initial backoff must be positive: %s", config.InitialBackoff)
	}
	return nil
}

func (c *Consumer) ensureGroup(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(
		ctx,
		c.config.Stream,
		c.config.Group,
		"0",
	).Err()
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return fmt.Errorf("create check-in consumer group: %w", err)
}

func (c *Consumer) readMessages(ctx context.Context, jobs chan<- redis.XMessage) error {
	if err := c.readStream(ctx, jobs, "0", -1); err != nil {
		return err
	}

	for {
		if err := c.readStream(ctx, jobs, ">", c.config.Block); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (c *Consumer) readStream(
	ctx context.Context,
	jobs chan<- redis.XMessage,
	streamID string,
	block time.Duration,
) error {
	for {
		streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.config.Group,
			Consumer: c.config.ConsumerName,
			Streams:  []string{c.config.Stream, streamID},
			Count:    c.config.BatchSize,
			Block:    block,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read check-in stream at %q: %w", streamID, err)
		}

		messageCount := 0
		for _, stream := range streams {
			for _, message := range stream.Messages {
				messageCount++
				select {
				case jobs <- message:
				case <-ctx.Done():
					return nil
				}
			}
		}

		if streamID == ">" || messageCount == 0 {
			return nil
		}
	}
}

func (c *Consumer) runWorker(
	ctx context.Context,
	cancel context.CancelFunc,
	jobs <-chan redis.XMessage,
	workerErrors chan<- error,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-jobs:
			if !ok {
				return
			}
			if err := c.processWithRetry(ctx, message); err != nil {
				if ctx.Err() != nil {
					return
				}
				select {
				case workerErrors <- err:
				default:
				}
				cancel()
				return
			}
		}
	}
}

func (c *Consumer) processWithRetry(ctx context.Context, message redis.XMessage) error {
	for {
		event, processingErr := decodeCheckinEvent(message)
		if processingErr == nil {
			processingErr = c.processor(ctx, event)
		}
		if processingErr == nil {
			return c.acknowledge(ctx, message.ID)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		retryCount, err := c.client.HIncrBy(
			ctx,
			c.config.RetryCountHash,
			c.retryField(message.ID),
			1,
		).Result()
		if err != nil {
			return fmt.Errorf("increment retry count for message %s: %w", message.ID, err)
		}

		if retryCount > c.config.MaxRetries {
			return c.routeToDLQ(ctx, message, processingErr, retryCount)
		}

		if err := waitForRetry(ctx, exponentialBackoff(c.config.InitialBackoff, retryCount)); err != nil {
			return err
		}
	}
}

func decodeCheckinEvent(message redis.XMessage) (CheckinEvent, error) {
	event := CheckinEvent{
		ID:           message.ID,
		TournamentID: streamValue(message.Values, "tournament_id"),
		PlayerID:     streamValue(message.Values, "player_id"),
		Timestamp:    streamValue(message.Values, "timestamp"),
	}

	if event.TournamentID == "" {
		return CheckinEvent{}, errors.New("check-in event tournament_id is required")
	}
	if event.PlayerID == "" {
		return CheckinEvent{}, errors.New("check-in event player_id is required")
	}
	if event.Timestamp == "" {
		return CheckinEvent{}, errors.New("check-in event timestamp is required")
	}
	return event, nil
}

func streamValue(values map[string]interface{}, key string) string {
	value, exists := values[key]
	if !exists || value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func (c *Consumer) acknowledge(ctx context.Context, messageID string) error {
	_, err := c.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.XAck(ctx, c.config.Stream, c.config.Group, messageID)
		pipe.HDel(ctx, c.config.RetryCountHash, c.retryField(messageID))
		return nil
	})
	if err != nil {
		return fmt.Errorf("acknowledge check-in message %s: %w", messageID, err)
	}
	return nil
}

func (c *Consumer) routeToDLQ(
	ctx context.Context,
	message redis.XMessage,
	processingErr error,
	retryCount int64,
) error {
	values := make(map[string]interface{}, len(message.Values)+5)
	for key, value := range message.Values {
		values[key] = value
	}
	values["original_stream"] = c.config.Stream
	values["original_message_id"] = message.ID
	values["retry_count"] = retryCount
	values["error_message"] = processingErr.Error()
	values["failed_at"] = time.Now().UTC().Format(time.RFC3339Nano)

	_, err := c.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: c.config.DLQStream,
			Values: values,
		})
		pipe.XAck(ctx, c.config.Stream, c.config.Group, message.ID)
		pipe.HDel(ctx, c.config.RetryCountHash, c.retryField(message.ID))
		return nil
	})
	if err != nil {
		return fmt.Errorf("route check-in message %s to DLQ: %w", message.ID, err)
	}
	return nil
}

func (c *Consumer) retryField(messageID string) string {
	return c.config.Stream + "|" + messageID
}

func exponentialBackoff(initial time.Duration, retryCount int64) time.Duration {
	const maximumDuration = time.Duration(1<<63 - 1)

	delay := initial
	for retry := int64(1); retry < retryCount; retry++ {
		if delay > maximumDuration/2 {
			return maximumDuration
		}
		delay *= 2
	}
	return delay
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
