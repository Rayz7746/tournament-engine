package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tournament-engine/internal/checkin"
	"tournament-engine/pkg/database"

	"google.golang.org/grpc"
)

const (
	defaultAddress       = ":50052"
	defaultRedisAddress  = "localhost:6379"
	defaultRedisPassword = "redis123"
)

func main() {
	if err := run(); err != nil {
		log.Printf("checkin stopped with error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	redisClient, err := database.OpenRedis(ctx, database.RedisConfig{
		Address:  environmentOrDefault("REDIS_ADDR", defaultRedisAddress),
		Password: environmentOrDefault("REDIS_PASSWORD", defaultRedisPassword),
		Database: 0,
		PoolSize: 16,
	})
	if err != nil {
		return fmt.Errorf("open Redis for check-in service: %w", err)
	}
	defer func() {
		if closeErr := redisClient.Close(); closeErr != nil {
			log.Printf("close checkin Redis client: %v", closeErr)
		}
	}()

	consumerConfig := checkin.DefaultConsumerConfig()
	consumerConfig.ConsumerName = environmentOrDefault("CHECKIN_CONSUMER_NAME", checkin.DefaultConsumerName)
	consumer, err := checkin.NewConsumer(
		redisClient,
		consumerConfig,
		processCheckinEvent,
	)
	if err != nil {
		return fmt.Errorf("create check-in stream consumer: %w", err)
	}

	address := os.Getenv("GRPC_ADDR")
	if address == "" {
		address = defaultAddress
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", address, err)
	}
	defer func() {
		if closeErr := listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			log.Printf("close checkin listener: %v", closeErr)
		}
	}()

	server := grpc.NewServer()
	// TODO: Register the check-in gRPC API when its protobuf contract is added.

	consumerErr := make(chan error, 1)
	go func() {
		consumerErr <- consumer.Run(ctx)
	}()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	log.Printf("checkin listening on %s", address)

	select {
	case err := <-serveErr:
		stop()
		if consumerStopErr := waitForConsumer(consumerErr, 2*time.Second); consumerStopErr != nil {
			log.Printf("stop check-in event consumer: %v", consumerStopErr)
		}
		return fmt.Errorf("serve gRPC: %w", err)
	case err := <-consumerErr:
		if err != nil {
			server.Stop()
			return fmt.Errorf("consume check-in events: %w", err)
		}
		server.Stop()
		return errors.New("check-in event consumer stopped unexpectedly")
	case <-ctx.Done():
		log.Print("checkin received shutdown signal")
	}

	gracefulStop(server, 10*time.Second)
	if err := waitForConsumer(consumerErr, 2*time.Second); err != nil {
		return fmt.Errorf("stop check-in event consumer: %w", err)
	}
	log.Print("checkin stopped")
	return nil
}

func processCheckinEvent(_ context.Context, event checkin.CheckinEvent) error {
	log.Printf(
		"processed check-in event id=%s tournament_id=%s player_id=%s",
		event.ID,
		event.TournamentID,
		event.PlayerID,
	)
	return nil
}

func environmentOrDefault(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}

func waitForConsumer(consumerErr <-chan error, timeout time.Duration) error {
	select {
	case err := <-consumerErr:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("consumer shutdown exceeded %s", timeout)
	}
}

func gracefulStop(server *grpc.Server, timeout time.Duration) {
	stopped := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(timeout):
		log.Printf("graceful shutdown exceeded %s; forcing stop", timeout)
		server.Stop()
	}
}
