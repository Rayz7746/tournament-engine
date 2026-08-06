package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tournament-engine/internal/checkin"
	"tournament-engine/internal/gateway"
	"tournament-engine/pkg/database"
)

const (
	defaultHTTPAddress    = ":8080"
	defaultPairingTarget  = "localhost:50053"
	defaultRedisAddress   = "localhost:6379"
	defaultRedisPassword  = "redis123"
	gatewayShutdownWindow = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Printf("gateway stopped with error: %v", err)
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
		PoolSize: 32,
	})
	if err != nil {
		return fmt.Errorf("open Redis for gateway: %w", err)
	}
	defer func() {
		if closeErr := redisClient.Close(); closeErr != nil {
			log.Printf("close gateway Redis client: %v", closeErr)
		}
	}()

	pairingClient, err := gateway.NewPairingGRPCClient(
		environmentOrDefault("PAIRING_GRPC_ADDR", defaultPairingTarget),
	)
	if err != nil {
		return fmt.Errorf("create pairing gRPC client: %w", err)
	}
	defer func() {
		if closeErr := pairingClient.Close(); closeErr != nil {
			log.Printf("close gateway pairing client: %v", closeErr)
		}
	}()

	httpGateway, err := gateway.New(
		checkin.NewCheckinManager(redisClient),
		pairingClient,
	)
	if err != nil {
		return fmt.Errorf("create HTTP gateway: %w", err)
	}

	server := &http.Server{
		Addr:              gatewayAddress(),
		Handler:           httpGateway.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.ListenAndServe()
	}()

	log.Printf("gateway HTTP listening on %s", server.Addr)

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve gateway HTTP: %w", err)
	case <-ctx.Done():
		log.Print("gateway received shutdown signal")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), gatewayShutdownWindow)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = server.Close()
		return fmt.Errorf("shutdown gateway HTTP server: %w", err)
	}

	log.Print("gateway stopped")
	return nil
}

func gatewayAddress() string {
	if address := os.Getenv("HTTP_ADDR"); address != "" {
		return address
	}
	return environmentOrDefault("GRPC_ADDR", defaultHTTPAddress)
}

func environmentOrDefault(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}
