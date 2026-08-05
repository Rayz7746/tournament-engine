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

	"google.golang.org/grpc"
)

const defaultAddress = ":50052"

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
	// TODO: Register check-in gRPC services and Redis Stream consumers here.

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	log.Printf("checkin listening on %s", address)

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve gRPC: %w", err)
	case <-ctx.Done():
		log.Print("checkin received shutdown signal")
	}

	gracefulStop(server, 10*time.Second)
	log.Print("checkin stopped")
	return nil
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
