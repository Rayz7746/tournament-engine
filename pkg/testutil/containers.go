// Package testutil provides infrastructure helpers shared by integration tests.
package testutil

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"tournament-engine/pkg/database"

	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

const (
	postgresImage    = "postgres:16-alpine"
	redisImage       = "redis:7-alpine"
	postgresDatabase = "chess_test"
	postgresUsername = "test_user"
	postgresPassword = "test_password"
)

// TestContainers owns an isolated PostgreSQL/Redis pair and their connection
// details. Call Terminate after all clients have been closed.
type TestContainers struct {
	PostgresDSN string
	RedisAddr   string

	postgres testcontainers.Container
	redis    testcontainers.Container

	terminateOnce sync.Once
	terminateErr  error
}

// SetupTestContainers starts PostgreSQL and Redis on dynamically assigned host
// ports, applies the application schema, and returns their connection details.
func SetupTestContainers(ctx context.Context) (*TestContainers, error) {
	postgresContainer, err := postgrescontainer.Run(
		ctx,
		postgresImage,
		postgrescontainer.WithDatabase(postgresDatabase),
		postgrescontainer.WithUsername(postgresUsername),
		postgrescontainer.WithPassword(postgresPassword),
		postgrescontainer.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("start ephemeral Postgres: %w", err)
	}

	environment := &TestContainers{postgres: postgresContainer}
	redisContainer, err := rediscontainer.Run(ctx, redisImage)
	if err != nil {
		return nil, environment.rollback(fmt.Errorf("start ephemeral Redis: %w", err))
	}
	environment.redis = redisContainer

	postgresDSN, err := postgresContainer.ConnectionString(
		ctx,
		"sslmode=disable",
		"timezone=UTC",
	)
	if err != nil {
		return nil, environment.rollback(fmt.Errorf("resolve ephemeral Postgres DSN: %w", err))
	}
	redisConnectionString, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		return nil, environment.rollback(fmt.Errorf("resolve ephemeral Redis address: %w", err))
	}
	redisURL, err := url.Parse(redisConnectionString)
	if err != nil || redisURL.Host == "" {
		if err == nil {
			err = errors.New("connection string has no host")
		}
		return nil, environment.rollback(fmt.Errorf("parse ephemeral Redis address: %w", err))
	}

	environment.PostgresDSN = postgresDSN
	environment.RedisAddr = redisURL.Host
	if err := applyMigrations(ctx, postgresDSN); err != nil {
		return nil, environment.rollback(err)
	}

	return environment, nil
}

// Terminate removes both containers. It is safe to call more than once.
func (environment *TestContainers) Terminate(ctx context.Context) error {
	if environment == nil {
		return nil
	}

	environment.terminateOnce.Do(func() {
		var terminationErrors []error
		if environment.redis != nil {
			if err := environment.redis.Terminate(ctx); err != nil {
				terminationErrors = append(terminationErrors, fmt.Errorf("terminate Redis: %w", err))
			}
		}
		if environment.postgres != nil {
			if err := environment.postgres.Terminate(ctx); err != nil {
				terminationErrors = append(terminationErrors, fmt.Errorf("terminate Postgres: %w", err))
			}
		}
		environment.terminateErr = errors.Join(terminationErrors...)
	})
	return environment.terminateErr
}

func (environment *TestContainers) rollback(setupErr error) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return errors.Join(setupErr, environment.Terminate(cleanupCtx))
}

// checkinRecordMigration mirrors the persisted check-in schema without making
// this test helper depend on an internal package (which would create an import
// cycle when internal/checkin's own integration tests use this helper).
type checkinRecordMigration struct {
	ID           uint      `gorm:"primaryKey"`
	TournamentID string    `gorm:"type:varchar(255);not null;index:idx_checkin_records_tournament_id;uniqueIndex:ux_checkin_records_tournament_player,priority:1"`
	PlayerID     string    `gorm:"type:varchar(255);not null;index:idx_checkin_records_player_id;uniqueIndex:ux_checkin_records_tournament_player,priority:2"`
	CheckedInAt  time.Time `gorm:"not null"`
}

func (checkinRecordMigration) TableName() string {
	return "checkin_records"
}

func applyMigrations(ctx context.Context, postgresDSN string) (migrationErr error) {
	db, err := database.OpenGORM(ctx, postgresDSN)
	if err != nil {
		return fmt.Errorf("open ephemeral Postgres for migrations: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("get ephemeral Postgres migration connection: %w", err)
	}
	defer func() {
		if err := sqlDB.Close(); err != nil {
			migrationErr = errors.Join(
				migrationErr,
				fmt.Errorf("close ephemeral Postgres migration connection: %w", err),
			)
		}
	}()

	if err := db.WithContext(ctx).AutoMigrate(&checkinRecordMigration{}); err != nil {
		return fmt.Errorf("apply ephemeral Postgres migrations: %w", err)
	}
	return nil
}
