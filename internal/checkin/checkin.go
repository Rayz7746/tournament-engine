package checkin

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed checkin.lua
var checkinScriptSource string

var (
	ErrTournamentIDRequired = errors.New("tournament ID is required")
	ErrPlayerIDRequired     = errors.New("player ID is required")
	ErrInvalidTTL           = errors.New("check-in TTL must be positive")
)

// CheckinManager atomically records tournament check-ins in Redis.
type CheckinManager struct {
	client redis.Scripter
	script *redis.Script
}

// NewCheckinManager creates a check-in manager backed by client.
func NewCheckinManager(client redis.Scripter) *CheckinManager {
	return &CheckinManager{
		client: client,
		script: redis.NewScript(checkinScriptSource),
	}
}

// TryCheckIn records a player's check-in for a tournament. It returns true
// only for the caller that creates the check-in key; concurrent and later
// callers receive false until the key expires.
func (m *CheckinManager) TryCheckIn(
	ctx context.Context,
	tournamentID, playerID string,
	ttl time.Duration,
) (bool, error) {
	if tournamentID == "" {
		return false, ErrTournamentIDRequired
	}
	if playerID == "" {
		return false, ErrPlayerIDRequired
	}
	if ttl <= 0 {
		return false, ErrInvalidTTL
	}

	ttlMilliseconds := ttl.Milliseconds()
	if ttlMilliseconds == 0 {
		ttlMilliseconds = 1
	}

	result, err := m.script.Run(
		ctx,
		m.client,
		[]string{checkinKey(tournamentID, playerID), CheckinEventStream},
		"1",
		ttlMilliseconds,
		tournamentID,
		playerID,
		time.Now().UTC().Format(time.RFC3339Nano),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("run atomic check-in script: %w", err)
	}

	switch result {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("run atomic check-in script: unexpected result %d", result)
	}
}

func checkinKey(tournamentID, playerID string) string {
	return fmt.Sprintf("checkin:%s:%s", tournamentID, playerID)
}
