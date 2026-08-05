package pairing

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pairingv1 "tournament-engine/pkg/proto/pairing/v1"
)

func TestCalculatePairingsSortsAndPairsEightPlayers(t *testing.T) {
	request := &pairingv1.PairingRequest{
		TournamentId: "tournament-1",
		Round:        3,
		Players: []*pairingv1.Player{
			{PlayerId: "p6", Score: 3},
			{PlayerId: "p2", Score: 7},
			{PlayerId: "p8", Score: 1},
			{PlayerId: "p4", Score: 5},
			{PlayerId: "p1", Score: 8},
			{PlayerId: "p7", Score: 2},
			{PlayerId: "p3", Score: 6},
			{PlayerId: "p5", Score: 4},
		},
	}

	matches, err := CalculatePairings(context.Background(), request)
	if err != nil {
		t.Fatalf("CalculatePairings() error = %v", err)
	}

	want := [][2]string{
		{"p1", "p2"},
		{"p3", "p4"},
		{"p5", "p6"},
		{"p7", "p8"},
	}
	if len(matches) != len(want) {
		t.Fatalf("match count = %d, want %d", len(matches), len(want))
	}

	for i, match := range matches {
		if got := [2]string{match.GetWhitePlayerId(), match.GetBlackPlayerId()}; got != want[i] {
			t.Errorf("match %d players = %v, want %v", i, got, want[i])
		}
		if match.GetBoardNumber() != int32(i+1) {
			t.Errorf("match %d board = %d, want %d", i, match.GetBoardNumber(), i+1)
		}
	}
}

func TestCalculatePairingsAvoidsPreviousOpponents(t *testing.T) {
	request := &pairingv1.PairingRequest{
		TournamentId: "tournament-avoidance",
		Round:        2,
		Players: []*pairingv1.Player{
			{PlayerId: "p1", Score: 4, AvoidedOpponents: []string{"p2"}},
			{PlayerId: "p2", Score: 3},
			{PlayerId: "p3", Score: 2},
			{PlayerId: "p4", Score: 1},
		},
	}

	matches, err := CalculatePairings(context.Background(), request)
	if err != nil {
		t.Fatalf("CalculatePairings() error = %v", err)
	}

	want := [][2]string{{"p1", "p3"}, {"p2", "p4"}}
	for i, match := range matches {
		if got := [2]string{match.GetWhitePlayerId(), match.GetBlackPlayerId()}; got != want[i] {
			t.Errorf("match %d players = %v, want %v", i, got, want[i])
		}
	}
}

func TestWorkerPoolProcessesRequestsConcurrently(t *testing.T) {
	const (
		workerCount  = 4
		requestCount = 24
	)

	var activeWorkers atomic.Int32
	var maximumActiveWorkers atomic.Int32

	calculator := func(
		ctx context.Context,
		request *pairingv1.PairingRequest,
	) ([]*pairingv1.Match, error) {
		active := activeWorkers.Add(1)
		defer activeWorkers.Add(-1)
		updateMaximum(&maximumActiveWorkers, active)

		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		return CalculatePairings(ctx, request)
	}

	pool, err := newWorkerPool(workerCount, requestCount, calculator)
	if err != nil {
		t.Fatalf("newWorkerPool() error = %v", err)
	}
	t.Cleanup(pool.Close)

	start := make(chan struct{})
	errorsByRequest := make(chan error, requestCount)
	var callers sync.WaitGroup
	callers.Add(requestCount)

	for i := range requestCount {
		go func(requestNumber int) {
			defer callers.Done()
			<-start

			matches, err := pool.GeneratePairings(
				context.Background(),
				newFourPlayerRequest(requestNumber),
			)
			if err == nil && len(matches) != 2 {
				err = fmt.Errorf("match count = %d, want 2", len(matches))
			}
			errorsByRequest <- err
		}(i)
	}

	close(start)
	callers.Wait()
	close(errorsByRequest)

	for err := range errorsByRequest {
		if err != nil {
			t.Errorf("GeneratePairings() error = %v", err)
		}
	}

	maximum := maximumActiveWorkers.Load()
	if maximum < 2 {
		t.Fatalf("maximum concurrent workers = %d, want at least 2", maximum)
	}
	if maximum > workerCount {
		t.Fatalf("maximum concurrent workers = %d, exceeds configured limit %d", maximum, workerCount)
	}
}

func updateMaximum(maximum *atomic.Int32, candidate int32) {
	for {
		current := maximum.Load()
		if candidate <= current || maximum.CompareAndSwap(current, candidate) {
			return
		}
	}
}

func newFourPlayerRequest(requestNumber int) *pairingv1.PairingRequest {
	return &pairingv1.PairingRequest{
		TournamentId: fmt.Sprintf("tournament-%d", requestNumber),
		Round:        1,
		Players: []*pairingv1.Player{
			{PlayerId: "p1", Score: 4},
			{PlayerId: "p2", Score: 3},
			{PlayerId: "p3", Score: 2},
			{PlayerId: "p4", Score: 1},
		},
	}
}
