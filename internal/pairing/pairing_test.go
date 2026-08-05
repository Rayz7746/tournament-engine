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

	result, err := CalculatePairings(context.Background(), request)
	if err != nil {
		t.Fatalf("CalculatePairings() error = %v", err)
	}

	matches := result.Matches
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

	result, err := CalculatePairings(context.Background(), request)
	if err != nil {
		t.Fatalf("CalculatePairings() error = %v", err)
	}

	matches := result.Matches
	want := [][2]string{{"p1", "p3"}, {"p2", "p4"}}
	for i, match := range matches {
		if got := [2]string{match.GetWhitePlayerId(), match.GetBlackPlayerId()}; got != want[i] {
			t.Errorf("match %d players = %v, want %v", i, got, want[i])
		}
	}
}

func TestCalculatePairingsAssignsByeToLowestEligiblePlayer(t *testing.T) {
	request := &pairingv1.PairingRequest{
		TournamentId: "tournament-bye",
		Round:        4,
		Players: []*pairingv1.Player{
			{PlayerId: "p1", Score: 5},
			{PlayerId: "p2", Score: 2},
			{PlayerId: "p3", Score: 1, ReceivedBye: true},
		},
	}

	result, err := CalculatePairings(context.Background(), request)
	if err != nil {
		t.Fatalf("CalculatePairings() error = %v", err)
	}

	if result.ByePlayerID != "p2" {
		t.Errorf("bye player = %q, want %q", result.ByePlayerID, "p2")
	}
	if len(result.Matches) != 1 {
		t.Fatalf("match count = %d, want 1", len(result.Matches))
	}
	match := result.Matches[0]
	if got := [2]string{match.GetWhitePlayerId(), match.GetBlackPlayerId()}; got != [2]string{"p1", "p3"} {
		t.Errorf("match players = %v, want [p1 p3]", got)
	}
}

func TestCalculatePairingsGivesWhiteToPlayerWithMoreBlackGames(t *testing.T) {
	request := &pairingv1.PairingRequest{
		TournamentId: "tournament-color-balance",
		Round:        5,
		Players: []*pairingv1.Player{
			{
				PlayerId:   "higher-ranked",
				Score:      8,
				WhiteCount: 2,
				BlackCount: 2,
				LastColor:  "WHITE",
			},
			{
				PlayerId:   "more-black-games",
				Score:      7,
				WhiteCount: 1,
				BlackCount: 4,
				LastColor:  "BLACK",
			},
		},
	}

	result, err := CalculatePairings(context.Background(), request)
	if err != nil {
		t.Fatalf("CalculatePairings() error = %v", err)
	}

	match := result.Matches[0]
	if match.GetWhitePlayerId() != "more-black-games" {
		t.Errorf("white player = %q, want %q", match.GetWhitePlayerId(), "more-black-games")
	}
}

func TestCalculatePairingsAlternatesLastColor(t *testing.T) {
	request := &pairingv1.PairingRequest{
		TournamentId: "tournament-color-alternation",
		Round:        3,
		Players: []*pairingv1.Player{
			{
				PlayerId:   "higher-ranked",
				Score:      8,
				WhiteCount: 2,
				BlackCount: 2,
				LastColor:  "WHITE",
			},
			{
				PlayerId:   "lower-ranked",
				Score:      7,
				WhiteCount: 2,
				BlackCount: 2,
				LastColor:  "BLACK",
			},
		},
	}

	result, err := CalculatePairings(context.Background(), request)
	if err != nil {
		t.Fatalf("CalculatePairings() error = %v", err)
	}

	match := result.Matches[0]
	if match.GetWhitePlayerId() != "lower-ranked" {
		t.Errorf("white player = %q, want %q", match.GetWhitePlayerId(), "lower-ranked")
	}
}

func TestCalculatePairingsBacktracksToAvoidAllRematches(t *testing.T) {
	request := &pairingv1.PairingRequest{
		TournamentId: "tournament-backtracking",
		Round:        6,
		Players: []*pairingv1.Player{
			{PlayerId: "p1", Score: 4, AvoidedOpponents: []string{"p4"}},
			{PlayerId: "p2", Score: 3},
			{PlayerId: "p3", Score: 2, AvoidedOpponents: []string{"p4"}},
			{PlayerId: "p4", Score: 1},
		},
	}

	result, err := CalculatePairings(context.Background(), request)
	if err != nil {
		t.Fatalf("CalculatePairings() error = %v", err)
	}

	want := [][2]string{{"p1", "p3"}, {"p2", "p4"}}
	for i, match := range result.Matches {
		if got := [2]string{match.GetWhitePlayerId(), match.GetBlackPlayerId()}; got != want[i] {
			t.Errorf("match %d players = %v, want %v", i, got, want[i])
		}
	}
}

func TestCalculatePairingsRejectsUnavoidableRematch(t *testing.T) {
	request := &pairingv1.PairingRequest{
		TournamentId: "tournament-no-valid-pairing",
		Round:        2,
		Players: []*pairingv1.Player{
			{PlayerId: "p1", Score: 2, AvoidedOpponents: []string{"p2"}},
			{PlayerId: "p2", Score: 1},
		},
	}

	if _, err := CalculatePairings(context.Background(), request); err == nil {
		t.Fatal("CalculatePairings() error = nil, want unavoidable-rematch error")
	}
}

func TestServiceReturnsByePlayerID(t *testing.T) {
	pool, err := NewWorkerPool(1)
	if err != nil {
		t.Fatalf("NewWorkerPool() error = %v", err)
	}
	t.Cleanup(pool.Close)

	response, err := NewService(pool).GeneratePairings(
		context.Background(),
		&pairingv1.PairingRequest{
			TournamentId: "tournament-service-bye",
			Round:        1,
			Players: []*pairingv1.Player{
				{PlayerId: "p1", Score: 3},
				{PlayerId: "p2", Score: 2},
				{PlayerId: "p3", Score: 1},
			},
		},
	)
	if err != nil {
		t.Fatalf("GeneratePairings() error = %v", err)
	}
	if !response.GetSuccess() {
		t.Fatalf("response success = false, error = %q", response.GetErrorMessage())
	}
	if response.GetByePlayerId() != "p3" {
		t.Errorf("bye player = %q, want %q", response.GetByePlayerId(), "p3")
	}
}

func TestWorkerPoolProcessesRequestsConcurrently(t *testing.T) {
	const (
		workerCount  = 4
		requestCount = 64
	)

	var activeWorkers atomic.Int32
	var maximumActiveWorkers atomic.Int32

	calculator := func(
		ctx context.Context,
		request *pairingv1.PairingRequest,
	) (*PairingResult, error) {
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

			result, err := pool.GeneratePairings(
				context.Background(),
				newFourPlayerRequest(requestNumber),
			)
			if err == nil && len(result.Matches) != 2 {
				err = fmt.Errorf("match count = %d, want 2", len(result.Matches))
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
