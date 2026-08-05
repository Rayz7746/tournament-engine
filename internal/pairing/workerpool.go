package pairing

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	pairingv1 "tournament-engine/pkg/proto/pairing/v1"
)

var ErrWorkerPoolClosed = errors.New("pairing worker pool is closed")

type pairingCalculator func(context.Context, *pairingv1.PairingRequest) ([]*pairingv1.Match, error)

type pairingResult struct {
	matches []*pairingv1.Match
	err     error
}

type pairingJob struct {
	ctx      context.Context
	request  *pairingv1.PairingRequest
	resultCh chan<- pairingResult
}

// WorkerPool bounds concurrent pairing calculations and queues excess work.
type WorkerPool struct {
	jobs       chan pairingJob
	stop       chan struct{}
	calculator pairingCalculator

	mu      sync.RWMutex
	closed  bool
	workers sync.WaitGroup
}

// NewWorkerPool starts workerCount pairing workers and a queue sized to absorb
// two pending jobs per worker.
func NewWorkerPool(workerCount int) (*WorkerPool, error) {
	if workerCount <= 0 {
		return nil, fmt.Errorf("worker count must be positive: %d", workerCount)
	}

	return newWorkerPool(workerCount, workerCount*2, CalculatePairings)
}

func newWorkerPool(
	workerCount, queueSize int,
	calculator pairingCalculator,
) (*WorkerPool, error) {
	if workerCount <= 0 {
		return nil, fmt.Errorf("worker count must be positive: %d", workerCount)
	}
	if queueSize < 0 {
		return nil, fmt.Errorf("queue size must not be negative: %d", queueSize)
	}
	if calculator == nil {
		return nil, errors.New("pairing calculator is required")
	}

	pool := &WorkerPool{
		jobs:       make(chan pairingJob, queueSize),
		stop:       make(chan struct{}),
		calculator: calculator,
	}

	pool.workers.Add(workerCount)
	for range workerCount {
		go pool.work()
	}

	return pool, nil
}

// GeneratePairings submits a calculation and waits for its context-aware
// result. Calculation concurrency is limited by the configured worker count.
func (p *WorkerPool) GeneratePairings(
	ctx context.Context,
	request *pairingv1.PairingRequest,
) ([]*pairingv1.Match, error) {
	resultCh := make(chan pairingResult, 1)
	job := pairingJob{
		ctx:      ctx,
		request:  request,
		resultCh: resultCh,
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrWorkerPoolClosed
	}

	select {
	case p.jobs <- job:
		p.mu.RUnlock()
	case <-ctx.Done():
		p.mu.RUnlock()
		return nil, ctx.Err()
	}

	select {
	case result := <-resultCh:
		return result.matches, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.stop:
		return nil, ErrWorkerPoolClosed
	}
}

// Close stops all workers. It is safe to call more than once.
func (p *WorkerPool) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.stop)
	}
	p.mu.Unlock()

	p.workers.Wait()
}

func (p *WorkerPool) work() {
	defer p.workers.Done()

	for {
		select {
		case <-p.stop:
			return
		case job := <-p.jobs:
			if err := job.ctx.Err(); err != nil {
				job.resultCh <- pairingResult{err: err}
				continue
			}

			matches, err := p.calculator(job.ctx, job.request)
			job.resultCh <- pairingResult{matches: matches, err: err}
		}
	}
}

// CalculatePairings creates deterministic score-grouped pairings. It prefers
// adjacent players after sorting, scanning forward only when an adjacent player
// is listed as a previous opponent by either player.
func CalculatePairings(
	ctx context.Context,
	request *pairingv1.PairingRequest,
) ([]*pairingv1.Match, error) {
	if request == nil {
		return nil, errors.New("pairing request is required")
	}
	if request.GetTournamentId() == "" {
		return nil, errors.New("tournament ID is required")
	}
	if request.GetRound() <= 0 {
		return nil, errors.New("round must be positive")
	}
	if len(request.GetPlayers()) < 2 {
		return nil, errors.New("at least two players are required")
	}
	if len(request.GetPlayers())%2 != 0 {
		return nil, errors.New("an even number of players is required")
	}

	players := append([]*pairingv1.Player(nil), request.GetPlayers()...)
	seenPlayerIDs := make(map[string]struct{}, len(players))
	for _, player := range players {
		if player == nil || player.GetPlayerId() == "" {
			return nil, errors.New("every player must have a player ID")
		}
		if _, exists := seenPlayerIDs[player.GetPlayerId()]; exists {
			return nil, fmt.Errorf("duplicate player ID %q", player.GetPlayerId())
		}
		seenPlayerIDs[player.GetPlayerId()] = struct{}{}
	}

	sort.Slice(players, func(i, j int) bool {
		if players[i].GetScore() == players[j].GetScore() {
			return players[i].GetPlayerId() < players[j].GetPlayerId()
		}
		return players[i].GetScore() > players[j].GetScore()
	})

	matches := make([]*pairingv1.Match, 0, len(players)/2)
	for len(players) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		white := players[0]
		opponentIndex := -1
		for i := 1; i < len(players); i++ {
			if !playersAvoidEachOther(white, players[i]) {
				opponentIndex = i
				break
			}
		}
		if opponentIndex == -1 {
			return nil, fmt.Errorf("no eligible opponent for player %q", white.GetPlayerId())
		}

		black := players[opponentIndex]
		boardNumber := int32(len(matches) + 1)
		matches = append(matches, &pairingv1.Match{
			MatchId:       fmt.Sprintf("%s-r%d-b%d", request.GetTournamentId(), request.GetRound(), boardNumber),
			WhitePlayerId: white.GetPlayerId(),
			BlackPlayerId: black.GetPlayerId(),
			BoardNumber:   boardNumber,
		})

		players = append(players[1:opponentIndex], players[opponentIndex+1:]...)
	}

	return matches, nil
}

func playersAvoidEachOther(first, second *pairingv1.Player) bool {
	return containsPlayerID(first.GetAvoidedOpponents(), second.GetPlayerId()) ||
		containsPlayerID(second.GetAvoidedOpponents(), first.GetPlayerId())
}

func containsPlayerID(playerIDs []string, target string) bool {
	for _, playerID := range playerIDs {
		if playerID == target {
			return true
		}
	}
	return false
}
