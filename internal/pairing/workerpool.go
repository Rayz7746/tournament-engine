package pairing

import (
	"context"
	"errors"
	"fmt"
	"sync"

	pairingv1 "tournament-engine/pkg/proto/pairing/v1"
)

var ErrWorkerPoolClosed = errors.New("pairing worker pool is closed")

type pairingCalculator func(context.Context, *pairingv1.PairingRequest) (*PairingResult, error)

type pairingResult struct {
	result *PairingResult
	err    error
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
) (*PairingResult, error) {
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
		return result.result, result.err
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

			result, err := p.calculator(job.ctx, job.request)
			job.resultCh <- pairingResult{result: result, err: err}
		}
	}
}
