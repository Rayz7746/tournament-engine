package pairing

import (
	"context"
	"errors"

	pairingv1 "tournament-engine/pkg/proto/pairing/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the pairing.v1.PairingService gRPC API.
type Service struct {
	pairingv1.UnimplementedPairingServiceServer
	pool *WorkerPool
}

func NewService(pool *WorkerPool) *Service {
	return &Service{pool: pool}
}

func (s *Service) GeneratePairings(
	ctx context.Context,
	request *pairingv1.PairingRequest,
) (*pairingv1.PairingResponse, error) {
	result, err := s.pool.GeneratePairings(ctx, request)
	if err == nil {
		return &pairingv1.PairingResponse{
			Success:     true,
			Matches:     result.Matches,
			ByePlayerId: result.ByePlayerID,
		}, nil
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, status.FromContextError(err).Err()
	}
	if errors.Is(err, ErrWorkerPoolClosed) {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	return &pairingv1.PairingResponse{
		Success:      false,
		ErrorMessage: err.Error(),
	}, nil
}
