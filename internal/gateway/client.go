package gateway

import (
	"context"
	"errors"

	pairingv1 "tournament-engine/pkg/proto/pairing/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PairingClient is the subset of the generated client used by the gateway.
type PairingClient interface {
	GeneratePairings(
		ctx context.Context,
		request *pairingv1.PairingRequest,
		options ...grpc.CallOption,
	) (*pairingv1.PairingResponse, error)
}

// PairingGRPCClient owns the gRPC connection used by the HTTP gateway.
type PairingGRPCClient struct {
	connection *grpc.ClientConn
	client     pairingv1.PairingServiceClient
}

func NewPairingGRPCClient(target string) (*PairingGRPCClient, error) {
	if target == "" {
		return nil, errors.New("pairing gRPC target is required")
	}

	connection, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &PairingGRPCClient{
		connection: connection,
		client:     pairingv1.NewPairingServiceClient(connection),
	}, nil
}

func (c *PairingGRPCClient) GeneratePairings(
	ctx context.Context,
	request *pairingv1.PairingRequest,
	options ...grpc.CallOption,
) (*pairingv1.PairingResponse, error) {
	return c.client.GeneratePairings(ctx, request, options...)
}

func (c *PairingGRPCClient) Close() error {
	return c.connection.Close()
}
