package gateway

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pairingv1 "tournament-engine/pkg/proto/pairing/v1"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestHealthEndpoint(t *testing.T) {
	server, _, _ := newTestGatewayServer(t)

	response, body := doRequest(t, http.MethodGet, server.URL+"/health", "")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /health status = %d, want %d; body=%s", response.StatusCode, http.StatusOK, body)
	}
	if !strings.Contains(body, `"status":"ok"`) {
		t.Errorf("GET /health body = %q, want healthy status", body)
	}
}

func TestCheckinEndpoint(t *testing.T) {
	server, checkins, _ := newTestGatewayServer(t)

	response, body := doRequest(
		t,
		http.MethodPost,
		server.URL+"/api/v1/tournaments/t1/checkin",
		`{"player_id":"p1","ttl_seconds":60}`,
	)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("POST checkin status = %d, want %d; body=%s", response.StatusCode, http.StatusOK, body)
	}

	call := checkins.lastCall(t)
	if call.tournamentID != "t1" || call.playerID != "p1" {
		t.Errorf("TryCheckIn() IDs = %q/%q, want t1/p1", call.tournamentID, call.playerID)
	}
	if call.ttl != time.Minute {
		t.Errorf("TryCheckIn() TTL = %s, want %s", call.ttl, time.Minute)
	}
}

func TestCheckinEndpointRejectsInvalidRequest(t *testing.T) {
	server, checkins, _ := newTestGatewayServer(t)

	response, body := doRequest(
		t,
		http.MethodPost,
		server.URL+"/api/v1/tournaments/t1/checkin",
		`{"ttl_seconds":60}`,
	)
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid checkin status = %d, want %d; body=%s", response.StatusCode, http.StatusBadRequest, body)
	}
	if checkins.callCount() != 0 {
		t.Errorf("TryCheckIn() calls = %d, want 0 for invalid request", checkins.callCount())
	}
}

func TestCheckinEndpointReturnsConflictForDuplicate(t *testing.T) {
	server, checkins, _ := newTestGatewayServer(t)
	checkins.setResult(false, nil)

	response, body := doRequest(
		t,
		http.MethodPost,
		server.URL+"/api/v1/tournaments/t1/checkin",
		`{"player_id":"p1"}`,
	)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate checkin status = %d, want %d; body=%s", response.StatusCode, http.StatusConflict, body)
	}
}

func TestPairingsEndpointMapsJSONToGRPC(t *testing.T) {
	server, _, pairings := newTestGatewayServer(t)
	pairings.setResponse(&pairingv1.PairingResponse{
		Success: true,
		Matches: []*pairingv1.Match{
			{
				MatchId:       "t1-r1-b1",
				WhitePlayerId: "p1",
				BlackPlayerId: "p2",
				BoardNumber:   1,
			},
		},
	})

	response, body := doRequest(
		t,
		http.MethodPost,
		server.URL+"/api/v1/tournaments/t1/pairings",
		`{"round":1,"players":[{"player_id":"p1","score":2},{"player_id":"p2","score":1}]}`,
	)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("POST pairings status = %d, want %d; body=%s", response.StatusCode, http.StatusOK, body)
	}
	if !strings.Contains(body, `"match_id":"t1-r1-b1"`) {
		t.Errorf("POST pairings body = %q, want mapped match", body)
	}

	request := pairings.lastRequest(t)
	if request.GetTournamentId() != "t1" || request.GetRound() != 1 {
		t.Errorf("GeneratePairings() tournament/round = %q/%d, want t1/1", request.GetTournamentId(), request.GetRound())
	}
	if len(request.GetPlayers()) != 2 || request.GetPlayers()[0].GetPlayerId() != "p1" {
		t.Errorf("GeneratePairings() players = %+v, want two mapped players", request.GetPlayers())
	}
}

type checkinCall struct {
	tournamentID string
	playerID     string
	ttl          time.Duration
}

type fakeCheckinService struct {
	mu        sync.Mutex
	succeeded bool
	err       error
	calls     []checkinCall
}

func (service *fakeCheckinService) TryCheckIn(
	_ context.Context,
	tournamentID, playerID string,
	ttl time.Duration,
) (bool, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.calls = append(service.calls, checkinCall{
		tournamentID: tournamentID,
		playerID:     playerID,
		ttl:          ttl,
	})
	return service.succeeded, service.err
}

func (service *fakeCheckinService) setResult(succeeded bool, err error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.succeeded = succeeded
	service.err = err
}

func (service *fakeCheckinService) lastCall(t *testing.T) checkinCall {
	t.Helper()
	service.mu.Lock()
	defer service.mu.Unlock()
	if len(service.calls) == 0 {
		t.Fatal("TryCheckIn() was not called")
	}
	return service.calls[len(service.calls)-1]
}

func (service *fakeCheckinService) callCount() int {
	service.mu.Lock()
	defer service.mu.Unlock()
	return len(service.calls)
}

type fakePairingClient struct {
	mu       sync.Mutex
	request  *pairingv1.PairingRequest
	response *pairingv1.PairingResponse
	err      error
}

func (client *fakePairingClient) GeneratePairings(
	_ context.Context,
	request *pairingv1.PairingRequest,
	_ ...grpc.CallOption,
) (*pairingv1.PairingResponse, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.request = proto.Clone(request).(*pairingv1.PairingRequest)
	return client.response, client.err
}

func (client *fakePairingClient) setResponse(response *pairingv1.PairingResponse) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.response = response
}

func (client *fakePairingClient) lastRequest(t *testing.T) *pairingv1.PairingRequest {
	t.Helper()
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.request == nil {
		t.Fatal("GeneratePairings() was not called")
	}
	return proto.Clone(client.request).(*pairingv1.PairingRequest)
}

func newTestGatewayServer(
	t *testing.T,
) (*httptest.Server, *fakeCheckinService, *fakePairingClient) {
	t.Helper()

	checkins := &fakeCheckinService{succeeded: true}
	pairings := &fakePairingClient{response: &pairingv1.PairingResponse{Success: true}}
	httpGateway, err := New(checkins, pairings)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	server := httptest.NewServer(httpGateway.Handler())
	t.Cleanup(server.Close)
	return server, checkins, pairings
}

func doRequest(
	t *testing.T,
	method, url, body string,
) (*http.Response, string) {
	t.Helper()

	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("create HTTP request: %v", err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("execute HTTP request: %v", err)
	}
	responseBody, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		t.Fatalf("read HTTP response: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("close HTTP response: %v", closeErr)
	}
	return response, string(responseBody)
}
