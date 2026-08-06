package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"tournament-engine/internal/checkin"
	pairingv1 "tournament-engine/pkg/proto/pairing/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultCheckinTTL = 15 * time.Minute
	maximumCheckinTTL = 24 * time.Hour
	maximumBodyBytes  = 1 << 20
)

type CheckinService interface {
	TryCheckIn(
		ctx context.Context,
		tournamentID, playerID string,
		ttl time.Duration,
	) (bool, error)
}

type Gateway struct {
	checkins CheckinService
	pairings PairingClient
	handler  http.Handler
}

func New(checkins CheckinService, pairings PairingClient) (*Gateway, error) {
	if checkins == nil {
		return nil, errors.New("check-in service is required")
	}
	if pairings == nil {
		return nil, errors.New("pairing service client is required")
	}

	gateway := &Gateway{
		checkins: checkins,
		pairings: pairings,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", gateway.handleHealth)
	mux.HandleFunc("POST /api/v1/tournaments/{id}/checkin", gateway.handleCheckin)
	mux.HandleFunc("POST /api/v1/tournaments/{id}/pairings", gateway.handlePairings)
	gateway.handler = mux
	return gateway, nil
}

func (g *Gateway) Handler() http.Handler {
	return g.handler
}

type checkinRequest struct {
	PlayerID   string `json:"player_id"`
	TTLSeconds int64  `json:"ttl_seconds,omitempty"`
}

type checkinResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

func (g *Gateway) handleHealth(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
}

func (g *Gateway) handleCheckin(writer http.ResponseWriter, request *http.Request) {
	var payload checkinRequest
	if err := decodeJSON(writer, request, &payload); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if payload.PlayerID == "" {
		writeError(writer, http.StatusBadRequest, "player_id is required")
		return
	}

	ttl := defaultCheckinTTL
	if payload.TTLSeconds != 0 {
		if payload.TTLSeconds < 0 || payload.TTLSeconds > int64(maximumCheckinTTL/time.Second) {
			writeError(writer, http.StatusBadRequest, "ttl_seconds must be between 1 and 86400")
			return
		}
		ttl = time.Duration(payload.TTLSeconds) * time.Second
	}

	succeeded, err := g.checkins.TryCheckIn(
		request.Context(),
		request.PathValue("id"),
		payload.PlayerID,
		ttl,
	)
	if err != nil {
		if errors.Is(err, checkin.ErrTournamentIDRequired) ||
			errors.Is(err, checkin.ErrPlayerIDRequired) ||
			errors.Is(err, checkin.ErrInvalidTTL) {
			writeError(writer, http.StatusBadRequest, err.Error())
			return
		}
		writeError(writer, http.StatusServiceUnavailable, "check-in service unavailable")
		return
	}
	if !succeeded {
		writeJSON(writer, http.StatusConflict, checkinResponse{
			Success: false,
			Message: "player is already checked in",
		})
		return
	}

	writeJSON(writer, http.StatusOK, checkinResponse{Success: true})
}

type pairingRequest struct {
	Round   int32           `json:"round"`
	Players []pairingPlayer `json:"players"`
}

type pairingPlayer struct {
	PlayerID         string   `json:"player_id"`
	Score            int32    `json:"score"`
	AvoidedOpponents []string `json:"avoided_opponents,omitempty"`
	WhiteCount       int32    `json:"white_count,omitempty"`
	BlackCount       int32    `json:"black_count,omitempty"`
	LastColor        string   `json:"last_color,omitempty"`
	ReceivedBye      bool     `json:"received_bye,omitempty"`
}

type pairingResponse struct {
	Success      bool           `json:"success"`
	Matches      []pairingMatch `json:"matches"`
	ErrorMessage string         `json:"error_message,omitempty"`
	ByePlayerID  string         `json:"bye_player_id,omitempty"`
}

type pairingMatch struct {
	MatchID       string `json:"match_id"`
	WhitePlayerID string `json:"white_player_id"`
	BlackPlayerID string `json:"black_player_id"`
	BoardNumber   int32  `json:"board_number"`
}

func (g *Gateway) handlePairings(writer http.ResponseWriter, request *http.Request) {
	var payload pairingRequest
	if err := decodeJSON(writer, request, &payload); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if payload.Round <= 0 {
		writeError(writer, http.StatusBadRequest, "round must be positive")
		return
	}
	if len(payload.Players) == 0 {
		writeError(writer, http.StatusBadRequest, "players are required")
		return
	}

	players := make([]*pairingv1.Player, 0, len(payload.Players))
	for _, player := range payload.Players {
		if player.PlayerID == "" {
			writeError(writer, http.StatusBadRequest, "every player must have a player_id")
			return
		}
		players = append(players, &pairingv1.Player{
			PlayerId:         player.PlayerID,
			Score:            player.Score,
			AvoidedOpponents: player.AvoidedOpponents,
			WhiteCount:       player.WhiteCount,
			BlackCount:       player.BlackCount,
			LastColor:        player.LastColor,
			ReceivedBye:      player.ReceivedBye,
		})
	}

	response, err := g.pairings.GeneratePairings(request.Context(), &pairingv1.PairingRequest{
		TournamentId: request.PathValue("id"),
		Round:        payload.Round,
		Players:      players,
	})
	if err != nil {
		writeError(writer, pairingHTTPStatus(err), "pairing service request failed")
		return
	}
	if response == nil {
		writeError(writer, http.StatusBadGateway, "pairing service returned an empty response")
		return
	}

	httpStatus := http.StatusOK
	if !response.GetSuccess() {
		httpStatus = http.StatusUnprocessableEntity
	}
	writeJSON(writer, httpStatus, pairingResponseFromProto(response))
}

func pairingResponseFromProto(response *pairingv1.PairingResponse) pairingResponse {
	matches := make([]pairingMatch, 0, len(response.GetMatches()))
	for _, match := range response.GetMatches() {
		matches = append(matches, pairingMatch{
			MatchID:       match.GetMatchId(),
			WhitePlayerID: match.GetWhitePlayerId(),
			BlackPlayerID: match.GetBlackPlayerId(),
			BoardNumber:   match.GetBoardNumber(),
		})
	}
	return pairingResponse{
		Success:      response.GetSuccess(),
		Matches:      matches,
		ErrorMessage: response.GetErrorMessage(),
		ByePlayerID:  response.GetByePlayerId(),
	}
}

func pairingHTTPStatus(err error) int {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadGateway
	}
}

func decodeJSON(writer http.ResponseWriter, request *http.Request, destination any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maximumBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("invalid JSON request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func writeError(writer http.ResponseWriter, statusCode int, message string) {
	writeJSON(writer, statusCode, map[string]string{"error": message})
}

func writeJSON(writer http.ResponseWriter, statusCode int, payload any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_ = json.NewEncoder(writer).Encode(payload)
}
