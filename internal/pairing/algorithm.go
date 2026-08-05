package pairing

import (
	"context"
	"errors"
	"fmt"
	"sort"

	pairingv1 "tournament-engine/pkg/proto/pairing/v1"
)

const (
	colorWhite = "WHITE"
	colorBlack = "BLACK"
)

// PairingResult contains the matches for a round and, for an odd-sized field,
// the player awarded a one-point bye.
type PairingResult struct {
	Matches     []*pairingv1.Match
	ByePlayerID string
}

type playerPair struct {
	higherRanked *pairingv1.Player
	lowerRanked  *pairingv1.Player
}

// CalculatePairings creates deterministic Swiss-system pairings. Players are
// ranked by score and then player ID. For an odd-sized field, the lowest-ranked
// eligible player receives the bye. Pair selection uses backtracking so that a
// legal complete round is found whenever one exists without allowing rematches.
func CalculatePairings(
	ctx context.Context,
	request *pairingv1.PairingRequest,
) (*PairingResult, error) {
	players, err := validateAndRankPlayers(request)
	if err != nil {
		return nil, err
	}

	result := &PairingResult{
		Matches: make([]*pairingv1.Match, 0, len(players)/2),
	}
	if len(players)%2 != 0 {
		players, result.ByePlayerID, err = removeByePlayer(players)
		if err != nil {
			return nil, err
		}
	}

	pairs, found, err := findPairings(ctx, players)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.New("no complete pairing exists without a rematch")
	}

	for i, pair := range pairs {
		white, black := assignColors(pair.higherRanked, pair.lowerRanked)
		boardNumber := int32(i + 1)
		result.Matches = append(result.Matches, &pairingv1.Match{
			MatchId:       fmt.Sprintf("%s-r%d-b%d", request.GetTournamentId(), request.GetRound(), boardNumber),
			WhitePlayerId: white.GetPlayerId(),
			BlackPlayerId: black.GetPlayerId(),
			BoardNumber:   boardNumber,
		})
	}

	return result, nil
}

func validateAndRankPlayers(request *pairingv1.PairingRequest) ([]*pairingv1.Player, error) {
	if request == nil {
		return nil, errors.New("pairing request is required")
	}
	if request.GetTournamentId() == "" {
		return nil, errors.New("tournament ID is required")
	}
	if request.GetRound() <= 0 {
		return nil, errors.New("round must be positive")
	}
	if len(request.GetPlayers()) == 0 {
		return nil, errors.New("at least one player is required")
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

		if player.GetWhiteCount() < 0 || player.GetBlackCount() < 0 {
			return nil, fmt.Errorf("player %q has a negative color count", player.GetPlayerId())
		}
		switch player.GetLastColor() {
		case "", colorWhite, colorBlack:
		default:
			return nil, fmt.Errorf(
				"player %q has invalid last color %q",
				player.GetPlayerId(),
				player.GetLastColor(),
			)
		}
	}

	sort.SliceStable(players, func(i, j int) bool {
		if players[i].GetScore() == players[j].GetScore() {
			return players[i].GetPlayerId() < players[j].GetPlayerId()
		}
		return players[i].GetScore() > players[j].GetScore()
	})

	return players, nil
}

func removeByePlayer(players []*pairingv1.Player) ([]*pairingv1.Player, string, error) {
	for i := len(players) - 1; i >= 0; i-- {
		if players[i].GetReceivedBye() {
			continue
		}

		byePlayerID := players[i].GetPlayerId()
		remaining := make([]*pairingv1.Player, 0, len(players)-1)
		remaining = append(remaining, players[:i]...)
		remaining = append(remaining, players[i+1:]...)
		return remaining, byePlayerID, nil
	}

	return nil, "", errors.New("no player is eligible to receive a bye")
}

func findPairings(
	ctx context.Context,
	players []*pairingv1.Player,
) ([]playerPair, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if len(players) == 0 {
		return nil, true, nil
	}

	first := players[0]
	for opponentIndex := 1; opponentIndex < len(players); opponentIndex++ {
		opponent := players[opponentIndex]
		if playersAvoidEachOther(first, opponent) {
			continue
		}

		remaining := make([]*pairingv1.Player, 0, len(players)-2)
		remaining = append(remaining, players[1:opponentIndex]...)
		remaining = append(remaining, players[opponentIndex+1:]...)

		followingPairs, found, err := findPairings(ctx, remaining)
		if err != nil {
			return nil, false, err
		}
		if found {
			pairs := make([]playerPair, 0, len(followingPairs)+1)
			pairs = append(pairs, playerPair{
				higherRanked: first,
				lowerRanked:  opponent,
			})
			pairs = append(pairs, followingPairs...)
			return pairs, true, nil
		}
	}

	return nil, false, nil
}

func assignColors(higherRanked, lowerRanked *pairingv1.Player) (*pairingv1.Player, *pairingv1.Player) {
	higherBalance := higherRanked.GetWhiteCount() - higherRanked.GetBlackCount()
	lowerBalance := lowerRanked.GetWhiteCount() - lowerRanked.GetBlackCount()
	if higherBalance != lowerBalance {
		if higherBalance < lowerBalance {
			return higherRanked, lowerRanked
		}
		return lowerRanked, higherRanked
	}

	if higherRanked.GetWhiteCount() != lowerRanked.GetWhiteCount() {
		if higherRanked.GetWhiteCount() < lowerRanked.GetWhiteCount() {
			return higherRanked, lowerRanked
		}
		return lowerRanked, higherRanked
	}

	higherPreference := nextWhitePreference(higherRanked.GetLastColor())
	lowerPreference := nextWhitePreference(lowerRanked.GetLastColor())
	if higherPreference != lowerPreference {
		if higherPreference > lowerPreference {
			return higherRanked, lowerRanked
		}
		return lowerRanked, higherRanked
	}

	return higherRanked, lowerRanked
}

func nextWhitePreference(lastColor string) int {
	switch lastColor {
	case colorBlack:
		return 1
	case colorWhite:
		return -1
	default:
		return 0
	}
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
