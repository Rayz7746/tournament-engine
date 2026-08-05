package checkin

const (
	CheckinEventStream    = "stream:checkin_events"
	CheckinDLQStream      = "stream:checkin_dlq"
	CheckinConsumerGroup  = "checkin_processors"
	DefaultConsumerName   = "worker-1"
	defaultRetryCountHash = "checkin:stream:retry_counts"
)

// CheckinEvent is the application-level representation of a Redis Stream
// check-in message.
type CheckinEvent struct {
	ID           string
	TournamentID string
	PlayerID     string
	Timestamp    string
}
