package checkin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CheckinRecord is the durable PostgreSQL representation of a successful
// tournament check-in.
type CheckinRecord struct {
	ID           uint      `gorm:"primaryKey"`
	TournamentID string    `gorm:"type:varchar(255);not null;index:idx_checkin_records_tournament_id;uniqueIndex:ux_checkin_records_tournament_player,priority:1"`
	PlayerID     string    `gorm:"type:varchar(255);not null;index:idx_checkin_records_player_id;uniqueIndex:ux_checkin_records_tournament_player,priority:2"`
	CheckedInAt  time.Time `gorm:"not null"`
}

// CheckinRepository is the persistence boundary used by stream workers.
type CheckinRepository interface {
	SaveCheckin(
		ctx context.Context,
		tournamentID, playerID string,
		checkedInAt time.Time,
	) error
}

type Repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) (*Repository, error) {
	if db == nil {
		return nil, errors.New("GORM database is required")
	}
	return &Repository{db: db}, nil
}

// Migrate creates or updates the check-in table and its indexes.
func (r *Repository) Migrate(ctx context.Context) error {
	if err := r.db.WithContext(ctx).AutoMigrate(&CheckinRecord{}); err != nil {
		return fmt.Errorf("migrate check-in records: %w", err)
	}
	return nil
}

// SaveCheckin persists a check-in idempotently. Duplicate stream delivery is
// treated as success because the tournament/player pair already exists.
func (r *Repository) SaveCheckin(
	ctx context.Context,
	tournamentID, playerID string,
	checkedInAt time.Time,
) error {
	if tournamentID == "" {
		return ErrTournamentIDRequired
	}
	if playerID == "" {
		return ErrPlayerIDRequired
	}
	if checkedInAt.IsZero() {
		return errors.New("check-in timestamp is required")
	}

	record := CheckinRecord{
		TournamentID: tournamentID,
		PlayerID:     playerID,
		CheckedInAt:  checkedInAt.UTC(),
	}
	err := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "tournament_id"},
				{Name: "player_id"},
			},
			DoNothing: true,
		}).
		Create(&record).Error
	if err != nil {
		return fmt.Errorf("save check-in record: %w", err)
	}
	return nil
}
