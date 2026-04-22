package postgres

import (
	"context"
	"fmt"
	"time"
)

type ProcessedEventRecord struct {
	EventID         string
	ChainID         int64
	TxHash          string
	LogIndex        int64
	ContractAddress string
	ProcessedAt     time.Time
}

type ProcessedEventRepo struct {
	db DBTX
}

func NewProcessedEventRepo(db DBTX) *ProcessedEventRepo {
	return &ProcessedEventRepo{db: db}
}

func (r *ProcessedEventRepo) Exists(ctx context.Context, eventID string) (bool, error) {
	const query = `
		SELECT EXISTS(
			SELECT 1
			FROM processed_events
			WHERE event_id = $1
		)
	`

	var exists bool
	if err := r.db.QueryRow(ctx, query, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check processed event exists: %w", err)
	}

	return exists, nil
}

func (r *ProcessedEventRepo) Insert(ctx context.Context, record ProcessedEventRecord) error {
	const query = `
		INSERT INTO processed_events (
			event_id,
			chain_id,
			tx_hash,
			log_index,
			contract_address,
			processed_at
		) VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := r.db.Exec(
		ctx,
		query,
		record.EventID,
		record.ChainID,
		record.TxHash,
		record.LogIndex,
		record.ContractAddress,
		record.ProcessedAt,
	)
	if err != nil {
		return fmt.Errorf("insert processed event: %w", err)
	}

	return nil
}
