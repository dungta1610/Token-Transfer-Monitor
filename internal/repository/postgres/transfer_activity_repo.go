package postgres

import (
	"context"
	"fmt"
	"time"
)

type TransferActivityRecord struct {
	EventID         string
	ChainID         int64
	ContractAddress string
	TokenSymbol     string
	FromAddress     string
	ToAddress       string
	AmountRaw       string
	TxHash          string
	LogIndex        int64
	BlockNumber     int64
	BlockHash       string
	Direction       string
	MatchedWallet   string
	ObservedAt      time.Time
}

type TransferActivityRepo struct {
	db DBTX
}

func NewTransferActivityRepo(db DBTX) *TransferActivityRepo {
	return &TransferActivityRepo{db: db}
}

func (r *TransferActivityRepo) Insert(ctx context.Context, record TransferActivityRecord) error {
	const query = `
		INSERT INTO transfer_activities (
			event_id,
			chain_id,
			contract_address,
			token_symbol,
			from_address,
			to_address,
			amount_raw,
			tx_hash,
			log_index,
			block_number,
			block_hash,
			direction,
			matched_wallet,
			observed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`

	_, err := r.db.Exec(
		ctx,
		query,
		record.EventID,
		record.ChainID,
		record.ContractAddress,
		record.TokenSymbol,
		record.FromAddress,
		record.ToAddress,
		record.AmountRaw,
		record.TxHash,
		record.LogIndex,
		record.BlockNumber,
		record.BlockHash,
		record.Direction,
		record.MatchedWallet,
		record.ObservedAt,
	)
	if err != nil {
		return fmt.Errorf("insert transfer activity: %w", err)
	}

	return nil
}
