package postgres

import (
	"context"
	"fmt"
	"strings"
)

type TrackedTokenRecord struct {
	ID              int64
	ChainID         int64
	ContractAddress string
	Symbol          string
	Decimals        int
	IsActive        bool
}

type TrackedTokenRepo struct {
	db DBTX
}

func NewTrackedTokenRepo(db DBTX) *TrackedTokenRepo {
	return &TrackedTokenRepo{db: db}
}

func (r *TrackedTokenRepo) ListActiveAddresses(ctx context.Context, chainID int64) ([]string, error) {
	const query = `
		SELECT contract_address
		FROM tracked_tokens
		WHERE chain_id = $1
		  AND is_active = true
		ORDER BY id ASC
	`

	rows, err := r.db.Query(ctx, query, chainID)
	if err != nil {
		return nil, fmt.Errorf("query active tracked token addresses: %w", err)
	}
	defer rows.Close()

	addresses := make([]string, 0)

	for rows.Next() {
		var address string

		if err := rows.Scan(&address); err != nil {
			return nil, fmt.Errorf("scan tracked token address: %w", err)
		}

		address = strings.ToLower(strings.TrimSpace(address))
		if address == "" {
			continue
		}

		addresses = append(addresses, address)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tracked token addresses: %w", err)
	}

	return addresses, nil
}

func (r *TrackedTokenRepo) FindActiveByAddress(
	ctx context.Context,
	chainID int64,
	contractAddress string,
) (*TrackedTokenRecord, error) {
	const query = `
		SELECT
			id,
			chain_id,
			contract_address,
			COALESCE(symbol, ''),
			COALESCE(decimals, 0),
			is_active
		FROM tracked_tokens
		WHERE chain_id = $1
		  AND lower(contract_address) = lower($2)
		  AND is_active = true
		LIMIT 1
	`

	var record TrackedTokenRecord

	err := r.db.QueryRow(ctx, query, chainID, contractAddress).Scan(
		&record.ID,
		&record.ChainID,
		&record.ContractAddress,
		&record.Symbol,
		&record.Decimals,
		&record.IsActive,
	)
	if err != nil {
		return nil, fmt.Errorf("find active tracked token by address: %w", err)
	}

	record.ContractAddress = strings.ToLower(strings.TrimSpace(record.ContractAddress))

	return &record, nil
}
