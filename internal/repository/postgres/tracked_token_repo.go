package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type TrackedTokenRecord struct {
	ID              int64     `json:"id"`
	ChainID         int64     `json:"chainId"`
	ContractAddress string    `json:"contractAddress"`
	Symbol          string    `json:"symbol"`
	Decimals        int       `json:"decimals"`
	IsActive        bool      `json:"isActive"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
}

type CreateTrackedTokenParams struct {
	ChainID         int64
	ContractAddress string
	Symbol          string
	Decimals        int
	IsActive        bool
}

type UpdateTrackedTokenParams struct {
	Symbol   *string
	Decimals *int
	IsActive *bool
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

		address = normalizeAddress(address)
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
			is_active,
			created_at,
			updated_at
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
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("find active tracked token by address: %w", err)
	}

	record.ContractAddress = normalizeAddress(record.ContractAddress)

	return &record, nil
}

func (r *TrackedTokenRepo) Create(ctx context.Context, params CreateTrackedTokenParams) (*TrackedTokenRecord, error) {
	const query = `
		INSERT INTO tracked_tokens (
			chain_id,
			contract_address,
			symbol,
			decimals,
			is_active
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (chain_id, contract_address)
		DO UPDATE SET
			symbol = EXCLUDED.symbol,
			decimals = EXCLUDED.decimals,
			is_active = EXCLUDED.is_active,
			updated_at = NOW()
		RETURNING
			id,
			chain_id,
			contract_address,
			COALESCE(symbol, ''),
			COALESCE(decimals, 0),
			is_active,
			created_at,
			updated_at
	`

	var record TrackedTokenRecord

	err := r.db.QueryRow(
		ctx,
		query,
		params.ChainID,
		normalizeAddress(params.ContractAddress),
		strings.TrimSpace(params.Symbol),
		params.Decimals,
		params.IsActive,
	).Scan(
		&record.ID,
		&record.ChainID,
		&record.ContractAddress,
		&record.Symbol,
		&record.Decimals,
		&record.IsActive,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("create tracked token: %w", err)
	}

	record.ContractAddress = normalizeAddress(record.ContractAddress)

	return &record, nil
}

func (r *TrackedTokenRepo) Update(ctx context.Context, id int64, params UpdateTrackedTokenParams) (*TrackedTokenRecord, error) {
	const query = `
		UPDATE tracked_tokens
		SET
			symbol = COALESCE($2, symbol),
			decimals = COALESCE($3, decimals),
			is_active = COALESCE($4, is_active),
			updated_at = NOW()
		WHERE id = $1
		RETURNING
			id,
			chain_id,
			contract_address,
			COALESCE(symbol, ''),
			COALESCE(decimals, 0),
			is_active,
			created_at,
			updated_at
	`

	var symbol any
	var decimals any
	var isActive any

	if params.Symbol != nil {
		symbol = strings.TrimSpace(*params.Symbol)
	}

	if params.Decimals != nil {
		decimals = *params.Decimals
	}

	if params.IsActive != nil {
		isActive = *params.IsActive
	}

	var record TrackedTokenRecord

	err := r.db.QueryRow(
		ctx,
		query,
		id,
		symbol,
		decimals,
		isActive,
	).Scan(
		&record.ID,
		&record.ChainID,
		&record.ContractAddress,
		&record.Symbol,
		&record.Decimals,
		&record.IsActive,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("update tracked token: %w", err)
	}

	record.ContractAddress = normalizeAddress(record.ContractAddress)

	return &record, nil
}

func (r *TrackedTokenRepo) Deactivate(ctx context.Context, id int64) (*TrackedTokenRecord, error) {
	inactive := false

	return r.Update(ctx, id, UpdateTrackedTokenParams{
		IsActive: &inactive,
	})
}

func normalizeAddress(address string) string {
	return strings.ToLower(strings.TrimSpace(address))
}
