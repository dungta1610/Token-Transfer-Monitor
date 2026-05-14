package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type WalletWatchlistRecord struct {
	ID        int64     `json:"id"`
	Address   string    `json:"address"`
	Label     string    `json:"label"`
	IsActive  bool      `json:"isActive"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type CreateWalletWatchlistParams struct {
	Address  string
	Label    string
	IsActive bool
}

type UpdateWalletWatchlistParams struct {
	Label    *string
	IsActive *bool
}

type WalletWatchlistRepo struct {
	db DBTX
}

func NewWalletWatchlistRepo(db DBTX) *WalletWatchlistRepo {
	return &WalletWatchlistRepo{db: db}
}

func (r *WalletWatchlistRepo) ListActiveAddresses(ctx context.Context) ([]string, error) {
	const query = `
		SELECT address
		FROM wallet_watchlist
		WHERE is_active = true
		ORDER BY id ASC
	`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query active wallet watchlist addresses: %w", err)
	}
	defer rows.Close()

	addresses := make([]string, 0)

	for rows.Next() {
		var address string

		if err := rows.Scan(&address); err != nil {
			return nil, fmt.Errorf("scan wallet watchlist address: %w", err)
		}

		address = normalizeAddress(address)
		if address == "" {
			continue
		}

		addresses = append(addresses, address)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallet watchlist addresses: %w", err)
	}

	return addresses, nil
}

func (r *WalletWatchlistRepo) ExistsActiveAddress(ctx context.Context, address string) (bool, error) {
	const query = `
		SELECT EXISTS(
			SELECT 1
			FROM wallet_watchlist
			WHERE lower(address) = lower($1)
			  AND is_active = true
		)
	`

	var exists bool

	if err := r.db.QueryRow(ctx, query, address).Scan(&exists); err != nil {
		return false, fmt.Errorf("check active wallet watchlist address exists: %w", err)
	}

	return exists, nil
}

func (r *WalletWatchlistRepo) Create(ctx context.Context, params CreateWalletWatchlistParams) (*WalletWatchlistRecord, error) {
	const query = `
		INSERT INTO wallet_watchlist (
			address,
			label,
			is_active
		) VALUES ($1, $2, $3)
		ON CONFLICT (address)
		DO UPDATE SET
			label = EXCLUDED.label,
			is_active = EXCLUDED.is_active,
			updated_at = NOW()
		RETURNING
			id,
			address,
			COALESCE(label, ''),
			is_active,
			created_at,
			updated_at
	`

	var record WalletWatchlistRecord

	err := r.db.QueryRow(
		ctx,
		query,
		normalizeAddress(params.Address),
		strings.TrimSpace(params.Label),
		params.IsActive,
	).Scan(
		&record.ID,
		&record.Address,
		&record.Label,
		&record.IsActive,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("create wallet watchlist: %w", err)
	}

	record.Address = normalizeAddress(record.Address)

	return &record, nil
}

func (r *WalletWatchlistRepo) Update(ctx context.Context, id int64, params UpdateWalletWatchlistParams) (*WalletWatchlistRecord, error) {
	const query = `
		UPDATE wallet_watchlist
		SET
			label = COALESCE($2, label),
			is_active = COALESCE($3, is_active),
			updated_at = NOW()
		WHERE id = $1
		RETURNING
			id,
			address,
			COALESCE(label, ''),
			is_active,
			created_at,
			updated_at
	`

	var label any
	var isActive any

	if params.Label != nil {
		label = strings.TrimSpace(*params.Label)
	}

	if params.IsActive != nil {
		isActive = *params.IsActive
	}

	var record WalletWatchlistRecord

	err := r.db.QueryRow(
		ctx,
		query,
		id,
		label,
		isActive,
	).Scan(
		&record.ID,
		&record.Address,
		&record.Label,
		&record.IsActive,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("update wallet watchlist: %w", err)
	}

	record.Address = normalizeAddress(record.Address)

	return &record, nil
}

func (r *WalletWatchlistRepo) Deactivate(ctx context.Context, id int64) (*WalletWatchlistRecord, error) {
	inactive := false

	return r.Update(ctx, id, UpdateWalletWatchlistParams{
		IsActive: &inactive,
	})
}
