package postgres

import (
	"context"
	"fmt"
	"strings"
)

type WalletWatchlistRecord struct {
	ID       int64
	Address  string
	Label    string
	IsActive bool
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

		address = strings.ToLower(strings.TrimSpace(address))
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
