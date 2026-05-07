package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type TransferActivityView struct {
	ID              int64      `json:"id"`
	EventID         string     `json:"eventId"`
	ChainID         int64      `json:"chainId"`
	ContractAddress string     `json:"contractAddress"`
	TokenSymbol     string     `json:"tokenSymbol"`
	FromAddress     string     `json:"fromAddress"`
	ToAddress       string     `json:"toAddress"`
	AmountRaw       string     `json:"amountRaw"`
	TxHash          string     `json:"txHash"`
	LogIndex        int64      `json:"logIndex"`
	BlockNumber     int64      `json:"blockNumber"`
	BlockHash       string     `json:"blockHash"`
	Direction       string     `json:"direction"`
	MatchedWallet   string     `json:"matchedWallet"`
	ObservedAt      *time.Time `json:"observedAt,omitempty"`
	CreatedAt       time.Time  `json:"createdAt"`
}

type TrackedTokenView struct {
	ID              int64     `json:"id"`
	ChainID         int64     `json:"chainId"`
	ContractAddress string    `json:"contractAddress"`
	Symbol          string    `json:"symbol"`
	Decimals        int       `json:"decimals"`
	IsActive        bool      `json:"isActive"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
}

type WalletWatchlistView struct {
	ID        int64     `json:"id"`
	Address   string    `json:"address"`
	Label     string    `json:"label"`
	IsActive  bool      `json:"isActive"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type QueryRepo struct {
	db DBTX
}

func NewQueryRepo(db DBTX) *QueryRepo {
	return &QueryRepo{db: db}
}

func (r *QueryRepo) ListTransfers(ctx context.Context, limit int, offset int) ([]TransferActivityView, error) {
	const query = `
		SELECT
			id,
			event_id,
			chain_id,
			contract_address,
			COALESCE(token_symbol, ''),
			from_address,
			to_address,
			amount_raw,
			tx_hash,
			log_index,
			block_number,
			COALESCE(block_hash, ''),
			COALESCE(direction, ''),
			COALESCE(matched_wallet, ''),
			observed_at,
			created_at
		FROM transfer_activities
		ORDER BY created_at DESC, id DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := r.db.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query transfer activities: %w", err)
	}
	defer rows.Close()

	return scanTransferActivities(rows)
}

func (r *QueryRepo) GetTransferByEventID(ctx context.Context, eventID string) (*TransferActivityView, error) {
	const query = `
		SELECT
			id,
			event_id,
			chain_id,
			contract_address,
			COALESCE(token_symbol, ''),
			from_address,
			to_address,
			amount_raw,
			tx_hash,
			log_index,
			block_number,
			COALESCE(block_hash, ''),
			COALESCE(direction, ''),
			COALESCE(matched_wallet, ''),
			observed_at,
			created_at
		FROM transfer_activities
		WHERE event_id = $1
		LIMIT 1
	`

	var item TransferActivityView

	err := r.db.QueryRow(ctx, query, eventID).Scan(
		&item.ID,
		&item.EventID,
		&item.ChainID,
		&item.ContractAddress,
		&item.TokenSymbol,
		&item.FromAddress,
		&item.ToAddress,
		&item.AmountRaw,
		&item.TxHash,
		&item.LogIndex,
		&item.BlockNumber,
		&item.BlockHash,
		&item.Direction,
		&item.MatchedWallet,
		&item.ObservedAt,
		&item.CreatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get transfer by event id: %w", err)
	}

	return &item, nil
}

func (r *QueryRepo) ListTransfersByWallet(ctx context.Context, address string, limit int, offset int) ([]TransferActivityView, error) {
	const query = `
		SELECT
			id,
			event_id,
			chain_id,
			contract_address,
			COALESCE(token_symbol, ''),
			from_address,
			to_address,
			amount_raw,
			tx_hash,
			log_index,
			block_number,
			COALESCE(block_hash, ''),
			COALESCE(direction, ''),
			COALESCE(matched_wallet, ''),
			observed_at,
			created_at
		FROM transfer_activities
		WHERE lower(from_address) = lower($1)
		   OR lower(to_address) = lower($1)
		   OR lower(matched_wallet) = lower($1)
		ORDER BY created_at DESC, id DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.db.Query(ctx, query, strings.TrimSpace(address), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query transfer activities by wallet: %w", err)
	}
	defer rows.Close()

	return scanTransferActivities(rows)
}

func (r *QueryRepo) ListTrackedTokens(ctx context.Context) ([]TrackedTokenView, error) {
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
		ORDER BY chain_id ASC, id ASC
	`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query tracked tokens: %w", err)
	}
	defer rows.Close()

	items := make([]TrackedTokenView, 0)

	for rows.Next() {
		var item TrackedTokenView

		err := rows.Scan(
			&item.ID,
			&item.ChainID,
			&item.ContractAddress,
			&item.Symbol,
			&item.Decimals,
			&item.IsActive,
			&item.CreatedAt,
			&item.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan tracked token: %w", err)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tracked tokens: %w", err)
	}

	return items, nil
}

func (r *QueryRepo) ListWalletWatchlist(ctx context.Context) ([]WalletWatchlistView, error) {
	const query = `
		SELECT
			id,
			address,
			COALESCE(label, ''),
			is_active,
			created_at,
			updated_at
		FROM wallet_watchlist
		ORDER BY id ASC
	`

	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query wallet watchlist: %w", err)
	}
	defer rows.Close()

	items := make([]WalletWatchlistView, 0)

	for rows.Next() {
		var item WalletWatchlistView

		err := rows.Scan(
			&item.ID,
			&item.Address,
			&item.Label,
			&item.IsActive,
			&item.CreatedAt,
			&item.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan wallet watchlist item: %w", err)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wallet watchlist: %w", err)
	}

	return items, nil
}

func scanTransferActivities(rows pgx.Rows) ([]TransferActivityView, error) {
	items := make([]TransferActivityView, 0)

	for rows.Next() {
		var item TransferActivityView

		err := rows.Scan(
			&item.ID,
			&item.EventID,
			&item.ChainID,
			&item.ContractAddress,
			&item.TokenSymbol,
			&item.FromAddress,
			&item.ToAddress,
			&item.AmountRaw,
			&item.TxHash,
			&item.LogIndex,
			&item.BlockNumber,
			&item.BlockHash,
			&item.Direction,
			&item.MatchedWallet,
			&item.ObservedAt,
			&item.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan transfer activity: %w", err)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate transfer activities: %w", err)
	}

	return items, nil
}
