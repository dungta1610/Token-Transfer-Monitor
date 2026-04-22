package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func RunInTx(
	ctx context.Context,
	pool *pgxpool.Pool,
	fn func(tx pgx.Tx) error,
) (err error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if err = fn(tx); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}
