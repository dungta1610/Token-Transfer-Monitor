package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"token-transfer-monitor/internal/contract"
	pgrepo "token-transfer-monitor/internal/repository/postgres"
	"token-transfer-monitor/internal/storage"
)

type ProcessResult string

const (
	ProcessResultSuccess        ProcessResult = "success"
	ProcessResultDuplicate      ProcessResult = "duplicate"
	ProcessResultPermanentError ProcessResult = "permanent_error"
	ProcessResultRetryableError ProcessResult = "retryable_error"
)

type ProcessOutput struct {
	Result  ProcessResult
	Message string
}

type PermanentError struct {
	Err error
}

func (e PermanentError) Error() string {
	return e.Err.Error()
}

func (e PermanentError) Unwrap() error {
	return e.Err
}

type RetryableError struct {
	Err error
}

func (e RetryableError) Error() string {
	return e.Err.Error()
}

func (e RetryableError) Unwrap() error {
	return e.Err
}

type TransferProcessor struct {
	pool      *pgxpool.Pool
	watchlist map[string]struct{}
}

func NewTransferProcessor(
	pool *pgxpool.Pool,
	watchlistAddresses []string,
) *TransferProcessor {
	watchlist := make(map[string]struct{}, len(watchlistAddresses))
	for _, addr := range watchlistAddresses {
		normalized := strings.ToLower(strings.TrimSpace(addr))
		if normalized == "" {
			continue
		}
		watchlist[normalized] = struct{}{}
	}

	return &TransferProcessor{
		pool:      pool,
		watchlist: watchlist,
	}
}

func (p *TransferProcessor) Process(ctx context.Context, msg contract.TransferDetectedMessage) (ProcessOutput, error) {
	msg.Normalize()

	if err := msg.Validate(); err != nil {
		return ProcessOutput{
			Result:  ProcessResultPermanentError,
			Message: "invalid message contract",
		}, PermanentError{Err: fmt.Errorf("validate transfer message: %w", err)}
	}

	baseProcessedRepo := pgrepo.NewProcessedEventRepo(p.pool)

	exists, err := baseProcessedRepo.Exists(ctx, msg.EventID)
	if err != nil {
		return ProcessOutput{
			Result:  ProcessResultRetryableError,
			Message: "failed to check processed event",
		}, RetryableError{Err: err}
	}

	if exists {
		return ProcessOutput{
			Result:  ProcessResultDuplicate,
			Message: "event already processed",
		}, nil
	}

	err = storage.RunInTx(ctx, p.pool, func(tx pgx.Tx) error {
		processedRepo := pgrepo.NewProcessedEventRepo(tx)
		activityRepo := pgrepo.NewTransferActivityRepo(tx)

		matchedWallet, direction := p.detectMatch(msg)
		if matchedWallet == "" {
			direction = "untracked"
		}

		activity := pgrepo.TransferActivityRecord{
			EventID:         msg.EventID,
			ChainID:         msg.ChainID,
			ContractAddress: msg.ContractAddress,
			TokenSymbol:     msg.TokenSymbol,
			FromAddress:     msg.From,
			ToAddress:       msg.To,
			AmountRaw:       msg.AmountRaw,
			TxHash:          msg.TxHash,
			LogIndex:        msg.LogIndex,
			BlockNumber:     msg.BlockNumber,
			BlockHash:       msg.BlockHash,
			Direction:       direction,
			MatchedWallet:   matchedWallet,
			ObservedAt:      msg.ObservedAt,
		}

		if err := activityRepo.Insert(ctx, activity); err != nil {
			if isUniqueViolation(err) {
				return PermanentError{Err: fmt.Errorf("duplicate transfer activity: %w", err)}
			}
			return RetryableError{Err: err}
		}

		processed := pgrepo.ProcessedEventRecord{
			EventID:         msg.EventID,
			ChainID:         msg.ChainID,
			TxHash:          msg.TxHash,
			LogIndex:        msg.LogIndex,
			ContractAddress: msg.ContractAddress,
			ProcessedAt:     time.Now().UTC(),
		}

		if err := processedRepo.Insert(ctx, processed); err != nil {
			if isUniqueViolation(err) {
				return PermanentError{Err: fmt.Errorf("duplicate processed event: %w", err)}
			}
			return RetryableError{Err: err}
		}

		return nil
	})
	if err != nil {
		switch {
		case IsPermanentError(err):
			return ProcessOutput{
				Result:  ProcessResultPermanentError,
				Message: "permanent processing error",
			}, err
		case IsRetryableError(err):
			return ProcessOutput{
				Result:  ProcessResultRetryableError,
				Message: "retryable processing error",
			}, err
		default:
			return ProcessOutput{
				Result:  ProcessResultRetryableError,
				Message: "unexpected processing error",
			}, RetryableError{Err: err}
		}
	}

	return ProcessOutput{
		Result:  ProcessResultSuccess,
		Message: "transfer processed successfully",
	}, nil
}

func (p *TransferProcessor) detectMatch(msg contract.TransferDetectedMessage) (matchedWallet string, direction string) {
	_, fromMatched := p.watchlist[msg.From]
	_, toMatched := p.watchlist[msg.To]

	switch {
	case fromMatched && toMatched:
		return msg.From, "self_transfer"
	case toMatched:
		return msg.To, "incoming"
	case fromMatched:
		return msg.From, "outgoing"
	default:
		return "", ""
	}
}

func IsPermanentError(err error) bool {
	var target PermanentError
	return errors.As(err, &target)
}

func IsRetryableError(err error) bool {
	var target RetryableError
	return errors.As(err, &target)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}
