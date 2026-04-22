package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"token-transfer-monitor/internal/contract"
	pgrepo "token-transfer-monitor/internal/repository/postgres"
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
	processedRepo *pgrepo.ProcessedEventRepo
	activityRepo  *pgrepo.TransferActivityRepo
	watchlist     map[string]struct{}
}

func NewTransferProcessor(
	processedRepo *pgrepo.ProcessedEventRepo,
	activityRepo *pgrepo.TransferActivityRepo,
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
		processedRepo: processedRepo,
		activityRepo:  activityRepo,
		watchlist:     watchlist,
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

	exists, err := p.processedRepo.Exists(ctx, msg.EventID)
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

	if err := p.activityRepo.Insert(ctx, activity); err != nil {
		return ProcessOutput{
			Result:  ProcessResultRetryableError,
			Message: "failed to insert transfer activity",
		}, RetryableError{Err: err}
	}

	processed := pgrepo.ProcessedEventRecord{
		EventID:         msg.EventID,
		ChainID:         msg.ChainID,
		TxHash:          msg.TxHash,
		LogIndex:        msg.LogIndex,
		ContractAddress: msg.ContractAddress,
		ProcessedAt:     time.Now().UTC(),
	}

	if err := p.processedRepo.Insert(ctx, processed); err != nil {
		return ProcessOutput{
			Result:  ProcessResultRetryableError,
			Message: "failed to insert processed event",
		}, RetryableError{Err: err}
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
