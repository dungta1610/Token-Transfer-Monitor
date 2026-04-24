package blockchain

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type TransferHandler func(ctx context.Context, event ERC20TransferEvent) error

type TransferSubscriber struct {
	client  *EVMClient
	decoder *TransferDecoder
}

func NewTransferSubscriber(client *EVMClient, decoder *TransferDecoder) *TransferSubscriber {
	return &TransferSubscriber{
		client:  client,
		decoder: decoder,
	}
}

func (s *TransferSubscriber) Subscribe(
	ctx context.Context,
	tokenAddresses []string,
	handler TransferHandler,
) error {
	addresses, err := parseAddresses(tokenAddresses)
	if err != nil {
		return err
	}

	if len(addresses) == 0 {
		return fmt.Errorf("no tracked token addresses configured")
	}

	query := ethereum.FilterQuery{
		Addresses: addresses,
		Topics: [][]common.Hash{
			{ERC20TransferTopic},
		},
	}

	logs := make(chan types.Log, 128)

	sub, err := s.client.WS().SubscribeFilterLogs(ctx, query, logs)
	if err != nil {
		return fmt.Errorf("subscribe transfer logs: %w", err)
	}
	defer sub.Unsubscribe()

	log.Printf("subscribed ERC20 Transfer logs; token_count=%d", len(addresses))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-sub.Err():
			if err == nil {
				return fmt.Errorf("subscription closed")
			}
			return fmt.Errorf("subscription error: %w", err)

		case vLog := <-logs:
			event, err := s.decoder.Decode(vLog)
			if err != nil {
				log.Printf(
					"failed to decode transfer log | tx_hash=%s log_index=%d err=%v",
					vLog.TxHash.Hex(),
					vLog.Index,
					err,
				)
				continue
			}

			if event.Removed {
				log.Printf(
					"skip removed log | event_id=%s tx_hash=%s log_index=%d",
					event.EventID(),
					event.TxHash.Hex(),
					event.LogIndex,
				)
				continue
			}

			handleCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err = handler(handleCtx, event)
			cancel()

			if err != nil {
				log.Printf(
					"failed to handle transfer event | event_id=%s tx_hash=%s log_index=%d err=%v",
					event.EventID(),
					event.TxHash.Hex(),
					event.LogIndex,
					err,
				)
				continue
			}

			log.Printf(
				"handled transfer event | event_id=%s tx_hash=%s from=%s to=%s amount=%s",
				event.EventID(),
				event.TxHash.Hex(),
				event.From.Hex(),
				event.To.Hex(),
				event.Amount.String(),
			)
		}
	}
}

func parseAddresses(values []string) ([]common.Address, error) {
	addresses := make([]common.Address, 0, len(values))

	for _, value := range values {
		normalized := strings.TrimSpace(value)
		if normalized == "" {
			continue
		}

		if !common.IsHexAddress(normalized) {
			return nil, fmt.Errorf("invalid ethereum address: %s", normalized)
		}

		addresses = append(addresses, common.HexToAddress(normalized))
	}

	return addresses, nil
}
