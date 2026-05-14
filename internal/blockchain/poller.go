package blockchain

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type TransferPoller struct {
	client  *ethclient.Client
	decoder *TransferDecoder
}

type PollTransfersParams struct {
	TokenAddresses []string
	FromBlock      uint64
	ToBlock        uint64
	ChunkSize      uint64
}

type TransferEventHandler func(ctx context.Context, event ERC20TransferEvent) error

func NewTransferPoller(client *ethclient.Client, decoder *TransferDecoder) *TransferPoller {
	return &TransferPoller{
		client:  client,
		decoder: decoder,
	}
}

func (p *TransferPoller) PollTransfers(
	ctx context.Context,
	params PollTransfersParams,
	handler TransferEventHandler,
) error {
	if p.client == nil {
		return fmt.Errorf("evm http client is nil")
	}

	if params.FromBlock > params.ToBlock {
		return fmt.Errorf("fromBlock must be <= toBlock")
	}

	if params.ChunkSize == 0 {
		params.ChunkSize = 500
	}

	addresses, err := parsePollerAddresses(params.TokenAddresses)
	if err != nil {
		return err
	}

	if len(addresses) == 0 {
		return fmt.Errorf("no token addresses configured")
	}

	log.Printf(
		"backfill started | from_block=%d to_block=%d chunk_size=%d token_count=%d",
		params.FromBlock,
		params.ToBlock,
		params.ChunkSize,
		len(addresses),
	)

	for start := params.FromBlock; start <= params.ToBlock; {
		end := start + params.ChunkSize - 1
		if end > params.ToBlock {
			end = params.ToBlock
		}

		if err := p.pollChunk(ctx, addresses, start, end, handler); err != nil {
			return err
		}

		if end == params.ToBlock {
			break
		}

		start = end + 1
	}

	log.Printf(
		"backfill completed | from_block=%d to_block=%d",
		params.FromBlock,
		params.ToBlock,
	)

	return nil
}

func (p *TransferPoller) pollChunk(
	ctx context.Context,
	addresses []common.Address,
	fromBlock uint64,
	toBlock uint64,
	handler TransferEventHandler,
) error {
	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Addresses: addresses,
		Topics: [][]common.Hash{
			{ERC20TransferTopic},
		},
	}

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	logs, err := p.client.FilterLogs(queryCtx, query)
	if err != nil {
		return fmt.Errorf("filter logs from block %d to %d: %w", fromBlock, toBlock, err)
	}

	log.Printf(
		"backfill chunk fetched | from_block=%d to_block=%d log_count=%d",
		fromBlock,
		toBlock,
		len(logs),
	)

	for _, vLog := range logs {
		if vLog.Removed {
			continue
		}

		event, err := p.decoder.Decode(vLog)
		if err != nil {
			log.Printf(
				"failed to decode transfer log | block=%d tx_hash=%s log_index=%d err=%v",
				vLog.BlockNumber,
				vLog.TxHash.Hex(),
				vLog.Index,
				err,
			)
			continue
		}

		handleCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = handler(handleCtx, event)
		cancel()

		if err != nil {
			return fmt.Errorf(
				"handle transfer event failed | event_id=%s tx_hash=%s log_index=%d: %w",
				event.EventID(),
				event.TxHash.Hex(),
				event.LogIndex,
				err,
			)
		}
	}

	return nil
}

func parsePollerAddresses(values []string) ([]common.Address, error) {
	addresses := make([]common.Address, 0, len(values))

	for _, value := range values {
		normalized := strings.TrimSpace(value)
		if normalized == "" {
			continue
		}

		if !common.IsHexAddress(normalized) {
			return nil, fmt.Errorf("invalid token address: %s", normalized)
		}

		addresses = append(addresses, common.HexToAddress(normalized))
	}

	return addresses, nil
}
