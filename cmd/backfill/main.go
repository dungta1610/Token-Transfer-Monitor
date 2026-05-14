package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	amqp "github.com/rabbitmq/amqp091-go"

	"token-transfer-monitor/internal/blockchain"
	"token-transfer-monitor/internal/broker"
	"token-transfer-monitor/internal/config"
	pgrepo "token-transfer-monitor/internal/repository/postgres"
	"token-transfer-monitor/internal/shutdown"
	"token-transfer-monitor/internal/storage"
)

func main() {
	var (
		fromBlock int64
		toBlock   int64
		chunkSize uint64
	)

	flag.Int64Var(&fromBlock, "from", -1, "start block number")
	flag.Int64Var(&toBlock, "to", -1, "end block number; use -1 for latest block")
	flag.Uint64Var(&chunkSize, "chunk-size", 500, "number of blocks per RPC FilterLogs request")
	flag.Parse()

	ctx, stop := shutdown.NewSignalContext()
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if fromBlock < 0 {
		log.Fatalf("--from is required and must be >= 0")
	}

	pool, err := storage.NewPostgresPool(ctx, cfg.Postgres)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	trackedTokenRepo := pgrepo.NewTrackedTokenRepo(pool)

	trackedTokenAddresses, err := trackedTokenRepo.ListActiveAddresses(ctx, cfg.Blockchain.ChainID)
	if err != nil {
		log.Fatalf("load tracked tokens from postgres: %v", err)
	}

	if len(trackedTokenAddresses) == 0 {
		trackedTokenAddresses = cfg.Blockchain.TrackedTokenAddrs
		log.Printf(
			"no active tracked tokens found in postgres, fallback to env; token_count=%d",
			len(trackedTokenAddresses),
		)
	}

	if len(trackedTokenAddresses) == 0 {
		log.Fatalf("no tracked token addresses configured from postgres or env")
	}

	if cfg.Blockchain.EVMHTTPURL == "" {
		log.Fatalf("EVM_HTTP_URL is required for backfill")
	}

	httpClient, err := ethclient.DialContext(ctx, cfg.Blockchain.EVMHTTPURL)
	if err != nil {
		log.Fatalf("connect evm http rpc: %v", err)
	}
	defer httpClient.Close()

	resolvedToBlock, err := resolveToBlock(ctx, httpClient, toBlock)
	if err != nil {
		log.Fatalf("resolve to block: %v", err)
	}

	if uint64(fromBlock) > resolvedToBlock {
		log.Fatalf("--from must be <= resolved toBlock; from=%d to=%d", fromBlock, resolvedToBlock)
	}

	rabbitConn, err := broker.NewConnection(cfg.RabbitMQ.AMQPURL)
	if err != nil {
		log.Fatalf("connect rabbitmq: %v", err)
	}
	defer rabbitConn.Close()

	session, err := rabbitConn.OpenSession()
	if err != nil {
		log.Fatalf("open rabbitmq session: %v", err)
	}
	defer session.Close()

	if err := broker.EnablePublisherConfirms(session.Channel); err != nil {
		log.Fatalf("enable publisher confirms: %v", err)
	}

	if err := broker.DeclareTopology(session.Channel, *cfg); err != nil {
		log.Fatalf("declare topology: %v", err)
	}

	decoder := blockchain.NewTransferDecoder(cfg.Blockchain.ChainID)
	poller := blockchain.NewTransferPoller(httpClient, decoder)

	handler := func(ctx context.Context, event blockchain.ERC20TransferEvent) error {
		msg := event.ToMessage()

		body, err := msg.ToJSON()
		if err != nil {
			return fmt.Errorf("marshal transfer message: %w", err)
		}

		headers := amqp.Table{
			broker.HeaderEventID:     msg.EventID,
			broker.HeaderEventType:   msg.EventType,
			broker.HeaderChainID:     msg.ChainID,
			broker.HeaderRetryCount:  0,
			broker.HeaderPublishedAt: time.Now().UTC().Format(time.RFC3339Nano),
			"x-source":               "backfill",
			"x-backfill-from-block":  uint64(fromBlock),
			"x-backfill-to-block":    resolvedToBlock,
		}

		err = broker.PublishJSONWithConfirm(
			ctx,
			session.Channel,
			cfg.RabbitMQ.Topology.MainExchange,
			cfg.RabbitMQ.Topology.TransferRoutingKey,
			body,
			headers,
			true,
		)
		if err != nil {
			return fmt.Errorf("publish backfilled transfer event: %w", err)
		}

		log.Printf(
			"published backfilled transfer | event_id=%s block=%d tx_hash=%s log_index=%d",
			msg.EventID,
			msg.BlockNumber,
			msg.TxHash,
			msg.LogIndex,
		)

		return nil
	}

	err = poller.PollTransfers(ctx, blockchain.PollTransfersParams{
		TokenAddresses: trackedTokenAddresses,
		FromBlock:      uint64(fromBlock),
		ToBlock:        resolvedToBlock,
		ChunkSize:      chunkSize,
	}, handler)
	if err != nil {
		log.Fatalf("backfill failed: %v", err)
	}

	log.Println("backfill command completed successfully")
}

func resolveToBlock(ctx context.Context, client *ethclient.Client, toBlock int64) (uint64, error) {
	if toBlock >= 0 {
		return uint64(toBlock), nil
	}

	blockCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	latestBlock, err := client.BlockNumber(blockCtx)
	if err != nil {
		return 0, fmt.Errorf("get latest block number: %w", err)
	}

	return latestBlock, nil
}
