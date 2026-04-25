package main

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"token-transfer-monitor/internal/blockchain"
	"token-transfer-monitor/internal/broker"
	"token-transfer-monitor/internal/config"
	pgrepo "token-transfer-monitor/internal/repository/postgres"
	"token-transfer-monitor/internal/shutdown"
	"token-transfer-monitor/internal/storage"
)

const reconnectDelay = 3 * time.Second

func main() {
	ctx, stop := shutdown.NewSignalContext()
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	pool, err := storage.NewPostgresPool(ctx, cfg.Postgres)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	tokenRepo := pgrepo.NewTrackedTokenRepo(pool)

	trackedTokenAddresses, err := tokenRepo.ListActiveAddresses(ctx, cfg.Blockchain.ChainID)
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

	log.Printf(
		"listener supervisor started; chain_id=%d token_count=%d reconnect_delay=%s source=postgres_with_env_fallback",
		cfg.Blockchain.ChainID,
		len(trackedTokenAddresses),
		reconnectDelay,
	)

	for {
		if ctx.Err() != nil {
			log.Println("listener supervisor received shutdown signal")
			return
		}

		err := runListenerSession(ctx, *cfg, trackedTokenAddresses)
		if err == nil {
			log.Println("listener stopped cleanly")
			return
		}

		if ctx.Err() != nil {
			log.Printf("listener stopped because context was cancelled: %v", ctx.Err())
			return
		}

		log.Printf("listener session failed: %v", err)
		log.Printf("reconnecting listener in %s...", reconnectDelay)

		select {
		case <-ctx.Done():
			log.Println("shutdown while waiting to reconnect listener")
			return
		case <-time.After(reconnectDelay):
		}
	}
}

func runListenerSession(
	ctx context.Context,
	cfg config.Config,
	trackedTokenAddresses []string,
) error {
	if len(trackedTokenAddresses) == 0 {
		return fmt.Errorf("trackedTokenAddresses is empty")
	}

	evmClient, err := blockchain.NewEVMClient(
		ctx,
		cfg.Blockchain.EVMWSURL,
		cfg.Blockchain.EVMHTTPURL,
	)
	if err != nil {
		return err
	}
	defer evmClient.Close()

	rabbitConn, err := broker.NewConnection(cfg.RabbitMQ.AMQPURL)
	if err != nil {
		return err
	}
	defer rabbitConn.Close()

	session, err := rabbitConn.OpenSession()
	if err != nil {
		return err
	}
	defer session.Close()

	if err := broker.EnablePublisherConfirms(session.Channel); err != nil {
		return err
	}

	if err := broker.DeclareTopology(session.Channel, cfg); err != nil {
		return err
	}

	decoder := blockchain.NewTransferDecoder(cfg.Blockchain.ChainID)
	subscriber := blockchain.NewTransferSubscriber(evmClient, decoder)

	handler := func(ctx context.Context, event blockchain.ERC20TransferEvent) error {
		msg := event.ToMessage()

		body, err := msg.ToJSON()
		if err != nil {
			return err
		}

		headers := amqp.Table{
			broker.HeaderEventID:     msg.EventID,
			broker.HeaderEventType:   msg.EventType,
			broker.HeaderChainID:     msg.ChainID,
			broker.HeaderRetryCount:  0,
			broker.HeaderPublishedAt: time.Now().UTC().Format(time.RFC3339Nano),
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
			return err
		}

		log.Printf(
			"published transfer event to rabbitmq | event_id=%s tx_hash=%s exchange=%s routing_key=%s",
			msg.EventID,
			msg.TxHash,
			cfg.RabbitMQ.Topology.MainExchange,
			cfg.RabbitMQ.Topology.TransferRoutingKey,
		)

		return nil
	}

	return subscriber.Subscribe(
		ctx,
		trackedTokenAddresses,
		handler,
	)
}
