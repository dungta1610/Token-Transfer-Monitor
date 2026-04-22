package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"token-transfer-monitor/internal/broker"
	"token-transfer-monitor/internal/config"
	"token-transfer-monitor/internal/contract"
	pgrepo "token-transfer-monitor/internal/repository/postgres"
	"token-transfer-monitor/internal/service"
	"token-transfer-monitor/internal/storage"
)

func main() {
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	pool, err := storage.NewPostgresPool(ctx, cfg.Postgres)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	conn, err := broker.NewConnection(cfg.RabbitMQ.AMQPURL)
	if err != nil {
		log.Fatalf("connect rabbitmq: %v", err)
	}
	defer conn.Close()

	session, err := conn.OpenSession()
	if err != nil {
		log.Fatalf("open rabbitmq session: %v", err)
	}
	defer session.Close()

	if err := broker.DeclareTopology(session.Channel, *cfg); err != nil {
		log.Fatalf("declare topology: %v", err)
	}

	if err := session.Channel.Qos(cfg.Worker.PrefetchCount, 0, false); err != nil {
		log.Fatalf("set qos: %v", err)
	}

	deliveries, err := session.Channel.Consume(
		cfg.RabbitMQ.Topology.TransferQueue,
		cfg.Worker.ConsumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("consume queue: %v", err)
	}

	processedRepo := pgrepo.NewProcessedEventRepo(pool)
	activityRepo := pgrepo.NewTransferActivityRepo(pool)
	processor := service.NewTransferProcessor(
		processedRepo,
		activityRepo,
		cfg.Blockchain.WatchlistAddrs,
	)

	log.Printf("worker started; queue=%s consumer_tag=%s", cfg.RabbitMQ.Topology.TransferQueue, cfg.Worker.ConsumerTag)

	for d := range deliveries {
		handleDelivery(ctx, d, processor)
	}

	log.Println("delivery channel closed, worker exiting")
}

func handleDelivery(
	parentCtx context.Context,
	d amqp.Delivery,
	processor *service.TransferProcessor,
) {
	processCtx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
	defer cancel()

	msg, err := contract.ParseTransferDetectedMessage(d.Body)
	if err != nil {
		log.Printf("invalid message body -> send to DLQ | err=%v body=%s", err, string(d.Body))
		if nackErr := d.Nack(false, false); nackErr != nil {
			log.Printf("nack delivery failed: %v", nackErr)
		}
		return
	}

	result, err := processor.Process(processCtx, msg)
	if err != nil {
		switch {
		case service.IsPermanentError(err):
			log.Printf(
				"permanent processing error -> send to DLQ | event_id=%s tx_hash=%s err=%v",
				msg.EventID,
				msg.TxHash,
				err,
			)
			if nackErr := d.Nack(false, false); nackErr != nil {
				log.Printf("nack delivery failed: %v", nackErr)
			}
			return

		case service.IsRetryableError(err):
			log.Printf(
				"retryable processing error -> currently send to DLQ in phase-1 | event_id=%s tx_hash=%s err=%v",
				msg.EventID,
				msg.TxHash,
				err,
			)
			if nackErr := d.Nack(false, false); nackErr != nil {
				log.Printf("nack delivery failed: %v", nackErr)
			}
			return

		default:
			log.Printf(
				"unexpected processing error -> send to DLQ | event_id=%s tx_hash=%s err=%v",
				msg.EventID,
				msg.TxHash,
				err,
			)
			if nackErr := d.Nack(false, false); nackErr != nil {
				log.Printf("nack delivery failed: %v", nackErr)
			}
			return
		}
	}

	switch result.Result {
	case service.ProcessResultSuccess:
		log.Printf(
			"processed successfully | event_id=%s tx_hash=%s result=%s",
			msg.EventID,
			msg.TxHash,
			result.Result,
		)
	case service.ProcessResultDuplicate:
		log.Printf(
			"duplicate detected, ack without side effect | event_id=%s tx_hash=%s result=%s",
			msg.EventID,
			msg.TxHash,
			result.Result,
		)
	default:
		log.Printf(
			"processed with status | event_id=%s tx_hash=%s result=%s",
			msg.EventID,
			msg.TxHash,
			result.Result,
		)
	}

	if err := d.Ack(false); err != nil {
		log.Printf("ack delivery failed: %v", err)
	}
}
