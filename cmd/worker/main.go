package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"token-transfer-monitor/internal/broker"
	"token-transfer-monitor/internal/config"
	"token-transfer-monitor/internal/contract"
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

	processor := service.NewTransferProcessor(pool, cfg.Blockchain.WatchlistAddrs)

	log.Printf(
		"worker started; queue=%s consumer_tag=%s max_retry=%d retry_delay_ms=%d",
		cfg.RabbitMQ.Topology.TransferQueue,
		cfg.Worker.ConsumerTag,
		cfg.Worker.MaxRetryCount,
		cfg.Worker.RetryDelayMS,
	)

	for d := range deliveries {
		handleDelivery(ctx, d, session.Channel, *cfg, processor)
	}

	log.Println("delivery channel closed, worker exiting")
}

func handleDelivery(
	parentCtx context.Context,
	d amqp.Delivery,
	ch *amqp.Channel,
	cfg config.Config,
	processor *service.TransferProcessor,
) {
	processCtx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
	defer cancel()

	retryCount, err := broker.GetRetryCount(d.Headers)
	if err != nil {
		log.Printf("invalid retry header -> send to DLQ | headers=%v err=%v", d.Headers, err)
		if nackErr := d.Nack(false, false); nackErr != nil {
			log.Printf("nack delivery failed: %v", nackErr)
		}
		return
	}

	msg, err := contract.ParseTransferDetectedMessage(d.Body)
	if err != nil {
		log.Printf(
			"invalid message body -> send to DLQ | retry_count=%d err=%v body=%s",
			retryCount,
			err,
			string(d.Body),
		)
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
				"permanent processing error -> send to DLQ | event_id=%s tx_hash=%s retry_count=%d err=%v",
				msg.EventID,
				msg.TxHash,
				retryCount,
				err,
			)
			if nackErr := d.Nack(false, false); nackErr != nil {
				log.Printf("nack delivery failed: %v", nackErr)
			}
			return

		case service.IsRetryableError(err):
			if retryCount >= cfg.Worker.MaxRetryCount {
				log.Printf(
					"retry limit exceeded -> send to DLQ | event_id=%s tx_hash=%s retry_count=%d max_retry=%d err=%v",
					msg.EventID,
					msg.TxHash,
					retryCount,
					cfg.Worker.MaxRetryCount,
					err,
				)
				if nackErr := d.Nack(false, false); nackErr != nil {
					log.Printf("nack delivery failed: %v", nackErr)
				}
				return
			}

			if pubErr := broker.PublishToRetryQueue(
				processCtx,
				ch,
				cfg,
				d.Body,
				d.Headers,
				retryCount,
			); pubErr != nil {
				log.Printf(
					"publish retry copy failed -> requeue original | event_id=%s tx_hash=%s retry_count=%d err=%v",
					msg.EventID,
					msg.TxHash,
					retryCount,
					pubErr,
				)
				if nackErr := d.Nack(false, true); nackErr != nil {
					log.Printf("nack requeue original failed: %v", nackErr)
				}
				return
			}

			log.Printf(
				"published to retry queue | event_id=%s tx_hash=%s current_retry=%d next_retry=%d",
				msg.EventID,
				msg.TxHash,
				retryCount,
				retryCount+1,
			)

			if ackErr := d.Ack(false); ackErr != nil {
				log.Printf("ack original after retry publish failed: %v", ackErr)
			}
			return

		default:
			log.Printf(
				"unexpected processing error -> send to DLQ | event_id=%s tx_hash=%s retry_count=%d err=%v",
				msg.EventID,
				msg.TxHash,
				retryCount,
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
			"processed successfully | event_id=%s tx_hash=%s retry_count=%d result=%s",
			msg.EventID,
			msg.TxHash,
			retryCount,
			result.Result,
		)
	case service.ProcessResultDuplicate:
		log.Printf(
			"duplicate detected, ack without side effect | event_id=%s tx_hash=%s retry_count=%d result=%s",
			msg.EventID,
			msg.TxHash,
			retryCount,
			result.Result,
		)
	default:
		log.Printf(
			"processed with status | event_id=%s tx_hash=%s retry_count=%d result=%s",
			msg.EventID,
			msg.TxHash,
			retryCount,
			result.Result,
		)
	}

	if err := d.Ack(false); err != nil {
		log.Printf("ack delivery failed: %v", err)
	}
}
