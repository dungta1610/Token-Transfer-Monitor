package main

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"token-transfer-monitor/internal/broker"
	"token-transfer-monitor/internal/config"
	"token-transfer-monitor/internal/contract"
)

func main() {
	ctx := context.Background()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

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

	deliveries, err := session.Channel.Consume(
		cfg.RabbitMQ.Topology.TransferDLQ,
		"transfer-dlq-consumer",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("consume dlq: %v", err)
	}

	log.Printf("dlq consumer started; queue=%s", cfg.RabbitMQ.Topology.TransferDLQ)

	for d := range deliveries {
		handleDLQMessage(ctx, d)
	}

	log.Println("dlq delivery channel closed, exiting")
}

func handleDLQMessage(_ context.Context, d amqp.Delivery) {
	retryCount, _ := broker.GetRetryCount(d.Headers)

	msg, err := contract.ParseTransferDetectedMessage(d.Body)
	if err != nil {
		log.Printf(
			"DLQ invalid body | retry_count=%d headers=%v body=%s parse_err=%v",
			retryCount,
			d.Headers,
			string(d.Body),
			err,
		)
	} else {
		log.Printf(
			"DLQ message | event_id=%s tx_hash=%s retry_count=%d headers=%v",
			msg.EventID,
			msg.TxHash,
			retryCount,
			d.Headers,
		)
	}

	if err := d.Ack(false); err != nil {
		log.Printf("ack dlq delivery failed: %v", err)
	}
}
