package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"token-transfer-monitor/internal/broker"
	"token-transfer-monitor/internal/config"
	"token-transfer-monitor/internal/contract"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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

	msg := contract.NewSampleTransferMessage()

	body, err := msg.ToJSON()
	if err != nil {
		log.Fatalf("marshal message: %v", err)
	}

	headers := amqp.Table{
		broker.HeaderEventID:     msg.EventID,
		broker.HeaderEventType:   msg.EventType,
		broker.HeaderChainID:     msg.ChainID,
		broker.HeaderRetryCount:  0,
		broker.HeaderPublishedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}

	err = broker.PublishJSON(
		ctx,
		session.Channel,
		cfg.RabbitMQ.Topology.MainExchange,
		cfg.RabbitMQ.Topology.TransferRoutingKey,
		body,
		headers,
		true,
	)
	if err != nil {
		log.Fatalf("publish transfer message: %v", err)
	}

	log.Printf(
		"published test message | exchange=%s routing_key=%s event_id=%s tx_hash=%s",
		cfg.RabbitMQ.Topology.MainExchange,
		cfg.RabbitMQ.Topology.TransferRoutingKey,
		msg.EventID,
		msg.TxHash,
	)
}
