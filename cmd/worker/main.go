package main

import (
	"context"
	"log"
	"time"

	"token-transfer-monitor/internal/broker"
	"token-transfer-monitor/internal/config"
	"token-transfer-monitor/internal/storage"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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

	log.Println("foundation OK: postgres connected with pgxpool, rabbitmq connected, topology declared")
}
