package broker

import (
	"fmt"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"

	"token-transfer-monitor/internal/config"
)

func DeclareTopology(ch *amqp.Channel, cfg config.Config) error {
	if ch == nil {
		return fmt.Errorf("amqp channel is nil")
	}

	topology := cfg.RabbitMQ.Topology

	if err := declareExchanges(ch, topology); err != nil {
		return err
	}

	if err := declareTransferMainQueue(ch, cfg); err != nil {
		return err
	}

	if err := declareTransferRetryQueue(ch, cfg); err != nil {
		return err
	}

	if err := declareTransferDLQ(ch, cfg); err != nil {
		return err
	}

	return nil
}

func declareExchanges(ch *amqp.Channel, topology config.RabbitMQTopologyConfig) error {
	if err := ch.ExchangeDeclare(
		topology.MainExchange,
		topology.MainExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare main exchange: %w", err)
	}

	if err := ch.ExchangeDeclare(
		topology.RetryExchange,
		topology.RetryExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare retry exchange: %w", err)
	}

	if err := ch.ExchangeDeclare(
		topology.DLXExchange,
		topology.DLXExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare dlx exchange: %w", err)
	}

	return nil
}

func declareTransferMainQueue(ch *amqp.Channel, cfg config.Config) error {
	topology := cfg.RabbitMQ.Topology

	args := amqp.Table{
		"x-dead-letter-exchange":    topology.DLXExchange,
		"x-dead-letter-routing-key": topology.TransferDLQKey,
	}

	_, err := ch.QueueDeclare(
		topology.TransferQueue,
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		return fmt.Errorf("declare main transfer queue: %w", err)
	}

	if err := ch.QueueBind(
		topology.TransferQueue,
		topology.TransferRoutingKey,
		topology.MainExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind main transfer queue: %w", err)
	}

	return nil
}

func declareTransferRetryQueue(ch *amqp.Channel, cfg config.Config) error {
	topology := cfg.RabbitMQ.Topology

	args := amqp.Table{
		"x-message-ttl":             int32(cfg.Worker.RetryDelayMS),
		"x-dead-letter-exchange":    topology.MainExchange,
		"x-dead-letter-routing-key": topology.TransferRoutingKey,
		"x-queue-mode":              "default",
		"x-retry-delay-ms":          strconv.Itoa(cfg.Worker.RetryDelayMS),
	}

	_, err := ch.QueueDeclare(
		topology.TransferRetryQueue,
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		return fmt.Errorf("declare transfer retry queue: %w", err)
	}

	if err := ch.QueueBind(
		topology.TransferRetryQueue,
		topology.TransferRetryKey,
		topology.RetryExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind transfer retry queue: %w", err)
	}

	return nil
}

func declareTransferDLQ(ch *amqp.Channel, cfg config.Config) error {
	topology := cfg.RabbitMQ.Topology

	_, err := ch.QueueDeclare(
		topology.TransferDLQ,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("declare transfer dlq: %w", err)
	}

	if err := ch.QueueBind(
		topology.TransferDLQ,
		topology.TransferDLQKey,
		topology.DLXExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind transfer dlq: %w", err)
	}

	return nil
}
