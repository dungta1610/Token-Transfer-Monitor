package broker

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"token-transfer-monitor/internal/config"
)

func PublishToRetryQueue(
	ctx context.Context,
	ch *amqp.Channel,
	cfg config.Config,
	body []byte,
	headers amqp.Table,
	currentRetryCount int,
) error {
	newHeaders := SetRetryCount(headers, currentRetryCount+1)

	err := PublishJSONWithConfirm(
		ctx,
		ch,
		cfg.RabbitMQ.Topology.RetryExchange,
		cfg.RabbitMQ.Topology.TransferRetryKey,
		body,
		newHeaders,
		true,
	)
	if err != nil {
		return fmt.Errorf("publish retry message with confirm: %w", err)
	}

	return nil
}
