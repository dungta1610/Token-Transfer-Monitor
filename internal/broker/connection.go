package broker

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Connection struct {
	conn *amqp.Connection
}

type Session struct {
	Conn    *amqp.Connection
	Channel *amqp.Channel
}

func NewConnection(amqpURL string) (*Connection, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	return &Connection{conn: conn}, nil
}

func (c *Connection) Raw() *amqp.Connection {
	return c.conn
}

func (c *Connection) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Connection) OpenSession() (*Session, error) {
	if c == nil || c.conn == nil {
		return nil, fmt.Errorf("rabbitmq connection is nil")
	}

	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}

	return &Session{
		Conn:    c.conn,
		Channel: ch,
	}, nil
}

func (s *Session) Close() error {
	if s == nil || s.Channel == nil {
		return nil
	}
	return s.Channel.Close()
}

func EnablePublisherConfirms(ch *amqp.Channel) error {
	if ch == nil {
		return fmt.Errorf("amqp channel is nil")
	}

	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable publisher confirms: %w", err)
	}

	return nil
}

func PublishJSON(
	ctx context.Context,
	ch *amqp.Channel,
	exchange string,
	routingKey string,
	body []byte,
	headers amqp.Table,
	persistent bool,
) error {
	if ch == nil {
		return fmt.Errorf("amqp channel is nil")
	}

	deliveryMode := uint8(amqp.Transient)
	if persistent {
		deliveryMode = amqp.Persistent
	}

	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := ch.PublishWithContext(
		pubCtx,
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			Headers:      headers,
			DeliveryMode: deliveryMode,
			Timestamp:    time.Now().UTC(),
		},
	)
	if err != nil {
		return fmt.Errorf("publish message: %w", err)
	}

	return nil
}

func PublishJSONWithConfirm(
	ctx context.Context,
	ch *amqp.Channel,
	exchange string,
	routingKey string,
	body []byte,
	headers amqp.Table,
	persistent bool,
) error {
	if ch == nil {
		return fmt.Errorf("amqp channel is nil")
	}

	deliveryMode := uint8(amqp.Transient)
	if persistent {
		deliveryMode = amqp.Persistent
	}

	pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		pubCtx,
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			Headers:      headers,
			DeliveryMode: deliveryMode,
			Timestamp:    time.Now().UTC(),
		},
	)
	if err != nil {
		return fmt.Errorf("publish message with confirm: %w", err)
	}

	if confirm == nil {
		return fmt.Errorf("publisher confirm is nil; confirm mode may not be enabled")
	}

	ok, err := confirm.WaitContext(pubCtx)
	if err != nil {
		return fmt.Errorf("wait publisher confirm: %w", err)
	}
	if !ok {
		return fmt.Errorf("publisher confirm was not acknowledged by broker")
	}

	return nil
}
