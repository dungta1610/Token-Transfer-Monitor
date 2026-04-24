package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"token-transfer-monitor/internal/broker"
	"token-transfer-monitor/internal/config"
	"token-transfer-monitor/internal/contract"
	"token-transfer-monitor/internal/service"
	"token-transfer-monitor/internal/shutdown"
	"token-transfer-monitor/internal/storage"
)

const reconnectDelay = 3 * time.Second

type workerSession struct {
	conn    *broker.Connection
	session *broker.Session
}

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

	processor := service.NewTransferProcessor(pool, cfg.Blockchain.WatchlistAddrs)

	log.Printf(
		"worker supervisor started; queue=%s consumer_tag=%s reconnect_delay=%s",
		cfg.RabbitMQ.Topology.TransferQueue,
		cfg.Worker.ConsumerTag,
		reconnectDelay,
	)

	for {
		if ctx.Err() != nil {
			log.Println("worker supervisor received shutdown signal")
			return
		}

		err := runConsumerSession(ctx, *cfg, processor)
		if err == nil {
			log.Println("consumer session stopped cleanly")
			return
		}

		if ctx.Err() != nil {
			log.Printf("consumer session stopped because context was cancelled: %v", ctx.Err())
			return
		}

		log.Printf("consumer session failed: %v", err)
		log.Printf("reconnecting in %s...", reconnectDelay)

		select {
		case <-ctx.Done():
			log.Println("shutdown while waiting to reconnect")
			return
		case <-time.After(reconnectDelay):
		}
	}
}

func runConsumerSession(
	ctx context.Context,
	cfg config.Config,
	processor *service.TransferProcessor,
) error {
	ws, err := openWorkerSession(cfg)
	if err != nil {
		return err
	}
	defer ws.close()

	connClosed := ws.session.Conn.NotifyClose(make(chan *amqp.Error, 1))
	chClosed := ws.session.Channel.NotifyClose(make(chan *amqp.Error, 1))
	consumerCancelled := ws.session.Channel.NotifyCancel(make(chan string, 1))

	if err := broker.EnablePublisherConfirms(ws.session.Channel); err != nil {
		return err
	}

	if err := broker.DeclareTopology(ws.session.Channel, cfg); err != nil {
		return err
	}

	if err := ws.session.Channel.Qos(cfg.Worker.PrefetchCount, 0, false); err != nil {
		return err
	}

	deliveries, err := ws.session.Channel.Consume(
		cfg.RabbitMQ.Topology.TransferQueue,
		cfg.Worker.ConsumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	sessionDone := make(chan error, 1)

	log.Printf(
		"consumer session started; queue=%s consumer_tag=%s max_retry=%d retry_delay_ms=%d publisher_confirms=true",
		cfg.RabbitMQ.Topology.TransferQueue,
		cfg.Worker.ConsumerTag,
		cfg.Worker.MaxRetryCount,
		cfg.Worker.RetryDelayMS,
	)

	go func() {
		for d := range deliveries {
			wg.Add(1)

			delivery := d
			go func() {
				defer wg.Done()
				handleDelivery(ctx, delivery, ws.session.Channel, cfg, processor)
			}()
		}

		wg.Wait()
		sessionDone <- nil
	}()

	select {
	case <-ctx.Done():
		log.Println("shutdown requested, cancelling consumer")

		cancelCtx, cancel := shutdown.WithTimeout(context.Background(), cfg.Shutdown.Timeout)
		defer cancel()

		cancelErr := ws.session.Channel.Cancel(cfg.Worker.ConsumerTag, false)
		if cancelErr != nil {
			log.Printf("cancel consumer failed: %v", cancelErr)
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			log.Println("in-flight deliveries drained")
			return nil
		case <-cancelCtx.Done():
			return errors.New("shutdown timeout while draining deliveries")
		}

	case err := <-connClosed:
		if err == nil {
			return errors.New("rabbitmq connection closed")
		}
		return err

	case err := <-chClosed:
		if err == nil {
			return errors.New("rabbitmq channel closed")
		}
		return err

	case tag := <-consumerCancelled:
		return errors.New("consumer cancelled by broker: " + tag)

	case err := <-sessionDone:
		return err
	}
}

func openWorkerSession(cfg config.Config) (*workerSession, error) {
	conn, err := broker.NewConnection(cfg.RabbitMQ.AMQPURL)
	if err != nil {
		return nil, err
	}

	session, err := conn.OpenSession()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	return &workerSession{
		conn:    conn,
		session: session,
	}, nil
}

func (s *workerSession) close() {
	if s == nil {
		return
	}

	if s.session != nil {
		_ = s.session.Close()
	}

	if s.conn != nil {
		_ = s.conn.Close()
	}
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
					"publish retry copy with confirm failed -> requeue original | event_id=%s tx_hash=%s retry_count=%d err=%v",
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
				"published to retry queue and confirmed by broker | event_id=%s tx_hash=%s current_retry=%d next_retry=%d",
				msg.EventID,
				msg.TxHash,
				retryCount,
				retryCount+1,
			)

			if ackErr := d.Ack(false); ackErr != nil {
				log.Printf("ack original after confirmed retry publish failed: %v", ackErr)
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
