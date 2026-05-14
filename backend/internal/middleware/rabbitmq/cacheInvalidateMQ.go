package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type LocalCacheMQ struct {
	*RabbitMQ
}

const cacheInvalidateExchange = "cache.invalidate.events"

type LocalCacheInvalidateEvent struct {
	EventID    string    `json:"event_id"`
	Namespace  string    `json:"namespace"`
	Key        string    `json:"key"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewLocalCacheMQ(base *RabbitMQ) (*LocalCacheMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := base.DeclareFanout(cacheInvalidateExchange); err != nil {
		return nil, err
	}
	return &LocalCacheMQ{RabbitMQ: base}, nil
}

func (m *LocalCacheMQ) PublishInvalidate(ctx context.Context, namespace, key string) error {
	if m == nil || m.RabbitMQ == nil {
		return errors.New("local cache mq is not initialized")
	}
	if namespace == "" || key == "" {
		return errors.New("namespace and key are required")
	}
	eventID, err := newEventID(16)
	if err != nil {
		return err
	}
	return m.PublishJSON(ctx, cacheInvalidateExchange, "", LocalCacheInvalidateEvent{
		EventID:    eventID,
		Namespace:  namespace,
		Key:        key,
		OccurredAt: time.Now().UTC(),
	})
}

func (m *LocalCacheMQ) Consume(queueName string) (<-chan amqp.Delivery, error) {
	if m == nil || m.Ch == nil {
		return nil, errors.New("local cache mq is not initialized")
	}

	autoQueue := queueName == ""
	q, err := m.Ch.QueueDeclare(
		queueName,
		false,
		autoQueue,
		autoQueue,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	if err := m.Ch.QueueBind(q.Name, "", cacheInvalidateExchange, false, nil); err != nil {
		return nil, err
	}
	return m.Ch.Consume(q.Name, "", false, false, false, false, nil)
}
