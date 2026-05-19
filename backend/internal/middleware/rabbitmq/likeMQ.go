package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type LikeMQ struct {
	*RabbitMQ
}

const (
	likeExchange            = "like.events"
	likeQueue               = "like.events"
	likeBindingKey          = "like.*"
	likeRetryExchange       = "like.events.retry"
	likeRetryQueue          = "like.events.retry"
	likeDLX                 = "like.events.dlx"
	likeDLQ                 = "like.events.dlq"
	likeRetryTTLMS    int32 = 5000

	likeLikeRK   = "like.like"
	likeUnlikeRK = "like.unlike"
)

type LikeEvent struct {
	EventID    string    `json:"event_id"`
	Action     string    `json:"action"`
	UserID     uint      `json:"user_id"`
	VideoID    uint      `json:"video_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewLikeMQ(base *RabbitMQ) (*LikeMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := DeclareLikeTopology(base.Ch); err != nil {
		return nil, err
	}
	return &LikeMQ{RabbitMQ: base}, nil
}

func (l *LikeMQ) Like(ctx context.Context, userID, videoID uint) error {
	return l.publish(ctx, "like", likeLikeRK, userID, videoID)
}

func (l *LikeMQ) Unlike(ctx context.Context, userID, videoID uint) error {
	return l.publish(ctx, "unlike", likeUnlikeRK, userID, videoID)
}

func (l *LikeMQ) publish(ctx context.Context, action, routingKey string, userID, videoID uint) error {
	if l == nil || l.RabbitMQ == nil {
		return errors.New("like mq is not initialized")
	}
	if userID == 0 || videoID == 0 {
		return errors.New("userID and videoID are required")
	}
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	event := LikeEvent{
		EventID:    id,
		Action:     action,
		UserID:     userID,
		VideoID:    videoID,
		OccurredAt: time.Now(),
	}
	return l.PublishJSON(ctx, likeExchange, routingKey, event)
}

func DeclareLikeTopology(ch *amqp.Channel) error {
	if ch == nil {
		return errors.New("rabbitmq channel is nil")
	}

	if err := ch.ExchangeDeclare(
		likeExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	if _, err := ch.QueueDeclare(
		likeQueue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	if err := ch.QueueBind(
		likeQueue,
		likeBindingKey,
		likeExchange,
		false,
		nil,
	); err != nil {
		return err
	}

	if err := ch.ExchangeDeclare(
		likeRetryExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	if _, err := ch.QueueDeclare(
		likeRetryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl":          likeRetryTTLMS,
			"x-dead-letter-exchange": likeExchange,
		},
	); err != nil {
		return err
	}

	if err := ch.QueueBind(
		likeRetryQueue,
		likeBindingKey,
		likeRetryExchange,
		false,
		nil,
	); err != nil {
		return err
	}

	if err := ch.ExchangeDeclare(
		likeDLX,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	if _, err := ch.QueueDeclare(
		likeDLQ,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	return ch.QueueBind(
		likeDLQ,
		likeBindingKey,
		likeDLX,
		false,
		nil,
	)
}

func LikeRoutingKey(action string) (string, error) {
	switch action {
	case "like":
		return likeLikeRK, nil
	case "unlike":
		return likeUnlikeRK, nil
	default:
		return "", errors.New("unknown like action")
	}
}

func PublishLikeRetry(ctx context.Context, ch *amqp.Channel, routingKey string, body []byte, headers amqp.Table) error {
	return publishLikeWithHeaders(ctx, ch, likeRetryExchange, routingKey, body, headers)
}

func PublishLikeDLQ(ctx context.Context, ch *amqp.Channel, routingKey string, body []byte, headers amqp.Table) error {
	return publishLikeWithHeaders(ctx, ch, likeDLX, routingKey, body, headers)
}

func publishLikeWithHeaders(ctx context.Context, ch *amqp.Channel, exchange, routingKey string, body []byte, headers amqp.Table) error {
	if ch == nil {
		return errors.New("rabbitmq channel is nil")
	}
	if exchange == "" || routingKey == "" {
		return errors.New("exchange and routingKey are required")
	}
	return ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now().UTC(),
		Headers:      headers,
		Body:         body,
	})
}
