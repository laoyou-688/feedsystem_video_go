package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type CommentMQ struct {
	*RabbitMQ
}

const (
	commentExchange            = "comment.events"
	commentQueue               = "comment.events"
	commentBindingKey          = "comment.*"
	commentRetryExchange       = "comment.events.retry"
	commentRetryQueue          = "comment.events.retry"
	commentDLX                 = "comment.events.dlx"
	commentDLQ                 = "comment.events.dlq"
	commentRetryTTLMS    int32 = 5000

	commentPublishRK = "comment.publish"
	commentDeleteRK  = "comment.delete"
)

type CommentEvent struct {
	EventID     string    `json:"event_id"`
	Action      string    `json:"action"`
	CommentID   uint      `json:"comment_id,omitempty"`
	ClientToken string    `json:"client_token,omitempty"`
	Username    string    `json:"username,omitempty"`
	VideoID     uint      `json:"video_id,omitempty"`
	AuthorID    uint      `json:"author_id,omitempty"`
	Content     string    `json:"content,omitempty"`
	OccurredAt  time.Time `json:"occurred_at"`
}

func NewCommentMQ(base *RabbitMQ) (*CommentMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := DeclareCommentTopology(base.Ch); err != nil {
		return nil, err
	}
	return &CommentMQ{RabbitMQ: base}, nil
}

func (c *CommentMQ) Publish(ctx context.Context, username string, videoID, authorID uint, content string, clientToken string) error {
	return c.publish(ctx, "publish", commentPublishRK, CommentEvent{
		ClientToken: clientToken,
		Username:    username,
		VideoID:     videoID,
		AuthorID:    authorID,
		Content:     content,
	})
}

func (c *CommentMQ) Delete(ctx context.Context, commentID uint) error {
	return c.publish(ctx, "delete", commentDeleteRK, CommentEvent{
		CommentID: commentID,
	})
}

func (c *CommentMQ) publish(ctx context.Context, action, routingKey string, evt CommentEvent) error {
	if c == nil || c.RabbitMQ == nil {
		return errors.New("comment mq is not initialized")
	}
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	evt.EventID = id
	evt.Action = action
	evt.OccurredAt = time.Now().UTC()
	return c.PublishJSON(ctx, commentExchange, routingKey, evt)
}

func DeclareCommentTopology(ch *amqp.Channel) error {
	if ch == nil {
		return errors.New("rabbitmq channel is nil")
	}

	if err := ch.ExchangeDeclare(
		commentExchange,
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
		commentQueue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	if err := ch.QueueBind(
		commentQueue,
		commentBindingKey,
		commentExchange,
		false,
		nil,
	); err != nil {
		return err
	}

	if err := ch.ExchangeDeclare(
		commentRetryExchange,
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
		commentRetryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl":          commentRetryTTLMS,
			"x-dead-letter-exchange": commentExchange,
		},
	); err != nil {
		return err
	}

	if err := ch.QueueBind(
		commentRetryQueue,
		commentBindingKey,
		commentRetryExchange,
		false,
		nil,
	); err != nil {
		return err
	}

	if err := ch.ExchangeDeclare(
		commentDLX,
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
		commentDLQ,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	return ch.QueueBind(
		commentDLQ,
		commentBindingKey,
		commentDLX,
		false,
		nil,
	)
}

func CommentRoutingKey(action string) (string, error) {
	switch action {
	case "publish":
		return commentPublishRK, nil
	case "delete":
		return commentDeleteRK, nil
	default:
		return "", errors.New("unknown comment action")
	}
}

func PublishCommentRetry(ctx context.Context, ch *amqp.Channel, routingKey string, body []byte, headers amqp.Table) error {
	return publishCommentWithHeaders(ctx, ch, commentRetryExchange, routingKey, body, headers)
}

func PublishCommentDLQ(ctx context.Context, ch *amqp.Channel, routingKey string, body []byte, headers amqp.Table) error {
	return publishCommentWithHeaders(ctx, ch, commentDLX, routingKey, body, headers)
}

func publishCommentWithHeaders(ctx context.Context, ch *amqp.Channel, exchange, routingKey string, body []byte, headers amqp.Table) error {
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
