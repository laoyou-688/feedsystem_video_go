package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SocialWorker struct {
	ch    *amqp.Channel
	cache *rediscache.Client
	queue string
}

func NewSocialWorker(ch *amqp.Channel, cache *rediscache.Client, queue string) *SocialWorker {
	return &SocialWorker{ch: ch, cache: cache, queue: queue}
}

func (w *SocialWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil {
		return errors.New("social worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}

	deliveries, err := w.ch.Consume(
		w.queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("deliveries channel closed")
			}
			w.handleDelivery(ctx, d)
		}
	}
}

func (w *SocialWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("social worker: failed to process message: %v", err)
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}

func (w *SocialWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.SocialEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil
	}
	if evt.FollowerID == 0 || evt.VloggerID == 0 {
		return nil
	}
	claimed, err := claimEvent(ctx, w.cache, "social", evt.EventID)
	if err == nil && !claimed {
		return nil
	}

	switch evt.Action {
	case "follow", "unfollow":
	default:
		return nil
	}

	if w.cache == nil {
		return nil
	}

	opCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()
	return w.cache.Del(opCtx, fmt.Sprintf("feed:following:timeline:%d", evt.FollowerID))
}
