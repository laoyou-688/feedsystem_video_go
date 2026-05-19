package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const likeMaxRetryCount = 3

type LikeWorker struct {
	ch           *amqp.Channel
	cache        *rediscache.Client
	likes        *video.LikeRepository
	videos       *video.VideoRepository
	queue        string
	localCacheMQ *rabbitmq.LocalCacheMQ
}

func NewLikeWorker(ch *amqp.Channel, cache *rediscache.Client, likes *video.LikeRepository, videos *video.VideoRepository, queue string, localCacheMQ *rabbitmq.LocalCacheMQ) *LikeWorker {
	return &LikeWorker{ch: ch, cache: cache, likes: likes, videos: videos, queue: queue, localCacheMQ: localCacheMQ}
}

func (w *LikeWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.likes == nil || w.videos == nil {
		return errors.New("like worker is not initialized")
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

func (w *LikeWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		w.handleFailure(ctx, d, err)
		return
	}
	_ = d.Ack(false)
}

func (w *LikeWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.LikeEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil
	}
	if evt.UserID == 0 || evt.VideoID == 0 {
		return nil
	}

	done, err := hasProcessedEvent(ctx, w.cache, "like", evt.EventID)
	if err == nil && done {
		return nil
	}
	if err != nil {
		return err
	}

	switch evt.Action {
	case "like":
		if err := w.applyLike(ctx, evt.UserID, evt.VideoID); err != nil {
			return err
		}
	case "unlike":
		if err := w.applyUnlike(ctx, evt.UserID, evt.VideoID); err != nil {
			return err
		}
	default:
		return nil
	}

	return markEventDone(ctx, w.cache, "like", evt.EventID)
}

func (w *LikeWorker) applyLike(ctx context.Context, userID, videoID uint) error {
	ok, err := w.videos.IsExist(ctx, videoID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	created, err := w.likes.LikeIgnoreDuplicate(ctx, &video.Like{
		VideoID:   videoID,
		AccountID: userID,
		CreatedAt: time.Now(),
	})
	if err != nil {
		return err
	}
	if !created {
		return nil
	}

	if err := w.videos.ChangeLikesCount(ctx, videoID, 1); err != nil {
		return err
	}
	if err := w.videos.ChangePopularity(ctx, videoID, 1); err != nil {
		return err
	}
	video.PublishVideoCacheInvalidation(ctx, w.localCacheMQ, videoID)
	return nil
}

func (w *LikeWorker) applyUnlike(ctx context.Context, userID, videoID uint) error {
	ok, err := w.videos.IsExist(ctx, videoID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	deleted, err := w.likes.DeleteByVideoAndAccount(ctx, videoID, userID)
	if err != nil {
		return err
	}
	if !deleted {
		return nil
	}

	if err := w.videos.ChangeLikesCount(ctx, videoID, -1); err != nil {
		return err
	}
	if err := w.videos.ChangePopularity(ctx, videoID, -1); err != nil {
		return err
	}
	video.PublishVideoCacheInvalidation(ctx, w.localCacheMQ, videoID)
	return nil
}

func (w *LikeWorker) handleFailure(ctx context.Context, d amqp.Delivery, err error) {
	retryCount := readRetryCount(d.Headers)
	routingKey := d.RoutingKey
	if routingKey == "" {
		var evt rabbitmq.LikeEvent
		if jsonErr := json.Unmarshal(d.Body, &evt); jsonErr == nil {
			if derived, routeErr := rabbitmq.LikeRoutingKey(evt.Action); routeErr == nil {
				routingKey = derived
			}
		}
	}

	headers := copyHeaders(d.Headers)
	headers["x-retry-count"] = retryCount + 1
	headers["x-last-error"] = err.Error()
	headers["x-last-failed-at"] = time.Now().UTC().Format(time.RFC3339)

	publishCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if retryCount >= likeMaxRetryCount {
		if dlqErr := rabbitmq.PublishLikeDLQ(publishCtx, w.ch, routingKey, d.Body, headers); dlqErr != nil {
			log.Printf("like worker: failed to move message to dlq: event_routing=%s retry_count=%d err=%v original_err=%v", routingKey, retryCount, dlqErr, err)
			_ = d.Nack(false, true)
			return
		}
		log.Printf("like worker: moved message to dlq: routing=%s retry_count=%d err=%v", routingKey, retryCount, err)
		_ = d.Ack(false)
		return
	}

	if retryErr := rabbitmq.PublishLikeRetry(publishCtx, w.ch, routingKey, d.Body, headers); retryErr != nil {
		log.Printf("like worker: failed to publish retry message: routing=%s retry_count=%d err=%v original_err=%v", routingKey, retryCount, retryErr, err)
		_ = d.Nack(false, true)
		return
	}

	log.Printf("like worker: scheduled retry: routing=%s retry_count=%d err=%v", routingKey, retryCount+1, err)
	_ = d.Ack(false)
}
