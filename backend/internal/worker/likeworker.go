package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

type LikeWorker struct {
	ch     *amqp.Channel
	cache  *rediscache.Client
	likes  *video.LikeRepository
	videos *video.VideoRepository
	queue  string
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
		log.Printf("like worker: failed to process message: %v", err)
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}

func (w *LikeWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.LikeEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		// 解析事件失败，直接丢弃
		return nil
	}
	if evt.UserID == 0 || evt.VideoID == 0 {
		return nil
	}
	claimed, err := claimEvent(ctx, w.cache, "like", evt.EventID)
	if err == nil && !claimed {
		return nil
	}

	switch evt.Action {
	case "like":
		return w.applyLike(ctx, evt.UserID, evt.VideoID)
	case "unlike":
		return w.applyUnlike(ctx, evt.UserID, evt.VideoID)
	default:
		return nil
	}
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
