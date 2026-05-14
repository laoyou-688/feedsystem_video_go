package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"
	"log"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
)

type CommentWorker struct {
	ch       *amqp.Channel
	cache    *rediscache.Client
	comments *video.CommentRepository
	videos   *video.VideoRepository
	queue    string
	localCacheMQ *rabbitmq.LocalCacheMQ
}

func NewCommentWorker(ch *amqp.Channel, cache *rediscache.Client, comments *video.CommentRepository, videos *video.VideoRepository, queue string, localCacheMQ *rabbitmq.LocalCacheMQ) *CommentWorker {
	return &CommentWorker{ch: ch, cache: cache, comments: comments, videos: videos, queue: queue, localCacheMQ: localCacheMQ}
}

func (w *CommentWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.comments == nil || w.videos == nil {
		return errors.New("comment worker is not initialized")
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

func (w *CommentWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("comment worker: failed to process message: %v", err)
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}

func (w *CommentWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.CommentEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil
	}
	claimed, err := claimEvent(ctx, w.cache, "comment", evt.EventID)
	if err == nil && !claimed {
		return nil
	}
	switch evt.Action {
	case "publish":
		return w.applyPublish(ctx, &evt)
	case "delete":
		return w.applyDelete(ctx, &evt)
	default:
		return nil
	}
}

func (w *CommentWorker) applyPublish(ctx context.Context, evt *rabbitmq.CommentEvent) error {
	if evt == nil || evt.VideoID == 0 || evt.AuthorID == 0 || strings.TrimSpace(evt.Content) == "" {
		return nil
	}

	ok, err := w.videos.IsExist(ctx, evt.VideoID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	c := &video.Comment{
		Username: strings.TrimSpace(evt.Username),
		VideoID:  evt.VideoID,
		AuthorID: evt.AuthorID,
		Content:  strings.TrimSpace(evt.Content),
	}
	if err := w.comments.CreateComment(ctx, c); err != nil {
		return err
	}
	if err := w.videos.ChangePopularity(ctx, evt.VideoID, 1); err != nil {
		return err
	}
	video.PublishVideoCacheInvalidation(ctx, w.localCacheMQ, evt.VideoID)
	return nil
}

func (w *CommentWorker) applyDelete(ctx context.Context, evt *rabbitmq.CommentEvent) error {
	if evt == nil || evt.CommentID == 0 {
		return nil
	}
	c, err := w.comments.GetByID(ctx, evt.CommentID)
	if err != nil {
		return err
	}
	if c == nil {
		return nil
	}
	if err := w.comments.DeleteComment(ctx, c); err != nil {
		return err
	}
	if err := w.videos.ChangePopularity(ctx, c.VideoID, -1); err != nil {
		return err
	}
	video.PublishVideoCacheInvalidation(ctx, w.localCacheMQ, c.VideoID)
	return nil
}
