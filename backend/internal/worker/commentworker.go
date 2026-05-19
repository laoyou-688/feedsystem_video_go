package worker

import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"
	"log"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const commentMaxRetryCount = 3

type CommentWorker struct {
	ch           *amqp.Channel
	cache        *rediscache.Client
	comments     *video.CommentRepository
	videos       *video.VideoRepository
	queue        string
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
		w.handleFailure(ctx, d, err)
		return
	}
	_ = d.Ack(false)
}

func (w *CommentWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.CommentEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil
	}
	done, err := hasProcessedEvent(ctx, w.cache, "comment", evt.EventID)
	if err == nil && done {
		return nil
	}
	if err != nil {
		return err
	}
	switch evt.Action {
	case "publish":
		if err := w.applyPublish(ctx, &evt); err != nil {
			return err
		}
	case "delete":
		if err := w.applyDelete(ctx, &evt); err != nil {
			return err
		}
	default:
		return nil
	}
	return markEventDone(ctx, w.cache, "comment", evt.EventID)
}

func (w *CommentWorker) applyPublish(ctx context.Context, evt *rabbitmq.CommentEvent) error {
	if evt == nil || evt.VideoID == 0 || evt.AuthorID == 0 || strings.TrimSpace(evt.Content) == "" {
		return nil
	}
	clientToken := strings.TrimSpace(evt.ClientToken)
	if clientToken != "" {
		canceled, err := video.IsCommentCanceled(ctx, w.cache, clientToken)
		if err != nil {
			return err
		}
		if canceled {
			_ = video.ClearPendingComment(context.Background(), w.cache, clientToken)
			return nil
		}
		existing, err := w.comments.GetByClientToken(ctx, clientToken)
		if err != nil {
			return err
		}
		if existing != nil {
			_ = video.ClearPendingComment(context.Background(), w.cache, clientToken)
			return nil
		}
	}

	ok, err := w.videos.IsExist(ctx, evt.VideoID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	c := &video.Comment{
		Username:    strings.TrimSpace(evt.Username),
		VideoID:     evt.VideoID,
		AuthorID:    evt.AuthorID,
		ClientToken: clientToken,
		Content:     strings.TrimSpace(evt.Content),
	}
	if err := w.comments.CreateComment(ctx, c); err != nil {
		return err
	}
	_ = video.ClearPendingComment(context.Background(), w.cache, clientToken)
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
	if err := video.MarkCommentCanceled(context.Background(), w.cache, c.ClientToken); err != nil {
		return err
	}
	_ = video.ClearPendingComment(context.Background(), w.cache, c.ClientToken)
	if err := w.videos.ChangePopularity(ctx, c.VideoID, -1); err != nil {
		return err
	}
	video.PublishVideoCacheInvalidation(ctx, w.localCacheMQ, c.VideoID)
	return nil
}

func (w *CommentWorker) handleFailure(ctx context.Context, d amqp.Delivery, err error) {
	retryCount := readRetryCount(d.Headers)
	routingKey := d.RoutingKey
	if routingKey == "" {
		var evt rabbitmq.CommentEvent
		if jsonErr := json.Unmarshal(d.Body, &evt); jsonErr == nil {
			if derived, routeErr := rabbitmq.CommentRoutingKey(evt.Action); routeErr == nil {
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

	if retryCount >= commentMaxRetryCount {
		if dlqErr := rabbitmq.PublishCommentDLQ(publishCtx, w.ch, routingKey, d.Body, headers); dlqErr != nil {
			log.Printf("comment worker: failed to move message to dlq: event_routing=%s retry_count=%d err=%v original_err=%v", routingKey, retryCount, dlqErr, err)
			_ = d.Nack(false, true)
			return
		}
		log.Printf("comment worker: moved message to dlq: routing=%s retry_count=%d err=%v", routingKey, retryCount, err)
		_ = d.Ack(false)
		return
	}

	if retryErr := rabbitmq.PublishCommentRetry(publishCtx, w.ch, routingKey, d.Body, headers); retryErr != nil {
		log.Printf("comment worker: failed to publish retry message: routing=%s retry_count=%d err=%v original_err=%v", routingKey, retryCount, retryErr, err)
		_ = d.Nack(false, true)
		return
	}

	log.Printf("comment worker: scheduled retry: routing=%s retry_count=%d err=%v", routingKey, retryCount+1, err)
	_ = d.Ack(false)
}

func readRetryCount(headers amqp.Table) int {
	if len(headers) == 0 {
		return 0
	}
	raw, ok := headers["x-retry-count"]
	if !ok || raw == nil {
		return 0
	}
	switch v := raw.(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	case int16:
		return int(v)
	case int8:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	case uint:
		return int(v)
	case string:
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	case []byte:
		n, err := strconv.Atoi(string(v))
		if err == nil {
			return n
		}
	}
	return 0
}

func copyHeaders(src amqp.Table) amqp.Table {
	if len(src) == 0 {
		return amqp.Table{}
	}
	dst := make(amqp.Table, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
