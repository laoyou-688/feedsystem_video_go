package worker

import (
	"context"
	"fmt"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
)

func claimEvent(ctx context.Context, cache *rediscache.Client, topic, eventID string) (bool, error) {
	if cache == nil || eventID == "" {
		return true, nil
	}

	opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	return cache.SetNXBytes(opCtx, fmt.Sprintf("mq:idempotency:%s:%s", topic, eventID), []byte("1"), 24*time.Hour)
}

func markEventDone(ctx context.Context, cache *rediscache.Client, topic, eventID string) error {
	if cache == nil || eventID == "" {
		return nil
	}

	opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	return cache.SetBytes(opCtx, fmt.Sprintf("mq:idempotency:%s:%s", topic, eventID), []byte("1"), 24*time.Hour)
}

func hasProcessedEvent(ctx context.Context, cache *rediscache.Client, topic, eventID string) (bool, error) {
	if cache == nil || eventID == "" {
		return false, nil
	}

	opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	_, err := cache.GetBytes(opCtx, fmt.Sprintf("mq:idempotency:%s:%s", topic, eventID))
	if err == nil {
		return true, nil
	}
	if rediscache.IsMiss(err) {
		return false, nil
	}
	return false, err
}
