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
