package localcache

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"

	"github.com/patrickmn/go-cache"
)

const (
	NamespaceVideoEntity = "video_entity"
	NamespaceVideoDetail = "video_detail"
)

var manager = cache.New(10*time.Second, 30*time.Second)

func compositeKey(namespace, key string) string {
	return namespace + ":" + key
}

func Get(namespace, key string) (any, bool) {
	return manager.Get(compositeKey(namespace, key))
}

func Set(namespace, key string, value any, ttl time.Duration) {
	manager.Set(compositeKey(namespace, key), value, ttl)
}

func Delete(namespace, key string) {
	manager.Delete(compositeKey(namespace, key))
}

func VideoEntityKey(videoID uint) string {
	return fmt.Sprintf("%d", videoID)
}

func VideoDetailKey(videoID uint) string {
	return fmt.Sprintf("%d", videoID)
}

func StartInvalidationConsumer(mq *rabbitmq.LocalCacheMQ, queueName string) error {
	if mq == nil {
		return nil
	}
	deliveries, err := mq.Consume(queueName)
	if err != nil {
		return err
	}

	go func() {
		for d := range deliveries {
			var evt rabbitmq.LocalCacheInvalidateEvent
			if err := json.Unmarshal(d.Body, &evt); err != nil {
				log.Printf("local cache consumer: decode failed: %v", err)
				_ = d.Ack(false)
				continue
			}
			Delete(evt.Namespace, evt.Key)
			_ = d.Ack(false)
		}
	}()
	return nil
}

func PublishVideoInvalidation(ctx context.Context, mq *rabbitmq.LocalCacheMQ, videoID uint) {
	if mq == nil || videoID == 0 {
		return
	}
	_ = mq.PublishInvalidate(ctx, NamespaceVideoEntity, VideoEntityKey(videoID))
	_ = mq.PublishInvalidate(ctx, NamespaceVideoDetail, VideoDetailKey(videoID))
}
