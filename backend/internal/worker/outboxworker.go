package worker

import (
	"context"
	"encoding/json"
	"feedsystem_video_go/internal/config"
	"fmt"
	"log"
	"strconv"
	"time"

	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"

	oredis "github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const (
	activeViewerTimelineKey = "feed:active_users"
	authorOutboxTTL         = 7 * 24 * time.Hour
)

type TimelineFanoutConfig struct {
	FollowingFanoutThreshold int
	ActiveFollowerWindow     time.Duration
}

func StartOutboxPoller(db *gorm.DB, tmq *rabbitmq.TimelineMQ) {
	if db == nil || tmq == nil {
		return
	}
	go func() {
		for {
			var messages []video.OutboxMsg

			err := db.Where("status = ?", "pending").Order("create_time ASC").Limit(100).Find(&messages).Error
			if err != nil || len(messages) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}

			for _, msg := range messages {
				err := tmq.PublishVideo(context.Background(), msg.VideoID, msg.AuthorID, msg.CreateTime)
				if err == nil {
					db.Delete(&msg)
				} else {
					log.Printf("publish timeline message failed: video_id=%d err=%v", msg.VideoID, err)
				}
			}
		}
	}()
}

func NewTimelineFanoutConfig(cfg config.FeedConfig) TimelineFanoutConfig {
	threshold := cfg.FollowingFanoutThreshold
	if threshold <= 0 {
		threshold = 2000
	}
	windowHours := cfg.ActiveFollowerWindowHours
	if windowHours <= 0 {
		windowHours = 72
	}
	return TimelineFanoutConfig{
		FollowingFanoutThreshold: threshold,
		ActiveFollowerWindow:     time.Duration(windowHours) * time.Hour,
	}
}

func StartConsumer(tmq *rabbitmq.TimelineMQ, queueName string, redisClient *redis.Client, socialRepo *social.SocialRepository, fanoutCfg TimelineFanoutConfig) {
	if tmq == nil || tmq.Ch == nil || redisClient == nil {
		return
	}
	msgs, err := tmq.Ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Printf("register timeline consumer failed: %v", err)
		return
	}

	go func() {
		for msg := range msgs {
			var event rabbitmq.TimelineEvent
			if err := json.Unmarshal(msg.Body, &event); err != nil {
				log.Printf("decode timeline event failed: %v", err)
				_ = msg.Ack(false)
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			if err := redisClient.ZAdd(ctx, "feed:global_timeline", oredis.Z{
				Score:  float64(event.CreateTime),
				Member: fmt.Sprintf("%d", event.VideoID),
			}); err != nil {
				log.Printf("write global timeline failed: %v", err)
				cancel()
				_ = msg.Nack(false, true)
				continue
			}
			_ = redisClient.ZRemRangeByRank(ctx, "feed:global_timeline", 0, -1001)
			_ = redisClient.Expire(ctx, "feed:global_timeline", 6*time.Hour)

			if event.AuthorID > 0 {
				authorOutboxKey := fmt.Sprintf("feed:author:outbox:%d", event.AuthorID)
				if err := redisClient.ZAdd(ctx, authorOutboxKey, oredis.Z{
					Score:  float64(event.CreateTime),
					Member: fmt.Sprintf("%d", event.VideoID),
				}); err != nil {
					log.Printf("write author outbox failed: author_id=%d video_id=%d err=%v", event.AuthorID, event.VideoID, err)
					cancel()
					_ = msg.Nack(false, true)
					continue
				}
				_ = redisClient.ZRemRangeByRank(ctx, authorOutboxKey, 0, -1001)
				_ = redisClient.Expire(ctx, authorOutboxKey, authorOutboxTTL)
			}

			fanoutFailed := false
			if socialRepo != nil && event.AuthorID > 0 {
				followerIDs, err := socialRepo.ListFollowerIDs(ctx, event.AuthorID)
				if err != nil {
					log.Printf("query follower ids failed: author_id=%d err=%v", event.AuthorID, err)
					cancel()
					_ = msg.Nack(false, true)
					continue
				}
				// Hybrid distribution:
				// 1. big v uses read diffusion, skip inbox fanout entirely
				// 2. normal authors only fanout to users active within 72h
				if len(followerIDs) > fanoutCfg.FollowingFanoutThreshold {
					cancel()
					_ = msg.Ack(false)
					continue
				}
				cutoff := float64(time.Now().Add(-fanoutCfg.ActiveFollowerWindow).UnixMilli())
				_ = redisClient.ZRemRangeByScore(ctx, activeViewerTimelineKey, "-inf", strconv.FormatInt(int64(cutoff), 10))
				for _, followerID := range followerIDs {
					score, scoreErr := redisClient.ZScore(ctx, activeViewerTimelineKey, strconv.FormatUint(uint64(followerID), 10))
					if scoreErr != nil || score < cutoff {
						continue
					}
					followerKey := fmt.Sprintf("feed:following:timeline:%d", followerID)
					if err := redisClient.ZAdd(ctx, followerKey, oredis.Z{
						Score:  float64(event.CreateTime),
						Member: fmt.Sprintf("%d", event.VideoID),
					}); err != nil {
						log.Printf("fanout following timeline failed: follower_id=%d video_id=%d err=%v", followerID, event.VideoID, err)
						fanoutFailed = true
						break
					}
					_ = redisClient.ZRemRangeByRank(ctx, followerKey, 0, -1001)
					_ = redisClient.Expire(ctx, followerKey, 6*time.Hour)
				}
			}
			if fanoutFailed {
				cancel()
				_ = msg.Nack(false, true)
				continue
			}

			cancel()
			_ = msg.Ack(false)
		}
	}()
}
