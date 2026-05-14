package canal

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"feedsystem_video_go/internal/config"
	rabbitmq "feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
)

type Service struct {
	cfg        config.CanalConfig
	cache      *rediscache.Client
	timelineMQ *rabbitmq.TimelineMQ
	socialRepo *social.SocialRepository
	localCacheMQ *rabbitmq.LocalCacheMQ
}

func NewService(cfg config.CanalConfig, cache *rediscache.Client, timelineMQ *rabbitmq.TimelineMQ, socialRepo *social.SocialRepository, localCacheMQ *rabbitmq.LocalCacheMQ) *Service {
	return &Service{
		cfg:        cfg,
		cache:      cache,
		timelineMQ: timelineMQ,
		socialRepo: socialRepo,
		localCacheMQ: localCacheMQ,
	}
}

func (s *Service) Enabled() bool {
	return s != nil && s.cfg.Enabled
}

func (s *Service) ValidateToken(token string) bool {
	if s == nil || strings.TrimSpace(s.cfg.Token) == "" {
		return true
	}
	return strings.TrimSpace(token) == strings.TrimSpace(s.cfg.Token)
}

func (s *Service) Process(ctx context.Context, req *SyncRequest) (int, error) {
	if s == nil || req == nil {
		return 0, nil
	}

	processed := 0
	for _, evt := range req.Events {
		if err := s.processEvent(ctx, evt); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func (s *Service) processEvent(ctx context.Context, evt RowEvent) error {
	if evt.EventID != "" && s.cache != nil {
		ok, err := s.cache.SetNXBytes(ctx, "canal:idempotency:"+evt.EventID, []byte("1"), 24*time.Hour)
		if err == nil && !ok {
			return nil
		}
	}

	switch strings.ToLower(strings.TrimSpace(evt.Table)) {
	case "video", "videos":
		return s.processVideoEvent(ctx, evt)
	case "social", "socials":
		return s.processSocialEvent(ctx, evt)
	case "like", "likes", "comment", "comments":
		return s.processEngagementEvent(ctx, evt)
	default:
		return nil
	}
}

func (s *Service) processVideoEvent(ctx context.Context, evt RowEvent) error {
	for _, row := range evt.Rows {
		videoID := uintValue(row, "id", "video_id")
		authorID := uintValue(row, "author_id")
		if videoID == 0 {
			continue
		}

		switch strings.ToLower(strings.TrimSpace(evt.Action)) {
		case "insert":
			createTime := timeValue(row, "create_time", "created_at")
			if createTime.IsZero() {
				createTime = evt.OccurredAt
			}
			if createTime.IsZero() {
				createTime = time.Now().UTC()
			}
			if s.timelineMQ != nil && authorID > 0 {
				if err := s.timelineMQ.PublishVideo(ctx, videoID, authorID, createTime); err != nil {
					return err
				}
			}
		case "update":
			if err := s.invalidateVideoCache(ctx, videoID); err != nil {
				return err
			}
		case "delete":
			if err := s.invalidateVideoCache(ctx, videoID); err != nil {
				return err
			}
			if err := s.removeVideoFromTimelines(ctx, videoID, authorID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Service) processSocialEvent(ctx context.Context, evt RowEvent) error {
	for _, row := range evt.Rows {
		followerID := uintValue(row, "follower_id")
		if followerID == 0 || s.cache == nil {
			continue
		}
		if err := s.cache.Del(ctx, fmt.Sprintf("feed:following:timeline:%d", followerID)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) processEngagementEvent(ctx context.Context, evt RowEvent) error {
	for _, row := range evt.Rows {
		videoID := uintValue(row, "video_id")
		if videoID == 0 {
			continue
		}
		if err := s.invalidateVideoCache(ctx, videoID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) invalidateVideoCache(ctx context.Context, videoID uint) error {
	if videoID == 0 {
		return nil
	}
	if s.cache != nil {
		if err := s.cache.Del(ctx, fmt.Sprintf("video:detail:id=%d", videoID)); err != nil {
			return err
		}
		if err := s.cache.Del(ctx, fmt.Sprintf("video:entity:%d", videoID)); err != nil {
			return err
		}
	}
	video.PublishVideoCacheInvalidation(ctx, s.localCacheMQ, videoID)
	return nil
}

func (s *Service) removeVideoFromTimelines(ctx context.Context, videoID uint, authorID uint) error {
	if s.cache == nil || videoID == 0 {
		return nil
	}
	member := strconv.FormatUint(uint64(videoID), 10)
	if err := s.cache.ZRem(ctx, "feed:global_timeline", member); err != nil {
		return err
	}
	if s.socialRepo == nil || authorID == 0 {
		return nil
	}
	followerIDs, err := s.socialRepo.ListFollowerIDs(ctx, authorID)
	if err != nil {
		return err
	}
	for _, followerID := range followerIDs {
		if err := s.cache.ZRem(ctx, fmt.Sprintf("feed:following:timeline:%d", followerID), member); err != nil {
			return err
		}
	}
	return nil
}

func uintValue(row map[string]interface{}, keys ...string) uint {
	for _, key := range keys {
		if v, ok := row[key]; ok {
			switch value := v.(type) {
			case float64:
				if value > 0 {
					return uint(value)
				}
			case int:
				if value > 0 {
					return uint(value)
				}
			case int64:
				if value > 0 {
					return uint(value)
				}
			case uint:
				return value
			case uint64:
				return uint(value)
			case string:
				if parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64); err == nil {
					return uint(parsed)
				}
			}
		}
	}
	return 0
}

func timeValue(row map[string]interface{}, keys ...string) time.Time {
	for _, key := range keys {
		if v, ok := row[key]; ok {
			switch value := v.(type) {
			case time.Time:
				return value
			case string:
				candidates := []string{time.RFC3339, "2006-01-02 15:04:05", time.RFC3339Nano}
				for _, layout := range candidates {
					if parsed, err := time.Parse(layout, strings.TrimSpace(value)); err == nil {
						return parsed
					}
				}
				if ms, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil && ms > 0 {
					return time.UnixMilli(ms)
				}
			case int64:
				if value > 0 {
					return time.UnixMilli(value)
				}
			case float64:
				if value > 0 {
					return time.UnixMilli(int64(value))
				}
			}
		}
	}
	return time.Time{}
}
