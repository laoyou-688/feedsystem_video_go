package video

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"feedsystem_video_go/internal/localcache"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"

	"gorm.io/gorm"
)

type VideoService struct {
	repo         *VideoRepository
	cache        *rediscache.Client
	cacheTTL     time.Duration
	popularityMQ *rabbitmq.PopularityMQ
	localCacheMQ *rabbitmq.LocalCacheMQ
}

type videoDetailCacheModeKey struct{}

const (
	videoDetailCacheModeAuto  = "auto"
	videoDetailCacheModeLocal = "local"
	videoDetailCacheModeRedis = "redis"
	videoDetailCacheModeMySQL = "mysql"
)

func NewVideoService(repo *VideoRepository, cache *rediscache.Client, popularityMQ *rabbitmq.PopularityMQ, localCacheMQ *rabbitmq.LocalCacheMQ) *VideoService {
	return &VideoService{
		repo:         repo,
		cache:        cache,
		cacheTTL:     5 * time.Minute,
		popularityMQ: popularityMQ,
		localCacheMQ: localCacheMQ,
	}
}

func (vs *VideoService) Publish(ctx context.Context, video *Video) error {
	if video == nil {
		return errors.New("video is nil")
	}
	video.Title = strings.TrimSpace(video.Title)
	video.PlayURL = strings.TrimSpace(video.PlayURL)
	video.CoverURL = strings.TrimSpace(video.CoverURL)

	if video.Title == "" {
		return errors.New("title is required")
	}
	if video.PlayURL == "" {
		return errors.New("play url is required")
	}
	if video.CoverURL == "" {
		return errors.New("cover url is required")
	}

	return vs.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(video).Error; err != nil {
			return err
		}

		msg := OutboxMsg{
			VideoID:    video.ID,
			AuthorID:   video.AuthorID,
			EventType:  "video_published",
			Status:     "pending",
			CreateTime: video.CreateTime,
		}
		return tx.Create(&msg).Error
	})
}

func (vs *VideoService) Delete(ctx context.Context, id uint, authorID uint) error {
	video, err := vs.repo.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if video == nil {
		return errors.New("video not found")
	}
	if video.AuthorID != authorID {
		return errors.New("unauthorized")
	}
	if err := vs.repo.DeleteVideo(ctx, id); err != nil {
		return err
	}
	if vs.cache != nil {
		_ = vs.cache.Del(context.Background(), fmt.Sprintf("video:detail:id=%d", id))
		_ = vs.cache.Del(context.Background(), fmt.Sprintf("video:entity:%d", id))
	}
	PublishVideoCacheInvalidation(context.Background(), vs.localCacheMQ, id)
	return nil
}

func (vs *VideoService) ListByAuthorID(ctx context.Context, authorID uint) ([]Video, error) {
	videos, err := vs.repo.ListByAuthorID(ctx, int64(authorID))
	if err != nil {
		return nil, err
	}
	return videos, nil
}

func (vs *VideoService) GetDetail(ctx context.Context, id uint) (*Video, error) {
	cacheKey := fmt.Sprintf("video:detail:id=%d", id)
	localKey := localcache.VideoDetailKey(id)
	mode, _ := ctx.Value(videoDetailCacheModeKey{}).(string)
	mode = normalizeVideoDetailCacheMode(mode)

	if mode != videoDetailCacheModeMySQL && mode != videoDetailCacheModeRedis {
		if v, ok := localcache.Get(localcache.NamespaceVideoDetail, localKey); ok {
			if cached, ok := v.(Video); ok {
				copy := cached
				return &copy, nil
			}
		}
	}

	getCached := func() (*Video, bool) {
		if vs.cache == nil {
			return nil, false
		}
		opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		b, err := vs.cache.GetBytes(opCtx, cacheKey)
		if err != nil {
			return nil, false
		}
		var cached Video
		if err := json.Unmarshal(b, &cached); err != nil {
			return nil, false
		}
		localcache.Set(localcache.NamespaceVideoDetail, localKey, cached, 5*time.Second)
		return &cached, true
	}

	setCached := func(video *Video) {
		if video == nil || vs.cache == nil {
			return
		}
		localcache.Set(localcache.NamespaceVideoDetail, localKey, *video, 5*time.Second)
		b, err := json.Marshal(video)
		if err != nil {
			return
		}
		opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		_ = vs.cache.SetBytes(opCtx, cacheKey, b, vs.cacheTTL)
	}

	if mode != videoDetailCacheModeMySQL && mode != videoDetailCacheModeLocal && vs.cache != nil {
		if v, ok := getCached(); ok {
			return v, nil
		}

		opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		b, err := vs.cache.GetBytes(opCtx, cacheKey)
		cancel()
		if err == nil {
			var cached Video
			if err := json.Unmarshal(b, &cached); err == nil {
				localcache.Set(localcache.NamespaceVideoDetail, localKey, cached, 5*time.Second)
				return &cached, nil
			}
		} else if rediscache.IsMiss(err) {
			lockKey := "lock:" + cacheKey

			lockCtx, lockCancel := context.WithTimeout(ctx, 50*time.Millisecond)
			token, locked, lockErr := vs.cache.Lock(lockCtx, lockKey, 2*time.Second)
			lockCancel()

			if lockErr == nil && locked {
				defer func() { _ = vs.cache.Unlock(context.Background(), lockKey, token) }()

				if v, ok := getCached(); ok {
					return v, nil
				}

				video, err := vs.repo.GetByID(ctx, id)
				if err != nil {
					return nil, err
				}
				setCached(video)
				return video, nil
			}

			for i := 0; i < 5; i++ {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(20 * time.Millisecond):
				}
				if v, ok := getCached(); ok {
					return v, nil
				}
			}
		}
	}

	video, err := vs.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if mode == videoDetailCacheModeAuto && vs.cache != nil {
		setCached(video)
	}
	return video, nil
}

func (vs *VideoService) UpdateLikesCount(ctx context.Context, id uint, likesCount int64) error {
	return vs.repo.UpdateLikesCount(ctx, id, likesCount)
}

func (vs *VideoService) UpdatePopularity(ctx context.Context, id uint, change int64) error {
	if err := vs.repo.UpdatePopularity(ctx, id, change); err != nil {
		return err
	}

	if vs.popularityMQ != nil {
		if err := vs.popularityMQ.Update(ctx, id, change); err == nil {
			PublishVideoCacheInvalidation(context.Background(), vs.localCacheMQ, id)
			return nil
		}
	}

	if vs.cache != nil {
		_ = vs.cache.Del(context.Background(), fmt.Sprintf("video:detail:id=%d", id))
		_ = vs.cache.Del(context.Background(), fmt.Sprintf("video:entity:%d", id))

		now := time.Now().UTC().Truncate(time.Minute)
		windowKey := "hot:video:1m:" + now.Format("200601021504")
		member := strconv.FormatUint(uint64(id), 10)

		opCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		_ = vs.cache.ZincrBy(opCtx, windowKey, member, float64(change))
		_ = vs.cache.Expire(opCtx, windowKey, 2*time.Hour)
	}

	PublishVideoCacheInvalidation(context.Background(), vs.localCacheMQ, id)
	return nil
}

func WithVideoDetailCacheMode(ctx context.Context, mode string) context.Context {
	return context.WithValue(ctx, videoDetailCacheModeKey{}, normalizeVideoDetailCacheMode(mode))
}

func normalizeVideoDetailCacheMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case videoDetailCacheModeLocal:
		return videoDetailCacheModeLocal
	case videoDetailCacheModeRedis:
		return videoDetailCacheModeRedis
	case videoDetailCacheModeMySQL:
		return videoDetailCacheModeMySQL
	default:
		return videoDetailCacheModeAuto
	}
}
