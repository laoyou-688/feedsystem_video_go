package feed

import (
	"context"
	"encoding/json"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/social"
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"feedsystem_video_go/internal/localcache"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"

	redis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type FeedService struct {
	repo         *FeedRepository
	socialRepo   *social.SocialRepository
	likeRepo     *video.LikeRepository
	rediscache   *rediscache.Client
	cacheTTL     time.Duration
	runtimeCfg   FeedRuntimeConfig
	requestGroup singleflight.Group
}

const (
	feedSortLatest = "latest"
	feedSortHybrid = "hybrid"

	feedSourceModeAuto     = "auto"
	feedSourceModeTimeline = "timeline"
	feedSourceModeMySQL    = "mysql"

	feedEntityCacheModeAuto  = "auto"
	feedEntityCacheModeLocal = "local"
	feedEntityCacheModeRedis = "redis"
	feedEntityCacheModeMySQL = "mysql"

	globalTimelineKey       = "feed:global_timeline"
	timelineRebuildLimit    = 1000
	followingTimelineTTL    = 6 * time.Hour
	activeViewerZSetKey     = "feed:active_users"
	activeViewerWindow      = 72 * time.Hour
	exposureBloomBits int64 = 1 << 18
	exposureBloomTTL        = 72 * time.Hour
)

var exposureBloomSeeds = [...]string{"17", "31", "53", "97"}

type feedSourceModeKey struct{}
type feedEntityCacheModeKey struct{}

type CachedFeedData struct {
	PublicVideos []video.Video `json:"public_videos"`
}

type FeedRuntimeConfig struct {
	FollowingPullCacheTTL time.Duration
}

type feedTimeCursor struct {
	Time time.Time
	ID   uint
}

func NewFeedRuntimeConfig(cfg config.FeedConfig) FeedRuntimeConfig {
	pullTTL := cfg.FollowingPullCacheTTLSeconds
	if pullTTL <= 0 {
		pullTTL = 30
	}
	return FeedRuntimeConfig{
		FollowingPullCacheTTL: time.Duration(pullTTL) * time.Second,
	}
}

func NewFeedService(repo *FeedRepository, socialRepo *social.SocialRepository, likeRepo *video.LikeRepository, rediscache *rediscache.Client, runtimeCfg FeedRuntimeConfig) *FeedService {
	return &FeedService{
		repo:       repo,
		socialRepo: socialRepo,
		likeRepo:   likeRepo,
		rediscache: rediscache,
		cacheTTL:   10 * time.Second,
		runtimeCfg: runtimeCfg,
	}
}

func (f *FeedService) GetVideoByIDs(ctx context.Context, videoIDs []uint) ([]*video.Video, error) {
	if len(videoIDs) == 0 {
		return []*video.Video{}, nil
	}

	entityMode, _ := ctx.Value(feedEntityCacheModeKey{}).(string)
	entityMode = normalizeFeedEntityCacheMode(entityMode)

	videoMap := make(map[uint]*video.Video)
	var missedL1 []uint
	if entityMode != feedEntityCacheModeMySQL && entityMode != feedEntityCacheModeRedis {
		for _, id := range videoIDs {
			cacheKey := localcache.VideoEntityKey(id)
			if v, found := localcache.Get(localcache.NamespaceVideoEntity, cacheKey); found {
				if data, ok := v.(video.Video); ok {
					videoMap[id] = &data
					continue
				}
			}
			missedL1 = append(missedL1, id)
		}
	} else {
		missedL1 = append(missedL1, videoIDs...)
	}

	if len(missedL1) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	var missedL2 []uint
	if len(missedL1) > 0 && f.rediscache != nil && entityMode != feedEntityCacheModeMySQL && entityMode != feedEntityCacheModeLocal {
		cacheKeys := make([]string, len(missedL1))
		for i, id := range missedL1 {
			cacheKeys[i] = fmt.Sprintf("video:entity:%d", id)
		}

		cacheCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		results, err := f.rediscache.MGet(cacheCtx, cacheKeys...)
		cancel()

		if err == nil {
			for i, res := range results {
				id := missedL1[i]
				if res != nil {
					if str, ok := res.(string); ok {
						var v video.Video
						if err := json.Unmarshal([]byte(str), &v); err == nil {
							videoMap[id] = &v
							if entityMode == feedEntityCacheModeAuto {
								localcache.Set(localcache.NamespaceVideoEntity, localcache.VideoEntityKey(id), v, 5*time.Second)
							}
							continue
						}
					}
				}
				missedL2 = append(missedL2, id)
			}
		} else {
			missedL2 = missedL1
			log.Printf("feed service: redis mget failed, downgrade to mysql: %v", err)
		}
	}
	if f.rediscache == nil {
		missedL2 = missedL1
	}

	if len(missedL2) == 0 {
		return buildOrderedResult(videoIDs, videoMap), nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, id := range missedL2 {
		wg.Add(1)
		go func(videoID uint) {
			defer wg.Done()
			sfKey := fmt.Sprintf("sf:entity:%d", videoID)

			v, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
				videoList, err := f.repo.GetByIDs(ctx, []uint{videoID})
				if err != nil || len(videoList) == 0 {
					return nil, err
				}

				safeCopy := *videoList[0]
				cacheKey := fmt.Sprintf("video:entity:%d", safeCopy.ID)
				if f.rediscache != nil && entityMode != feedEntityCacheModeMySQL && entityMode != feedEntityCacheModeLocal {
					if b, err := json.Marshal(safeCopy); err == nil {
						go func(k string, payload []byte) {
							setCtx, setCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
							defer setCancel()
							_ = f.rediscache.SetBytes(setCtx, k, payload, time.Hour)
						}(cacheKey, b)
					}
				}
				return videoList[0], err
			})

			if err == nil && v != nil {
				safeCopy := *(v.(*video.Video))
				mu.Lock()
				videoMap[id] = &safeCopy
				mu.Unlock()
				if entityMode != feedEntityCacheModeRedis && entityMode != feedEntityCacheModeMySQL {
					localcache.Set(localcache.NamespaceVideoEntity, localcache.VideoEntityKey(safeCopy.ID), safeCopy, 5*time.Second)
				}
			}
		}(id)
	}
	wg.Wait()
	return buildOrderedResult(videoIDs, videoMap), nil
}

func (f *FeedService) ListLatest(ctx context.Context, limit int, latestBefore time.Time, latestIDBefore uint, viewerAccountID uint, sortMode string, viewerKey string) (ListLatestResponse, error) {
	f.markViewerActive(ctx, viewerAccountID)
	normalizedSortMode := normalizeFeedSortMode(sortMode)
	fetchLimit := candidateLimit(limit)
	cursor := buildFeedTimeCursor(latestBefore, latestIDBefore)

	sourceMode, _ := ctx.Value(feedSourceModeKey{}).(string)
	sourceMode = normalizeFeedSourceMode(sourceMode)

	var (
		baseVideos []*video.Video
		nextCursor *feedTimeCursor
		err        error
	)
	switch sourceMode {
	case feedSourceModeMySQL:
		baseVideos, err = f.repo.ListLatest(ctx, fetchLimit, cursorTime(cursor), cursorID(cursor))
		nextCursor = nextCursorFromVideos(baseVideos)
	default:
		baseVideos, nextCursor, err = f.loadLatestCandidates(ctx, fetchLimit, cursor)
	}
	if err != nil {
		return ListLatestResponse{}, err
	}

	if normalizedSortMode == feedSortHybrid {
		hotVideos, hotErr := f.loadHotCandidates(ctx, fetchLimit, 0)
		if hotErr == nil {
			baseVideos = mergeUniqueVideos(hotVideos, baseVideos)
		}
	}

	selectedVideos := f.applyExposureFilter(ctx, baseVideos, viewerKey, limit)
	feedVideos, err := f.buildFeedVideos(ctx, selectedVideos, viewerAccountID)
	if err != nil {
		return ListLatestResponse{}, err
	}

	resp := ListLatestResponse{
		VideoList: feedVideos,
		NextTime:  0,
		HasMore:   len(baseVideos) == fetchLimit,
		SortMode:  normalizedSortMode,
	}
	if nextCursor != nil {
		resp.NextTime = nextCursor.Time.UnixMilli()
		resp.NextIDBefore = uintPtr(nextCursor.ID)
	}
	return resp, nil
}

func (f *FeedService) ListLikesCount(ctx context.Context, limit int, cursor *LikesCountCursor, viewerAccountID uint, viewerKey string) (ListLikesCountResponse, error) {
	f.markViewerActive(ctx, viewerAccountID)
	videos, err := f.repo.ListLikesCountWithCursor(ctx, candidateLimit(limit), cursor)
	if err != nil {
		return ListLikesCountResponse{}, err
	}
	selectedVideos := f.applyExposureFilter(ctx, videos, viewerKey, limit)
	hasMore := len(videos) == candidateLimit(limit)
	feedVideos, err := f.buildFeedVideos(ctx, selectedVideos, viewerAccountID)
	if err != nil {
		return ListLikesCountResponse{}, err
	}
	resp := ListLikesCountResponse{
		VideoList: feedVideos,
		HasMore:   hasMore,
	}
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextLikesCountBefore := last.LikesCount
		nextIDBefore := last.ID
		resp.NextLikesCountBefore = &nextLikesCountBefore
		resp.NextIDBefore = &nextIDBefore
	}
	return resp, nil
}

func (f *FeedService) ListByFollowing(ctx context.Context, limit int, latestBefore time.Time, latestIDBefore uint, viewerAccountID uint, viewerKey string) (ListByFollowingResponse, error) {
	f.markViewerActive(ctx, viewerAccountID)
	cursor := buildFeedTimeCursor(latestBefore, latestIDBefore)
	videos, nextCursor, err := f.loadFollowingCandidates(ctx, candidateLimit(limit), viewerAccountID, cursor)
	if err != nil {
		return ListByFollowingResponse{}, err
	}

	selectedVideos := f.applyExposureFilter(ctx, videos, viewerKey, limit)
	feedVideos, err := f.buildFeedVideos(ctx, selectedVideos, viewerAccountID)
	if err != nil {
		return ListByFollowingResponse{}, err
	}

	resp := ListByFollowingResponse{
		VideoList: feedVideos,
		HasMore:   len(videos) == candidateLimit(limit),
	}
	if nextCursor != nil {
		resp.NextTime = nextCursor.Time.UnixMilli()
		resp.NextIDBefore = uintPtr(nextCursor.ID)
	}
	return resp, nil
}

func (f *FeedService) ListByPopularity(ctx context.Context, limit int, reqAsOf int64, offset int, viewerAccountID uint, viewerKey string, latestPopularity int64, latestBefore time.Time, latestIDBefore uint) (ListByPopularityResponse, error) {
	f.markViewerActive(ctx, viewerAccountID)
	if f.rediscache != nil {
		asOf := time.Now().UTC().Truncate(time.Minute)
		if reqAsOf > 0 {
			asOf = time.UnixMilli(reqAsOf).UTC().Truncate(time.Minute)
		}

		const win = 60
		keys := make([]string, 0, win)
		for i := 0; i < win; i++ {
			keys = append(keys, "hot:video:1m:"+asOf.Add(-time.Duration(i)*time.Minute).Format("200601021504"))
		}

		dest := "hot:video:merge:1m:" + asOf.Format("200601021504")
		opCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		defer cancel()

		exists, _ := f.rediscache.Exists(opCtx, dest)
		if !exists {
			_ = f.rediscache.ZUnionStore(opCtx, dest, keys, "SUM")
			_ = f.rediscache.Expire(opCtx, dest, 2*time.Minute)
		}

		fetchLimit := candidateLimit(limit)
		start := int64(offset)
		stop := start + int64(fetchLimit) - 1
		members, err := f.rediscache.ZRevRange(opCtx, dest, start, stop)
		if err == nil && len(members) == 0 && offset > 0 {
			return ListByPopularityResponse{
				VideoList:  []FeedVideoItem{},
				AsOf:       asOf.UnixMilli(),
				NextOffset: offset,
				HasMore:    false,
			}, nil
		}
		if err == nil && len(members) > 0 {
			ids := make([]uint, 0, len(members))
			for _, m := range members {
				u, err := strconv.ParseUint(m, 10, 64)
				if err == nil && u > 0 {
					ids = append(ids, uint(u))
				}
			}

			videos, err := f.repo.GetByIDs(ctx, ids)
			if err == nil {
				ordered := reorderVideosByIDs(videos, ids)
				selectedVideos := f.applyExposureFilter(ctx, ordered, viewerKey, limit)
				items, err := f.buildFeedVideos(ctx, selectedVideos, viewerAccountID)
				if err != nil {
					return ListByPopularityResponse{}, err
				}
				resp := ListByPopularityResponse{
					VideoList:  items,
					AsOf:       asOf.UnixMilli(),
					NextOffset: offset + len(ordered),
					HasMore:    len(ordered) == fetchLimit,
				}
				if len(ordered) > 0 {
					last := ordered[len(ordered)-1]
					nextPopularity := last.Popularity
					nextBefore := last.CreateTime
					nextID := last.ID
					resp.NextLatestPopularity = &nextPopularity
					resp.NextLatestBefore = &nextBefore
					resp.NextLatestIDBefore = &nextID
				}
				return resp, nil
			}
		}
	}

	videos, err := f.repo.ListByPopularity(ctx, candidateLimit(limit), latestPopularity, latestBefore, latestIDBefore)
	if err != nil {
		return ListByPopularityResponse{}, err
	}
	selectedVideos := f.applyExposureFilter(ctx, videos, viewerKey, limit)
	items, err := f.buildFeedVideos(ctx, selectedVideos, viewerAccountID)
	if err != nil {
		return ListByPopularityResponse{}, err
	}
	resp := ListByPopularityResponse{
		VideoList:  items,
		AsOf:       0,
		NextOffset: 0,
		HasMore:    len(videos) == candidateLimit(limit),
	}
	if len(videos) > 0 {
		last := videos[len(videos)-1]
		nextPopularity := last.Popularity
		nextBefore := last.CreateTime
		nextID := last.ID
		resp.NextLatestPopularity = &nextPopularity
		resp.NextLatestBefore = &nextBefore
		resp.NextLatestIDBefore = &nextID
	}
	return resp, nil
}

func (f *FeedService) loadLatestCandidates(ctx context.Context, limit int, cursor *feedTimeCursor) ([]*video.Video, *feedTimeCursor, error) {
	if f.rediscache == nil {
		videos, err := f.repo.ListLatest(ctx, limit, cursorTime(cursor), cursorID(cursor))
		if err != nil {
			return nil, nil, err
		}
		return videos, nextCursorFromVideos(videos), nil
	}

	zsetTail, err := f.rediscache.ZRangeWithScores(ctx, globalTimelineKey, 0, 0)
	if err != nil {
		return nil, nil, err
	}

	if len(zsetTail) == 0 {
		if err := f.rebuildGlobalTimeline(ctx); err != nil {
			return nil, nil, err
		}
		return f.loadLatestCandidates(ctx, limit, cursor)
	}

	watermark := int64(zsetTail[0].Score)
	reqTime := time.Now().UnixMilli()
	if cursor != nil {
		reqTime = cursor.Time.UnixMilli()
	}

	if reqTime <= watermark {
		videos, err := f.repo.ListLatest(ctx, limit, cursorTime(cursor), cursorID(cursor))
		if err != nil {
			return nil, nil, err
		}
		return videos, nextCursorFromVideos(videos), nil
	}

	baseVideos, err := f.loadTimelineVideos(ctx, globalTimelineKey, limit, cursor)
	if err != nil {
		return nil, nil, err
	}
	if len(baseVideos) < limit {
		stitchCursor := cursor
		if len(baseVideos) > 0 {
			stitchCursor = cursorFromVideo(baseVideos[len(baseVideos)-1])
		}
		moreVideos, stitchErr := f.repo.ListLatest(ctx, limit-len(baseVideos), cursorTime(stitchCursor), cursorID(stitchCursor))
		if stitchErr == nil {
			baseVideos = mergeUniqueVideos(baseVideos, moreVideos)
		}
	}
	if len(baseVideos) > limit {
		baseVideos = baseVideos[:limit]
	}
	return baseVideos, nextCursorFromVideos(baseVideos), nil
}

func (f *FeedService) loadFollowingCandidates(ctx context.Context, limit int, viewerAccountID uint, cursor *feedTimeCursor) ([]*video.Video, *feedTimeCursor, error) {
	if viewerAccountID == 0 {
		return []*video.Video{}, nil, nil
	}
	if f.rediscache == nil {
		videos, err := f.repo.ListByFollowing(ctx, limit, viewerAccountID, cursorTime(cursor), cursorID(cursor))
		if err != nil {
			return nil, nil, err
		}
		return videos, nextCursorFromVideos(videos), nil
	}

	timelineKey := followingTimelineKey(viewerAccountID)
	exists, _ := f.rediscache.Exists(ctx, timelineKey)
	if !exists && cursor == nil {
		if err := f.rebuildFollowingTimeline(ctx, viewerAccountID); err != nil {
			log.Printf("feed service: rebuild following timeline failed: uid=%d err=%v", viewerAccountID, err)
		}
	}

	baseVideos, err := f.loadTimelineVideos(ctx, timelineKey, limit, cursor)
	if err != nil {
		return nil, nil, err
	}
	pullVideos, pullErr := f.loadAuthorOutboxCandidates(ctx, viewerAccountID, limit, cursor)
	if pullErr == nil {
		baseVideos = mergeUniqueVideos(baseVideos, pullVideos)
		sortVideosByCreateTimeDesc(baseVideos)
	}

	if len(baseVideos) < limit {
		stitchCursor := cursor
		if len(baseVideos) > 0 {
			stitchCursor = cursorFromVideo(baseVideos[len(baseVideos)-1])
		}
		moreVideos, stitchErr := f.repo.ListByFollowing(ctx, limit-len(baseVideos), viewerAccountID, cursorTime(stitchCursor), cursorID(stitchCursor))
		if stitchErr == nil {
			baseVideos = mergeUniqueVideos(baseVideos, moreVideos)
			sortVideosByCreateTimeDesc(baseVideos)
		}
	}
	if len(baseVideos) > limit {
		baseVideos = baseVideos[:limit]
	}
	return baseVideos, nextCursorFromVideos(baseVideos), nil
}

func (f *FeedService) loadHotCandidates(ctx context.Context, limit int, reqAsOf int64) ([]*video.Video, error) {
	if f.rediscache != nil {
		asOf := time.Now().UTC().Truncate(time.Minute)
		if reqAsOf > 0 {
			asOf = time.UnixMilli(reqAsOf).UTC().Truncate(time.Minute)
		}

		const win = 60
		keys := make([]string, 0, win)
		for i := 0; i < win; i++ {
			keys = append(keys, "hot:video:1m:"+asOf.Add(-time.Duration(i)*time.Minute).Format("200601021504"))
		}

		dest := "hot:video:merge:1m:" + asOf.Format("200601021504")
		opCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		defer cancel()

		exists, _ := f.rediscache.Exists(opCtx, dest)
		if !exists {
			_ = f.rediscache.ZUnionStore(opCtx, dest, keys, "SUM")
			_ = f.rediscache.Expire(opCtx, dest, 2*time.Minute)
		}

		members, err := f.rediscache.ZRevRange(opCtx, dest, 0, int64(limit)-1)
		if err == nil && len(members) > 0 {
			ids := make([]uint, 0, len(members))
			for _, member := range members {
				u, parseErr := strconv.ParseUint(member, 10, 64)
				if parseErr == nil && u > 0 {
					ids = append(ids, uint(u))
				}
			}
			if len(ids) > 0 {
				videos, repoErr := f.repo.GetByIDs(ctx, ids)
				if repoErr == nil {
					return reorderVideosByIDs(videos, ids), nil
				}
			}
		}
	}

	return f.repo.ListByPopularity(ctx, limit, 0, time.Time{}, 0)
}

func (f *FeedService) applyExposureFilter(ctx context.Context, videos []*video.Video, viewerKey string, limit int) []*video.Video {
	if limit <= 0 || len(videos) == 0 {
		return []*video.Video{}
	}
	if viewerKey == "" || f.rediscache == nil {
		if len(videos) <= limit {
			return videos
		}
		return videos[:limit]
	}

	selected := make([]*video.Video, 0, minInt(limit, len(videos)))
	for _, item := range videos {
		if item == nil {
			continue
		}
		seen, err := f.exposureSeen(ctx, viewerKey, item.ID)
		if err != nil {
			seen = false
		}
		if seen {
			continue
		}
		selected = append(selected, item)
		_ = f.markExposure(ctx, viewerKey, item.ID)
		if len(selected) == limit {
			break
		}
	}

	return selected
}

func normalizeFeedSortMode(mode string) string {
	switch mode {
	case "", feedSortLatest:
		return feedSortLatest
	case feedSortHybrid:
		return feedSortHybrid
	default:
		return feedSortLatest
	}
}

func normalizeFeedSourceMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case feedSourceModeTimeline:
		return feedSourceModeTimeline
	case feedSourceModeMySQL:
		return feedSourceModeMySQL
	default:
		return feedSourceModeAuto
	}
}

func normalizeFeedEntityCacheMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case feedEntityCacheModeLocal:
		return feedEntityCacheModeLocal
	case feedEntityCacheModeRedis:
		return feedEntityCacheModeRedis
	case feedEntityCacheModeMySQL:
		return feedEntityCacheModeMySQL
	default:
		return feedEntityCacheModeAuto
	}
}

func WithFeedSourceMode(ctx context.Context, mode string) context.Context {
	return context.WithValue(ctx, feedSourceModeKey{}, normalizeFeedSourceMode(mode))
}

func WithFeedEntityCacheMode(ctx context.Context, mode string) context.Context {
	return context.WithValue(ctx, feedEntityCacheModeKey{}, normalizeFeedEntityCacheMode(mode))
}

func candidateLimit(limit int) int {
	if limit <= 0 {
		return 10
	}
	expanded := limit * 3
	if expanded < limit+5 {
		expanded = limit + 5
	}
	if expanded > 100 {
		expanded = 100
	}
	return expanded
}

func nextCursorFromVideos(videos []*video.Video) *feedTimeCursor {
	if len(videos) == 0 {
		return nil
	}
	return cursorFromVideo(videos[len(videos)-1])
}

func mergeUniqueVideos(primary []*video.Video, secondary []*video.Video) []*video.Video {
	merged := make([]*video.Video, 0, len(primary)+len(secondary))
	seen := make(map[uint]struct{}, len(primary)+len(secondary))
	for _, item := range append(primary, secondary...) {
		if item == nil {
			continue
		}
		if _, exists := seen[item.ID]; exists {
			continue
		}
		seen[item.ID] = struct{}{}
		merged = append(merged, item)
	}
	return merged
}

func reorderVideosByIDs(videos []*video.Video, ids []uint) []*video.Video {
	byID := make(map[uint]*video.Video, len(videos))
	for _, item := range videos {
		if item != nil {
			byID[item.ID] = item
		}
	}
	ordered := make([]*video.Video, 0, len(ids))
	for _, id := range ids {
		if item := byID[id]; item != nil {
			ordered = append(ordered, item)
		}
	}
	return ordered
}

func sortVideosByCreateTimeDesc(videos []*video.Video) {
	sort.Slice(videos, func(i, j int) bool {
		if videos[i] == nil {
			return false
		}
		if videos[j] == nil {
			return true
		}
		if videos[i].CreateTime.Equal(videos[j].CreateTime) {
			return videos[i].ID > videos[j].ID
		}
		return videos[i].CreateTime.After(videos[j].CreateTime)
	})
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (f *FeedService) buildFeedVideos(ctx context.Context, videos []*video.Video, viewerAccountID uint) ([]FeedVideoItem, error) {
	feedVideos := make([]FeedVideoItem, 0, len(videos))
	videoIDs := make([]uint, len(videos))
	for i, v := range videos {
		videoIDs[i] = v.ID
	}
	likedMap, err := f.likeRepo.BatchGetLiked(ctx, videoIDs, viewerAccountID)
	if err != nil {
		return nil, err
	}
	for _, video := range videos {
		feedVideos = append(feedVideos, FeedVideoItem{
			ID:          video.ID,
			Author:      FeedAuthor{ID: video.AuthorID, Username: video.Username},
			Title:       video.Title,
			Description: video.Description,
			PlayURL:     video.PlayURL,
			CoverURL:    video.CoverURL,
			CreateTime:  video.CreateTime.UnixMilli(),
			LikesCount:  video.LikesCount,
			IsLiked:     likedMap[video.ID],
		})
	}
	return feedVideos, nil
}

func buildOrderedResult(orderedIDs []uint, dataMap map[uint]*video.Video) []*video.Video {
	res := make([]*video.Video, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		if v, exists := dataMap[id]; exists && v != nil {
			res = append(res, v)
		}
	}
	return res
}

func buildFeedTimeCursor(ts time.Time, id uint) *feedTimeCursor {
	if ts.IsZero() {
		return nil
	}
	return &feedTimeCursor{Time: ts, ID: id}
}

func cursorFromVideo(item *video.Video) *feedTimeCursor {
	if item == nil {
		return nil
	}
	return &feedTimeCursor{Time: item.CreateTime, ID: item.ID}
}

func cursorTime(cursor *feedTimeCursor) time.Time {
	if cursor == nil {
		return time.Time{}
	}
	return cursor.Time
}

func cursorID(cursor *feedTimeCursor) uint {
	if cursor == nil {
		return 0
	}
	return cursor.ID
}

func uintPtr(v uint) *uint {
	if v == 0 {
		return nil
	}
	copied := v
	return &copied
}

func followingTimelineKey(viewerAccountID uint) string {
	return fmt.Sprintf("feed:following:timeline:%d", viewerAccountID)
}

func authorOutboxKey(authorID uint) string {
	return fmt.Sprintf("feed:author:outbox:%d", authorID)
}

func followingPullCacheKey(viewerAccountID uint) string {
	return fmt.Sprintf("feed:following:pull:%d", viewerAccountID)
}

func (f *FeedService) rebuildGlobalTimeline(ctx context.Context) error {
	sfKey := "sf:fallback:global_timeline_rebuild"
	_, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
		dbVideos, err := f.repo.ListLatest(ctx, timelineRebuildLimit, time.Time{}, 0)
		if err != nil {
			return nil, err
		}
		if len(dbVideos) == 0 {
			return nil, nil
		}

		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		zElements := make([]redis.Z, 0, len(dbVideos))
		for _, vid := range dbVideos {
			zElements = append(zElements, redis.Z{
				Score:  float64(vid.CreateTime.UnixMilli()),
				Member: fmt.Sprintf("%d", vid.ID),
			})
		}
		if err := f.rediscache.Del(bgCtx, globalTimelineKey); err != nil {
			return nil, err
		}
		if err := f.rediscache.ZAdd(bgCtx, globalTimelineKey, zElements...); err != nil {
			return nil, err
		}
		return nil, f.rediscache.Expire(bgCtx, globalTimelineKey, followingTimelineTTL)
	})
	return err
}

func (f *FeedService) rebuildFollowingTimeline(ctx context.Context, viewerAccountID uint) error {
	sfKey := fmt.Sprintf("sf:following:timeline:rebuild:%d", viewerAccountID)
	_, err, _ := f.requestGroup.Do(sfKey, func() (interface{}, error) {
		dbVideos, err := f.repo.ListByFollowing(ctx, timelineRebuildLimit, viewerAccountID, time.Time{}, 0)
		if err != nil {
			return nil, err
		}

		bgCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		key := followingTimelineKey(viewerAccountID)
		if err := f.rediscache.Del(bgCtx, key); err != nil {
			return nil, err
		}
		if len(dbVideos) == 0 {
			return nil, nil
		}

		zElements := make([]redis.Z, 0, len(dbVideos))
		for _, vid := range dbVideos {
			zElements = append(zElements, redis.Z{
				Score:  float64(vid.CreateTime.UnixMilli()),
				Member: fmt.Sprintf("%d", vid.ID),
			})
		}
		if err := f.rediscache.ZAdd(bgCtx, key, zElements...); err != nil {
			return nil, err
		}
		return nil, f.rediscache.Expire(bgCtx, key, followingTimelineTTL)
	})
	return err
}

func (f *FeedService) loadTimelineVideos(ctx context.Context, key string, limit int, cursor *feedTimeCursor) ([]*video.Video, error) {
	if f.rediscache == nil {
		return nil, nil
	}

	maxScore := "+inf"
	if cursor != nil {
		maxScore = strconv.FormatInt(cursor.Time.UnixMilli(), 10)
	}
	rangeCount := int64(maxInt(limit*3, limit+10))
	videoIDsStr, err := f.rediscache.ZRevRangeByScore(ctx, key, maxScore, "-inf", 0, rangeCount)
	if err != nil {
		return nil, err
	}
	if len(videoIDsStr) == 0 {
		return []*video.Video{}, nil
	}

	videoIDs := make([]uint, 0, len(videoIDsStr))
	for _, idStr := range videoIDsStr {
		if id, parseErr := strconv.ParseUint(idStr, 10, 64); parseErr == nil {
			videoIDs = append(videoIDs, uint(id))
		}
	}

	videos, err := f.GetVideoByIDs(ctx, videoIDs)
	if err != nil {
		return nil, err
	}

	filtered := filterVideosByCursor(videos, cursor)
	if len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return filtered, nil
}

func (f *FeedService) loadAuthorOutboxCandidates(ctx context.Context, viewerAccountID uint, limit int, cursor *feedTimeCursor) ([]*video.Video, error) {
	if f.rediscache == nil || f.socialRepo == nil || viewerAccountID == 0 || limit <= 0 {
		return []*video.Video{}, nil
	}

	vloggerIDs, err := f.socialRepo.ListVloggerIDs(ctx, viewerAccountID)
	if err != nil || len(vloggerIDs) == 0 {
		return []*video.Video{}, err
	}

	keys := make([]string, 0, len(vloggerIDs))
	for _, vloggerID := range vloggerIDs {
		keys = append(keys, authorOutboxKey(vloggerID))
	}

	opCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()

	pullKey := followingPullCacheKey(viewerAccountID)
	exists, _ := f.rediscache.Exists(opCtx, pullKey)
	if !exists {
		if err := f.rediscache.ZUnionStore(opCtx, pullKey, keys, "MAX"); err != nil {
			return nil, err
		}
		ttl := f.runtimeCfg.FollowingPullCacheTTL
		if ttl <= 0 {
			ttl = 30 * time.Second
		}
		_ = f.rediscache.Expire(opCtx, pullKey, ttl)
	}

	maxScore := "+inf"
	if cursor != nil {
		maxScore = strconv.FormatInt(cursor.Time.UnixMilli(), 10)
	}

	rangeCount := int64(maxInt(limit*3, limit+10))
	videoIDsStr, err := f.rediscache.ZRevRangeByScore(opCtx, pullKey, maxScore, "-inf", 0, rangeCount)
	if err != nil || len(videoIDsStr) == 0 {
		return []*video.Video{}, err
	}

	videoIDs := make([]uint, 0, len(videoIDsStr))
	for _, idStr := range videoIDsStr {
		if id, parseErr := strconv.ParseUint(idStr, 10, 64); parseErr == nil {
			videoIDs = append(videoIDs, uint(id))
		}
	}

	videos, err := f.GetVideoByIDs(ctx, videoIDs)
	if err != nil {
		return nil, err
	}

	filtered := filterVideosByCursor(videos, cursor)
	if len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return filtered, nil
}

func filterVideosByCursor(videos []*video.Video, cursor *feedTimeCursor) []*video.Video {
	if cursor == nil {
		return videos
	}

	filtered := make([]*video.Video, 0, len(videos))
	for _, item := range videos {
		if item == nil {
			continue
		}
		if item.CreateTime.Before(cursor.Time) {
			filtered = append(filtered, item)
			continue
		}
		if cursor.ID > 0 && item.CreateTime.Equal(cursor.Time) && item.ID < cursor.ID {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func (f *FeedService) exposureSeen(ctx context.Context, viewerKey string, videoID uint) (bool, error) {
	opCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	key := exposureBloomKey(viewerKey, time.Now().UTC())
	for _, offset := range exposureBloomOffsets(viewerKey, videoID) {
		bit, err := f.rediscache.GetBit(opCtx, key, offset)
		if err != nil {
			return false, err
		}
		if bit == 0 {
			return false, nil
		}
	}
	return true, nil
}

func (f *FeedService) markExposure(ctx context.Context, viewerKey string, videoID uint) error {
	opCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	key := exposureBloomKey(viewerKey, time.Now().UTC())
	for _, offset := range exposureBloomOffsets(viewerKey, videoID) {
		if err := f.rediscache.SetBit(opCtx, key, offset, 1); err != nil {
			return err
		}
	}
	return f.rediscache.Expire(opCtx, key, exposureBloomTTL)
}

func exposureBloomKey(viewerKey string, now time.Time) string {
	return fmt.Sprintf("feed:exposure:bloom:%s:%s", viewerKey, now.Format("20060102"))
}

func exposureBloomOffsets(viewerKey string, videoID uint) []int64 {
	base := fmt.Sprintf("%s:%d", viewerKey, videoID)
	offsets := make([]int64, 0, len(exposureBloomSeeds))
	for _, seed := range exposureBloomSeeds {
		hasher := fnv.New64a()
		_, _ = hasher.Write([]byte(seed))
		_, _ = hasher.Write([]byte(base))
		offsets = append(offsets, int64(hasher.Sum64()%uint64(exposureBloomBits)))
	}
	return offsets
}

func (f *FeedService) markViewerActive(ctx context.Context, viewerAccountID uint) {
	if viewerAccountID == 0 || f.rediscache == nil {
		return
	}

	opCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()

	now := time.Now().UTC()
	_ = f.rediscache.ZAdd(opCtx, activeViewerZSetKey, redis.Z{
		Score:  float64(now.UnixMilli()),
		Member: strconv.FormatUint(uint64(viewerAccountID), 10),
	})
	_ = f.rediscache.ZRemRangeByScore(opCtx, activeViewerZSetKey, "-inf", strconv.FormatInt(now.Add(-activeViewerWindow).UnixMilli(), 10))
	_ = f.rediscache.Expire(opCtx, activeViewerZSetKey, 7*24*time.Hour)
}
