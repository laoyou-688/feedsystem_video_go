package video

import (
	"context"

	"feedsystem_video_go/internal/localcache"
	"feedsystem_video_go/internal/middleware/rabbitmq"
)

func invalidateVideoLocalCache(videoID uint) {
	if videoID == 0 {
		return
	}
	localcache.Delete(localcache.NamespaceVideoEntity, localcache.VideoEntityKey(videoID))
	localcache.Delete(localcache.NamespaceVideoDetail, localcache.VideoDetailKey(videoID))
}

func PublishVideoCacheInvalidation(ctx context.Context, mq *rabbitmq.LocalCacheMQ, videoID uint) {
	invalidateVideoLocalCache(videoID)
	localcache.PublishVideoInvalidation(ctx, mq, videoID)
}
