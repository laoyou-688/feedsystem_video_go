package http

import (
	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/canal"
	"feedsystem_video_go/internal/config"
	"feedsystem_video_go/internal/feed"
	"feedsystem_video_go/internal/localcache"
	"feedsystem_video_go/internal/middleware/jwt"
	appobsmw "feedsystem_video_go/internal/middleware/observability"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	"feedsystem_video_go/internal/middleware/ratelimit"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/observability"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"feedsystem_video_go/internal/worker"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func SetRouter(db *gorm.DB, cache *rediscache.Client, rmq *rabbitmq.RabbitMQ, cfg config.Config, metrics *observability.Metrics) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(appobsmw.Metrics(metrics))
	r.Use(appobsmw.AccessLog())
	if err := r.SetTrustedProxies(nil); err != nil {
		log.Printf("SetTrustedProxies failed: %v", err)
	}
	r.Static("/static", "./.run/uploads")
	if cfg.ObservabilityConfig.Metrics.Enabled {
		metricsPath := cfg.ObservabilityConfig.Metrics.Path
		if metricsPath == "" {
			metricsPath = "/metrics"
		}
		r.GET(metricsPath, gin.WrapH(metrics.Handler()))
	}

	loginLimiter := ratelimit.Limit(cache, "account_login", 10, time.Minute, ratelimit.KeyByIP)
	registerLimiter := ratelimit.Limit(cache, "account_register", 5, time.Hour, ratelimit.KeyByIP)
	likeLimiter := ratelimit.Limit(cache, "like_write", 30, time.Minute, ratelimit.KeyByAccount)
	commentLimiter := ratelimit.Limit(cache, "comment_write", 10, time.Minute, ratelimit.KeyByAccount)
	socialLimiter := ratelimit.Limit(cache, "social_write", 20, time.Minute, ratelimit.KeyByAccount)

	videoRepository := video.NewVideoRepository(db)
	popularityMQ, err := rabbitmq.NewPopularityMQ(rmq)
	if err != nil {
		log.Printf("PopularityMQ init failed (mq disabled): %v", err)
		popularityMQ = nil
	}
	localCacheMQ, err := rabbitmq.NewLocalCacheMQ(rmq)
	if err != nil {
		log.Printf("LocalCacheMQ init failed (mq disabled): %v", err)
		localCacheMQ = nil
	} else {
		nodeID, hostErr := os.Hostname()
		if hostErr != nil || nodeID == "" {
			nodeID = "api-node"
		}
		if err := localcache.StartInvalidationConsumer(localCacheMQ, "cache.invalidate."+nodeID); err != nil {
			log.Printf("local cache invalidation consumer start failed: %v", err)
		}
	}

	accountRepository := account.NewAccountRepository(db)
	accountService := account.NewAccountService(accountRepository, cache, localCacheMQ)
	accountHandler := account.NewAccountHandler(accountService)
	accountGroup := r.Group("/account")
	{
		accountGroup.POST("/register", registerLimiter, accountHandler.CreateAccount)
		accountGroup.POST("/login", loginLimiter, accountHandler.Login)
		accountGroup.POST("/changePassword", accountHandler.ChangePassword)
		accountGroup.POST("/findByID", accountHandler.FindByID)
		accountGroup.POST("/findByUsername", accountHandler.FindByUsername)
	}
	protectedAccountGroup := accountGroup.Group("")
	protectedAccountGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedAccountGroup.POST("/logout", accountHandler.Logout)
		protectedAccountGroup.POST("/rename", accountHandler.Rename)
	}

	videoService := video.NewVideoService(videoRepository, cache, popularityMQ, localCacheMQ)
	videoHandler := video.NewVideoHandler(videoService, accountService)
	videoGroup := r.Group("/video")
	{
		videoGroup.POST("/listByAuthorID", videoHandler.ListByAuthorID)
		videoGroup.POST("/getDetail", videoHandler.GetDetail)
	}
	protectedVideoGroup := videoGroup.Group("")
	protectedVideoGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedVideoGroup.POST("/uploadVideo", videoHandler.UploadVideo)
		protectedVideoGroup.POST("/uploadCover", videoHandler.UploadCover)
		protectedVideoGroup.POST("/publish", videoHandler.PublishVideo)
	}

	likeMQ, err := rabbitmq.NewLikeMQ(rmq)
	if err != nil {
		log.Printf("LikeMQ init failed (mq disabled): %v", err)
		likeMQ = nil
	}
	likeRepository := video.NewLikeRepository(db)
	likeService := video.NewLikeService(likeRepository, videoRepository, cache, likeMQ, popularityMQ, localCacheMQ)
	likeHandler := video.NewLikeHandler(likeService)
	likeGroup := r.Group("/like")
	protectedLikeGroup := likeGroup.Group("")
	protectedLikeGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedLikeGroup.POST("/like", likeLimiter, likeHandler.Like)
		protectedLikeGroup.POST("/unlike", likeLimiter, likeHandler.Unlike)
		protectedLikeGroup.POST("/isLiked", likeHandler.IsLiked)
		protectedLikeGroup.POST("/listMyLikedVideos", likeHandler.ListMyLikedVideos)
	}

	commentRepository := video.NewCommentRepository(db)
	commentMQ, err := rabbitmq.NewCommentMQ(rmq)
	if err != nil {
		log.Printf("CommentMQ init failed (mq disabled): %v", err)
		commentMQ = nil
	}
	commentService := video.NewCommentService(commentRepository, videoRepository, cache, commentMQ, popularityMQ, localCacheMQ)
	commentHandler := video.NewCommentHandler(commentService, accountService)
	commentGroup := r.Group("/comment")
	{
		commentGroup.POST("/listAll", commentHandler.GetAllComments)
	}
	protectedCommentGroup := commentGroup.Group("")
	protectedCommentGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedCommentGroup.POST("/publish", commentLimiter, commentHandler.PublishComment)
		protectedCommentGroup.POST("/delete", commentLimiter, commentHandler.DeleteComment)
	}

	socialRepository := social.NewSocialRepository(db)
	socialService := social.NewSocialService(socialRepository, accountRepository)
	socialHandler := social.NewSocialHandler(socialService)
	socialGroup := r.Group("/social")
	protectedSocialGroup := socialGroup.Group("")
	protectedSocialGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedSocialGroup.POST("/follow", socialLimiter, socialHandler.Follow)
		protectedSocialGroup.POST("/unfollow", socialLimiter, socialHandler.Unfollow)
		protectedSocialGroup.POST("/getAllFollowers", socialHandler.GetAllFollowers)
		protectedSocialGroup.POST("/getAllVloggers", socialHandler.GetAllVloggers)
	}

	feedRepository := feed.NewFeedRepository(db)
	feedService := feed.NewFeedService(feedRepository, socialRepository, likeRepository, cache, feed.NewFeedRuntimeConfig(cfg.Feed))
	feedHandler := feed.NewFeedHandler(feedService)
	feedGroup := r.Group("/feed")
	feedGroup.Use(jwt.SoftJWTAuth(accountRepository, cache))
	{
		feedGroup.POST("/listLatest", feedHandler.ListLatest)
		feedGroup.POST("/listLikesCount", feedHandler.ListLikesCount)
		feedGroup.POST("/listByPopularity", feedHandler.ListByPopularity)
	}
	protectedFeedGroup := feedGroup.Group("")
	protectedFeedGroup.Use(jwt.JWTAuth(accountRepository, cache))
	{
		protectedFeedGroup.POST("/listByFollowing", feedHandler.ListByFollowing)
	}

	timelineMQ, err := rabbitmq.NewTimelineMQ(rmq)
	if err != nil {
		log.Printf("timelineMQ init failed (mq disabled): %v", err)
		timelineMQ = nil
	}
	canalService := canal.NewService(cfg.Canal, cache, timelineMQ, socialRepository, localCacheMQ)
	canalHandler := canal.NewHandler(canalService)
	if canalService.Enabled() {
		internalGroup := r.Group("/internal")
		{
			internalGroup.POST("/canal/sync", canalHandler.Sync)
		}
	}
	if timelineMQ != nil {
		worker.StartOutboxPoller(db, timelineMQ)
		worker.StartConsumer(timelineMQ, "video.timeline.update.queue", cache, socialRepository, worker.NewTimelineFanoutConfig(cfg.Feed))
	}
	return r
}
