package video

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"strings"

	"gorm.io/gorm"
)

type CommentService struct {
	repo            *CommentRepository
	VideoRepository *VideoRepository
	cache           *rediscache.Client
	commentMQ       *rabbitmq.CommentMQ
	popularityMQ    *rabbitmq.PopularityMQ
	localCacheMQ    *rabbitmq.LocalCacheMQ
}

func NewCommentService(repo *CommentRepository, videoRepo *VideoRepository, cache *rediscache.Client, commentMQ *rabbitmq.CommentMQ, popularityMQ *rabbitmq.PopularityMQ, localCacheMQ *rabbitmq.LocalCacheMQ) *CommentService {
	return &CommentService{repo: repo, VideoRepository: videoRepo, cache: cache, commentMQ: commentMQ, popularityMQ: popularityMQ, localCacheMQ: localCacheMQ}
}

func (s *CommentService) Publish(ctx context.Context, comment *Comment) error {
	if comment == nil {
		return errors.New("comment is nil")
	}
	if err := EnsureCommentClientToken(comment); err != nil {
		return err
	}
	comment.Username = strings.TrimSpace(comment.Username)
	comment.Content = strings.TrimSpace(comment.Content)
	if comment.VideoID == 0 || comment.AuthorID == 0 {
		return errors.New("video_id and author_id are required")
	}
	if comment.Content == "" {
		return errors.New("content is required")
	}

	exists, err := s.VideoRepository.IsExist(ctx, comment.VideoID)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("video not found")
	}
	existingComment, err := s.repo.GetByClientToken(ctx, comment.ClientToken)
	if err != nil {
		return err
	}
	if existingComment != nil {
		comment.ID = existingComment.ID
		return nil
	}

	mysqlEnqueued := false
	redisEnqueued := false
	pendingStored := false
	if s.commentMQ != nil {
		if s.cache != nil {
			if err := StorePendingComment(ctx, s.cache, comment.ClientToken, PendingCommentMeta{
				AuthorID: comment.AuthorID,
				VideoID:  comment.VideoID,
			}); err == nil {
				pendingStored = true
			}
		}
		if err := s.commentMQ.Publish(ctx, comment.Username, comment.VideoID, comment.AuthorID, comment.Content, comment.ClientToken); err == nil {
			mysqlEnqueued = true
		} else if pendingStored {
			_ = ClearPendingComment(context.Background(), s.cache, comment.ClientToken)
		}
	}
	if s.popularityMQ != nil {
		if err := s.popularityMQ.Update(ctx, comment.VideoID, 1); err == nil {
			redisEnqueued = true
		}
	}
	if mysqlEnqueued && redisEnqueued {
		return nil
	}

	// Fallback: direct MySQL write when comment MQ publish fails.
	if !mysqlEnqueued {
		if err := s.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Select("id").First(&Video{}, comment.VideoID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return errors.New("video not found")
				}
				return err
			}
			if err := tx.Create(comment).Error; err != nil {
				return err
			}
			return tx.Model(&Video{}).Where("id = ?", comment.VideoID).
				UpdateColumn("popularity", gorm.Expr("popularity + 1")).Error
		}); err != nil {
			return err
		}
		_ = ClearPendingComment(context.Background(), s.cache, comment.ClientToken)
	}

	// Fallback: direct Redis update when popularity MQ publish fails.
	if !redisEnqueued {
		UpdatePopularityCache(ctx, s.cache, comment.VideoID, 1)
	}
	PublishVideoCacheInvalidation(ctx, s.localCacheMQ, comment.VideoID)
	return nil
}

func (s *CommentService) Delete(ctx context.Context, commentID uint, clientToken string, accountID uint) error {
	clientToken = strings.TrimSpace(clientToken)

	if commentID > 0 {
		comment, err := s.repo.GetByID(ctx, commentID)
		if err != nil {
			return err
		}
		if comment != nil {
			if comment.AuthorID != accountID {
				return errors.New("permission denied")
			}
			return s.deletePersistedComment(ctx, comment)
		}
	}

	if clientToken != "" {
		comment, err := s.repo.GetByClientToken(ctx, clientToken)
		if err != nil {
			return err
		}
		if comment != nil {
			if comment.AuthorID != accountID {
				return errors.New("permission denied")
			}
			return s.deletePersistedComment(ctx, comment)
		}

		pendingMeta, err := LoadPendingComment(ctx, s.cache, clientToken)
		if err != nil {
			return err
		}
		if pendingMeta != nil {
			if pendingMeta.AuthorID != accountID {
				return errors.New("permission denied")
			}
			if err := MarkCommentCanceled(ctx, s.cache, clientToken); err != nil {
				return err
			}
			redisEnqueued := false
			if s.popularityMQ != nil {
				if err := s.popularityMQ.Update(ctx, pendingMeta.VideoID, -1); err == nil {
					redisEnqueued = true
				}
			}
			if !redisEnqueued {
				UpdatePopularityCache(ctx, s.cache, pendingMeta.VideoID, -1)
			}
			PublishVideoCacheInvalidation(ctx, s.localCacheMQ, pendingMeta.VideoID)
			return nil
		}

		canceled, err := IsCommentCanceled(ctx, s.cache, clientToken)
		if err != nil {
			return err
		}
		if canceled {
			return nil
		}
	}

	return errors.New("comment not found")
}

func (s *CommentService) deletePersistedComment(ctx context.Context, comment *Comment) error {
	if comment == nil {
		return errors.New("comment not found")
	}

	mysqlEnqueued := false
	redisEnqueued := false
	if s.commentMQ != nil {
		if err := s.commentMQ.Delete(ctx, comment.ID); err == nil {
			mysqlEnqueued = true
		}
	}
	if s.popularityMQ != nil {
		if err := s.popularityMQ.Update(ctx, comment.VideoID, -1); err == nil {
			redisEnqueued = true
		}
	}
	if mysqlEnqueued && redisEnqueued {
		return nil
	}

	if !mysqlEnqueued {
		if err := s.repo.DeleteComment(ctx, comment); err != nil {
			return err
		}
		if err := s.VideoRepository.ChangePopularity(ctx, comment.VideoID, -1); err != nil {
			return err
		}
	}
	if !redisEnqueued {
		UpdatePopularityCache(ctx, s.cache, comment.VideoID, -1)
	}
	if err := MarkCommentCanceled(context.Background(), s.cache, comment.ClientToken); err != nil {
		return err
	}
	PublishVideoCacheInvalidation(ctx, s.localCacheMQ, comment.VideoID)
	return nil
}

func (s *CommentService) GetAll(ctx context.Context, videoID uint) ([]Comment, error) {
	exists, err := s.VideoRepository.IsExist(ctx, videoID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.New("video not found")
	}
	return s.repo.GetAllComments(ctx, videoID)
}
