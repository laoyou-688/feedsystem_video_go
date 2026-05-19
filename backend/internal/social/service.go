package social

import (
	"context"
	"errors"
	"feedsystem_video_go/internal/account"
	"feedsystem_video_go/internal/video"
	"time"

	"gorm.io/gorm"
)

type SocialService struct {
	repo        *SocialRepository
	accountrepo *account.AccountRepository
}

func NewSocialService(repo *SocialRepository, accountrepo *account.AccountRepository) *SocialService {
	return &SocialService{repo: repo, accountrepo: accountrepo}
}

func (s *SocialService) Follow(ctx context.Context, social *Social) error {
	_, err := s.accountrepo.FindByID(ctx, social.FollowerID)
	if err != nil {
		return err
	}
	_, err = s.accountrepo.FindByID(ctx, social.VloggerID)
	if err != nil {
		return err
	}
	if social.FollowerID == social.VloggerID {
		return errors.New("can not follow self")
	}
	isFollowed, err := s.repo.IsFollowed(ctx, social)
	if err != nil {
		return err
	}
	if isFollowed {
		return errors.New("already followed")
	}
	return s.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(social).Error; err != nil {
			return err
		}
		msg := video.OutboxMsg{
			FollowerID: social.FollowerID,
			VloggerID:  social.VloggerID,
			EventType:  "social_follow",
			Status:     "pending",
			CreateTime: time.Now(),
		}
		return tx.Create(&msg).Error
	})
}

func (s *SocialService) Unfollow(ctx context.Context, social *Social) error {
	_, err := s.accountrepo.FindByID(ctx, social.FollowerID)
	if err != nil {
		return err
	}
	_, err = s.accountrepo.FindByID(ctx, social.VloggerID)
	if err != nil {
		return err
	}
	isFollowed, err := s.repo.IsFollowed(ctx, social)
	if err != nil {
		return err
	}
	if !isFollowed {
		return errors.New("not followed")
	}
	return s.repo.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		del := tx.Where("follower_id = ? AND vlogger_id = ?", social.FollowerID, social.VloggerID).Delete(&Social{})
		if del.Error != nil {
			return del.Error
		}
		if del.RowsAffected == 0 {
			return errors.New("not followed")
		}
		msg := video.OutboxMsg{
			FollowerID: social.FollowerID,
			VloggerID:  social.VloggerID,
			EventType:  "social_unfollow",
			Status:     "pending",
			CreateTime: time.Now(),
		}
		return tx.Create(&msg).Error
	})
}

func (s *SocialService) GetAllFollowers(ctx context.Context, VloggerID uint) ([]*account.Account, error) {
	_, err := s.accountrepo.FindByID(ctx, VloggerID)
	if err != nil {
		return nil, err
	}
	return s.repo.GetAllFollowers(ctx, VloggerID)
}

func (s *SocialService) GetAllVloggers(ctx context.Context, FollowerID uint) ([]*account.Account, error) {
	_, err := s.accountrepo.FindByID(ctx, FollowerID)
	if err != nil {
		return nil, err
	}
	return s.repo.GetAllVloggers(ctx, FollowerID)
}

func (s *SocialService) IsFollowed(ctx context.Context, social *Social) (bool, error) {
	_, err := s.accountrepo.FindByID(ctx, social.FollowerID)
	if err != nil {
		return false, err
	}
	_, err = s.accountrepo.FindByID(ctx, social.VloggerID)
	if err != nil {
		return false, err
	}
	return s.repo.IsFollowed(ctx, social)
}
