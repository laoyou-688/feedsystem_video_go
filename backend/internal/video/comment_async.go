package video

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
)

const (
	pendingCommentTTL  = 15 * time.Minute
	canceledCommentTTL = 15 * time.Minute
)

type PendingCommentMeta struct {
	AuthorID uint `json:"author_id"`
	VideoID  uint `json:"video_id"`
}

func EnsureCommentClientToken(comment *Comment) error {
	if comment == nil {
		return errors.New("comment is nil")
	}
	comment.ClientToken = strings.TrimSpace(comment.ClientToken)
	if comment.ClientToken != "" {
		return nil
	}
	token, err := newCommentClientToken(16)
	if err != nil {
		return err
	}
	comment.ClientToken = token
	return nil
}

func StorePendingComment(ctx context.Context, cache *rediscache.Client, token string, meta PendingCommentMeta) error {
	token = strings.TrimSpace(token)
	if cache == nil || token == "" {
		return nil
	}
	payload, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return cache.SetBytes(ctx, pendingCommentKey(token), payload, pendingCommentTTL)
}

func LoadPendingComment(ctx context.Context, cache *rediscache.Client, token string) (*PendingCommentMeta, error) {
	token = strings.TrimSpace(token)
	if cache == nil || token == "" {
		return nil, nil
	}
	payload, err := cache.GetBytes(ctx, pendingCommentKey(token))
	if err != nil {
		if rediscache.IsMiss(err) {
			return nil, nil
		}
		return nil, err
	}
	var meta PendingCommentMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func ClearPendingComment(ctx context.Context, cache *rediscache.Client, token string) error {
	token = strings.TrimSpace(token)
	if cache == nil || token == "" {
		return nil
	}
	return cache.Del(ctx, pendingCommentKey(token))
}

func MarkCommentCanceled(ctx context.Context, cache *rediscache.Client, token string) error {
	token = strings.TrimSpace(token)
	if cache == nil || token == "" {
		return nil
	}
	return cache.SetBytes(ctx, canceledCommentKey(token), []byte("1"), canceledCommentTTL)
}

func IsCommentCanceled(ctx context.Context, cache *rediscache.Client, token string) (bool, error) {
	token = strings.TrimSpace(token)
	if cache == nil || token == "" {
		return false, nil
	}
	_, err := cache.GetBytes(ctx, canceledCommentKey(token))
	if err != nil {
		if rediscache.IsMiss(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func pendingCommentKey(token string) string {
	return "comment:pending:" + token
}

func canceledCommentKey(token string) string {
	return "comment:cancel:" + token
}

func newCommentClientToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
