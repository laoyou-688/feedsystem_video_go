package video

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	rediscache "feedsystem_video_go/internal/middleware/redis"
)

const likeRequestTTL = 15 * time.Minute

func EnsureLikeClientToken(like *Like) error {
	if like == nil {
		return nil
	}
	like.ClientToken = strings.TrimSpace(like.ClientToken)
	if like.ClientToken != "" {
		return nil
	}
	token, err := newLikeClientToken(16)
	if err != nil {
		return err
	}
	like.ClientToken = token
	return nil
}

func ClaimLikeRequest(ctx context.Context, cache *rediscache.Client, action string, accountID, videoID uint, clientToken string) (bool, error) {
	clientToken = strings.TrimSpace(clientToken)
	if cache == nil || clientToken == "" || action == "" || accountID == 0 || videoID == 0 {
		return true, nil
	}
	return cache.SetNXBytes(ctx, likeRequestKey(action, accountID, videoID, clientToken), []byte("1"), likeRequestTTL)
}

func likeRequestKey(action string, accountID, videoID uint, clientToken string) string {
	return fmt.Sprintf("like:req:%s:%d:%d:%s", action, accountID, videoID, clientToken)
}

func newLikeClientToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
