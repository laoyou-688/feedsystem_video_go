package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func (c *Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	return c.rdb.Get(ctx, key).Bytes()
}

func (c *Client) SetBytes(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.rdb.Set(ctx, key, value, ttl).Err()
}

func (c *Client) SetNXBytes(ctx context.Context, key string, value []byte, ttl time.Duration) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, nil
	}
	return c.rdb.SetNX(ctx, key, value, ttl).Result()
}

func (c *Client) Del(ctx context.Context, key string) error {
	return c.rdb.Del(ctx, key).Err()
}

func (c *Client) DelMany(ctx context.Context, keys ...string) error {
	if c == nil || c.rdb == nil || len(keys) == 0 {
		return nil
	}
	valid := make([]string, 0, len(keys))
	for _, key := range keys {
		if key != "" {
			valid = append(valid, key)
		}
	}
	if len(valid) == 0 {
		return nil
	}
	return c.rdb.Del(ctx, valid...).Err()
}

func (c *Client) MGet(cacheCtx context.Context, cacheKeys ...string) ([]interface{}, error) {
	return c.rdb.MGet(cacheCtx, cacheKeys...).Result()
}

func (c *Client) SetBit(ctx context.Context, key string, offset int64, value int) error {
	if c == nil || c.rdb == nil {
		return nil
	}
	return c.rdb.SetBit(ctx, key, offset, value).Err()
}

func (c *Client) GetBit(ctx context.Context, key string, offset int64) (int64, error) {
	if c == nil || c.rdb == nil {
		return 0, nil
	}
	return c.rdb.GetBit(ctx, key, offset).Result()
}

func (c *Client) GetBits(ctx context.Context, key string, offsets []int64) ([]int64, error) {
	if c == nil || c.rdb == nil {
		return make([]int64, len(offsets)), nil
	}
	if len(offsets) == 0 {
		return []int64{}, nil
	}

	pipe := c.rdb.Pipeline()
	cmds := make([]*goredis.IntCmd, len(offsets))
	for i, offset := range offsets {
		cmds[i] = pipe.GetBit(ctx, key, offset)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, err
	}

	results := make([]int64, len(offsets))
	for i, cmd := range cmds {
		v, err := cmd.Result()
		if err != nil && err != goredis.Nil {
			return nil, err
		}
		results[i] = v
	}
	return results, nil
}

func (c *Client) SetBitsWithExpire(ctx context.Context, key string, offsets []int64, value int, ttl time.Duration) error {
	if c == nil || c.rdb == nil || len(offsets) == 0 {
		return nil
	}

	pipe := c.rdb.Pipeline()
	for _, offset := range offsets {
		pipe.SetBit(ctx, key, offset, value)
	}
	if ttl > 0 {
		pipe.Expire(ctx, key, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}
