package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb *redis.Client
}

func NewClient(addr string, password string, db int) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &Client{rdb: rdb}, nil
}

func (c *Client) Close() error {
	return c.rdb.Close()
}

// StreamAdd adds a message to a stream
func (c *Client) StreamAdd(ctx context.Context, stream string, values map[string]interface{}) (string, error) {
	return c.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}).Result()
}

// StreamReadGroup reads messages from a stream group
func (c *Client) StreamReadGroup(ctx context.Context, stream, group, consumer string, count int64, block time.Duration) ([]redis.XStream, error) {
	return c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
}

// StreamAck acknowledges a message
func (c *Client) StreamAck(ctx context.Context, stream, group string, id string) error {
	return c.rdb.XAck(ctx, stream, group, id).Err()
}

// HashSet sets a field in a hash
func (c *Client) HashSet(ctx context.Context, key string, field string, value interface{}) error {
	return c.rdb.HSet(ctx, key, field, value).Err()
}

// HashGet gets a field from a hash
func (c *Client) HashGet(ctx context.Context, key string, field string) (string, error) {
	return c.rdb.HGet(ctx, key, field).Result()
}

// SetNX sets a key if it does not exist
func (c *Client) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	return c.rdb.SetNX(ctx, key, value, expiration).Result()
}

// Get returns a value
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.rdb.Get(ctx, key).Result()
}
