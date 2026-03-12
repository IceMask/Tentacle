package redis

import (
	"context"
	"time"

	"mcp_for_appium/internal/config"
	"mcp_for_appium/internal/errors"

	"github.com/redis/go-redis/v9"
)

type Cache struct {
	client *redis.Client
}

// NewCache executes this operation.
func NewCache(cfg config.RedisConfig) (*Cache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, errors.Wrap(errors.CodeStoreConn, "failed to ping redis", err)
	}

	return &Cache{client: client}, nil
}

// Close executes this operation.
func (c *Cache) Close() error {
	return c.client.Close()
}

// CheckAndSet implements util.IdempotencyStore
func (c *Cache) CheckAndSet(ctx context.Context, key string, value string, ttl time.Duration) (bool, string, error) {
	// SET NX
	ok, err := c.client.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return false, "", err
	}
	if ok {
		return true, "", nil
	}

	// Key exists, get value
	val, err := c.client.Get(ctx, key).Result()
	if err != nil {
		return false, "", err
	}
	return false, val, nil
}

// AcquireConcurrencySlot executes this operation.
func (c *Cache) AcquireConcurrencySlot(ctx context.Context, key string, limit int) (bool, error) {
	// Simple implementation: INCR and check against limit.
	// Need to handle TTL for slots to avoid leaks if worker crashes?
	// Or use a sorted set for active slots?
	// Requirements say: "Redis key concurrency:{tenant}:{project}"

	// Lua script to increment only if below limit
	script := `
		local current = redis.call("GET", KEYS[1])
		if current == false then
			current = 0
		else
			current = tonumber(current)
		end
		
		if current < tonumber(ARGV[1]) then
			redis.call("INCR", KEYS[1])
			return 1
		else
			return 0
		end
	`

	res, err := c.client.Eval(ctx, script, []string{key}, limit).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// ReleaseConcurrencySlot executes this operation.
func (c *Cache) ReleaseConcurrencySlot(ctx context.Context, key string) error {
	// DECR, ensure not below 0
	script := `
		local current = redis.call("GET", KEYS[1])
		if current == false then
			return 0
		end
		current = tonumber(current)
		if current > 0 then
			redis.call("DECR", KEYS[1])
		end
		return 1
	`
	return c.client.Eval(ctx, script, []string{key}).Err()
}

// Redis Streams methods for Dispatcher

type StreamMessage struct {
	ID     string
	Values map[string]interface{}
}

// XAdd adds a message to a stream
func (c *Cache) XAdd(ctx context.Context, stream string, values map[string]interface{}, maxLen int64) (string, error) {
	args := &redis.XAddArgs{
		Stream: stream,
		MaxLen: maxLen,
		Approx: true,
		Values: values,
	}
	return c.client.XAdd(ctx, args).Result()
}

// XReadGroup reads from stream using consumer group
func (c *Cache) XReadGroup(ctx context.Context, group, consumer, stream, id string, count int64, block time.Duration) ([]StreamMessage, error) {
	args := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, id},
		Count:    count,
		Block:    block,
	}

	result, err := c.client.XReadGroup(ctx, args).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var messages []StreamMessage
	for _, stream := range result {
		for _, msg := range stream.Messages {
			messages = append(messages, StreamMessage{
				ID:     msg.ID,
				Values: msg.Values,
			})
		}
	}

	return messages, nil
}

// XAck acknowledges messages
func (c *Cache) XAck(ctx context.Context, stream, group, messageID string) error {
	return c.client.XAck(ctx, stream, group, messageID).Err()
}

// XAutoClaim claims messages idle for minIdleTime
func (c *Cache) XAutoClaim(ctx context.Context, stream, group, consumer string, minIdleTime time.Duration, start string, count int64) ([]StreamMessage, error) {
	args := &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdleTime,
		Start:    start,
		Count:    count,
	}

	messages, _, err := c.client.XAutoClaim(ctx, args).Result()
	if err != nil {
		return nil, err
	}

	var result []StreamMessage
	for _, msg := range messages {
		result = append(result, StreamMessage{
			ID:     msg.ID,
			Values: msg.Values,
		})
	}

	return result, nil
}

// XGroupCreateMkStream creates a consumer group, creating the stream if needed
func (c *Cache) XGroupCreateMkStream(ctx context.Context, stream, group string) error {
	return c.client.XGroupCreateMkStream(ctx, stream, group, "0").Err()
}

// Hash operations for WorkerRegistry

// HSet sets a hash field
func (c *Cache) HSet(ctx context.Context, key string, values map[string]interface{}) error {
	return c.client.HSet(ctx, key, values).Err()
}

// HGetAll gets all hash fields
func (c *Cache) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.client.HGetAll(ctx, key).Result()
}

// HDel deletes hash fields
func (c *Cache) HDel(ctx context.Context, key string, fields ...string) error {
	return c.client.HDel(ctx, key, fields...).Err()
}

// Expire sets key expiration
func (c *Cache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return c.client.Expire(ctx, key, ttl).Err()
}

// Keys returns keys matching pattern
func (c *Cache) Keys(ctx context.Context, pattern string) ([]string, error) {
	return c.client.Keys(ctx, pattern).Result()
}

// Get gets a key value
func (c *Cache) Get(ctx context.Context, key string) (string, error) {
	return c.client.Get(ctx, key).Result()
}

// Set sets a key value
func (c *Cache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}

// SetNX sets key if not exists
func (c *Cache) SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, value, ttl).Result()
}

// Del deletes one or more keys.
func (c *Cache) Del(ctx context.Context, keys ...string) error {
	return c.client.Del(ctx, keys...).Err()
}

// Incr increments a key and sets TTL on first creation; returns the new value.
func (c *Cache) Incr(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	pipe := c.client.TxPipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return incr.Val(), nil
}

// Ping checks connection
func (c *Cache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Publish publishes a message to a channel
func (c *Cache) Publish(ctx context.Context, channel string, message interface{}) error {
	return c.client.Publish(ctx, channel, message).Err()
}

// Subscribe subscribes to channels
func (c *Cache) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return c.client.Subscribe(ctx, channels...)
}
