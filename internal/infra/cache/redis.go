package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	redisgo "github.com/redis/go-redis/v9"

	"djs/internal/config"
	"djs/internal/domain"
)

const (
	workerSnapshotKeyPrefix  = "djs:worker_snapshot:"
	recentFailedInstancesKey = "djs:cache:recent_failed_instances"
)

type RedisCache struct {
	client      *redisgo.Client
	snapshotTTL time.Duration
	cacheTTL    time.Duration
}

type WorkerSnapshot struct {
	WorkerID       string    `json:"worker_id"`
	GRPCAddr       string    `json:"grpc_addr"`
	ActiveAttempts int       `json:"active_attempts"`
	LastSeenAt     time.Time `json:"last_seen_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func NewRedisCache(cfg config.RedisConfig) *RedisCache {
	if cfg.Addr == "" {
		return nil
	}

	return &RedisCache{
		client: redisgo.NewClient(&redisgo.Options{
			Addr:     cfg.Addr,
			Password: cfg.Password,
			DB:       cfg.DB,
		}),
		snapshotTTL: cfg.SnapshotTTL,
		cacheTTL:    cfg.CacheTTL,
	}
}

func (c *RedisCache) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}

func (c *RedisCache) Ping(ctx context.Context) error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Ping(ctx).Err()
}

func (c *RedisCache) PutWorkerSnapshot(ctx context.Context, snapshot WorkerSnapshot) error {
	if c == nil || c.client == nil {
		return nil
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal worker snapshot failed: %w", err)
	}
	return c.client.Set(ctx, workerSnapshotKey(snapshot.WorkerID), data, c.snapshotTTL).Err()
}

func (c *RedisCache) GetWorkerSnapshot(ctx context.Context, workerID string) (*WorkerSnapshot, bool, error) {
	if c == nil || c.client == nil {
		return nil, false, nil
	}

	raw, err := c.client.Get(ctx, workerSnapshotKey(workerID)).Bytes()
	if err != nil {
		if err == redisgo.Nil {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get worker snapshot failed: %w", err)
	}

	var snapshot WorkerSnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil, false, fmt.Errorf("unmarshal worker snapshot failed: %w", err)
	}
	return &snapshot, true, nil
}

func (c *RedisCache) PutRecentFailedInstances(ctx context.Context, instances []*domain.JobInstance) error {
	if c == nil || c.client == nil {
		return nil
	}

	data, err := json.Marshal(instances)
	if err != nil {
		return fmt.Errorf("marshal recent failed instances failed: %w", err)
	}
	return c.client.Set(ctx, recentFailedInstancesKey, data, c.cacheTTL).Err()
}

func (c *RedisCache) GetRecentFailedInstances(ctx context.Context) ([]*domain.JobInstance, bool, error) {
	if c == nil || c.client == nil {
		return nil, false, nil
	}

	raw, err := c.client.Get(ctx, recentFailedInstancesKey).Bytes()
	if err != nil {
		if err == redisgo.Nil {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get recent failed instances failed: %w", err)
	}

	var instances []*domain.JobInstance
	if err := json.Unmarshal(raw, &instances); err != nil {
		return nil, false, fmt.Errorf("unmarshal recent failed instances failed: %w", err)
	}
	return instances, true, nil
}

func workerSnapshotKey(workerID string) string {
	return workerSnapshotKeyPrefix + workerID
}
