package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type WorkerInfo struct {
	WorkerID     string    `json:"worker_id"`
	GRPCAddr     string    `json:"grpc_addr"`
	RegisteredAt time.Time `json:"registered_at"`
}

type WorkerRegistry struct {
	client  clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
	prefix  string
	ttl     int64

	mu      sync.RWMutex
	workers map[string]WorkerInfo
}

func NewWorkerRegistry(client *clientv3.Client, prefix string, leaseTTL time.Duration) *WorkerRegistry {
	ttlSeconds := int64(leaseTTL.Seconds())
	if ttlSeconds <= 0 {
		ttlSeconds = 10
	}
	return &WorkerRegistry{
		client:  client,
		lease:   client,
		watcher: client,
		prefix:  strings.TrimRight(prefix, "/"),
		ttl:     ttlSeconds,
		workers: make(map[string]WorkerInfo),
	}
}

func (r *WorkerRegistry) Register(ctx context.Context, info WorkerInfo, onLeaseStateChange func(ready bool)) error {
	key := path.Join(r.prefix, info.WorkerID)
	valueBytes, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal worker info failed: %w", err)
	}

	for ctx.Err() == nil {
		leaseResp, err := r.lease.Grant(ctx, r.ttl)
		if err != nil {
			if onLeaseStateChange != nil {
				onLeaseStateChange(false)
			}
			if waitErr := sleepContext(ctx, time.Second); waitErr != nil {
				return waitErr
			}
			continue
		}

		if _, err := r.client.Put(ctx, key, string(valueBytes), clientv3.WithLease(leaseResp.ID)); err != nil {
			if onLeaseStateChange != nil {
				onLeaseStateChange(false)
			}
			if waitErr := sleepContext(ctx, time.Second); waitErr != nil {
				return waitErr
			}
			continue
		}

		keepAliveCh, err := r.lease.KeepAlive(ctx, leaseResp.ID)
		if err != nil {
			if onLeaseStateChange != nil {
				onLeaseStateChange(false)
			}
			if waitErr := sleepContext(ctx, time.Second); waitErr != nil {
				return waitErr
			}
			continue
		}
		if onLeaseStateChange != nil {
			onLeaseStateChange(true)
		}

		for {
			select {
			case <-ctx.Done():
				if onLeaseStateChange != nil {
					onLeaseStateChange(false)
				}
				return ctx.Err()
			case _, ok := <-keepAliveCh:
				if !ok {
					if onLeaseStateChange != nil {
						onLeaseStateChange(false)
					}
					goto retry
				}
			}
		}
	retry:
		if waitErr := sleepContext(ctx, time.Second); waitErr != nil {
			return waitErr
		}
	}

	return ctx.Err()
}

func (r *WorkerRegistry) RunWatcher(ctx context.Context) error {
	if err := r.syncWorkers(ctx); err != nil {
		return err
	}

	watchCh := r.watcher.Watch(ctx, r.prefix+"/", clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp, ok := <-watchCh:
			if !ok {
				return nil
			}
			if err := resp.Err(); err != nil {
				if syncErr := r.syncWorkers(ctx); syncErr != nil {
					return syncErr
				}
				watchCh = r.watcher.Watch(ctx, r.prefix+"/", clientv3.WithPrefix())
				continue
			}
			for _, ev := range resp.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					var info WorkerInfo
					if err := json.Unmarshal(ev.Kv.Value, &info); err != nil {
						continue
					}
					r.mu.Lock()
					r.workers[info.WorkerID] = info
					r.mu.Unlock()
				case clientv3.EventTypeDelete:
					workerID := path.Base(string(ev.Kv.Key))
					r.mu.Lock()
					delete(r.workers, workerID)
					r.mu.Unlock()
				}
			}
		}
	}
}

func (r *WorkerRegistry) Workers() []WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	workers := make([]WorkerInfo, 0, len(r.workers))
	for _, info := range r.workers {
		workers = append(workers, info)
	}
	return workers
}

func (r *WorkerRegistry) Get(workerID string) (WorkerInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.workers[workerID]
	return info, ok
}

func (r *WorkerRegistry) syncWorkers(ctx context.Context) error {
	resp, err := r.client.Get(ctx, r.prefix+"/", clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("list workers failed: %w", err)
	}

	workers := make(map[string]WorkerInfo, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var info WorkerInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			continue
		}
		workers[info.WorkerID] = info
	}

	r.mu.Lock()
	r.workers = workers
	r.mu.Unlock()
	return nil
}
