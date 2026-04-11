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

	"djs/internal/domain"
)

type LeaderInfo struct {
	MasterID  string    `json:"master_id"`
	GRPCAddr  string    `json:"grpc_addr"`
	ElectedAt time.Time `json:"elected_at"`
}

type LeadershipEvent struct {
	IsLeader bool
	Info     LeaderInfo
}

type Election struct {
	client *clientv3.Client
	key    string
	ttl    int64
	self   LeaderInfo
	events chan LeadershipEvent
	mu     sync.RWMutex
	leader bool
}

func NewElection(client *clientv3.Client, prefix string, self LeaderInfo, leaseTTL time.Duration) *Election {
	ttlSeconds := int64(leaseTTL.Seconds())
	if ttlSeconds <= 0 {
		ttlSeconds = 10
	}
	return &Election{
		client: client,
		key:    path.Join(strings.TrimRight(prefix, "/"), "current"),
		ttl:    ttlSeconds,
		self:   self,
		events: make(chan LeadershipEvent, 8),
	}
}

func (e *Election) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		leaseResp, err := e.client.Grant(ctx, e.ttl)
		if err != nil {
			if waitErr := sleepContext(ctx, time.Second); waitErr != nil {
				return waitErr
			}
			continue
		}

		info := e.self
		info.ElectedAt = time.Now().UTC()
		valueBytes, err := json.Marshal(info)
		if err != nil {
			return fmt.Errorf("marshal leader info failed: %w", err)
		}

		txnResp, err := e.client.Txn(ctx).
			If(clientv3.Compare(clientv3.CreateRevision(e.key), "=", 0)).
			Then(clientv3.OpPut(e.key, string(valueBytes), clientv3.WithLease(leaseResp.ID))).
			Else(clientv3.OpGet(e.key)).
			Commit()
		if err != nil {
			if waitErr := sleepContext(ctx, time.Second); waitErr != nil {
				return waitErr
			}
			continue
		}

		if txnResp.Succeeded {
			e.setLeader(true, info)
			keepAliveCh, err := e.client.KeepAlive(ctx, leaseResp.ID)
			if err != nil {
				e.setLeader(false, LeaderInfo{})
				if waitErr := sleepContext(ctx, time.Second); waitErr != nil {
					return waitErr
				}
				continue
			}
			for {
				select {
				case <-ctx.Done():
					e.setLeader(false, LeaderInfo{})
					return ctx.Err()
				case _, ok := <-keepAliveCh:
					if !ok {
						e.setLeader(false, LeaderInfo{})
						goto retry
					}
				}
			}
		}

		e.setLeader(false, LeaderInfo{})
		if err := e.waitForLeaderChange(ctx); err != nil {
			return err
		}
	retry:
		if waitErr := sleepContext(ctx, time.Second); waitErr != nil {
			return waitErr
		}
	}
	return ctx.Err()
}

func (e *Election) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.leader
}

func (e *Election) Events() <-chan LeadershipEvent {
	return e.events
}

func (e *Election) setLeader(isLeader bool, info LeaderInfo) {
	e.mu.Lock()
	changed := e.leader != isLeader
	e.leader = isLeader
	e.mu.Unlock()

	if changed || isLeader {
		select {
		case e.events <- LeadershipEvent{IsLeader: isLeader, Info: info}:
		default:
		}
	}
}

func (e *Election) waitForLeaderChange(ctx context.Context) error {
	resp, err := e.client.Get(ctx, e.key)
	if err != nil {
		return fmt.Errorf("get current leader failed: %w", err)
	}
	revision := resp.Header.Revision + 1
	watchCh := e.client.Watch(ctx, e.key, clientv3.WithRev(revision))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case watchResp, ok := <-watchCh:
			if !ok {
				return nil
			}
			if watchResp.Err() != nil {
				return nil
			}
			if len(watchResp.Events) > 0 {
				return nil
			}
		}
	}
}

type LeaderResolver struct {
	client *clientv3.Client
	key    string
}

func NewLeaderResolver(client *clientv3.Client, prefix string) *LeaderResolver {
	return &LeaderResolver{
		client: client,
		key:    path.Join(strings.TrimRight(prefix, "/"), "current"),
	}
}

func (r *LeaderResolver) Current(ctx context.Context) (LeaderInfo, error) {
	resp, err := r.client.Get(ctx, r.key)
	if err != nil {
		return LeaderInfo{}, fmt.Errorf("get leader failed: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return LeaderInfo{}, fmt.Errorf("%w", domain.ErrNoLeader)
	}

	var info LeaderInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &info); err != nil {
		return LeaderInfo{}, fmt.Errorf("unmarshal leader failed: %w", err)
	}
	return info, nil
}
