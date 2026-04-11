package master

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"djs/internal/config"
	"djs/internal/domain"
	cacheinfra "djs/internal/infra/cache"
	loggerinfra "djs/internal/infra/logger"
	messaginginfra "djs/internal/infra/messaging"
	"djs/internal/observability"
	registryetcd "djs/internal/registry/etcd"
	"djs/internal/repository"
	transportgrpc "djs/internal/transport/grpc"
)

const (
	defaultTimezone   = "Asia/Shanghai"
	maxSlotIterations = 600000
	defaultBatchLimit = 100
)

type Service struct {
	cfg          *config.Config
	store        repository.Store
	workers      *registryetcd.WorkerRegistry
	election     *registryetcd.Election
	workerClient *transportgrpc.WorkerClient
	logger       *loggerinfra.Logger
	metrics      *observability.MasterMetrics
	readiness    *observability.Readiness
	producer     messaginginfra.Producer
	cache        *cacheinfra.RedisCache
	parser       cron.Parser
	now          func() time.Time
	stateMu      sync.RWMutex
	previousLeaderID string
}

type CreateJobInput struct {
	Name                string
	CronExpr            string
	Timezone            string
	Payload             []byte
	TimeoutSeconds      uint32
	MaxRetries          uint32
	RetryBackoffSeconds uint32
	AllowConcurrent     bool
	Status              string
}

func NewService(
	cfg *config.Config,
	store repository.Store,
	workers *registryetcd.WorkerRegistry,
	election *registryetcd.Election,
	workerClient *transportgrpc.WorkerClient,
	logger *loggerinfra.Logger,
	metrics *observability.MasterMetrics,
	readiness *observability.Readiness,
	producer messaginginfra.Producer,
	cache *cacheinfra.RedisCache,
) *Service {
	if logger == nil {
		logger = loggerinfra.NewCommandLogger("djs", "master", "unknown", nil)
	}
	return &Service{
		cfg:          cfg,
		store:        store,
		workers:      workers,
		election:     election,
		workerClient: workerClient,
		logger:       logger,
		metrics:      metrics,
		readiness:    readiness,
		producer:     producer,
		cache:        cache,
		parser: cron.NewParser(
			cron.Minute |
				cron.Hour |
				cron.Dom |
				cron.Month |
				cron.Dow |
				cron.Descriptor,
		),
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func (s *Service) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.readiness.Set("worker_watcher", true)
		if err := s.workers.RunWatcher(ctx); err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Error("worker_watcher_stopped", "worker watcher stopped", err, nil)
		}
		s.readiness.Set("worker_watcher", false)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.readiness.Set("election_loop", true)
		if err := s.election.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Error("election_loop_stopped", "election loop stopped", err, nil)
		}
		s.readiness.Set("election_loop", false)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runCreateLoop(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runDispatchLoop(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runReconcileLoop(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runMetricsLoop(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runOutboxRelayLoop(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runWorkerSnapshotLoop(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case event := <-s.election.Events():
			if event.IsLeader {
				if s.metrics != nil {
					s.metrics.MasterIsLeader.Set(1)
					s.metrics.LeaderTransitionsTotal.WithLabelValues("acquired").Inc()
				}
				s.setPreviousLeaderID("")
				s.logger.Info("leader_acquired", "leadership acquired", loggerinfra.Fields{
					"leader":    true,
					"worker_id": "",
					"msg":       "leadership acquired",
					"leader_id": s.cfg.App.ID,
				})
				go s.onLeadershipAcquired(ctx, event.Info)
				continue
			}
			if s.metrics != nil {
				s.metrics.MasterIsLeader.Set(0)
				s.metrics.LeaderTransitionsTotal.WithLabelValues("lost").Inc()
			}
			s.setPreviousLeaderID(s.cfg.App.ID)
			s.logger.Warn("leader_lost", "leadership lost", loggerinfra.Fields{
				"leader":    false,
				"leader_id": s.cfg.App.ID,
			})
		}
	}
}

func (s *Service) onLeadershipAcquired(ctx context.Context, info registryetcd.LeaderInfo) {
	if err := s.enqueueLeaderFailoverEvent(ctx, info); err != nil {
		s.logger.Error("outbox_enqueued", "enqueue leader failover event failed", err, loggerinfra.Fields{
			"leader_id": s.cfg.App.ID,
			"event_type": domain.EventTypeLeaderFailoverHappened,
		})
	}
	if _, err := s.MaterializeDueInstances(ctx, s.now().UTC(), s.batchLimit()); err != nil {
		s.logger.Error("create_cycle", "leader backfill materialize failed", err, nil)
	}
	if err := s.DispatchPending(ctx); err != nil {
		s.logger.Error("dispatch_cycle", "leader dispatch failed", err, nil)
	}
	if err := s.Reconcile(ctx); err != nil {
		s.logger.Error("reconcile_cycle", "leader reconcile failed", err, nil)
	}
}

func (s *Service) batchLimit() int {
	if s.cfg != nil && s.cfg.Scheduling.BatchSize > 0 {
		return s.cfg.Scheduling.BatchSize
	}
	return defaultBatchLimit
}

func (s *Service) isLeader() bool {
	return s.election != nil && s.election.IsLeader()
}

func (s *Service) ensureLeader() error {
	if !s.isLeader() {
		return domain.ErrNotLeader
	}
	return nil
}

func (s *Service) runCreateLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Scheduling.CreateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.isLeader() {
				continue
			}
			start := time.Now()
			instances, err := s.MaterializeDueInstances(ctx, s.now().UTC(), s.batchLimit())
			if s.metrics != nil {
				s.metrics.ObserveCreateCycle(time.Since(start))
			}
			if err != nil {
				s.logger.Error("create_cycle", "create loop failed", err, nil)
				continue
			}
			s.logger.Info("create_cycle", "create loop completed", loggerinfra.Fields{
				"created_count": len(instances),
			})
		}
	}
}

func (s *Service) runDispatchLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Scheduling.DispatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.isLeader() {
				continue
			}
			start := time.Now()
			err := s.DispatchPending(ctx)
			if s.metrics != nil {
				s.metrics.ObserveDispatchCycle(time.Since(start))
			}
			if err != nil {
				s.logger.Error("dispatch_cycle", "dispatch loop failed", err, nil)
			}
		}
	}
}

func (s *Service) runReconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Scheduling.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.isLeader() {
				continue
			}
			if err := s.Reconcile(ctx); err != nil {
				s.logger.Error("reconcile_cycle", "reconcile loop failed", err, nil)
			}
		}
	}
}

func (s *Service) runMetricsLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Scheduling.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refreshMetrics(ctx)
		}
	}
}

func (s *Service) runOutboxRelayLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Messaging.RelayInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.isLeader() || s.producer == nil {
				continue
			}
			if err := s.relayPendingOutbox(ctx); err != nil {
				s.logger.Error("outbox_publish_failed", "relay pending outbox failed", err, loggerinfra.Fields{
					"leader_id": s.cfg.App.ID,
				})
			}
		}
	}
}

func (s *Service) runWorkerSnapshotLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Scheduling.DispatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.isLeader() || s.cache == nil {
				continue
			}
			if err := s.refreshWorkerSnapshots(ctx); err != nil {
				s.logger.Error("redis_snapshot_refreshed", "refresh worker snapshots failed", err, loggerinfra.Fields{
					"leader_id": s.cfg.App.ID,
				})
			}
		}
	}
}

func (s *Service) refreshMetrics(ctx context.Context) {
	if s.metrics == nil {
		return
	}

	if s.isLeader() {
		s.metrics.MasterIsLeader.Set(1)
	} else {
		s.metrics.MasterIsLeader.Set(0)
	}
	s.metrics.WorkersOnline.Set(float64(len(s.workers.Workers())))

	if pending, err := s.store.Instances().CountPending(ctx); err == nil {
		s.metrics.InstancesPending.Set(float64(pending))
	} else {
		s.logger.Error("metrics_refresh_failed", "count pending instances failed", err, nil)
	}
	if running, err := s.store.Instances().CountRunning(ctx); err == nil {
		s.metrics.InstancesRunning.Set(float64(running))
	} else {
		s.logger.Error("metrics_refresh_failed", "count running instances failed", err, nil)
	}
	if active, err := s.store.Attempts().CountActive(ctx); err == nil {
		s.metrics.AttemptsActive.Set(float64(active))
	} else {
		s.logger.Error("metrics_refresh_failed", "count active attempts failed", err, nil)
	}
}

func (s *Service) requireLeaderRPC() error {
	if err := s.ensureLeader(); err != nil {
		return grpcstatus.Error(codes.FailedPrecondition, err.Error())
	}
	return nil
}

func unixMS(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}

func sortWorkers(workers []registryetcd.WorkerInfo) {
	sort.Slice(workers, func(i, j int) bool {
		return workers[i].WorkerID < workers[j].WorkerID
	})
}

func (s *Service) setPreviousLeaderID(id string) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.previousLeaderID = id
}

func (s *Service) getPreviousLeaderID() string {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.previousLeaderID
}
