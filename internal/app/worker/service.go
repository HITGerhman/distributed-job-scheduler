package worker

import (
	"context"
	"sync"
	"time"

	"djs/internal/config"
	"djs/internal/domain"
	loggerinfra "djs/internal/infra/logger"
	"djs/internal/observability"
	registryetcd "djs/internal/registry/etcd"
	transportgrpc "djs/internal/transport/grpc"

	oteltrace "go.opentelemetry.io/otel/trace"
)

type attemptKey struct {
	InstanceID uint64
	AttemptNo  uint32
}

type executionHandle struct {
	key      attemptKey
	workerID string
	payload  *domain.TaskPayload
	timeout  time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	traceCtx context.Context

	mu               sync.Mutex
	startedAt        time.Time
	finished         bool
	killRequested    bool
	timeoutTriggered bool
	pid              int
	pgid             int
}

type executionManager struct {
	mu      sync.Mutex
	handles map[attemptKey]*executionHandle
}

type Service struct {
	cfg          *config.Config
	registry     *registryetcd.WorkerRegistry
	resolver     *registryetcd.LeaderResolver
	masterClient *transportgrpc.MasterClient
	logger       *loggerinfra.Logger
	metrics      *observability.WorkerMetrics
	readiness    *observability.Readiness
	now          func() time.Time

	runCtx  context.Context
	runMu   sync.RWMutex
	manager *executionManager
}

func NewService(
	cfg *config.Config,
	registry *registryetcd.WorkerRegistry,
	resolver *registryetcd.LeaderResolver,
	masterClient *transportgrpc.MasterClient,
	logger *loggerinfra.Logger,
	metrics *observability.WorkerMetrics,
	readiness *observability.Readiness,
) *Service {
	if logger == nil {
		logger = loggerinfra.NewCommandLogger("djs", "worker", "unknown", nil)
	}
	return &Service{
		cfg:          cfg,
		registry:     registry,
		resolver:     resolver,
		masterClient: masterClient,
		logger:       logger,
		metrics:      metrics,
		readiness:    readiness,
		now: func() time.Time {
			return time.Now().UTC()
		},
		manager: &executionManager{
			handles: make(map[attemptKey]*executionHandle),
		},
	}
}

func (s *Service) Run(ctx context.Context) error {
	s.runMu.Lock()
	s.runCtx = ctx
	s.runMu.Unlock()

	return s.registry.Register(ctx, registryetcd.WorkerInfo{
		WorkerID:     s.cfg.App.ID,
		GRPCAddr:     s.cfg.GRPC.WorkerAdvertise,
		RegisteredAt: s.now().UTC(),
	}, func(ready bool) {
		s.readiness.Set("etcd_registration", ready)
	})
}

func (s *Service) baseContext() context.Context {
	s.runMu.RLock()
	defer s.runMu.RUnlock()
	if s.runCtx != nil {
		return s.runCtx
	}
	return context.Background()
}

func (m *executionManager) Add(handle *executionHandle) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.handles[handle.key]; exists {
		return false
	}
	m.handles[handle.key] = handle
	return true
}

func (m *executionManager) Get(key attemptKey) (*executionHandle, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	handle, ok := m.handles[key]
	return handle, ok
}

func (m *executionManager) Remove(key attemptKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.handles, key)
}

func (h *executionHandle) markStarted(startedAt time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.startedAt = startedAt
}

func (h *executionHandle) setProcess(pid int, pgid int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pid = pid
	h.pgid = pgid
}

func (h *executionHandle) markFinished() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.finished {
		return
	}
	h.finished = true
	close(h.done)
}

func (h *executionHandle) requestKill(timedOut bool) (pid int, pgid int, shouldSignal bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.finished {
		return 0, 0, false
	}
	if timedOut {
		if h.timeoutTriggered {
			return h.pid, h.pgid, false
		}
		h.timeoutTriggered = true
	} else {
		if h.killRequested {
			return h.pid, h.pgid, false
		}
		h.killRequested = true
	}
	return h.pid, h.pgid, true
}

func (h *executionHandle) flags() (killRequested bool, timeoutTriggered bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.killRequested, h.timeoutTriggered
}

func contextWithTrace(base context.Context, traceCtx context.Context) context.Context {
	if base == nil {
		base = context.Background()
	}
	if traceCtx == nil {
		return base
	}
	spanCtx := oteltrace.SpanContextFromContext(traceCtx)
	if !spanCtx.IsValid() {
		return base
	}
	return oteltrace.ContextWithSpanContext(base, spanCtx)
}
