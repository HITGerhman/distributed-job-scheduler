package learnsite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/robfig/cron/v3"
	clientv3 "go.etcd.io/etcd/client/v3"

	"djs/internal/config"
	"djs/internal/domain"
	cacheinfra "djs/internal/infra/cache"
	registryetcd "djs/internal/registry/etcd"
	mysqlrepo "djs/internal/repository/mysql"
	transportgrpc "djs/internal/transport/grpc"
	"djs/proto/workerpb"
)

var (
	errNoActiveSession       = errors.New("no active learning session")
	errNoTrackedInstance     = errors.New("no tracked instance")
	errFocusInstanceMismatch = errors.New("instance does not belong to the current session")
)

const maxSlotIterations = 1024

type recentFailuresCache interface {
	GetRecentFailedInstances(ctx context.Context) ([]*domain.JobInstance, bool, error)
	PutRecentFailedInstances(ctx context.Context, instances []*domain.JobInstance) error
}

type Service struct {
	cfg            *config.Config
	db             *sql.DB
	store          *mysqlrepo.Store
	etcd           *clientv3.Client
	leaderResolver *registryetcd.LeaderResolver
	connPool       *transportgrpc.ConnPool
	masterClient   *transportgrpc.MasterClient
	processes      *processManager
	redis          *cacheinfra.RedisCache
	parser         cron.Parser
	now            func() time.Time

	mu      sync.RWMutex
	session *demoSession
}

type demoSession struct {
	ID                  string
	JobID               uint64
	JobName             string
	CreatedAt           time.Time
	PreferredInstanceID uint64
	HasPreferred        bool
	TrackedInstanceID   uint64
	HasTracked          bool
	AbortedAt           *time.Time
	AbortReason         string
	AutoDisabled        bool
}

type runtimeData struct {
	job       *domain.Job
	instances []*domain.JobInstance
	tracked   *domain.JobInstance
	attempts  []*domain.Attempt
	outbox    []*domain.OutboxEvent
	audit     []*domain.AuditEvent
	leader    *registryetcd.LeaderInfo
	workers   []registryetcd.WorkerInfo
}

func NewService(cfg *config.Config, rootDir string, configPath string) (*Service, error) {
	db, err := openDB(context.Background(), cfg.MySQL.DSN)
	if err != nil {
		return nil, err
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.ETCD.Endpoints,
		DialTimeout: cfg.ETCD.DialTimeout,
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	connPool := transportgrpc.NewConnPool()
	redisCache := cacheinfra.NewRedisCache(cfg.Redis)

	return &Service{
		cfg:            cfg,
		db:             db,
		store:          mysqlrepo.NewStore(db),
		etcd:           etcdClient,
		leaderResolver: registryetcd.NewLeaderResolver(etcdClient, cfg.ETCD.ElectionPrefix),
		connPool:       connPool,
		masterClient:   transportgrpc.NewMasterClient(connPool),
		processes:      newProcessManager(rootDir, configPath, cfg),
		redis:          redisCache,
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
	}, nil
}

func (s *Service) Close() error {
	var errs []string
	if s.redis != nil {
		if err := s.redis.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if s.etcd != nil {
		if err := s.etcd.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("close learnsite service: %s", strings.Join(errs, "; "))
}

func (s *Service) CurrentScene(ctx context.Context) (SceneSnapshot, error) {
	callCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()

	session := s.currentSession()
	snapshotNow := s.now()
	scene := SceneSnapshot{
		GeneratedAt: snapshotNow,
		Session:     publicSession(session),
		Stage:       StageIdle,
		SourceKey:   "control.create_job",
		Summary:     "先创建一轮教学任务，再用按钮把视角推进到 instance 和 dispatch。",
	}

	deps, leader, workers, depBlockers := s.dependencies(callCtx)
	scene.Dependencies = deps
	scene.Blockers = append(scene.Blockers, depBlockers...)
	scene.Processes = s.localProcesses(callCtx, leader, workers)
	scene.Tracked.LeaderID = leader.MasterID
	scene.Tracked.LeaderAddr = leader.GRPCAddr
	scene.Tracked.AvailableWorkers = workers

	if session == nil {
		scene.Checkpoints = checkpointStates(scene.Stage)
		scene.Actors = actorsForScene(scene.Stage, nil, nil, nil, nil, leader, workers)
		scene.Packets = packetsForStage(scene.Stage)
		scene.Slots = s.slotWindow(snapshotNow, nil, nil, nil)
		scene.Tracked.CurrentSource = scene.SourceKey
		return scene, nil
	}

	data, runtimeBlockers := s.loadRuntimeData(callCtx, session)
	scene.Blockers = append(scene.Blockers, runtimeBlockers...)
	if data.job != nil {
		scene.Tracked.JobID = data.job.ID
	}
	if data.tracked != nil {
		scene.Tracked.InstanceID = data.tracked.ID
		scene.Tracked.InstanceStatus = data.tracked.Status
		if data.tracked.WorkerID != nil {
			scene.Tracked.WorkerID = *data.tracked.WorkerID
		}
	}
	if latest := latestAttempt(data.attempts); latest != nil {
		scene.Tracked.AttemptNo = latest.AttemptNo
		scene.Tracked.AttemptStatus = latest.Status
		scene.Tracked.WorkerID = latest.WorkerID
		scene.Tracked.LastHeartbeatAt = latest.LastHeartbeatAt
	}
	scene.Tracked.Instances = trackedInstances(data.instances, data.tracked)
	scene.Tracked.Outbox = outboxStates(data.outbox)
	scene.Tracked.Audit = auditStates(data.audit)
	scene.Slots = s.slotWindow(snapshotNow, data.job, data.instances, data.tracked)

	stage := deriveStage(session, data.job, data.tracked, data.attempts, data.outbox, data.audit)
	scene.Stage = stage
	scene.SourceKey = sourceKeyForStage(stage)
	scene.Tracked.CurrentSource = scene.SourceKey
	scene.Checkpoints = checkpointStates(stage)
	scene.Packets = packetsForStage(stage)
	scene.Timeline = buildTimeline(session, data.job, data.tracked, data.attempts, data.outbox, data.audit)
	scene.Actors = actorsForScene(stage, data.job, data.tracked, data.attempts, data.outbox, leader, workers)
	scene.Summary = summaryForStage(stage, data.job, data.tracked, data.attempts, data.outbox, data.audit)
	if scene.Session.Status == "active" && stage == StageAuditReceived {
		scene.Session.Status = "completed"
	}
	if session.AbortedAt != nil {
		scene.Session.Status = "aborted"
	}
	return scene, nil
}

func (s *Service) localProcesses(ctx context.Context, leader registryetcd.LeaderInfo, workers []WorkerState) []LocalProcess {
	if s.processes == nil {
		return nil
	}
	return s.processes.states(ctx, leader, workers)
}

func (s *Service) StartLocalProcess(ctx context.Context, id string) (SceneSnapshot, error) {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := s.processes.start(callCtx, id); err != nil {
		return SceneSnapshot{}, err
	}

	select {
	case <-callCtx.Done():
	case <-time.After(350 * time.Millisecond):
	}

	return s.CurrentScene(callCtx)
}

func (s *Service) AwaitInstance(ctx context.Context) (SceneSnapshot, error) {
	return s.waitForStage(ctx, StageInstanceCreated, 12*time.Second)
}

func (s *Service) AdvanceToDispatch(ctx context.Context) (SceneSnapshot, error) {
	return s.waitForStage(ctx, StageDispatched, 12*time.Second)
}

func (s *Service) StartDemo(ctx context.Context) (SceneSnapshot, error) {
	callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	leader, err := s.leaderResolver.Current(callCtx)
	if err != nil {
		return SceneSnapshot{}, fmt.Errorf("resolve leader: %w", err)
	}

	previous := s.currentSession()
	if previous != nil && previous.JobID > 0 {
		_, _ = s.store.Jobs().UpdateStatus(callCtx, previous.JobID, domain.JobStatusEnabled, domain.JobStatusDisabled)
	}

	sessionID := newSessionID()
	name := "learn-demo-" + s.now().Format("20060102-150405")
	payload, err := json.Marshal(map[string]any{
		"kind":        "mock",
		"duration_ms": 7000,
		"result_summary": map[string]any{
			"session_id": sessionID,
			"message":    "learning site demo finished",
		},
	})
	if err != nil {
		return SceneSnapshot{}, fmt.Errorf("marshal demo payload: %w", err)
	}

	resp, err := s.masterClient.CreateJob(callCtx, leader.GRPCAddr, &workerpb.CreateJobRequest{
		Name:                name,
		CronExpr:            "* * * * *",
		Timezone:            "Asia/Shanghai",
		Payload:             payload,
		TimeoutSeconds:      30,
		MaxRetries:          0,
		RetryBackoffSeconds: 0,
		AllowConcurrent:     false,
		Status:              domain.JobStatusEnabled,
	})
	if err != nil {
		return SceneSnapshot{}, fmt.Errorf("create demo job: %w", err)
	}

	s.mu.Lock()
	s.session = &demoSession{
		ID:        sessionID,
		JobID:     resp.JobId,
		JobName:   name,
		CreatedAt: s.now(),
	}
	s.mu.Unlock()

	return s.CurrentScene(ctx)
}

func (s *Service) waitForStage(ctx context.Context, target string, maxWait time.Duration) (SceneSnapshot, error) {
	if s.currentSession() == nil {
		return SceneSnapshot{}, errNoActiveSession
	}

	callCtx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()

	var last SceneSnapshot
	for {
		if err := callCtx.Err(); err != nil {
			if !last.GeneratedAt.IsZero() {
				return last, nil
			}
			return SceneSnapshot{}, err
		}

		scene, err := s.CurrentScene(callCtx)
		if err != nil {
			if callCtx.Err() != nil && !last.GeneratedAt.IsZero() {
				return last, nil
			}
			return SceneSnapshot{}, err
		}
		last = scene

		if stageRank(scene.Stage) >= stageRank(target) || scene.Stage == StageAborted {
			return scene, nil
		}

		select {
		case <-callCtx.Done():
			return last, nil
		case <-time.After(350 * time.Millisecond):
		}
	}
}

func (s *Service) FocusDemo(ctx context.Context, instanceID uint64) (SceneSnapshot, error) {
	callCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	if instanceID == 0 {
		return SceneSnapshot{}, errFocusInstanceMismatch
	}

	session := s.currentSession()
	if session == nil {
		return SceneSnapshot{}, errNoActiveSession
	}

	instances, err := s.listInstancesByJob(callCtx, session.JobID, 12)
	if err != nil {
		return SceneSnapshot{}, err
	}
	found := false
	for _, instance := range instances {
		if instance.ID == instanceID {
			found = true
			break
		}
	}
	if !found {
		return SceneSnapshot{}, errFocusInstanceMismatch
	}

	s.mu.Lock()
	if s.session != nil && s.session.JobID == session.JobID {
		s.session.PreferredInstanceID = instanceID
		s.session.HasPreferred = true
	}
	s.mu.Unlock()
	return s.CurrentScene(callCtx)
}

func (s *Service) KillDemo(ctx context.Context) (SceneSnapshot, error) {
	callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	session := s.currentSession()
	if session == nil {
		return SceneSnapshot{}, errNoActiveSession
	}
	scene, err := s.CurrentScene(callCtx)
	if err != nil {
		return SceneSnapshot{}, err
	}
	if scene.Tracked.InstanceID == 0 {
		return SceneSnapshot{}, errNoTrackedInstance
	}

	leader, err := s.leaderResolver.Current(callCtx)
	if err != nil {
		return SceneSnapshot{}, fmt.Errorf("resolve leader: %w", err)
	}
	if _, err := s.masterClient.KillInstance(callCtx, leader.GRPCAddr, &workerpb.KillInstanceRequest{
		InstanceId: scene.Tracked.InstanceID,
		Reason:     "learning site manual kill",
	}); err != nil {
		return SceneSnapshot{}, fmt.Errorf("kill instance: %w", err)
	}

	now := s.now()
	s.mu.Lock()
	if s.session != nil && s.session.JobID == session.JobID {
		s.session.AbortedAt = &now
		s.session.AbortReason = "learning site manual kill"
	}
	s.mu.Unlock()
	return s.CurrentScene(ctx)
}

func (s *Service) RecentFailures(ctx context.Context, limit int) (RecentFailuresResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()

	if limit <= 0 {
		limit = 6
	}
	instances, source, err := loadRecentFailures(callCtx, s.redis, func(ctx context.Context, limit int) ([]*domain.JobInstance, error) {
		return s.store.Instances().ListRecentFailed(ctx, limit)
	}, limit)
	if err != nil {
		return RecentFailuresResponse{}, err
	}
	return RecentFailuresResponse{
		Source:    source,
		Instances: trackedInstances(instances, nil),
	}, nil
}

func loadRecentFailures(ctx context.Context, cache recentFailuresCache, loader func(context.Context, int) ([]*domain.JobInstance, error), limit int) ([]*domain.JobInstance, string, error) {
	if cache != nil {
		if instances, hit, err := cache.GetRecentFailedInstances(ctx); err == nil && hit {
			return instances, "redis", nil
		}
	}
	instances, err := loader(ctx, limit)
	if err != nil {
		return nil, "", err
	}
	if cache != nil {
		_ = cache.PutRecentFailedInstances(ctx, instances)
	}
	return instances, "mysql", nil
}

func (s *Service) currentSession() *demoSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.session == nil {
		return nil
	}
	copy := *s.session
	return &copy
}

func (s *Service) dependencies(ctx context.Context) ([]DependencyState, registryetcd.LeaderInfo, []WorkerState, []string) {
	var (
		deps     []DependencyState
		blockers []string
		leader   registryetcd.LeaderInfo
		workers  []registryetcd.WorkerInfo
	)

	mysqlHealthy := s.db != nil && s.db.PingContext(ctx) == nil
	mysqlDetail := "connected"
	if !mysqlHealthy {
		mysqlDetail = "无法连接 MySQL，真实状态暂时不可用。"
		blockers = append(blockers, mysqlDetail)
	}
	deps = append(deps, DependencyState{
		ID:      "mysql",
		Label:   "MySQL",
		Status:  ternary(mysqlHealthy, "ok", "error"),
		Detail:  mysqlDetail,
		Healthy: mysqlHealthy,
	})

	leaderInfo, err := s.leaderResolver.Current(ctx)
	etcdHealthy := true
	etcdDetail := "leader resolved"
	switch {
	case err == nil:
		leader = leaderInfo
		etcdDetail = fmt.Sprintf("leader %s @ %s", leaderInfo.MasterID, leaderInfo.GRPCAddr)
	case errors.Is(err, domain.ErrNoLeader):
		etcdDetail = "etcd 可用，但当前没有 leader。"
		blockers = append(blockers, etcdDetail)
	default:
		etcdHealthy = false
		etcdDetail = "无法连接 etcd，无法解析 leader / worker。"
		blockers = append(blockers, etcdDetail)
	}
	deps = append(deps, DependencyState{
		ID:      "etcd",
		Label:   "etcd",
		Status:  dependencyStatus(etcdHealthy, err == nil || errors.Is(err, domain.ErrNoLeader)),
		Detail:  etcdDetail,
		Healthy: etcdHealthy,
	})

	if etcdHealthy {
		list, listErr := s.listWorkers(ctx)
		if listErr == nil {
			workers = list
		} else {
			blockers = append(blockers, "无法读取 worker 注册信息。")
		}
	}

	redisHealthy := true
	redisStatus := "ok"
	redisDetail := "缓存和 worker snapshot 可用"
	switch {
	case s.redis == nil:
		redisHealthy = false
		redisStatus = "disabled"
		redisDetail = "Redis 未配置，recent failures 会直接回源 MySQL。"
	case s.redis.Ping(ctx) != nil:
		redisHealthy = false
		redisStatus = "warn"
		redisDetail = "Redis 当前不可用，不影响主舞台。"
	default:
	}
	deps = append(deps, DependencyState{
		ID:       "redis",
		Label:    "Redis",
		Status:   redisStatus,
		Detail:   redisDetail,
		Healthy:  redisHealthy,
		Optional: true,
	})

	return deps, leader, workerStates(workers, leader), blockers
}

func (s *Service) loadRuntimeData(ctx context.Context, session *demoSession) (*runtimeData, []string) {
	data := &runtimeData{}
	var blockers []string

	job, err := s.store.Jobs().GetByID(ctx, session.JobID)
	switch {
	case err == nil:
		data.job = job
	case errors.Is(err, domain.ErrJobNotFound):
		blockers = append(blockers, "当前教学任务定义不存在，可能已被清理。")
		return data, blockers
	default:
		blockers = append(blockers, "无法读取 demo job 定义。")
		return data, blockers
	}

	instances, err := s.listInstancesByJob(ctx, session.JobID, 12)
	if err != nil {
		blockers = append(blockers, "无法读取当前 demo 的实例列表。")
		return data, blockers
	}
	data.instances = instances

	if len(instances) > 0 && !session.AutoDisabled {
		if ok, _ := s.store.Jobs().UpdateStatus(ctx, session.JobID, domain.JobStatusEnabled, domain.JobStatusDisabled); ok {
			s.mu.Lock()
			if s.session != nil && s.session.JobID == session.JobID {
				s.session.AutoDisabled = true
			}
			s.mu.Unlock()
		}
	}

	tracked := selectTrackedInstance(instances, session)
	data.tracked = tracked
	if tracked == nil {
		return data, blockers
	}

	s.mu.Lock()
	if s.session != nil && s.session.JobID == session.JobID {
		s.session.TrackedInstanceID = tracked.ID
		s.session.HasTracked = true
	}
	s.mu.Unlock()

	attempts, err := s.store.Attempts().ListByInstance(ctx, tracked.ID, 8)
	if err == nil {
		data.attempts = attempts
	}
	outbox, err := s.listOutboxByInstance(ctx, tracked.ID, 12)
	if err == nil {
		data.outbox = outbox
	}
	audit, err := s.listAuditByInstance(ctx, tracked.ID, 12)
	if err == nil {
		data.audit = audit
	}
	return data, blockers
}

func (s *Service) listWorkers(ctx context.Context) ([]registryetcd.WorkerInfo, error) {
	resp, err := s.etcd.Get(ctx, strings.TrimRight(s.cfg.ETCD.WorkerPrefix, "/")+"/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	workers := make([]registryetcd.WorkerInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var info registryetcd.WorkerInfo
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			continue
		}
		workers = append(workers, info)
	}
	sort.Slice(workers, func(i, j int) bool {
		return workers[i].WorkerID < workers[j].WorkerID
	})
	return workers, nil
}

func (s *Service) listInstancesByJob(ctx context.Context, jobID uint64, limit int) ([]*domain.JobInstance, error) {
	const q = `
SELECT
    id, job_id, scheduled_at, status, worker_id, latest_attempt_no,
    started_at, finished_at, next_retry_at, final_error, version,
    created_at, updated_at
FROM job_instances
WHERE job_id = ?
ORDER BY scheduled_at DESC, id DESC
LIMIT ?
`
	rows, err := s.db.QueryContext(ctx, q, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var instances []*domain.JobInstance
	for rows.Next() {
		instance, err := scanJobInstance(rows)
		if err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}
	return instances, rows.Err()
}

func (s *Service) listOutboxByInstance(ctx context.Context, instanceID uint64, limit int) ([]*domain.OutboxEvent, error) {
	const q = `
SELECT
    id, topic, event_type, aggregate_type, aggregate_id, event_key,
    payload, headers, status, retry_count, last_error,
    created_at, available_at, sent_at, updated_at
FROM outbox_events
WHERE aggregate_type = 'job_instance'
  AND aggregate_id = ?
ORDER BY id ASC
LIMIT ?
`
	rows, err := s.db.QueryContext(ctx, q, fmt.Sprintf("%d", instanceID), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*domain.OutboxEvent
	for rows.Next() {
		event, err := scanOutboxEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Service) listAuditByInstance(ctx context.Context, instanceID uint64, limit int) ([]*domain.AuditEvent, error) {
	const q = `
SELECT
    id, event_id, event_type, aggregate_type, aggregate_id,
    instance_id, attempt_no, job_id, worker_id, trace_id,
    payload, received_at
FROM audit_events
WHERE instance_id = ?
ORDER BY id ASC
LIMIT ?
`
	rows, err := s.db.QueryContext(ctx, q, instanceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*domain.AuditEvent
	for rows.Next() {
		event, err := scanAuditEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func scanJobInstance(scanner interface {
	Scan(dest ...any) error
}) (*domain.JobInstance, error) {
	var instance domain.JobInstance
	if err := scanner.Scan(
		&instance.ID,
		&instance.JobID,
		&instance.ScheduledAt,
		&instance.Status,
		&instance.WorkerID,
		&instance.LatestAttemptNo,
		&instance.StartedAt,
		&instance.FinishedAt,
		&instance.NextRetryAt,
		&instance.FinalError,
		&instance.Version,
		&instance.CreatedAt,
		&instance.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &instance, nil
}

func scanOutboxEvent(scanner interface {
	Scan(dest ...any) error
}) (*domain.OutboxEvent, error) {
	var event domain.OutboxEvent
	if err := scanner.Scan(
		&event.ID,
		&event.Topic,
		&event.EventType,
		&event.AggregateType,
		&event.AggregateID,
		&event.EventKey,
		&event.Payload,
		&event.Headers,
		&event.Status,
		&event.RetryCount,
		&event.LastError,
		&event.CreatedAt,
		&event.AvailableAt,
		&event.SentAt,
		&event.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &event, nil
}

func scanAuditEvent(scanner interface {
	Scan(dest ...any) error
}) (*domain.AuditEvent, error) {
	var event domain.AuditEvent
	if err := scanner.Scan(
		&event.ID,
		&event.EventID,
		&event.EventType,
		&event.AggregateType,
		&event.AggregateID,
		&event.InstanceID,
		&event.AttemptNo,
		&event.JobID,
		&event.WorkerID,
		&event.TraceID,
		&event.Payload,
		&event.ReceivedAt,
	); err != nil {
		return nil, err
	}
	return &event, nil
}

func deriveStage(session *demoSession, job *domain.Job, tracked *domain.JobInstance, attempts []*domain.Attempt, outbox []*domain.OutboxEvent, audit []*domain.AuditEvent) string {
	if session != nil && session.AbortedAt != nil {
		return StageAborted
	}
	if len(audit) > 0 {
		return StageAuditReceived
	}
	if hasSentOutbox(outbox) {
		return StageOutboxSent
	}
	attempt := latestAttempt(attempts)
	switch {
	case attempt != nil && domain.IsAttemptTerminalStatus(attempt.Status):
		return StageFinished
	case attempt != nil && attempt.LastHeartbeatAt != nil && attempt.StartedAt != nil && attempt.LastHeartbeatAt.After(*attempt.StartedAt):
		return StageHeartbeatSeen
	case attempt != nil && (attempt.Status == domain.AttemptStatusRunning || attempt.StartedAt != nil):
		return StageRunning
	case attempt != nil && (attempt.Status == domain.AttemptStatusDispatched || attempt.DispatchedAt != nil):
		return StageDispatched
	case tracked != nil:
		return StageInstanceCreated
	case job != nil:
		return StageJobCreated
	default:
		return StageIdle
	}
}

func latestAttempt(attempts []*domain.Attempt) *domain.Attempt {
	if len(attempts) == 0 {
		return nil
	}
	latest := attempts[0]
	for _, attempt := range attempts[1:] {
		if attempt.AttemptNo > latest.AttemptNo {
			latest = attempt
		}
	}
	return latest
}

func hasSentOutbox(events []*domain.OutboxEvent) bool {
	for _, event := range events {
		if event.SentAt != nil || event.Status == domain.OutboxStatusSent {
			return true
		}
	}
	return false
}

func selectTrackedInstance(instances []*domain.JobInstance, session *demoSession) *domain.JobInstance {
	if len(instances) == 0 {
		return nil
	}
	if session != nil && session.HasPreferred {
		for _, instance := range instances {
			if instance.ID == session.PreferredInstanceID {
				return instance
			}
		}
	}

	best := instances[0]
	bestScore := instanceScore(best)
	for _, instance := range instances[1:] {
		score := instanceScore(instance)
		if score > bestScore || (score == bestScore && instance.ScheduledAt.After(best.ScheduledAt)) {
			best = instance
			bestScore = score
		}
	}
	return best
}

func instanceScore(instance *domain.JobInstance) int {
	switch instance.Status {
	case domain.InstanceStatusRunning:
		return 40
	case domain.InstanceStatusDispatched:
		return 30
	case domain.InstanceStatusPending:
		return 20
	default:
		return 10
	}
}

func checkpointStates(stage string) []CheckpointState {
	items := []CheckpointState{
		{ID: "create", Label: "创建任务", Description: "Control 发起真实 CreateJob", SourceKey: "control.create_job"},
		{ID: "instance", Label: "看到实例", Description: "Leader 把 cron slot materialize 成 instance", SourceKey: "master.materialize_due_instances"},
		{ID: "dispatch", Label: "看到派发", Description: "Master 选 worker 并写 attempt", SourceKey: "master.dispatch"},
		{ID: "running", Label: "看到运行", Description: "Worker 上报 started / heartbeat", SourceKey: "worker.heartbeat"},
		{ID: "finished", Label: "看到回调", Description: "Master 收到 finished 并收敛状态", SourceKey: "master.report_finished"},
		{ID: "relay", Label: "看到 relay / audit", Description: "Outbox sent，审计事件落库", SourceKey: "master.outbox_relay"},
	}

	current := checkpointIndex(stage)
	for i := range items {
		switch {
		case i < current:
			items[i].State = "done"
		case i == current:
			items[i].State = "current"
		default:
			items[i].State = "pending"
		}
	}
	if stage == StageAborted {
		for i := range items {
			if items[i].State == "current" {
				items[i].State = "aborted"
			}
		}
	}
	return items
}

func checkpointIndex(stage string) int {
	switch stage {
	case StageJobCreated:
		return 0
	case StageInstanceCreated:
		return 1
	case StageDispatched:
		return 2
	case StageRunning, StageHeartbeatSeen:
		return 3
	case StageFinished:
		return 4
	case StageOutboxSent, StageAuditReceived:
		return 5
	default:
		return 0
	}
}

func actorsForScene(stage string, job *domain.Job, tracked *domain.JobInstance, attempts []*domain.Attempt, outbox []*domain.OutboxEvent, leader registryetcd.LeaderInfo, workers []WorkerState) []ActorState {
	attempt := latestAttempt(attempts)
	workerDetail := fmt.Sprintf("%d worker online", len(workers))
	if attempt != nil {
		workerDetail = fmt.Sprintf("worker %s · attempt %d · %s", attempt.WorkerID, attempt.AttemptNo, attempt.Status)
	}
	mysqlDetail := "等待真实状态"
	if job != nil {
		mysqlDetail = fmt.Sprintf("job #%d 已创建", job.ID)
	}
	if tracked != nil {
		mysqlDetail = fmt.Sprintf("instance #%d · %s", tracked.ID, tracked.Status)
	}
	kafkaDetail := "等待 outbox relay"
	if hasSentOutbox(outbox) {
		kafkaDetail = fmt.Sprintf("%d 条 outbox 已发送", len(outbox))
	}
	return []ActorState{
		{ID: "control", Label: "Control", Role: "入口", Status: actorStatusFor("control", stage), Detail: "CreateJob / refresh / kill demo", Active: stageRank(stage) >= stageRank(StageJobCreated), Online: true, SourceKey: "control.create_job"},
		{ID: "master", Label: "Master", Role: "调度中枢", Status: actorStatusFor("master", stage), Detail: leaderDetail(leader), Active: stageRank(stage) >= stageRank(StageJobCreated), Online: leader.MasterID != "", SourceKey: "master.dispatch"},
		{ID: "worker", Label: "Worker", Role: "执行者", Status: actorStatusFor("worker", stage), Detail: workerDetail, Active: stageRank(stage) >= stageRank(StageDispatched), Online: len(workers) > 0, SourceKey: "worker.dispatch_task"},
		{ID: "mysql", Label: "MySQL", Role: "业务真相", Status: actorStatusFor("mysql", stage), Detail: mysqlDetail, Active: stageRank(stage) >= stageRank(StageJobCreated), Online: true, SourceKey: "master.materialize_due_instances"},
		{ID: "kafka", Label: "Kafka", Role: "事件总线", Status: actorStatusFor("kafka", stage), Detail: kafkaDetail, Active: stageRank(stage) >= stageRank(StageOutboxSent), Online: true, SourceKey: "master.outbox_relay"},
		{ID: "audit", Label: "Audit", Role: "审计消费者", Status: actorStatusFor("audit", stage), Detail: "幂等写入 audit_events", Active: stageRank(stage) >= stageRank(StageAuditReceived), Online: true, SourceKey: "audit.consumer"},
	}
}

func packetsForStage(stage string) []PacketState {
	switch stage {
	case StageJobCreated:
		return []PacketState{{ID: "job", Label: "CreateJob", Route: []string{"control", "master", "mysql"}, Emphasis: "job"}}
	case StageInstanceCreated:
		return []PacketState{{ID: "instance", Label: "instance", Route: []string{"master", "mysql"}, Emphasis: "instance"}}
	case StageDispatched:
		return []PacketState{{ID: "attempt", Label: "DispatchTask", Route: []string{"master", "worker"}, Emphasis: "attempt"}}
	case StageRunning:
		return []PacketState{{ID: "attempt", Label: "started", Route: []string{"worker", "master", "mysql"}, Emphasis: "attempt"}}
	case StageHeartbeatSeen:
		return []PacketState{{ID: "heartbeat", Label: "heartbeat", Route: []string{"worker", "master"}, Emphasis: "heartbeat"}}
	case StageFinished:
		return []PacketState{{ID: "finish", Label: "finished", Route: []string{"worker", "master", "mysql"}, Emphasis: "finish"}}
	case StageOutboxSent:
		return []PacketState{{ID: "event", Label: "outbox", Route: []string{"mysql", "kafka"}, Emphasis: "event"}}
	case StageAuditReceived:
		return []PacketState{{ID: "audit", Label: "audit", Route: []string{"kafka", "audit"}, Emphasis: "audit"}}
	case StageAborted:
		return []PacketState{{ID: "kill", Label: "kill", Route: []string{"control", "master", "worker"}, Emphasis: "kill"}}
	default:
		return nil
	}
}

func buildTimeline(session *demoSession, job *domain.Job, tracked *domain.JobInstance, attempts []*domain.Attempt, outbox []*domain.OutboxEvent, audit []*domain.AuditEvent) []TimelineEvent {
	var items []TimelineEvent
	if job != nil {
		items = append(items, TimelineEvent{
			ID:         "job-created",
			Label:      "CreateJob accepted",
			Detail:     fmt.Sprintf("job #%d %s", job.ID, job.Name),
			OccurredAt: job.CreatedAt,
			SourceKey:  "control.create_job",
		})
	}
	if tracked != nil {
		items = append(items, TimelineEvent{
			ID:         fmt.Sprintf("instance-%d", tracked.ID),
			Label:      "Instance materialized",
			Detail:     fmt.Sprintf("instance #%d at %s", tracked.ID, tracked.ScheduledAt.Local().Format("15:04:05")),
			OccurredAt: tracked.CreatedAt,
			SourceKey:  "master.materialize_due_instances",
		})
	}
	if attempt := latestAttempt(attempts); attempt != nil {
		if attempt.DispatchedAt != nil {
			items = append(items, TimelineEvent{
				ID:         fmt.Sprintf("attempt-%d-dispatched", attempt.AttemptNo),
				Label:      "DispatchTask",
				Detail:     fmt.Sprintf("attempt %d -> %s", attempt.AttemptNo, attempt.WorkerID),
				OccurredAt: *attempt.DispatchedAt,
				SourceKey:  "master.dispatch",
			})
		}
		if attempt.StartedAt != nil {
			items = append(items, TimelineEvent{
				ID:         fmt.Sprintf("attempt-%d-started", attempt.AttemptNo),
				Label:      "Worker started",
				Detail:     fmt.Sprintf("attempt %d entered running", attempt.AttemptNo),
				OccurredAt: *attempt.StartedAt,
				SourceKey:  "worker.dispatch_task",
			})
		}
		if attempt.LastHeartbeatAt != nil {
			items = append(items, TimelineEvent{
				ID:         fmt.Sprintf("attempt-%d-heartbeat", attempt.AttemptNo),
				Label:      "Heartbeat seen",
				Detail:     fmt.Sprintf("worker %s is still alive", attempt.WorkerID),
				OccurredAt: *attempt.LastHeartbeatAt,
				SourceKey:  "worker.heartbeat",
			})
		}
		if attempt.FinishedAt != nil {
			items = append(items, TimelineEvent{
				ID:         fmt.Sprintf("attempt-%d-finished", attempt.AttemptNo),
				Label:      "Finished callback",
				Detail:     fmt.Sprintf("attempt %d -> %s", attempt.AttemptNo, attempt.Status),
				OccurredAt: *attempt.FinishedAt,
				SourceKey:  "master.report_finished",
			})
		}
	}
	for _, event := range outbox {
		if event.SentAt == nil {
			continue
		}
		items = append(items, TimelineEvent{
			ID:         fmt.Sprintf("outbox-%d", event.ID),
			Label:      "Outbox sent",
			Detail:     event.EventType,
			OccurredAt: *event.SentAt,
			SourceKey:  "master.outbox_relay",
		})
	}
	for _, event := range audit {
		items = append(items, TimelineEvent{
			ID:         fmt.Sprintf("audit-%d", event.ID),
			Label:      "Audit received",
			Detail:     event.EventType,
			OccurredAt: event.ReceivedAt,
			SourceKey:  "audit.consumer",
		})
	}
	if session != nil && session.AbortedAt != nil {
		items = append(items, TimelineEvent{
			ID:         "manual-kill",
			Label:      "Manual kill requested",
			Detail:     session.AbortReason,
			OccurredAt: *session.AbortedAt,
			SourceKey:  "master.kill_instance",
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].OccurredAt.Before(items[j].OccurredAt)
	})
	return items
}

func (s *Service) slotWindow(now time.Time, job *domain.Job, instances []*domain.JobInstance, focused *domain.JobInstance) SlotWindow {
	window := SlotWindow{
		CursorAt:  now,
		Lookback:  s.cfg.Scheduling.Lookback.String(),
		Lookahead: s.cfg.Scheduling.Lookahead.String(),
		Detail:    "创建 Job 后，这里会显示 leader create loop 正在观察的 cron 时间槽。",
	}
	if job == nil {
		return window
	}

	window.Available = true
	window.CronExpr = job.CronExpr
	window.Timezone = job.Timezone
	window.WindowStart = now.Add(-s.cfg.Scheduling.Lookback).UTC()
	window.WindowEnd = now.Add(s.cfg.Scheduling.Lookahead).UTC()
	window.Detail = fmt.Sprintf(
		"lookback %s / lookahead %s · cron %s (%s)",
		s.cfg.Scheduling.Lookback,
		s.cfg.Scheduling.Lookahead,
		job.CronExpr,
		job.Timezone,
	)

	slots, err := s.slotsInWindow(job, window.WindowStart, window.WindowEnd)
	if err != nil {
		window.Detail = fmt.Sprintf("无法解析 cron 时间槽：%v", err)
		return window
	}

	location, locErr := time.LoadLocation(job.Timezone)
	if locErr != nil {
		location = time.UTC
	}

	instanceBySlot := make(map[string]*domain.JobInstance, len(instances))
	for _, instance := range instances {
		instanceBySlot[instance.ScheduledAt.UTC().Format(time.RFC3339Nano)] = instance
	}

	items := make([]SlotState, 0, len(slots))
	for _, slot := range slots {
		instance := instanceBySlot[slot.UTC().Format(time.RFC3339Nano)]
		item := SlotState{
			ScheduledAt: slot,
			Label:       slot.In(location).Format("15:04"),
			State:       ternary(slot.After(now), "future", "due"),
			Detail:      "该时间槽已进入 leader create window，等待 materialize。",
		}
		if slot.After(now) {
			item.Detail = "该时间槽还在未来，已经进入 lookahead 观察范围。"
		}
		if instance != nil {
			item.Materialized = true
			item.InstanceID = instance.ID
			item.InstanceStatus = instance.Status
			item.State = "materialized"
			item.Detail = fmt.Sprintf("已 materialize 为 instance #%d · %s", instance.ID, instance.Status)
			if focused != nil && focused.ID == instance.ID {
				item.Focused = true
				item.State = "focused"
				item.Detail = fmt.Sprintf("当前聚焦 instance #%d · %s", instance.ID, instance.Status)
			}
		}
		items = append(items, item)
	}
	window.Slots = items
	return window
}

func (s *Service) slotsInWindow(job *domain.Job, windowStart time.Time, windowEnd time.Time) ([]time.Time, error) {
	location, err := time.LoadLocation(job.Timezone)
	if err != nil {
		return nil, fmt.Errorf("load job timezone failed: %w", err)
	}

	schedule, err := s.parser.Parse(job.CronExpr)
	if err != nil {
		return nil, fmt.Errorf("parse cron expression failed: %w", err)
	}

	localStart := windowStart.In(location)
	localEnd := windowEnd.In(location)
	cursor := localStart.Add(-time.Minute)

	var slots []time.Time
	for i := 0; i < maxSlotIterations; i++ {
		next := schedule.Next(cursor)
		if next.IsZero() || next.After(localEnd) {
			break
		}
		if !next.Before(localStart) {
			slots = append(slots, next.UTC())
		}
		cursor = next
	}
	return slots, nil
}

func trackedInstances(instances []*domain.JobInstance, focused *domain.JobInstance) []TrackedInstance {
	items := make([]TrackedInstance, 0, len(instances))
	for _, instance := range instances {
		item := TrackedInstance{
			ID:              instance.ID,
			Status:          instance.Status,
			ScheduledAt:     instance.ScheduledAt,
			LatestAttemptNo: instance.LatestAttemptNo,
			Focused:         focused != nil && instance.ID == focused.ID,
			StartedAt:       instance.StartedAt,
			FinishedAt:      instance.FinishedAt,
		}
		if instance.WorkerID != nil {
			item.WorkerID = *instance.WorkerID
		}
		items = append(items, item)
	}
	return items
}

func outboxStates(events []*domain.OutboxEvent) []OutboxState {
	items := make([]OutboxState, 0, len(events))
	for _, event := range events {
		items = append(items, OutboxState{
			ID:        event.ID,
			EventType: event.EventType,
			Status:    event.Status,
			CreatedAt: event.CreatedAt,
			SentAt:    event.SentAt,
		})
	}
	return items
}

func auditStates(events []*domain.AuditEvent) []AuditState {
	items := make([]AuditState, 0, len(events))
	for _, event := range events {
		items = append(items, AuditState{
			ID:         event.ID,
			EventType:  event.EventType,
			ReceivedAt: event.ReceivedAt,
		})
	}
	return items
}

func publicSession(session *demoSession) SceneSession {
	if session == nil {
		return SceneSession{Status: "idle"}
	}
	status := "active"
	if session.AbortedAt != nil {
		status = "aborted"
	}
	return SceneSession{
		ID:          session.ID,
		Status:      status,
		JobID:       session.JobID,
		JobName:     session.JobName,
		CreatedAt:   &session.CreatedAt,
		AbortedAt:   session.AbortedAt,
		AbortReason: session.AbortReason,
	}
}

func sourceKeyForStage(stage string) string {
	switch stage {
	case StageJobCreated:
		return "control.create_job"
	case StageInstanceCreated:
		return "master.materialize_due_instances"
	case StageDispatched:
		return "master.dispatch"
	case StageRunning:
		return "worker.dispatch_task"
	case StageHeartbeatSeen:
		return "worker.heartbeat"
	case StageFinished:
		return "master.report_finished"
	case StageOutboxSent:
		return "master.outbox_relay"
	case StageAuditReceived:
		return "audit.consumer"
	case StageAborted:
		return "master.kill_instance"
	default:
		return "control.create_job"
	}
}

func summaryForStage(stage string, job *domain.Job, tracked *domain.JobInstance, attempts []*domain.Attempt, outbox []*domain.OutboxEvent, audit []*domain.AuditEvent) string {
	switch stage {
	case StageIdle:
		return "点击“开始任务”后，页面会实时跟踪一轮真实 demo job。"
	case StageJobCreated:
		return fmt.Sprintf("CreateJob 已成功，正在等待 leader create loop 为 job #%d 补出 instance。", job.ID)
	case StageInstanceCreated:
		return fmt.Sprintf("instance #%d 已 materialize，下一步会进入 dispatch。", tracked.ID)
	case StageDispatched:
		attempt := latestAttempt(attempts)
		return fmt.Sprintf("instance #%d 已派发，attempt %d 正在等待 worker started。", tracked.ID, attempt.AttemptNo)
	case StageRunning:
		return fmt.Sprintf("worker %s 已开始执行，接下来观察 heartbeat。", trackedWorker(attempts))
	case StageHeartbeatSeen:
		return fmt.Sprintf("heartbeat 已出现，说明真实 worker 正在上报存活。")
	case StageFinished:
		return fmt.Sprintf("执行结果已回调到 master，正在等待 outbox / audit 追上。")
	case StageOutboxSent:
		return fmt.Sprintf("outbox 已 sent，Kafka 一侧已经接过 baton。")
	case StageAuditReceived:
		return fmt.Sprintf("audit 已落库，这一轮正常主链路已经完整走通。")
	case StageAborted:
		return "你触发了手动 kill，主线被中止，后续状态会按 killed / failed 路径收敛。"
	default:
		return "真实状态正在刷新。"
	}
}

func trackedWorker(attempts []*domain.Attempt) string {
	if attempt := latestAttempt(attempts); attempt != nil && attempt.WorkerID != "" {
		return attempt.WorkerID
	}
	return "worker"
}

func actorStatusFor(actorID string, stage string) string {
	switch actorID {
	case "control":
		if stageRank(stage) >= stageRank(StageJobCreated) {
			return "fired"
		}
		return "ready"
	case "master":
		if stage == StageIdle {
			return "watching"
		}
		if stage == StageAborted {
			return "killing"
		}
		return "orchestrating"
	case "worker":
		switch {
		case stageRank(stage) >= stageRank(StageFinished):
			return "reported"
		case stageRank(stage) >= stageRank(StageRunning):
			return "running"
		case stageRank(stage) >= stageRank(StageDispatched):
			return "accepted"
		default:
			return "waiting"
		}
	case "mysql":
		if stage == StageIdle {
			return "standby"
		}
		return "truth"
	case "kafka":
		if stageRank(stage) >= stageRank(StageOutboxSent) {
			return "relay"
		}
		return "idle"
	case "audit":
		if stageRank(stage) >= stageRank(StageAuditReceived) {
			return "recorded"
		}
		return "idle"
	default:
		return "idle"
	}
}

func stageRank(stage string) int {
	for idx, candidate := range stageOrder {
		if candidate == stage {
			return idx
		}
	}
	return 0
}

func dependencyStatus(healthy bool, connected bool) string {
	switch {
	case healthy:
		return "ok"
	case connected:
		return "warn"
	default:
		return "error"
	}
}

func leaderDetail(leader registryetcd.LeaderInfo) string {
	if leader.MasterID == "" {
		return "当前没有 leader"
	}
	return fmt.Sprintf("leader %s @ %s", leader.MasterID, leader.GRPCAddr)
}

func workerStates(workers []registryetcd.WorkerInfo, leader registryetcd.LeaderInfo) []WorkerState {
	items := make([]WorkerState, 0, len(workers))
	for _, worker := range workers {
		items = append(items, WorkerState{
			ID:      worker.WorkerID,
			Addr:    worker.GRPCAddr,
			Online:  true,
			Primary: worker.GRPCAddr == leader.GRPCAddr,
		})
	}
	return items
}

func openDB(ctx context.Context, dsn string) (*sql.DB, error) {
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ParseTime = true
	cfg.Loc = time.UTC

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	return db, nil
}

func newSessionID() string {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("session-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(raw[:])
}

func ternary(ok bool, a string, b string) string {
	if ok {
		return a
	}
	return b
}
