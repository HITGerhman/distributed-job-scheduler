package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	scheduler "github.com/HITGerhman/distributed-job-scheduler/api/proto"
	"github.com/HITGerhman/distributed-job-scheduler/internal/discovery"
	"github.com/HITGerhman/distributed-job-scheduler/internal/election"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultMasterReportTimeout     = 12 * time.Second
	defaultWorkerHeartbeatInterval = 2 * time.Second
	defaultCompletedStateTTL       = 30 * time.Minute
	defaultKillGracePeriod         = 3 * time.Second
)

var (
	errManualKillRequested = errors.New("manual kill requested")
	errExecutionTimedOut   = errors.New("execution timeout exceeded")
)

type config struct {
	WorkerGRPCAddr        string
	WorkerHTTPAddr        string
	WorkerAdvertiseAddr   string
	MasterGRPCAddrs       []string
	WorkerID              string
	LogDir                string
	EtcdEndpoints         []string
	EtcdDialTimeout       time.Duration
	WorkerDiscoveryPrefix string
	WorkerLeaseTTL        int64
	LeaderElectionKey     string
	MasterReportTimeout   time.Duration
	HeartbeatInterval     time.Duration
	CompletedStateTTL     time.Duration
	KillGracePeriod       time.Duration
	LogBatcher            logBatcherConfig
}

type workerServer struct {
	scheduler.UnimplementedWorkerServer
	cfg        config
	logger     *log.Logger
	etcd       *clientv3.Client
	metrics    *workerMetrics
	logBatcher *logBatcher
	mu         sync.Mutex
	running    map[int64]*runningExecution
	done       map[int64]completedExecution
}

type runningExecution struct {
	cancel    context.CancelCauseFunc
	cleanup   func()
	attempt   uint32
	startedAt time.Time
	stopOnce  sync.Once
}

type completedExecution struct {
	attempt      uint32
	status       scheduler.JobInstanceStatus
	exitCode     int32
	errorMessage string
	startedAt    time.Time
	finishedAt   time.Time
	recordedAt   time.Time
}

func (e *runningExecution) requestStop(cause error) {
	e.stopOnce.Do(func() {
		e.cancel(cause)
	})
}

func main() {
	etcdDialTimeout, err := durationEnvOrDefault("ETCD_DIAL_TIMEOUT", discovery.DefaultEtcdDialTimeout)
	if err != nil {
		log.Fatalf("invalid ETCD_DIAL_TIMEOUT: %v", err)
	}
	masterReportTimeout, err := durationEnvOrDefault("MASTER_REPORT_TIMEOUT", defaultMasterReportTimeout)
	if err != nil {
		log.Fatalf("invalid MASTER_REPORT_TIMEOUT: %v", err)
	}
	if masterReportTimeout <= 0 {
		log.Fatalf("MASTER_REPORT_TIMEOUT must be > 0, got %s", masterReportTimeout)
	}
	heartbeatInterval, err := durationEnvOrDefault("WORKER_HEARTBEAT_INTERVAL", defaultWorkerHeartbeatInterval)
	if err != nil {
		log.Fatalf("invalid WORKER_HEARTBEAT_INTERVAL: %v", err)
	}
	if heartbeatInterval <= 0 {
		log.Fatalf("WORKER_HEARTBEAT_INTERVAL must be > 0, got %s", heartbeatInterval)
	}
	completedStateTTL, err := durationEnvOrDefault("WORKER_COMPLETED_TTL", defaultCompletedStateTTL)
	if err != nil {
		log.Fatalf("invalid WORKER_COMPLETED_TTL: %v", err)
	}
	if completedStateTTL <= 0 {
		log.Fatalf("WORKER_COMPLETED_TTL must be > 0, got %s", completedStateTTL)
	}
	killGracePeriod, err := durationEnvOrDefault("WORKER_KILL_GRACE_PERIOD", defaultKillGracePeriod)
	if err != nil {
		log.Fatalf("invalid WORKER_KILL_GRACE_PERIOD: %v", err)
	}
	if killGracePeriod <= 0 {
		log.Fatalf("WORKER_KILL_GRACE_PERIOD must be > 0, got %s", killGracePeriod)
	}
	logMongoConnectTimeout, err := durationEnvOrDefault("WORKER_LOG_MONGO_CONNECT_TIMEOUT", defaultMongoConnectTimeout)
	if err != nil {
		log.Fatalf("invalid WORKER_LOG_MONGO_CONNECT_TIMEOUT: %v", err)
	}
	if logMongoConnectTimeout <= 0 {
		log.Fatalf("WORKER_LOG_MONGO_CONNECT_TIMEOUT must be > 0, got %s", logMongoConnectTimeout)
	}
	logMongoWriteTimeout, err := durationEnvOrDefault("WORKER_LOG_MONGO_WRITE_TIMEOUT", defaultMongoWriteTimeout)
	if err != nil {
		log.Fatalf("invalid WORKER_LOG_MONGO_WRITE_TIMEOUT: %v", err)
	}
	if logMongoWriteTimeout <= 0 {
		log.Fatalf("WORKER_LOG_MONGO_WRITE_TIMEOUT must be > 0, got %s", logMongoWriteTimeout)
	}
	logBufferSize, err := intEnvOrDefault("WORKER_LOG_BUFFER_SIZE", defaultWorkerLogBufferSize)
	if err != nil {
		log.Fatalf("invalid WORKER_LOG_BUFFER_SIZE: %v", err)
	}
	if logBufferSize <= 0 {
		log.Fatalf("WORKER_LOG_BUFFER_SIZE must be > 0, got %d", logBufferSize)
	}
	logBatchSize, err := intEnvOrDefault("WORKER_LOG_BATCH_SIZE", defaultWorkerLogBatchSize)
	if err != nil {
		log.Fatalf("invalid WORKER_LOG_BATCH_SIZE: %v", err)
	}
	if logBatchSize <= 0 {
		log.Fatalf("WORKER_LOG_BATCH_SIZE must be > 0, got %d", logBatchSize)
	}
	logBatchWindow, err := durationEnvOrDefault("WORKER_LOG_BATCH_WINDOW", defaultWorkerLogBatchWindow)
	if err != nil {
		log.Fatalf("invalid WORKER_LOG_BATCH_WINDOW: %v", err)
	}
	if logBatchWindow <= 0 {
		log.Fatalf("WORKER_LOG_BATCH_WINDOW must be > 0, got %s", logBatchWindow)
	}
	workerLeaseTTL, err := int64EnvOrDefault("WORKER_LEASE_TTL", discovery.DefaultLeaseTTLSeconds)
	if err != nil {
		log.Fatalf("invalid WORKER_LEASE_TTL: %v", err)
	}
	if workerLeaseTTL <= 0 {
		log.Fatalf("WORKER_LEASE_TTL must be > 0, got %d", workerLeaseTTL)
	}

	cfg := config{
		WorkerGRPCAddr:        envOrDefault("WORKER_GRPC_ADDR", ":50051"),
		WorkerHTTPAddr:        envOrDefault("WORKER_HTTP_ADDR", defaultWorkerHTTPAddr),
		WorkerAdvertiseAddr:   envOrDefault("WORKER_ADVERTISE_ADDR", envOrDefault("WORKER_GRPC_ADDR", ":50051")),
		MasterGRPCAddrs:       parseMasterGRPCAddrs(envOrDefault("MASTER_GRPC_ADDRS", envOrDefault("MASTER_GRPC_ADDR", "master:50052"))),
		WorkerID:              envOrDefault("WORKER_ID", "worker-1"),
		LogDir:                envOrDefault("LOG_DIR", "/app/logs"),
		EtcdEndpoints:         discovery.ParseEtcdEndpoints(envOrDefault("ETCD_ENDPOINTS", discovery.DefaultEtcdEndpointsRaw)),
		EtcdDialTimeout:       etcdDialTimeout,
		WorkerDiscoveryPrefix: discovery.NormalizeWorkerPrefix(envOrDefault("WORKER_DISCOVERY_PREFIX", discovery.DefaultWorkerPrefix)),
		WorkerLeaseTTL:        workerLeaseTTL,
		LeaderElectionKey:     election.NormalizeLeaderKey(envOrDefault("LEADER_ELECTION_KEY", election.DefaultLeaderKey)),
		MasterReportTimeout:   masterReportTimeout,
		HeartbeatInterval:     heartbeatInterval,
		CompletedStateTTL:     completedStateTTL,
		KillGracePeriod:       killGracePeriod,
		LogBatcher: logBatcherConfig{
			URI:            envOrDefault("WORKER_LOG_MONGO_URI", defaultMongoURI),
			Database:       envOrDefault("WORKER_LOG_MONGO_DATABASE", defaultMongoDatabase),
			Collection:     envOrDefault("WORKER_LOG_MONGO_COLLECTION", defaultMongoCollection),
			ConnectTimeout: logMongoConnectTimeout,
			WriteTimeout:   logMongoWriteTimeout,
			BufferSize:     logBufferSize,
			BatchSize:      logBatchSize,
			BatchWindow:    logBatchWindow,
		},
	}
	if len(cfg.EtcdEndpoints) == 0 {
		log.Fatalf("no ETCD_ENDPOINTS configured")
	}

	if err := os.MkdirAll(cfg.LogDir, 0o755); err != nil {
		log.Fatalf("create log dir failed: %v", err)
	}

	logger := log.New(os.Stdout, "[worker] ", log.LstdFlags|log.Lmicroseconds)
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: cfg.EtcdDialTimeout,
	})
	if err != nil {
		logger.Fatalf("create etcd client failed: %v", err)
	}
	defer etcdClient.Close()

	metrics := newWorkerMetrics()
	logBatcher, err := newLogBatcher(context.Background(), cfg.LogBatcher, logger, metrics)
	if err != nil {
		logger.Fatalf("create mongo log batcher failed: %v", err)
	}
	defer logBatcher.Close()

	srv := &workerServer{
		cfg:        cfg,
		logger:     logger,
		etcd:       etcdClient,
		metrics:    metrics,
		logBatcher: logBatcher,
		running:    make(map[int64]*runningExecution),
		done:       make(map[int64]completedExecution),
	}

	lis, err := net.Listen("tcp", cfg.WorkerGRPCAddr)
	if err != nil {
		logger.Fatalf("listen worker grpc failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	scheduler.RegisterWorkerServer(grpcServer, srv)

	go srv.runRegistrationLoop(context.Background())
	errCh := make(chan error, 2)

	go func() {
		errCh <- grpcServer.Serve(lis)
	}()

	go func() {
		errCh <- srv.serveHTTP(cfg.WorkerHTTPAddr)
	}()

	logger.Printf(
		"worker started: grpc=%s http=%s advertise=%s masters=%v etcd=%v leader_key=%s lease_ttl=%d report_timeout=%s heartbeat_interval=%s completed_ttl=%s kill_grace=%s mongo_uri=%s mongo_db=%s mongo_collection=%s log_buffer=%d log_batch=%d log_window=%s",
		cfg.WorkerGRPCAddr,
		cfg.WorkerHTTPAddr,
		cfg.WorkerAdvertiseAddr,
		cfg.MasterGRPCAddrs,
		cfg.EtcdEndpoints,
		cfg.LeaderElectionKey,
		cfg.WorkerLeaseTTL,
		cfg.MasterReportTimeout,
		cfg.HeartbeatInterval,
		cfg.CompletedStateTTL,
		cfg.KillGracePeriod,
		cfg.LogBatcher.URI,
		cfg.LogBatcher.Database,
		cfg.LogBatcher.Collection,
		cfg.LogBatcher.BufferSize,
		cfg.LogBatcher.BatchSize,
		cfg.LogBatcher.BatchWindow,
	)
	if err := <-errCh; err != nil {
		logger.Fatalf("server exited: %v", err)
	}
}

func (s *workerServer) runRegistrationLoop(ctx context.Context) {
	for {
		err := s.registerWorker(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Printf("worker registration loop error: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (s *workerServer) registerWorker(ctx context.Context) error {
	registration := discovery.WorkerRegistration{
		ID:           s.cfg.WorkerID,
		Addr:         s.cfg.WorkerAdvertiseAddr,
		RegisteredAt: time.Now().UTC(),
	}
	value, err := discovery.EncodeWorkerRegistration(registration)
	if err != nil {
		return fmt.Errorf("encode worker registration failed: %w", err)
	}

	leaseCtx, cancelLease := context.WithTimeout(ctx, 5*time.Second)
	leaseResp, err := s.etcd.Grant(leaseCtx, s.cfg.WorkerLeaseTTL)
	cancelLease()
	if err != nil {
		return fmt.Errorf("grant worker lease failed: %w", err)
	}

	key := discovery.WorkerKey(s.cfg.WorkerDiscoveryPrefix, s.cfg.WorkerID)
	putCtx, cancelPut := context.WithTimeout(ctx, 5*time.Second)
	_, err = s.etcd.Put(putCtx, key, string(value), clientv3.WithLease(leaseResp.ID))
	cancelPut()
	if err != nil {
		return fmt.Errorf("put worker registration failed: %w", err)
	}

	keepAliveCtx, cancelKeepAlive := context.WithCancel(ctx)
	defer cancelKeepAlive()

	keepAliveCh, err := s.etcd.KeepAlive(keepAliveCtx, leaseResp.ID)
	if err != nil {
		s.revokeLease(leaseResp.ID)
		return fmt.Errorf("worker keepalive failed: %w", err)
	}

	s.logger.Printf(
		"worker registered: key=%s id=%s addr=%s lease_id=%d ttl=%d",
		key,
		s.cfg.WorkerID,
		s.cfg.WorkerAdvertiseAddr,
		leaseResp.ID,
		s.cfg.WorkerLeaseTTL,
	)

	for {
		select {
		case <-ctx.Done():
			s.revokeLease(leaseResp.ID)
			return ctx.Err()
		case resp, ok := <-keepAliveCh:
			if !ok || resp == nil {
				return fmt.Errorf("worker keepalive channel closed")
			}
		}
	}
}

func (s *workerServer) revokeLease(leaseID clientv3.LeaseID) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := s.etcd.Revoke(ctx, leaseID); err != nil {
		s.logger.Printf("revoke worker lease failed: lease_id=%d err=%v", leaseID, err)
	}
}

func (s *workerServer) RunJob(ctx context.Context, req *scheduler.RunJobRequest) (*scheduler.RunJobResponse, error) {
	if strings.TrimSpace(req.GetCommand()) == "" {
		return &scheduler.RunJobResponse{Accepted: false, Message: "command is required"}, nil
	}
	instanceID := req.GetJobInstanceId()
	if instanceID <= 0 {
		return &scheduler.RunJobResponse{Accepted: false, Message: "job_instance_id must be > 0"}, nil
	}
	if req.GetAttempt() == 0 {
		return &scheduler.RunJobResponse{Accepted: false, Message: "attempt must be > 0"}, nil
	}

	s.mu.Lock()
	s.pruneCompletedLocked(time.Now().UTC())
	if execution, exists := s.running[instanceID]; exists {
		if req.GetAttempt() > execution.attempt {
			execution.attempt = req.GetAttempt()
		}
		attempt := execution.attempt
		startedAt := execution.startedAt
		s.mu.Unlock()
		go s.reportJobStatus(req.GetJobId(), instanceID, attempt, scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_RUNNING, 0, "", startedAt, time.Time{})
		return &scheduler.RunJobResponse{Accepted: true, Message: "job instance already running"}, nil
	}
	if completed, exists := s.done[instanceID]; exists {
		if completed.status == scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_SUCCESS || req.GetAttempt() <= completed.attempt {
			if req.GetAttempt() > completed.attempt {
				completed.attempt = req.GetAttempt()
				s.done[instanceID] = completed
			}
			s.mu.Unlock()
			go s.reportJobStatus(
				req.GetJobId(),
				instanceID,
				completed.attempt,
				completed.status,
				completed.exitCode,
				completed.errorMessage,
				completed.startedAt,
				completed.finishedAt,
			)
			return &scheduler.RunJobResponse{Accepted: true, Message: "job instance already completed"}, nil
		}
		delete(s.done, instanceID)
	}

	timeoutCtx := context.Background()
	cancelTimeout := func() {}
	if req.GetTimeoutSeconds() > 0 {
		timeoutCtx, cancelTimeout = context.WithTimeoutCause(
			timeoutCtx,
			time.Duration(req.GetTimeoutSeconds())*time.Second,
			errExecutionTimedOut,
		)
	}
	runCtx, cancelRun := context.WithCancelCause(timeoutCtx)
	s.running[instanceID] = &runningExecution{
		cancel: cancelRun,
		cleanup: func() {
			cancelRun(nil)
			cancelTimeout()
		},
		attempt:   req.GetAttempt(),
		startedAt: time.Now().UTC(),
	}
	s.metrics.setRunningJobs(len(s.running))
	s.mu.Unlock()

	go s.execute(runCtx, req)

	s.logger.Printf("accept job: instance_id=%d attempt=%d command=%s args=%v", instanceID, req.GetAttempt(), req.GetCommand(), req.GetArgs())
	return &scheduler.RunJobResponse{Accepted: true, Message: "accepted"}, nil
}

func (s *workerServer) KillJob(ctx context.Context, req *scheduler.KillJobRequest) (*scheduler.KillJobResponse, error) {
	instanceID := req.GetJobInstanceId()
	if instanceID <= 0 {
		return &scheduler.KillJobResponse{Accepted: false, Message: "job_instance_id must be > 0"}, nil
	}

	s.mu.Lock()
	execution, ok := s.running[instanceID]
	s.mu.Unlock()
	if !ok {
		return &scheduler.KillJobResponse{Accepted: false, Message: "job instance not running"}, nil
	}

	reason := strings.TrimSpace(req.GetReason())
	execution.requestStop(manualKillCause(reason))
	s.logger.Printf("kill requested: instance_id=%d reason=%s", instanceID, reason)
	return &scheduler.KillJobResponse{Accepted: true, Message: "kill signal sent"}, nil
}

func (s *workerServer) execute(runCtx context.Context, req *scheduler.RunJobRequest) {
	instanceID := req.GetJobInstanceId()
	startedAt, ok := s.runningStartedAt(instanceID)
	if !ok {
		startedAt = time.Now().UTC()
	}

	logPath := filepath.Join(s.cfg.LogDir, fmt.Sprintf("job_%d_instance_%d.log", req.GetJobId(), instanceID))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		s.logger.Printf("open log file failed: instance_id=%d err=%v", instanceID, err)
		finalAttempt := s.currentAttempt(instanceID, req.GetAttempt())
		finishedAt := time.Now().UTC()
		s.reportJobStatus(req.GetJobId(), instanceID, finalAttempt, scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_FAILED, -1, err.Error(), startedAt, finishedAt)
		s.markCompleted(instanceID, finalAttempt, scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_FAILED, -1, err.Error(), startedAt, finishedAt)
		return
	}
	defer logFile.Close()

	logSink := newInstanceLogSink(logFile, s.logBatcher, instanceLogMeta{
		WorkerID:      s.cfg.WorkerID,
		JobID:         req.GetJobId(),
		JobInstanceID: instanceID,
		Attempt:       req.GetAttempt(),
	})
	defer logSink.Flush()

	_, _ = fmt.Fprintf(logSink.System(), "[%s] start worker=%s attempt=%d command=%s args=%v\n", startedAt.Format(time.RFC3339Nano), s.cfg.WorkerID, req.GetAttempt(), req.GetCommand(), req.GetArgs())

	cmd := exec.CommandContext(runCtx, req.GetCommand(), req.GetArgs()...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Env = append(os.Environ(), flattenEnv(req.GetEnv())...)
	cmd.Stdout = logSink.Stdout()
	cmd.Stderr = logSink.Stderr()
	cmd.Cancel = buildCommandCanceler(runCtx, cmd, logSink.System(), s.cfg.KillGracePeriod)

	if err := cmd.Start(); err != nil {
		finished := time.Now().UTC()
		s.logger.Printf("start command failed: instance_id=%d err=%v", instanceID, err)
		_, _ = fmt.Fprintf(logSink.System(), "[%s] start failed: %v\n", finished.Format(time.RFC3339Nano), err)
		finalAttempt := s.currentAttempt(instanceID, req.GetAttempt())
		s.reportJobStatus(req.GetJobId(), instanceID, finalAttempt, scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_FAILED, -1, err.Error(), startedAt, finished)
		s.markCompleted(instanceID, finalAttempt, scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_FAILED, -1, err.Error(), startedAt, finished)
		return
	}

	go s.runHeartbeatLoop(runCtx, req.GetJobId(), instanceID, startedAt)
	s.reportJobStatus(req.GetJobId(), instanceID, s.currentAttempt(instanceID, req.GetAttempt()), scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_RUNNING, 0, "", startedAt, time.Time{})

	err = cmd.Wait()
	finishedAt := time.Now().UTC()

	status := scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_SUCCESS
	exitCode := int32(0)
	errMsg := ""

	if err != nil {
		cause := context.Cause(runCtx)
		if cause != nil && (errors.Is(cause, errManualKillRequested) || errors.Is(cause, errExecutionTimedOut) || errors.Is(runCtx.Err(), context.Canceled) || errors.Is(runCtx.Err(), context.DeadlineExceeded)) {
			status = scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_KILLED
			errMsg = cause.Error()
		} else {
			status = scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_FAILED
			errMsg = err.Error()
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = int32(exitErr.ExitCode())
		} else {
			exitCode = -1
		}
	}

	finalAttempt := s.currentAttempt(instanceID, req.GetAttempt())
	_, _ = fmt.Fprintf(logSink.System(), "[%s] finished attempt=%d status=%s exit_code=%d error=%s\n", finishedAt.Format(time.RFC3339Nano), finalAttempt, status.String(), exitCode, errMsg)
	logSink.Flush()
	s.reportJobStatus(req.GetJobId(), instanceID, finalAttempt, status, exitCode, errMsg, startedAt, finishedAt)
	s.markCompleted(instanceID, finalAttempt, status, exitCode, errMsg, startedAt, finishedAt)
}

func buildCommandCanceler(runCtx context.Context, cmd *exec.Cmd, logWriter io.Writer, gracePeriod time.Duration) func() error {
	return func() error {
		if cmd.Process == nil || cmd.Process.Pid <= 0 {
			return os.ErrProcessDone
		}

		processGroupID := cmd.Process.Pid
		cause := context.Cause(runCtx)
		if cause == nil {
			cause = context.Canceled
		}

		now := time.Now().UTC()
		_, _ = fmt.Fprintf(logWriter, "[%s] stop requested: pid=%d cause=%s\n", now.Format(time.RFC3339Nano), processGroupID, cause.Error())

		if err := signalProcessGroup(processGroupID, syscall.SIGTERM); err != nil {
			if errors.Is(err, os.ErrProcessDone) {
				_, _ = fmt.Fprintf(logWriter, "[%s] stop skipped: process_group=%d already exited\n", time.Now().UTC().Format(time.RFC3339Nano), processGroupID)
				return os.ErrProcessDone
			}
			_, _ = fmt.Fprintf(logWriter, "[%s] SIGTERM failed: process_group=%d err=%v\n", time.Now().UTC().Format(time.RFC3339Nano), processGroupID, err)
			return err
		}
		_, _ = fmt.Fprintf(logWriter, "[%s] SIGTERM sent: process_group=%d grace=%s\n", time.Now().UTC().Format(time.RFC3339Nano), processGroupID, gracePeriod)

		deadline := time.Now().Add(gracePeriod)
		for processGroupAlive(processGroupID) && time.Now().Before(deadline) {
			sleepFor := 100 * time.Millisecond
			if remaining := time.Until(deadline); remaining < sleepFor {
				sleepFor = remaining
			}
			if sleepFor <= 0 {
				break
			}
			time.Sleep(sleepFor)
		}

		if !processGroupAlive(processGroupID) {
			_, _ = fmt.Fprintf(logWriter, "[%s] process group exited after SIGTERM: process_group=%d\n", time.Now().UTC().Format(time.RFC3339Nano), processGroupID)
			return nil
		}

		if err := signalProcessGroup(processGroupID, syscall.SIGKILL); err != nil {
			if errors.Is(err, os.ErrProcessDone) {
				_, _ = fmt.Fprintf(logWriter, "[%s] process group exited before SIGKILL: process_group=%d\n", time.Now().UTC().Format(time.RFC3339Nano), processGroupID)
				return nil
			}
			_, _ = fmt.Fprintf(logWriter, "[%s] SIGKILL failed: process_group=%d err=%v\n", time.Now().UTC().Format(time.RFC3339Nano), processGroupID, err)
			return err
		}
		_, _ = fmt.Fprintf(logWriter, "[%s] SIGKILL sent: process_group=%d\n", time.Now().UTC().Format(time.RFC3339Nano), processGroupID)
		return nil
	}
}

func signalProcessGroup(processGroupID int, signal syscall.Signal) error {
	if processGroupID <= 0 {
		return os.ErrProcessDone
	}
	err := syscall.Kill(-processGroupID, signal)
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}

func processGroupAlive(processGroupID int) bool {
	if processGroupID <= 0 {
		return false
	}
	err := syscall.Kill(-processGroupID, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func manualKillCause(reason string) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return errManualKillRequested
	}
	return fmt.Errorf("%w: %s", errManualKillRequested, reason)
}

func (s *workerServer) runHeartbeatLoop(runCtx context.Context, jobID, instanceID int64, startedAt time.Time) {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-runCtx.Done():
			return
		case <-ticker.C:
			attempt, ok := s.currentAttemptWithOK(instanceID)
			if !ok {
				return
			}
			s.reportJobStatus(jobID, instanceID, attempt, scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_RUNNING, 0, "", startedAt, time.Time{})
		}
	}
}

func (s *workerServer) currentAttempt(instanceID int64, fallback uint32) uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if execution, ok := s.running[instanceID]; ok {
		return execution.attempt
	}
	if completed, ok := s.done[instanceID]; ok {
		return completed.attempt
	}
	return fallback
}

func (s *workerServer) currentAttemptWithOK(instanceID int64) (uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if execution, ok := s.running[instanceID]; ok {
		return execution.attempt, true
	}
	return 0, false
}

func (s *workerServer) runningStartedAt(instanceID int64) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if execution, ok := s.running[instanceID]; ok {
		return execution.startedAt, true
	}
	return time.Time{}, false
}

func (s *workerServer) markCompleted(
	instanceID int64,
	attempt uint32,
	status scheduler.JobInstanceStatus,
	exitCode int32,
	errMessage string,
	startedAt time.Time,
	finishedAt time.Time,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if execution, ok := s.running[instanceID]; ok {
		if execution.cleanup != nil {
			execution.cleanup()
		}
		delete(s.running, instanceID)
		s.metrics.setRunningJobs(len(s.running))
	}
	s.done[instanceID] = completedExecution{
		attempt:      attempt,
		status:       status,
		exitCode:     exitCode,
		errorMessage: errMessage,
		startedAt:    startedAt,
		finishedAt:   finishedAt,
		recordedAt:   time.Now().UTC(),
	}
	s.pruneCompletedLocked(time.Now().UTC())
}

func (s *workerServer) pruneCompletedLocked(now time.Time) {
	for instanceID, completed := range s.done {
		if now.Sub(completed.recordedAt) > s.cfg.CompletedStateTTL {
			delete(s.done, instanceID)
		}
	}
}

func (s *workerServer) reportJobStatus(
	jobID int64,
	instanceID int64,
	attempt uint32,
	status scheduler.JobInstanceStatus,
	exitCode int32,
	errMessage string,
	startedAt time.Time,
	finishedAt time.Time,
) {
	report := &scheduler.ReportResultRequest{
		JobId:         jobID,
		JobInstanceId: instanceID,
		WorkerId:      s.cfg.WorkerID,
		Status:        status,
		ExitCode:      exitCode,
		ErrorMessage:  errMessage,
		StartedAt:     timestamppb.New(startedAt),
		Attempt:       attempt,
	}
	if !finishedAt.IsZero() {
		report.FinishedAt = timestamppb.New(finishedAt)
	}

	deadline := time.Now().Add(s.cfg.MasterReportTimeout)
	transportAttempt := 0
	var lastErr error

	for {
		transportAttempt++

		targets, err := s.resolveMasterReportTargets()
		if err != nil {
			lastErr = err
		} else {
			for _, target := range targets {
				if err := s.sendResultReport(target, report); err == nil {
					s.logger.Printf(
						"report success: instance_id=%d status=%s report_attempt=%d target=%s transport_attempt=%d",
						instanceID,
						status.String(),
						attempt,
						target,
						transportAttempt,
					)
					return
				} else {
					lastErr = err
				}
			}
		}

		if time.Now().After(deadline) {
			break
		}

		s.logger.Printf(
			"report retry: instance_id=%d status=%s report_attempt=%d transport_attempt=%d err=%v",
			instanceID,
			status.String(),
			attempt,
			transportAttempt,
			lastErr,
		)

		sleepFor := time.Second
		if remaining := time.Until(deadline); remaining < sleepFor {
			sleepFor = remaining
		}
		if sleepFor <= 0 {
			break
		}
		time.Sleep(sleepFor)
	}

	s.logger.Printf(
		"report failed permanently: instance_id=%d status=%s report_attempt=%d transport_attempt=%d err=%v",
		instanceID,
		status.String(),
		attempt,
		transportAttempt,
		lastErr,
	)
}

func (s *workerServer) resolveMasterReportTargets() ([]string, error) {
	seen := make(map[string]struct{}, len(s.cfg.MasterGRPCAddrs)+1)
	targets := make([]string, 0, len(s.cfg.MasterGRPCAddrs)+1)

	if leaderAddr, err := s.lookupLeaderGRPCAddr(); err == nil && leaderAddr != "" {
		seen[leaderAddr] = struct{}{}
		targets = append(targets, leaderAddr)
	} else if err != nil {
		s.logger.Printf("lookup leader for report failed: err=%v", err)
	}

	for _, addr := range s.cfg.MasterGRPCAddrs {
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		targets = append(targets, addr)
	}

	if len(targets) == 0 {
		return nil, fmt.Errorf("no master grpc targets available")
	}
	return targets, nil
}

func (s *workerServer) lookupLeaderGRPCAddr() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := s.etcd.Get(ctx, s.cfg.LeaderElectionKey)
	if err != nil {
		return "", fmt.Errorf("load leader key failed: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("leader key %s not found", s.cfg.LeaderElectionKey)
	}

	record, err := election.DecodeLeaderRecord(resp.Kvs[0].Value)
	if err != nil {
		return "", fmt.Errorf("decode leader record failed: %w", err)
	}
	if strings.TrimSpace(record.GRPCAdvertiseAddr) == "" {
		return "", fmt.Errorf("leader record missing grpc addr")
	}
	return record.GRPCAdvertiseAddr, nil
}

func (s *workerServer) sendResultReport(target string, report *scheduler.ReportResultRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("dial master %s failed: %w", target, err)
	}
	defer conn.Close()

	client := scheduler.NewMasterClient(conn)
	resp, err := client.ReportResult(ctx, report)
	if err != nil {
		return fmt.Errorf("report result to %s failed: %w", target, err)
	}
	if !resp.GetAccepted() {
		return fmt.Errorf("report to %s rejected: %s", target, resp.GetMessage())
	}
	return nil
}

func flattenEnv(env map[string]string) []string {
	if len(env) == 0 {
		return nil
	}
	result := make([]string, 0, len(env))
	for k, v := range env {
		result = append(result, k+"="+v)
	}
	return result
}

func envOrDefault(key, defaultValue string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return defaultValue
}

func durationEnvOrDefault(key string, defaultValue time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue, nil
	}
	return time.ParseDuration(value)
}

func int64EnvOrDefault(key string, defaultValue int64) (int64, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

func intEnvOrDefault(key string, defaultValue int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue, nil
	}
	return strconv.Atoi(value)
}

func parseMasterGRPCAddrs(raw string) []string {
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		addr := strings.TrimSpace(part)
		if addr == "" {
			continue
		}
		addrs = append(addrs, addr)
	}
	return addrs
}
