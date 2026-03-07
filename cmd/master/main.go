package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	scheduler "github.com/HITGerhman/distributed-job-scheduler/api/proto"
	"github.com/HITGerhman/distributed-job-scheduler/internal/discovery"
	"github.com/HITGerhman/distributed-job-scheduler/internal/election"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/robfig/cron/v3"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	manualCronExpr                = "@manual"
	defaultSchedulerTickInterval  = time.Second
	defaultSchedulerCatchupWindow = time.Minute
	defaultSchedulerMaxCatchup    = 10
	defaultDispatchAckTimeout     = 5 * time.Second
	defaultJobMaxRetries          = 2
	defaultRetryBackoffSeconds    = 2
	defaultMaxRetryBackoffSeconds = 30
	defaultHeartbeatTimeoutSecs   = 15
)

var defaultCronParser = cron.NewParser(
	cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

type config struct {
	MySQLDSN                string
	MasterID                string
	MasterGRPCAddr          string
	MasterHTTPAddr          string
	MasterAdvertiseGRPCAddr string
	MasterAdvertiseHTTPAddr string
	WorkerGRPCAddr          string
	EtcdEndpoints           []string
	EtcdDialTimeout         time.Duration
	WorkerDiscoveryPrefix   string
	LeaderElectionKey       string
	MasterLeaseTTL          int64
	DispatchAckTimeout      time.Duration
	SchedulerTickInterval   time.Duration
	SchedulerCatchupWindow  time.Duration
	SchedulerMaxCatchup     int
}

type masterServer struct {
	scheduler.UnimplementedMasterServer
	db              *sql.DB
	cfg             config
	logger          *log.Logger
	etcd            *clientv3.Client
	workersMu       sync.RWMutex
	workers         map[string]discovery.WorkerRegistration
	workerOrder     []string
	nextWorkerIndex int
	leaderMu        sync.RWMutex
	isLeader        bool
	leaderRecord    election.LeaderRecord
}

type createJobRequest struct {
	Name                    string   `json:"name"`
	CronExpr                string   `json:"cron_expr"`
	Command                 string   `json:"command"`
	Args                    []string `json:"args"`
	TimeoutSeconds          uint32   `json:"timeout_seconds"`
	MaxRetries              *uint32  `json:"max_retries"`
	RetryBackoffSeconds     *uint32  `json:"retry_backoff_seconds"`
	MaxRetryBackoffSeconds  *uint32  `json:"max_retry_backoff_seconds"`
	HeartbeatTimeoutSeconds *uint32  `json:"heartbeat_timeout_seconds"`
	Enabled                 *bool    `json:"enabled"`
}

type createJobResponse struct {
	ID int64 `json:"id"`
}

type triggerJobResponse struct {
	JobInstanceID int64  `json:"job_instance_id"`
	Message       string `json:"message"`
}

type killJobInstanceRequest struct {
	Reason string `json:"reason"`
}

type killJobInstanceResponse struct {
	JobInstanceID int64  `json:"job_instance_id"`
	Status        string `json:"status"`
	Message       string `json:"message"`
}

type jobRecord struct {
	ID                      int64
	Name                    string
	CronExpr                string
	Command                 string
	Args                    []string
	TimeoutSeconds          uint32
	MaxRetries              int
	RetryBackoffSeconds     int
	MaxRetryBackoffSeconds  int
	HeartbeatTimeoutSeconds int
	CreatedAt               time.Time
}

type jobInstanceRecord struct {
	ID                      int64
	JobID                   int64
	Status                  string
	ScheduledAt             time.Time
	Attempt                 int
	MaxAttempts             int
	RetryBackoffSeconds     int
	MaxRetryBackoffSeconds  int
	HeartbeatTimeoutSeconds int
	NextRetryAt             sql.NullTime
	LastHeartbeatAt         sql.NullTime
	WorkerID                sql.NullString
	StartedAt               sql.NullTime
	FinishedAt              sql.NullTime
	ExitCode                sql.NullInt64
	ErrorMessage            sql.NullString
}

type triggerMode struct {
	Source                     string
	MarkFailedOnDispatchError  bool
	AllowExistingPendingReplay bool
}

func main() {
	masterID := strings.TrimSpace(os.Getenv("MASTER_ID"))
	if masterID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatalf("read hostname failed: %v", err)
		}
		masterID = hostname
	}
	etcdDialTimeout, err := durationEnvOrDefault("ETCD_DIAL_TIMEOUT", discovery.DefaultEtcdDialTimeout)
	if err != nil {
		log.Fatalf("invalid ETCD_DIAL_TIMEOUT: %v", err)
	}
	masterLeaseTTL, err := int64EnvOrDefault("MASTER_LEASE_TTL", election.DefaultMasterLeaseTTLSeconds)
	if err != nil {
		log.Fatalf("invalid MASTER_LEASE_TTL: %v", err)
	}
	if masterLeaseTTL <= 0 {
		log.Fatalf("MASTER_LEASE_TTL must be > 0, got %d", masterLeaseTTL)
	}
	dispatchAckTimeout, err := durationEnvOrDefault("DISPATCH_ACK_TIMEOUT", defaultDispatchAckTimeout)
	if err != nil {
		log.Fatalf("invalid DISPATCH_ACK_TIMEOUT: %v", err)
	}
	if dispatchAckTimeout <= 0 {
		log.Fatalf("DISPATCH_ACK_TIMEOUT must be > 0, got %s", dispatchAckTimeout)
	}
	schedulerTickInterval, err := durationEnvOrDefault("SCHEDULER_TICK_INTERVAL", defaultSchedulerTickInterval)
	if err != nil {
		log.Fatalf("invalid SCHEDULER_TICK_INTERVAL: %v", err)
	}
	schedulerCatchupWindow, err := durationEnvOrDefault("SCHEDULER_CATCHUP_WINDOW", defaultSchedulerCatchupWindow)
	if err != nil {
		log.Fatalf("invalid SCHEDULER_CATCHUP_WINDOW: %v", err)
	}
	schedulerMaxCatchup, err := intEnvOrDefault("SCHEDULER_MAX_CATCHUP", defaultSchedulerMaxCatchup)
	if err != nil {
		log.Fatalf("invalid SCHEDULER_MAX_CATCHUP: %v", err)
	}
	if schedulerMaxCatchup <= 0 {
		log.Fatalf("SCHEDULER_MAX_CATCHUP must be > 0, got %d", schedulerMaxCatchup)
	}

	cfg := config{
		MySQLDSN:               envOrDefault("MYSQL_DSN", "root:172600@tcp(mysql:3306)/DJS?parseTime=true&loc=UTC"),
		MasterID:               masterID,
		MasterGRPCAddr:         envOrDefault("MASTER_GRPC_ADDR", ":50052"),
		MasterHTTPAddr:         envOrDefault("MASTER_HTTP_ADDR", ":8080"),
		WorkerGRPCAddr:         envOrDefault("WORKER_GRPC_ADDR", "worker:50051"),
		EtcdEndpoints:          discovery.ParseEtcdEndpoints(envOrDefault("ETCD_ENDPOINTS", discovery.DefaultEtcdEndpointsRaw)),
		EtcdDialTimeout:        etcdDialTimeout,
		WorkerDiscoveryPrefix:  discovery.NormalizeWorkerPrefix(envOrDefault("WORKER_DISCOVERY_PREFIX", discovery.DefaultWorkerPrefix)),
		LeaderElectionKey:      election.NormalizeLeaderKey(envOrDefault("LEADER_ELECTION_KEY", election.DefaultLeaderKey)),
		MasterLeaseTTL:         masterLeaseTTL,
		DispatchAckTimeout:     dispatchAckTimeout,
		SchedulerTickInterval:  schedulerTickInterval,
		SchedulerCatchupWindow: schedulerCatchupWindow,
		SchedulerMaxCatchup:    schedulerMaxCatchup,
	}
	cfg.MasterAdvertiseGRPCAddr = envOrDefault("MASTER_ADVERTISE_GRPC_ADDR", defaultAdvertiseAddr(cfg.MasterGRPCAddr, cfg.MasterID))
	cfg.MasterAdvertiseHTTPAddr = envOrDefault("MASTER_ADVERTISE_HTTP_ADDR", defaultAdvertiseAddr(cfg.MasterHTTPAddr, cfg.MasterID))
	if len(cfg.EtcdEndpoints) == 0 {
		log.Fatalf("no ETCD_ENDPOINTS configured")
	}

	logger := log.New(os.Stdout, "[master] ", log.LstdFlags|log.Lmicroseconds)

	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		logger.Fatalf("open mysql failed: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		logger.Fatalf("ping mysql failed: %v", err)
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: cfg.EtcdDialTimeout,
	})
	if err != nil {
		logger.Fatalf("create etcd client failed: %v", err)
	}
	defer etcdClient.Close()

	srv := &masterServer{
		db:      db,
		cfg:     cfg,
		logger:  logger,
		etcd:    etcdClient,
		workers: make(map[string]discovery.WorkerRegistration),
	}
	errCh := make(chan error, 2)

	go func() {
		errCh <- srv.serveGRPC(cfg.MasterGRPCAddr)
	}()

	go func() {
		errCh <- srv.serveHTTP(cfg.MasterHTTPAddr)
	}()

	go srv.runWorkerWatchLoop(context.Background())
	go srv.runLeaderElectionLoop(context.Background())
	go srv.runSchedulerLoop(context.Background())

	logger.Printf(
		"master started: id=%s grpc=%s advertise_grpc=%s http=%s advertise_http=%s worker_fallback=%s etcd=%v leader_key=%s lease_ttl=%d dispatch_ack_timeout=%s scheduler_tick=%s catchup_window=%s max_catchup=%d",
		cfg.MasterID,
		cfg.MasterGRPCAddr,
		cfg.MasterAdvertiseGRPCAddr,
		cfg.MasterHTTPAddr,
		cfg.MasterAdvertiseHTTPAddr,
		cfg.WorkerGRPCAddr,
		cfg.EtcdEndpoints,
		cfg.LeaderElectionKey,
		cfg.MasterLeaseTTL,
		cfg.DispatchAckTimeout,
		cfg.SchedulerTickInterval,
		cfg.SchedulerCatchupWindow,
		cfg.SchedulerMaxCatchup,
	)
	if err := <-errCh; err != nil {
		logger.Fatalf("server exited: %v", err)
	}
}

func (s *masterServer) serveGRPC(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen grpc failed: %w", err)
	}
	grpcServer := grpc.NewServer()
	scheduler.RegisterMasterServer(grpcServer, s)
	return grpcServer.Serve(lis)
}

func (s *masterServer) serveHTTP(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/jobs", s.handleCreateJob)
	mux.HandleFunc("/jobs/", s.handleTriggerJob)
	mux.HandleFunc("/job-instances/", s.handleJobInstance)

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return httpServer.ListenAndServe()
}

func (s *masterServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	isLeader, leader := s.leaderSnapshot()
	role := "follower"
	if isLeader {
		role = "leader"
	}

	resp := map[string]interface{}{
		"status":    "ok",
		"master_id": s.cfg.MasterID,
		"role":      role,
		"is_leader": isLeader,
		"workers":   s.workerCount(),
	}
	if leader.MasterID != "" {
		resp["leader_id"] = leader.MasterID
	}
	if leader.HTTPAdvertiseAddr != "" {
		resp["leader_http_addr"] = leader.HTTPAdvertiseAddr
	}
	if leader.GRPCAdvertiseAddr != "" {
		resp["leader_grpc_addr"] = leader.GRPCAdvertiseAddr
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *masterServer) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/jobs" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}

	req.Name = strings.TrimSpace(req.Name)
	req.CronExpr = strings.TrimSpace(req.CronExpr)
	req.Command = strings.TrimSpace(req.Command)
	if req.Name == "" || req.Command == "" {
		http.Error(w, "name and command are required", http.StatusBadRequest)
		return
	}
	if req.CronExpr == "" || strings.EqualFold(req.CronExpr, manualCronExpr) {
		req.CronExpr = manualCronExpr
	} else if _, err := parseCronSchedule(req.CronExpr); err != nil {
		http.Error(w, fmt.Sprintf("invalid cron_expr: %v", err), http.StatusBadRequest)
		return
	}
	if req.Args == nil {
		req.Args = []string{}
	}
	maxRetries := uint32OrDefault(req.MaxRetries, defaultJobMaxRetries)
	retryBackoffSeconds := uint32OrDefault(req.RetryBackoffSeconds, defaultRetryBackoffSeconds)
	maxRetryBackoffSeconds := uint32OrDefault(req.MaxRetryBackoffSeconds, defaultMaxRetryBackoffSeconds)
	heartbeatTimeoutSeconds := uint32OrDefault(req.HeartbeatTimeoutSeconds, defaultHeartbeatTimeoutSecs)
	if retryBackoffSeconds == 0 {
		http.Error(w, "retry_backoff_seconds must be > 0", http.StatusBadRequest)
		return
	}
	if maxRetryBackoffSeconds == 0 {
		http.Error(w, "max_retry_backoff_seconds must be > 0", http.StatusBadRequest)
		return
	}
	if maxRetryBackoffSeconds < retryBackoffSeconds {
		http.Error(w, "max_retry_backoff_seconds must be >= retry_backoff_seconds", http.StatusBadRequest)
		return
	}
	if heartbeatTimeoutSeconds == 0 {
		http.Error(w, "heartbeat_timeout_seconds must be > 0", http.StatusBadRequest)
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	argsJSON, err := json.Marshal(req.Args)
	if err != nil {
		http.Error(w, "marshal args failed", http.StatusInternalServerError)
		return
	}

	res, err := s.db.Exec(
		`INSERT INTO jobs(
		     name, cron_expr, command, args_json, timeout_seconds,
		     max_retries, retry_backoff_seconds, max_retry_backoff_seconds, heartbeat_timeout_seconds, enabled
		 ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.Name,
		req.CronExpr,
		req.Command,
		argsJSON,
		req.TimeoutSeconds,
		maxRetries,
		retryBackoffSeconds,
		maxRetryBackoffSeconds,
		heartbeatTimeoutSeconds,
		enabled,
	)
	if err != nil {
		http.Error(w, fmt.Sprintf("insert job failed: %v", err), http.StatusInternalServerError)
		return
	}

	jobID, err := res.LastInsertId()
	if err != nil {
		http.Error(w, fmt.Sprintf("read job id failed: %v", err), http.StatusInternalServerError)
		return
	}

	s.logger.Printf(
		"create job: id=%d name=%s command=%s max_retries=%d retry_backoff=%ds max_retry_backoff=%ds heartbeat_timeout=%ds",
		jobID,
		req.Name,
		req.Command,
		maxRetries,
		retryBackoffSeconds,
		maxRetryBackoffSeconds,
		heartbeatTimeoutSeconds,
	)
	writeJSON(w, http.StatusOK, createJobResponse{ID: jobID})
}

func (s *masterServer) handleTriggerJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 3 || parts[0] != "jobs" || parts[2] != "trigger" {
		http.NotFound(w, r)
		return
	}

	jobID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || jobID <= 0 {
		http.Error(w, "invalid job id", http.StatusBadRequest)
		return
	}

	job, err := s.loadJob(r.Context(), jobID)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, sql.ErrNoRows) {
			status = http.StatusNotFound
		}
		http.Error(w, fmt.Sprintf("load job failed: %v", err), status)
		return
	}

	instance, err := s.triggerJob(
		r.Context(),
		job,
		time.Now().UTC(),
		triggerMode{
			Source:                    "manual",
			MarkFailedOnDispatchError: true,
		},
	)
	if err != nil {
		http.Error(w, fmt.Sprintf("trigger failed: %v", err), http.StatusBadGateway)
		return
	}

	writeJSON(w, http.StatusOK, triggerJobResponse{
		JobInstanceID: instance.ID,
		Message:       "triggered",
	})
}

func (s *masterServer) handleJobInstance(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "job-instances" {
		http.NotFound(w, r)
		return
	}

	instanceID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || instanceID <= 0 {
		http.Error(w, "invalid job instance id", http.StatusBadRequest)
		return
	}

	switch {
	case len(parts) == 2 && r.Method == http.MethodGet:
		s.handleGetJobInstance(w, r, instanceID)
	case len(parts) == 3 && parts[2] == "kill" && r.Method == http.MethodPost:
		s.handleKillJobInstance(w, r, instanceID)
	case len(parts) == 2 || (len(parts) == 3 && parts[2] == "kill"):
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	default:
		http.NotFound(w, r)
	}
}

func (s *masterServer) handleGetJobInstance(w http.ResponseWriter, r *http.Request, instanceID int64) {

	instance, err := s.getJobInstanceByID(r.Context(), instanceID)
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, sql.ErrNoRows) {
			code = http.StatusNotFound
		}
		http.Error(w, fmt.Sprintf("query job instance failed: %v", err), code)
		return
	}

	resp := map[string]interface{}{
		"id":           instanceID,
		"job_id":       instance.JobID,
		"status":       instance.Status,
		"scheduled_at": instance.ScheduledAt.UTC().Format(time.RFC3339Nano),
		"attempt":      instance.Attempt,
		"max_attempts": instance.MaxAttempts,
	}
	if instance.NextRetryAt.Valid {
		resp["next_retry_at"] = instance.NextRetryAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if instance.LastHeartbeatAt.Valid {
		resp["last_heartbeat_at"] = instance.LastHeartbeatAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if instance.WorkerID.Valid {
		resp["worker_id"] = instance.WorkerID.String
	}
	if instance.StartedAt.Valid {
		resp["started_at"] = instance.StartedAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if instance.FinishedAt.Valid {
		resp["finished_at"] = instance.FinishedAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if instance.ExitCode.Valid {
		resp["exit_code"] = instance.ExitCode.Int64
	}
	if instance.ErrorMessage.Valid {
		resp["error_message"] = instance.ErrorMessage.String
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *masterServer) handleKillJobInstance(w http.ResponseWriter, r *http.Request, instanceID int64) {
	var req killJobInstanceRequest
	if r.Body != nil && r.Body != http.NoBody {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
	}

	instance, message, err := s.killJobInstance(r.Context(), instanceID, req.Reason)
	if err != nil {
		statusCode := http.StatusBadGateway
		if errors.Is(err, sql.ErrNoRows) {
			statusCode = http.StatusNotFound
		} else if strings.Contains(err.Error(), "already terminal") {
			statusCode = http.StatusConflict
		}
		http.Error(w, fmt.Sprintf("kill instance failed: %v", err), statusCode)
		return
	}

	writeJSON(w, http.StatusOK, killJobInstanceResponse{
		JobInstanceID: instance.ID,
		Status:        instance.Status,
		Message:       message,
	})
}

func (s *masterServer) loadJob(ctx context.Context, jobID int64) (jobRecord, error) {
	var (
		job      jobRecord
		argsJSON []byte
	)
	job.ID = jobID
	err := s.db.QueryRowContext(
		ctx,
		`SELECT
		     name, cron_expr, command, args_json, timeout_seconds,
		     max_retries, retry_backoff_seconds, max_retry_backoff_seconds, heartbeat_timeout_seconds, created_at
		 FROM jobs WHERE id=?`,
		jobID,
	).Scan(
		&job.Name,
		&job.CronExpr,
		&job.Command,
		&argsJSON,
		&job.TimeoutSeconds,
		&job.MaxRetries,
		&job.RetryBackoffSeconds,
		&job.MaxRetryBackoffSeconds,
		&job.HeartbeatTimeoutSeconds,
		&job.CreatedAt,
	)
	if err != nil {
		return jobRecord{}, err
	}

	if len(argsJSON) > 0 {
		if err := json.Unmarshal(argsJSON, &job.Args); err != nil {
			return jobRecord{}, fmt.Errorf("unmarshal args_json failed: %w", err)
		}
	}
	return job, nil
}

func (s *masterServer) loadSchedulableJobs(ctx context.Context) ([]jobRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT
		     id, name, cron_expr, command, args_json, timeout_seconds,
		     max_retries, retry_backoff_seconds, max_retry_backoff_seconds, heartbeat_timeout_seconds, created_at
		 FROM jobs
		 WHERE enabled=1 AND cron_expr<>?
		 ORDER BY id`,
		manualCronExpr,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]jobRecord, 0)
	for rows.Next() {
		var (
			job      jobRecord
			argsJSON []byte
		)
		if err := rows.Scan(
			&job.ID,
			&job.Name,
			&job.CronExpr,
			&job.Command,
			&argsJSON,
			&job.TimeoutSeconds,
			&job.MaxRetries,
			&job.RetryBackoffSeconds,
			&job.MaxRetryBackoffSeconds,
			&job.HeartbeatTimeoutSeconds,
			&job.CreatedAt,
		); err != nil {
			return nil, err
		}
		if len(argsJSON) > 0 {
			if err := json.Unmarshal(argsJSON, &job.Args); err != nil {
				return nil, fmt.Errorf("unmarshal args_json for job %d failed: %w", job.ID, err)
			}
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *masterServer) runSchedulerLoop(ctx context.Context) {
	s.scanAndDispatchDueJobs(ctx, time.Now().UTC())

	ticker := time.NewTicker(s.cfg.SchedulerTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case tickAt := <-ticker.C:
			s.scanAndDispatchDueJobs(ctx, tickAt.UTC())
		}
	}
}

func (s *masterServer) scanAndDispatchDueJobs(ctx context.Context, now time.Time) {
	if !s.shouldSchedule() {
		return
	}

	s.recoverRunningTimeouts(ctx, now)
	s.dispatchDuePendingInstances(ctx, now, "recovery")

	jobs, err := s.loadSchedulableJobs(ctx)
	if err != nil {
		s.logger.Printf("scheduler load jobs failed: %v", err)
		return
	}

	windowStart := now.UTC().Add(-s.cfg.SchedulerCatchupWindow)
	mode := triggerMode{
		Source:                     "scheduler",
		AllowExistingPendingReplay: true,
	}

	for _, job := range jobs {
		schedule, err := parseCronSchedule(job.CronExpr)
		if err != nil {
			s.logger.Printf("scheduler skip job: job_id=%d invalid cron_expr=%q err=%v", job.ID, job.CronExpr, err)
			continue
		}

		jobWindowStart := windowStart
		if job.CreatedAt.UTC().After(jobWindowStart) {
			jobWindowStart = job.CreatedAt.UTC()
		}

		slots, _ := scheduledSlotsWithinWindow(schedule, jobWindowStart, now.UTC(), s.cfg.SchedulerMaxCatchup)
		for _, slot := range slots {
			instance, err := s.triggerJob(ctx, job, slot, mode)
			if err != nil {
				s.logger.Printf(
					"scheduler trigger failed: job_id=%d instance_id=%d slot=%s err=%v",
					job.ID,
					instance.ID,
					slot.Format(time.RFC3339Nano),
					err,
				)
			}
		}
	}

	s.dispatchDuePendingInstances(ctx, now, "recovery")
}

func (s *masterServer) triggerJob(
	ctx context.Context,
	job jobRecord,
	scheduledAt time.Time,
	mode triggerMode,
) (jobInstanceRecord, error) {
	normalizedScheduledAt := normalizeScheduledAt(scheduledAt)

	instance, created, err := s.ensureJobInstance(ctx, job, normalizedScheduledAt)
	if err != nil {
		return jobInstanceRecord{}, err
	}

	if !created {
		switch instance.Status {
		case "PENDING":
			if !mode.AllowExistingPendingReplay {
				return instance, nil
			}
		default:
			return instance, nil
		}
	}

	action := "trigger job"
	if !created {
		action = "redispatch pending job"
	}

	dispatched, err := s.dispatchPendingInstance(ctx, job, instance, mode.Source)
	if err != nil {
		return dispatched, err
	}

	s.logger.Printf(
		"%s: source=%s job_id=%d instance_id=%d attempt=%d scheduled_at=%s",
		action,
		mode.Source,
		job.ID,
		dispatched.ID,
		dispatched.Attempt,
		normalizedScheduledAt.Format(time.RFC3339Nano),
	)
	return dispatched, nil
}

func (s *masterServer) ensureJobInstance(ctx context.Context, job jobRecord, scheduledAt time.Time) (jobInstanceRecord, bool, error) {
	maxAttempts := job.MaxRetries + 1
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	res, err := s.db.ExecContext(
		ctx,
		`INSERT INTO job_instances(
		     job_id, scheduled_at, status, attempt, max_attempts,
		     retry_backoff_seconds, max_retry_backoff_seconds, heartbeat_timeout_seconds
		 ) VALUES (?, ?, 'PENDING', 0, ?, ?, ?, ?)`,
		job.ID,
		scheduledAt,
		maxAttempts,
		maxPositive(job.RetryBackoffSeconds, defaultRetryBackoffSeconds),
		maxPositive(job.MaxRetryBackoffSeconds, defaultMaxRetryBackoffSeconds),
		maxPositive(job.HeartbeatTimeoutSeconds, defaultHeartbeatTimeoutSecs),
	)
	if err == nil {
		instanceID, readErr := res.LastInsertId()
		if readErr != nil {
			return jobInstanceRecord{}, false, fmt.Errorf("read job instance id failed: %w", readErr)
		}
		instance, loadErr := s.getJobInstanceByID(ctx, instanceID)
		if loadErr != nil {
			return jobInstanceRecord{}, false, fmt.Errorf("load inserted job instance failed: %w", loadErr)
		}
		return instance, true, nil
	}
	if !isDuplicateKeyError(err) {
		return jobInstanceRecord{}, false, fmt.Errorf("insert job instance failed: %w", err)
	}

	instance, loadErr := s.getJobInstanceBySlot(ctx, job.ID, scheduledAt)
	if loadErr != nil {
		return jobInstanceRecord{}, false, fmt.Errorf("load existing job instance failed: %w", loadErr)
	}
	return instance, false, nil
}

func (s *masterServer) dispatchDuePendingInstances(ctx context.Context, now time.Time, source string) {
	instances, err := s.loadDuePendingInstances(ctx, now)
	if err != nil {
		s.logger.Printf("load due pending instances failed: %v", err)
		return
	}

	for _, instance := range instances {
		job, err := s.loadJob(ctx, instance.JobID)
		if err != nil {
			s.logger.Printf("load job for pending instance failed: instance_id=%d err=%v", instance.ID, err)
			continue
		}
		if _, err := s.dispatchPendingInstance(ctx, job, instance, source); err != nil {
			s.logger.Printf("dispatch pending instance failed: instance_id=%d err=%v", instance.ID, err)
		}
	}
}

func (s *masterServer) dispatchPendingInstance(
	ctx context.Context,
	job jobRecord,
	instance jobInstanceRecord,
	source string,
) (jobInstanceRecord, error) {
	if instance.Status != "PENDING" {
		return instance, nil
	}

	nextAttempt := instance.Attempt + 1
	if nextAttempt > instance.MaxAttempts {
		if err := s.markInstanceFailed(ctx, instance, instance.Attempt, instance.WorkerID, "retry limit exceeded", time.Now().UTC()); err != nil {
			return instance, err
		}
		return s.getJobInstanceByID(ctx, instance.ID)
	}

	preferredWorkerID := ""
	if instance.WorkerID.Valid {
		preferredWorkerID = instance.WorkerID.String
	}

	worker, err := s.selectWorker(preferredWorkerID)
	if err != nil {
		return s.scheduleRetry(ctx, instance, nextAttempt, "select worker failed: "+err.Error(), time.Now().UTC())
	}

	claimed, err := s.claimDispatchAttempt(ctx, instance, worker.ID, nextAttempt, time.Now().UTC())
	if err != nil {
		return instance, err
	}

	runReq := &scheduler.RunJobRequest{
		JobId:          job.ID,
		JobInstanceId:  claimed.ID,
		Command:        job.Command,
		Args:           job.Args,
		TimeoutSeconds: job.TimeoutSeconds,
		ScheduledAt:    timestamppb.New(claimed.ScheduledAt),
		Attempt:        uint32(nextAttempt),
	}

	worker, err = s.dispatchRunJob(ctx, runReq, worker)
	if err != nil {
		retried, retryErr := s.scheduleRetry(ctx, claimed, nextAttempt, "dispatch run job failed: "+err.Error(), time.Now().UTC())
		if retryErr != nil {
			return claimed, fmt.Errorf("%v (schedule retry failed: %w)", err, retryErr)
		}
		return retried, err
	}

	s.logger.Printf(
		"dispatch job: source=%s job_id=%d instance_id=%d attempt=%d scheduled_at=%s worker_id=%s worker_addr=%s",
		source,
		job.ID,
		claimed.ID,
		nextAttempt,
		claimed.ScheduledAt.Format(time.RFC3339Nano),
		worker.ID,
		worker.Addr,
	)
	return s.getJobInstanceByID(ctx, claimed.ID)
}

func (s *masterServer) killJobInstance(ctx context.Context, instanceID int64, reason string) (jobInstanceRecord, string, error) {
	instance, err := s.getJobInstanceByID(ctx, instanceID)
	if err != nil {
		return jobInstanceRecord{}, "", err
	}

	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "manual kill requested"
	}
	if isTerminalStatus(instance.Status) {
		return instance, "", fmt.Errorf("job instance %d already terminal: %s", instance.ID, instance.Status)
	}

	if instance.Status == "PENDING" && (!instance.WorkerID.Valid || strings.TrimSpace(instance.WorkerID.String) == "") {
		if err := s.markInstanceKilled(ctx, instance, reason, time.Now().UTC()); err != nil {
			return jobInstanceRecord{}, "", err
		}
		killed, err := s.getJobInstanceByID(ctx, instance.ID)
		if err != nil {
			return jobInstanceRecord{}, "", err
		}
		return killed, "killed before dispatch", nil
	}

	if !instance.WorkerID.Valid || strings.TrimSpace(instance.WorkerID.String) == "" {
		return instance, "", fmt.Errorf("job instance %d has no assigned worker", instance.ID)
	}

	worker, err := s.getWorkerByID(instance.WorkerID.String)
	if err != nil {
		return instance, "", err
	}
	if err := s.dispatchKillJob(ctx, instance, worker, reason); err != nil {
		return instance, "", err
	}

	updated, err := s.getJobInstanceByID(ctx, instance.ID)
	if err != nil {
		return jobInstanceRecord{}, "", err
	}
	return updated, "kill dispatched", nil
}

func (s *masterServer) getJobInstanceBySlot(ctx context.Context, jobID int64, scheduledAt time.Time) (jobInstanceRecord, error) {
	return s.scanJobInstance(
		s.db.QueryRowContext(
			ctx,
			`SELECT
			     id, job_id, status, scheduled_at, attempt, max_attempts,
			     retry_backoff_seconds, max_retry_backoff_seconds, heartbeat_timeout_seconds,
			     next_retry_at, last_heartbeat_at, worker_id, started_at, finished_at, exit_code, error_message
			 FROM job_instances WHERE job_id=? AND scheduled_at=?`,
			jobID,
			scheduledAt,
		),
	)
}

func (s *masterServer) getJobInstanceByID(ctx context.Context, instanceID int64) (jobInstanceRecord, error) {
	return s.scanJobInstance(
		s.db.QueryRowContext(
			ctx,
			`SELECT
			     id, job_id, status, scheduled_at, attempt, max_attempts,
			     retry_backoff_seconds, max_retry_backoff_seconds, heartbeat_timeout_seconds,
			     next_retry_at, last_heartbeat_at, worker_id, started_at, finished_at, exit_code, error_message
			 FROM job_instances WHERE id=?`,
			instanceID,
		),
	)
}

func (s *masterServer) scanJobInstance(row *sql.Row) (jobInstanceRecord, error) {
	var instance jobInstanceRecord
	err := row.Scan(
		&instance.ID,
		&instance.JobID,
		&instance.Status,
		&instance.ScheduledAt,
		&instance.Attempt,
		&instance.MaxAttempts,
		&instance.RetryBackoffSeconds,
		&instance.MaxRetryBackoffSeconds,
		&instance.HeartbeatTimeoutSeconds,
		&instance.NextRetryAt,
		&instance.LastHeartbeatAt,
		&instance.WorkerID,
		&instance.StartedAt,
		&instance.FinishedAt,
		&instance.ExitCode,
		&instance.ErrorMessage,
	)
	if err != nil {
		return jobInstanceRecord{}, err
	}
	return instance, nil
}

func (s *masterServer) loadDuePendingInstances(ctx context.Context, now time.Time) ([]jobInstanceRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT
		     id, job_id, status, scheduled_at, attempt, max_attempts,
		     retry_backoff_seconds, max_retry_backoff_seconds, heartbeat_timeout_seconds,
		     next_retry_at, last_heartbeat_at, worker_id, started_at, finished_at, exit_code, error_message
		 FROM job_instances
		 WHERE status='PENDING' AND (next_retry_at IS NULL OR next_retry_at <= ?)
		 ORDER BY COALESCE(next_retry_at, scheduled_at), id`,
		now.UTC(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	instances := make([]jobInstanceRecord, 0)
	for rows.Next() {
		var instance jobInstanceRecord
		if err := rows.Scan(
			&instance.ID,
			&instance.JobID,
			&instance.Status,
			&instance.ScheduledAt,
			&instance.Attempt,
			&instance.MaxAttempts,
			&instance.RetryBackoffSeconds,
			&instance.MaxRetryBackoffSeconds,
			&instance.HeartbeatTimeoutSeconds,
			&instance.NextRetryAt,
			&instance.LastHeartbeatAt,
			&instance.WorkerID,
			&instance.StartedAt,
			&instance.FinishedAt,
			&instance.ExitCode,
			&instance.ErrorMessage,
		); err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return instances, nil
}

func (s *masterServer) loadRunningInstances(ctx context.Context) ([]jobInstanceRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT
		     id, job_id, status, scheduled_at, attempt, max_attempts,
		     retry_backoff_seconds, max_retry_backoff_seconds, heartbeat_timeout_seconds,
		     next_retry_at, last_heartbeat_at, worker_id, started_at, finished_at, exit_code, error_message
		 FROM job_instances
		 WHERE status='RUNNING'
		 ORDER BY id`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	instances := make([]jobInstanceRecord, 0)
	for rows.Next() {
		var instance jobInstanceRecord
		if err := rows.Scan(
			&instance.ID,
			&instance.JobID,
			&instance.Status,
			&instance.ScheduledAt,
			&instance.Attempt,
			&instance.MaxAttempts,
			&instance.RetryBackoffSeconds,
			&instance.MaxRetryBackoffSeconds,
			&instance.HeartbeatTimeoutSeconds,
			&instance.NextRetryAt,
			&instance.LastHeartbeatAt,
			&instance.WorkerID,
			&instance.StartedAt,
			&instance.FinishedAt,
			&instance.ExitCode,
			&instance.ErrorMessage,
		); err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return instances, nil
}

func (s *masterServer) recoverRunningTimeouts(ctx context.Context, now time.Time) {
	instances, err := s.loadRunningInstances(ctx)
	if err != nil {
		s.logger.Printf("load running instances failed: %v", err)
		return
	}

	for _, instance := range instances {
		if !isRunningTimedOut(instance, now) {
			continue
		}

		reason := fmt.Sprintf("heartbeat timeout: attempt=%d worker=%s", instance.Attempt, instance.WorkerID.String)
		if _, err := s.scheduleRetry(ctx, instance, instance.Attempt, reason, now); err != nil {
			s.logger.Printf("recover running timeout failed: instance_id=%d err=%v", instance.ID, err)
		}
	}
}

func (s *masterServer) claimDispatchAttempt(
	ctx context.Context,
	instance jobInstanceRecord,
	workerID string,
	attempt int,
	now time.Time,
) (jobInstanceRecord, error) {
	nextRetryAt := now.Add(s.cfg.DispatchAckTimeout)
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE job_instances
		 SET status='PENDING', attempt=?, worker_id=?, started_at=NULL, finished_at=NULL, exit_code=NULL,
		     error_message=NULL, last_heartbeat_at=NULL, next_retry_at=?, updated_at=CURRENT_TIMESTAMP
		 WHERE id=? AND status='PENDING' AND attempt=?`,
		attempt,
		workerID,
		nextRetryAt,
		instance.ID,
		instance.Attempt,
	)
	if err != nil {
		return jobInstanceRecord{}, fmt.Errorf("claim dispatch attempt failed: %w", err)
	}
	return s.getJobInstanceByID(ctx, instance.ID)
}

func (s *masterServer) scheduleRetry(
	ctx context.Context,
	instance jobInstanceRecord,
	attempt int,
	reason string,
	now time.Time,
) (jobInstanceRecord, error) {
	if attempt <= 0 {
		attempt = maxPositive(instance.Attempt, 1)
	}
	if attempt >= instance.MaxAttempts {
		if err := s.markInstanceFailed(ctx, instance, attempt, instance.WorkerID, reason, now); err != nil {
			return jobInstanceRecord{}, err
		}
		return s.getJobInstanceByID(ctx, instance.ID)
	}

	delay := computeRetryDelay(
		attempt,
		maxPositive(instance.RetryBackoffSeconds, defaultRetryBackoffSeconds),
		maxPositive(instance.MaxRetryBackoffSeconds, defaultMaxRetryBackoffSeconds),
	)
	nextRetryAt := now.Add(delay)
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE job_instances
		 SET status='PENDING', attempt=?, worker_id=NULL, started_at=NULL, finished_at=NULL, exit_code=NULL,
		     error_message=?, last_heartbeat_at=NULL, next_retry_at=?, updated_at=CURRENT_TIMESTAMP
		 WHERE id=?`,
		attempt,
		reason,
		nextRetryAt,
		instance.ID,
	)
	if err != nil {
		return jobInstanceRecord{}, fmt.Errorf("schedule retry failed: %w", err)
	}

	s.logger.Printf(
		"retry scheduled: instance_id=%d attempt=%d/%d next_retry_at=%s reason=%s",
		instance.ID,
		attempt,
		instance.MaxAttempts,
		nextRetryAt.UTC().Format(time.RFC3339Nano),
		reason,
	)
	return s.getJobInstanceByID(ctx, instance.ID)
}

func (s *masterServer) markInstanceFailed(
	ctx context.Context,
	instance jobInstanceRecord,
	attempt int,
	workerID sql.NullString,
	reason string,
	failedAt time.Time,
) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE job_instances
		 SET status='FAILED', attempt=?, worker_id=?, finished_at=?, exit_code=COALESCE(exit_code, -1),
		     error_message=?, next_retry_at=NULL, last_heartbeat_at=NULL, updated_at=CURRENT_TIMESTAMP
		 WHERE id=?`,
		attempt,
		nullableStringValue(workerID),
		failedAt.UTC(),
		reason,
		instance.ID,
	)
	if err != nil {
		return fmt.Errorf("mark instance failed: %w", err)
	}

	s.logger.Printf(
		"instance failed permanently: instance_id=%d attempt=%d/%d reason=%s",
		instance.ID,
		attempt,
		instance.MaxAttempts,
		reason,
	)
	return nil
}

func (s *masterServer) markInstanceKilled(
	ctx context.Context,
	instance jobInstanceRecord,
	reason string,
	killedAt time.Time,
) error {
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE job_instances
		 SET status='KILLED', finished_at=?, exit_code=COALESCE(exit_code, -1),
		     error_message=?, next_retry_at=NULL, last_heartbeat_at=NULL, updated_at=CURRENT_TIMESTAMP
		 WHERE id=? AND status='PENDING'`,
		killedAt.UTC(),
		reason,
		instance.ID,
	)
	if err != nil {
		return fmt.Errorf("mark instance killed: %w", err)
	}
	rowsAffected, err := res.RowsAffected()
	if err == nil && rowsAffected == 0 {
		return fmt.Errorf("mark instance killed: no pending row updated")
	}

	s.logger.Printf(
		"instance killed before dispatch: instance_id=%d attempt=%d/%d reason=%s",
		instance.ID,
		instance.Attempt,
		instance.MaxAttempts,
		reason,
	)
	return nil
}

func (s *masterServer) runLeaderElectionLoop(ctx context.Context) {
	for {
		err := s.campaignForLeader(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Printf("leader election loop error: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (s *masterServer) campaignForLeader(ctx context.Context) error {
	record := election.LeaderRecord{
		MasterID:          s.cfg.MasterID,
		HTTPAdvertiseAddr: s.cfg.MasterAdvertiseHTTPAddr,
		GRPCAdvertiseAddr: s.cfg.MasterAdvertiseGRPCAddr,
		ElectedAt:         time.Now().UTC(),
	}
	value, err := election.EncodeLeaderRecord(record)
	if err != nil {
		return fmt.Errorf("encode leader record failed: %w", err)
	}

	leaseCtx, cancelLease := context.WithTimeout(ctx, 5*time.Second)
	leaseResp, err := s.etcd.Grant(leaseCtx, s.cfg.MasterLeaseTTL)
	cancelLease()
	if err != nil {
		return fmt.Errorf("grant leader lease failed: %w", err)
	}

	txnCtx, cancelTxn := context.WithTimeout(ctx, 5*time.Second)
	txnResp, err := s.etcd.Txn(txnCtx).
		If(clientv3.Compare(clientv3.CreateRevision(s.cfg.LeaderElectionKey), "=", 0)).
		Then(clientv3.OpPut(s.cfg.LeaderElectionKey, string(value), clientv3.WithLease(leaseResp.ID))).
		Else(clientv3.OpGet(s.cfg.LeaderElectionKey)).
		Commit()
	cancelTxn()
	if err != nil {
		s.revokeLeaderLease(leaseResp.ID)
		return fmt.Errorf("campaign leader failed: %w", err)
	}

	if txnResp.Succeeded {
		s.setLeaderState(true, record)

		keepAliveCtx, cancelKeepAlive := context.WithCancel(ctx)
		defer cancelKeepAlive()

		keepAliveCh, err := s.etcd.KeepAlive(keepAliveCtx, leaseResp.ID)
		if err != nil {
			s.setLeaderState(false, election.LeaderRecord{})
			s.revokeLeaderLease(leaseResp.ID)
			return fmt.Errorf("leader keepalive failed: %w", err)
		}

		for {
			select {
			case <-ctx.Done():
				s.revokeLeaderLease(leaseResp.ID)
				s.setLeaderState(false, election.LeaderRecord{})
				return ctx.Err()
			case resp, ok := <-keepAliveCh:
				if !ok || resp == nil {
					s.setLeaderState(false, election.LeaderRecord{})
					return fmt.Errorf("leader keepalive channel closed")
				}
			}
		}
	}

	s.revokeLeaderLease(leaseResp.ID)

	leader, watchRevision := leaderFromTxnResponse(txnResp)
	s.setLeaderState(false, leader)
	if leader.MasterID == "" {
		return nil
	}
	return s.waitForLeaderChange(ctx, watchRevision)
}

func leaderFromTxnResponse(resp *clientv3.TxnResponse) (election.LeaderRecord, int64) {
	if resp == nil {
		return election.LeaderRecord{}, 0
	}

	for _, opResp := range resp.Responses {
		rangeResp := opResp.GetResponseRange()
		if rangeResp == nil || len(rangeResp.Kvs) == 0 {
			continue
		}

		record, err := election.DecodeLeaderRecord(rangeResp.Kvs[0].Value)
		if err != nil {
			return election.LeaderRecord{}, resp.Header.Revision
		}
		return record, resp.Header.Revision
	}

	return election.LeaderRecord{}, resp.Header.Revision
}

func (s *masterServer) waitForLeaderChange(ctx context.Context, fromRevision int64) error {
	revision := fromRevision + 1
	if revision <= 0 {
		revision = 1
	}

	watchCh := s.etcd.Watch(ctx, s.cfg.LeaderElectionKey, clientv3.WithRev(revision))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case watchResp, ok := <-watchCh:
			if !ok {
				return fmt.Errorf("leader watch channel closed")
			}
			if err := watchResp.Err(); err != nil {
				return fmt.Errorf("leader watch failed: %w", err)
			}

			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					record, err := election.DecodeLeaderRecord(event.Kv.Value)
					if err != nil {
						s.logger.Printf("skip invalid leader record: key=%s err=%v", string(event.Kv.Key), err)
						continue
					}
					s.setLeaderState(false, record)
				case mvccpb.DELETE:
					s.setLeaderState(false, election.LeaderRecord{})
					return nil
				}
			}
		}
	}
}

func (s *masterServer) revokeLeaderLease(leaseID clientv3.LeaseID) {
	if leaseID == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := s.etcd.Revoke(ctx, leaseID); err != nil {
		s.logger.Printf("revoke leader lease failed: lease_id=%d err=%v", leaseID, err)
	}
}

func (s *masterServer) setLeaderState(isLeader bool, leader election.LeaderRecord) {
	s.leaderMu.Lock()
	prevIsLeader := s.isLeader
	prevLeader := s.leaderRecord

	s.isLeader = isLeader
	s.leaderRecord = leader
	s.leaderMu.Unlock()

	if s.logger == nil {
		return
	}

	switch {
	case isLeader && (!prevIsLeader || prevLeader.MasterID != leader.MasterID):
		s.logger.Printf("leader acquired: master_id=%s key=%s", s.cfg.MasterID, s.cfg.LeaderElectionKey)
	case !isLeader && prevIsLeader:
		if leader.MasterID != "" {
			s.logger.Printf("leader lost: master_id=%s new_leader=%s", s.cfg.MasterID, leader.MasterID)
		} else {
			s.logger.Printf("leader lost: master_id=%s", s.cfg.MasterID)
		}
	case !isLeader && prevLeader.MasterID != leader.MasterID && leader.MasterID != "":
		s.logger.Printf(
			"leader observed: leader_id=%s http=%s grpc=%s",
			leader.MasterID,
			leader.HTTPAdvertiseAddr,
			leader.GRPCAdvertiseAddr,
		)
	case !isLeader && prevLeader.MasterID != "" && leader.MasterID == "":
		s.logger.Printf("leader cleared")
	}
}

func (s *masterServer) leaderSnapshot() (bool, election.LeaderRecord) {
	s.leaderMu.RLock()
	defer s.leaderMu.RUnlock()
	return s.isLeader, s.leaderRecord
}

func (s *masterServer) shouldSchedule() bool {
	isLeader, _ := s.leaderSnapshot()
	return isLeader
}

func (s *masterServer) runWorkerWatchLoop(ctx context.Context) {
	for {
		err := s.syncAndWatchWorkers(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Printf("worker watch loop error: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (s *masterServer) syncAndWatchWorkers(ctx context.Context) error {
	getCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	resp, err := s.etcd.Get(getCtx, s.cfg.WorkerDiscoveryPrefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return fmt.Errorf("load workers snapshot failed: %w", err)
	}

	snapshot := make(map[string]discovery.WorkerRegistration, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		reg, decodeErr := discovery.DecodeWorkerRegistration(kv.Value)
		if decodeErr != nil {
			s.logger.Printf("skip invalid worker registration: key=%s err=%v", string(kv.Key), decodeErr)
			continue
		}
		snapshot[reg.ID] = reg
	}
	s.replaceWorkers(snapshot)

	watchCh := s.etcd.Watch(
		ctx,
		s.cfg.WorkerDiscoveryPrefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(resp.Header.Revision+1),
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case watchResp, ok := <-watchCh:
			if !ok {
				return fmt.Errorf("worker watch channel closed")
			}
			if err := watchResp.Err(); err != nil {
				return fmt.Errorf("worker watch failed: %w", err)
			}

			for _, event := range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT:
					reg, decodeErr := discovery.DecodeWorkerRegistration(event.Kv.Value)
					if decodeErr != nil {
						s.logger.Printf("skip invalid worker update: key=%s err=%v", string(event.Kv.Key), decodeErr)
						continue
					}
					s.upsertWorker(reg)
				case mvccpb.DELETE:
					workerID, ok := discovery.WorkerIDFromKey(s.cfg.WorkerDiscoveryPrefix, string(event.Kv.Key))
					if !ok {
						s.logger.Printf("skip invalid worker delete key: %s", string(event.Kv.Key))
						continue
					}
					s.removeWorker(workerID)
				}
			}
		}
	}
}

func (s *masterServer) replaceWorkers(snapshot map[string]discovery.WorkerRegistration) {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()

	for workerID, reg := range snapshot {
		if prev, ok := s.workers[workerID]; !ok {
			s.logger.Printf("worker discovered: id=%s addr=%s", reg.ID, reg.Addr)
		} else if prev.Addr != reg.Addr {
			s.logger.Printf("worker updated: id=%s addr=%s -> %s", reg.ID, prev.Addr, reg.Addr)
		}
	}
	for workerID, reg := range s.workers {
		if _, ok := snapshot[workerID]; !ok {
			s.logger.Printf("worker removed: id=%s addr=%s", reg.ID, reg.Addr)
		}
	}

	s.workers = snapshot
	s.workerOrder = sortedWorkerIDs(snapshot)
	if s.nextWorkerIndex >= len(s.workerOrder) {
		s.nextWorkerIndex = 0
	}
}

func (s *masterServer) upsertWorker(reg discovery.WorkerRegistration) {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()

	prev, existed := s.workers[reg.ID]
	s.workers[reg.ID] = reg
	s.workerOrder = sortedWorkerIDs(s.workers)
	if s.nextWorkerIndex >= len(s.workerOrder) {
		s.nextWorkerIndex = 0
	}

	if !existed {
		s.logger.Printf("worker discovered: id=%s addr=%s", reg.ID, reg.Addr)
		return
	}
	if prev.Addr != reg.Addr {
		s.logger.Printf("worker updated: id=%s addr=%s -> %s", reg.ID, prev.Addr, reg.Addr)
	}
}

func (s *masterServer) removeWorker(workerID string) {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()

	reg, existed := s.workers[workerID]
	if !existed {
		return
	}
	delete(s.workers, workerID)
	s.workerOrder = sortedWorkerIDs(s.workers)
	if s.nextWorkerIndex >= len(s.workerOrder) {
		s.nextWorkerIndex = 0
	}
	s.logger.Printf("worker removed: id=%s addr=%s", reg.ID, reg.Addr)
}

func (s *masterServer) selectWorker(preferredWorkerID string) (discovery.WorkerRegistration, error) {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()

	if len(s.workerOrder) == 0 {
		return discovery.WorkerRegistration{}, fmt.Errorf("no workers available")
	}

	if preferredWorkerID = strings.TrimSpace(preferredWorkerID); preferredWorkerID != "" {
		if reg, ok := s.workers[preferredWorkerID]; ok {
			return reg, nil
		}
	}

	for attempts := 0; attempts < len(s.workerOrder); attempts++ {
		if s.nextWorkerIndex >= len(s.workerOrder) {
			s.nextWorkerIndex = 0
		}
		workerID := s.workerOrder[s.nextWorkerIndex]
		s.nextWorkerIndex = (s.nextWorkerIndex + 1) % len(s.workerOrder)

		reg, ok := s.workers[workerID]
		if ok {
			return reg, nil
		}
	}

	return discovery.WorkerRegistration{}, fmt.Errorf("no workers available")
}

func (s *masterServer) getWorkerByID(workerID string) (discovery.WorkerRegistration, error) {
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return discovery.WorkerRegistration{}, fmt.Errorf("worker id is required")
	}

	s.workersMu.RLock()
	defer s.workersMu.RUnlock()

	worker, ok := s.workers[workerID]
	if !ok {
		return discovery.WorkerRegistration{}, fmt.Errorf("worker %s not available", workerID)
	}
	return worker, nil
}

func (s *masterServer) workerCount() int {
	s.workersMu.RLock()
	defer s.workersMu.RUnlock()
	return len(s.workers)
}

func sortedWorkerIDs(workers map[string]discovery.WorkerRegistration) []string {
	workerIDs := make([]string, 0, len(workers))
	for workerID := range workers {
		workerIDs = append(workerIDs, workerID)
	}
	sort.Strings(workerIDs)
	return workerIDs
}

func (s *masterServer) dispatchRunJob(parent context.Context, req *scheduler.RunJobRequest, worker discovery.WorkerRegistration) (discovery.WorkerRegistration, error) {
	ctx, cancel := context.WithTimeout(parent, 6*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, worker.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return discovery.WorkerRegistration{}, fmt.Errorf("dial worker %s(%s) failed: %w", worker.ID, worker.Addr, err)
	}
	defer conn.Close()

	client := scheduler.NewWorkerClient(conn)
	resp, err := client.RunJob(ctx, req)
	if err != nil {
		return discovery.WorkerRegistration{}, fmt.Errorf("worker %s(%s) RunJob failed: %w", worker.ID, worker.Addr, err)
	}
	if !resp.GetAccepted() {
		return discovery.WorkerRegistration{}, fmt.Errorf("worker %s(%s) rejected run job: %s", worker.ID, worker.Addr, resp.GetMessage())
	}
	return worker, nil
}

func (s *masterServer) dispatchKillJob(parent context.Context, instance jobInstanceRecord, worker discovery.WorkerRegistration, reason string) error {
	ctx, cancel := context.WithTimeout(parent, 6*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		worker.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("dial worker %s(%s) failed: %w", worker.ID, worker.Addr, err)
	}
	defer conn.Close()

	client := scheduler.NewWorkerClient(conn)
	resp, err := client.KillJob(ctx, &scheduler.KillJobRequest{
		JobInstanceId: instance.ID,
		Reason:        reason,
	})
	if err != nil {
		return fmt.Errorf("worker %s(%s) KillJob failed: %w", worker.ID, worker.Addr, err)
	}
	if !resp.GetAccepted() {
		return fmt.Errorf("worker %s(%s) rejected kill job: %s", worker.ID, worker.Addr, resp.GetMessage())
	}

	s.logger.Printf(
		"dispatch kill: instance_id=%d attempt=%d worker_id=%s worker_addr=%s reason=%s",
		instance.ID,
		instance.Attempt,
		worker.ID,
		worker.Addr,
		reason,
	)
	return nil
}

func (s *masterServer) ReportResult(ctx context.Context, req *scheduler.ReportResultRequest) (*scheduler.ReportResultResponse, error) {
	nextStatus, ok := protoStatusToDB(req.GetStatus())
	if !ok {
		return &scheduler.ReportResultResponse{Accepted: false, Message: "unsupported status"}, nil
	}

	instance, err := s.getJobInstanceByID(ctx, req.GetJobInstanceId())
	if err != nil {
		return &scheduler.ReportResultResponse{Accepted: false, Message: fmt.Sprintf("query instance failed: %v", err)}, nil
	}

	reportAttempt := int(req.GetAttempt())
	if reportAttempt <= 0 {
		reportAttempt = instance.Attempt
	}
	if reportAttempt < instance.Attempt {
		return &scheduler.ReportResultResponse{Accepted: true, Message: "stale attempt ignored"}, nil
	}
	if reportAttempt > instance.Attempt {
		return &scheduler.ReportResultResponse{
			Accepted: false,
			Message:  fmt.Sprintf("attempt mismatch: report=%d current=%d", reportAttempt, instance.Attempt),
		}, nil
	}
	if !isValidTransition(instance.Status, nextStatus) {
		return &scheduler.ReportResultResponse{
			Accepted: false,
			Message:  fmt.Sprintf("invalid transition: %s -> %s", instance.Status, nextStatus),
		}, nil
	}

	now := time.Now().UTC()
	workerID := nullableWorkerID(req.GetWorkerId(), instance.WorkerID)
	startedAt := coalesceOptionalTime(req.GetStartedAt(), instance.StartedAt)

	switch nextStatus {
	case "RUNNING":
		res, execErr := s.db.ExecContext(
			ctx,
			`UPDATE job_instances
			 SET status='RUNNING', worker_id=?, started_at=?, last_heartbeat_at=?, next_retry_at=NULL, updated_at=CURRENT_TIMESTAMP
			 WHERE id=? AND attempt=? AND status IN ('PENDING', 'RUNNING')`,
			workerID,
			startedAt,
			now,
			instance.ID,
			instance.Attempt,
		)
		if execErr != nil {
			return &scheduler.ReportResultResponse{Accepted: false, Message: fmt.Sprintf("update running heartbeat failed: %v", execErr)}, nil
		}
		rowsAffected, err := res.RowsAffected()
		if err == nil && rowsAffected == 0 {
			return &scheduler.ReportResultResponse{Accepted: true, Message: "stale running heartbeat ignored"}, nil
		}
	case "SUCCESS":
		res, execErr := s.db.ExecContext(
			ctx,
			`UPDATE job_instances
			 SET status='SUCCESS', worker_id=?, started_at=?, finished_at=?, exit_code=?, error_message=?, last_heartbeat_at=?, next_retry_at=NULL, updated_at=CURRENT_TIMESTAMP
			 WHERE id=? AND attempt=? AND status IN ('PENDING', 'RUNNING')`,
			workerID,
			startedAt,
			coalesceOptionalTime(req.GetFinishedAt(), instance.FinishedAt),
			req.GetExitCode(),
			strings.TrimSpace(req.GetErrorMessage()),
			now,
			instance.ID,
			instance.Attempt,
		)
		if execErr != nil {
			return &scheduler.ReportResultResponse{Accepted: false, Message: fmt.Sprintf("update success result failed: %v", execErr)}, nil
		}
		rowsAffected, err := res.RowsAffected()
		if err == nil && rowsAffected == 0 {
			return &scheduler.ReportResultResponse{Accepted: true, Message: "stale success report ignored"}, nil
		}
	case "FAILED":
		reason := strings.TrimSpace(req.GetErrorMessage())
		if reason == "" {
			reason = nextStatus
		}
		if reportAttempt < instance.MaxAttempts {
			retried, retryErr := s.scheduleRetry(ctx, instance, reportAttempt, reason, now)
			if retryErr != nil {
				return &scheduler.ReportResultResponse{Accepted: false, Message: retryErr.Error()}, nil
			}
			s.logger.Printf(
				"report scheduled retry: instance_id=%d status=%s attempt=%d/%d next_retry_at=%s worker=%s",
				retried.ID,
				nextStatus,
				reportAttempt,
				retried.MaxAttempts,
				optionalTimeString(retried.NextRetryAt),
				req.GetWorkerId(),
			)
			return &scheduler.ReportResultResponse{Accepted: true, Message: "retry scheduled"}, nil
		}

		res, execErr := s.db.ExecContext(
			ctx,
			`UPDATE job_instances
			 SET status='FAILED', worker_id=?, started_at=?, finished_at=?, exit_code=?, error_message=?, last_heartbeat_at=?, next_retry_at=NULL, updated_at=CURRENT_TIMESTAMP
			 WHERE id=? AND attempt=? AND status IN ('PENDING', 'RUNNING')`,
			workerID,
			startedAt,
			coalesceOptionalTime(req.GetFinishedAt(), instance.FinishedAt),
			req.GetExitCode(),
			reason,
			now,
			instance.ID,
			instance.Attempt,
		)
		if execErr != nil {
			return &scheduler.ReportResultResponse{Accepted: false, Message: fmt.Sprintf("update terminal result failed: %v", execErr)}, nil
		}
		rowsAffected, err := res.RowsAffected()
		if err == nil && rowsAffected == 0 {
			return &scheduler.ReportResultResponse{Accepted: true, Message: "stale terminal report ignored"}, nil
		}
	case "KILLED":
		reason := strings.TrimSpace(req.GetErrorMessage())
		if reason == "" {
			reason = nextStatus
		}
		res, execErr := s.db.ExecContext(
			ctx,
			`UPDATE job_instances
			 SET status='KILLED', worker_id=?, started_at=?, finished_at=?, exit_code=?, error_message=?, last_heartbeat_at=?, next_retry_at=NULL, updated_at=CURRENT_TIMESTAMP
			 WHERE id=? AND attempt=? AND status IN ('PENDING', 'RUNNING')`,
			workerID,
			startedAt,
			coalesceOptionalTime(req.GetFinishedAt(), instance.FinishedAt),
			req.GetExitCode(),
			reason,
			now,
			instance.ID,
			instance.Attempt,
		)
		if execErr != nil {
			return &scheduler.ReportResultResponse{Accepted: false, Message: fmt.Sprintf("update killed result failed: %v", execErr)}, nil
		}
		rowsAffected, err := res.RowsAffected()
		if err == nil && rowsAffected == 0 {
			return &scheduler.ReportResultResponse{Accepted: true, Message: "stale killed report ignored"}, nil
		}
	default:
		return &scheduler.ReportResultResponse{Accepted: false, Message: fmt.Sprintf("unsupported status transition target: %s", nextStatus)}, nil
	}

	s.logger.Printf(
		"report result: instance_id=%d status=%s attempt=%d/%d worker=%s exit_code=%d",
		req.GetJobInstanceId(),
		nextStatus,
		reportAttempt,
		instance.MaxAttempts,
		req.GetWorkerId(),
		req.GetExitCode(),
	)
	return &scheduler.ReportResultResponse{Accepted: true, Message: "ok"}, nil
}

func protoStatusToDB(status scheduler.JobInstanceStatus) (string, bool) {
	switch status {
	case scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_PENDING:
		return "PENDING", true
	case scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_RUNNING:
		return "RUNNING", true
	case scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_SUCCESS:
		return "SUCCESS", true
	case scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_FAILED:
		return "FAILED", true
	case scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_KILLED:
		return "KILLED", true
	default:
		return "", false
	}
}

func isValidTransition(from, to string) bool {
	if from == to {
		return true
	}
	switch from {
	case "PENDING":
		return to == "RUNNING" || to == "SUCCESS" || to == "FAILED" || to == "KILLED"
	case "RUNNING":
		return to == "SUCCESS" || to == "FAILED" || to == "KILLED"
	default:
		return false
	}
}

func isTerminalStatus(status string) bool {
	return status == "SUCCESS" || status == "FAILED" || status == "KILLED"
}

func coalesceOptionalTime(ts *timestamppb.Timestamp, fallback sql.NullTime) interface{} {
	if ts == nil {
		if fallback.Valid {
			return fallback.Time.UTC()
		}
		return nil
	}
	t := ts.AsTime()
	if t.IsZero() {
		if fallback.Valid {
			return fallback.Time.UTC()
		}
		return nil
	}
	return t.UTC()
}

func nullableWorkerID(workerID string, fallback sql.NullString) interface{} {
	workerID = strings.TrimSpace(workerID)
	if workerID != "" {
		return workerID
	}
	return nullableStringValue(fallback)
}

func nullableStringValue(value sql.NullString) interface{} {
	if value.Valid {
		return value.String
	}
	return nil
}

func optionalTimeString(value sql.NullTime) string {
	if !value.Valid {
		return ""
	}
	return value.Time.UTC().Format(time.RFC3339Nano)
}

func uint32OrDefault(value *uint32, fallback int) uint32 {
	if value == nil {
		return uint32(maxPositive(fallback, 0))
	}
	return *value
}

func maxPositive(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func computeRetryDelay(attempt, baseSeconds, maxSeconds int) time.Duration {
	baseSeconds = maxPositive(baseSeconds, defaultRetryBackoffSeconds)
	maxSeconds = maxPositive(maxSeconds, baseSeconds)
	if maxSeconds < baseSeconds {
		maxSeconds = baseSeconds
	}

	delaySeconds := int64(baseSeconds)
	for step := 1; step < attempt; step++ {
		delaySeconds *= 2
		if delaySeconds >= int64(maxSeconds) {
			delaySeconds = int64(maxSeconds)
			break
		}
	}
	if delaySeconds < 1 {
		delaySeconds = 1
	}
	return time.Duration(delaySeconds) * time.Second
}

func isRunningTimedOut(instance jobInstanceRecord, now time.Time) bool {
	if instance.Status != "RUNNING" {
		return false
	}

	anchor := instance.ScheduledAt
	if instance.LastHeartbeatAt.Valid {
		anchor = instance.LastHeartbeatAt.Time.UTC()
	} else if instance.StartedAt.Valid {
		anchor = instance.StartedAt.Time.UTC()
	}

	timeout := time.Duration(maxPositive(instance.HeartbeatTimeoutSeconds, defaultHeartbeatTimeoutSecs)) * time.Second
	return !anchor.Add(timeout).After(now.UTC())
}

func parseCronSchedule(expr string) (cron.Schedule, error) {
	return defaultCronParser.Parse(strings.TrimSpace(expr))
}

func scheduledSlotsWithinWindow(schedule cron.Schedule, windowStart, now time.Time, maxCatchup int) ([]time.Time, int) {
	if maxCatchup <= 0 || now.Before(windowStart) {
		return nil, 0
	}

	cursor := windowStart.Add(-time.Nanosecond)
	slots := make([]time.Time, 0, maxCatchup)
	skipped := 0

	for {
		next := schedule.Next(cursor)
		if next.After(now) {
			break
		}

		slots = append(slots, normalizeScheduledAt(next))
		if len(slots) > maxCatchup {
			slots = slots[1:]
			skipped++
		}
		cursor = next
	}

	return slots, skipped
}

func normalizeScheduledAt(t time.Time) time.Time {
	return t.UTC().Truncate(time.Millisecond)
}

func isDuplicateKeyError(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 1062
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

func intEnvOrDefault(key string, defaultValue int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue, nil
	}
	return strconv.Atoi(value)
}

func int64EnvOrDefault(key string, defaultValue int64) (int64, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

func defaultAdvertiseAddr(bindAddr, host string) string {
	bindAddr = strings.TrimSpace(bindAddr)
	host = strings.TrimSpace(host)
	if bindAddr == "" {
		return ""
	}

	resolvedHost, port, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return bindAddr
	}

	resolvedHost = strings.TrimSpace(resolvedHost)
	if resolvedHost == "" || resolvedHost == "0.0.0.0" || resolvedHost == "::" {
		if host == "" {
			return bindAddr
		}
		return net.JoinHostPort(host, port)
	}
	return net.JoinHostPort(resolvedHost, port)
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
