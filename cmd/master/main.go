package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	scheduler "github.com/HITGerhman/distributed-job-scheduler/api/proto"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	manualCronExpr                = "@manual"
	defaultSchedulerTickInterval  = time.Second
	defaultSchedulerCatchupWindow = time.Minute
	defaultSchedulerMaxCatchup    = 10
)

var defaultCronParser = cron.NewParser(
	cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

type config struct {
	MySQLDSN               string
	MasterGRPCAddr         string
	MasterHTTPAddr         string
	WorkerGRPCAddr         string
	SchedulerTickInterval  time.Duration
	SchedulerCatchupWindow time.Duration
	SchedulerMaxCatchup    int
}

type masterServer struct {
	scheduler.UnimplementedMasterServer
	db     *sql.DB
	cfg    config
	logger *log.Logger
}

type createJobRequest struct {
	Name           string   `json:"name"`
	CronExpr       string   `json:"cron_expr"`
	Command        string   `json:"command"`
	Args           []string `json:"args"`
	TimeoutSeconds uint32   `json:"timeout_seconds"`
	Enabled        *bool    `json:"enabled"`
}

type createJobResponse struct {
	ID int64 `json:"id"`
}

type triggerJobResponse struct {
	JobInstanceID int64  `json:"job_instance_id"`
	Message       string `json:"message"`
}

type jobRecord struct {
	ID             int64
	Name           string
	CronExpr       string
	Command        string
	Args           []string
	TimeoutSeconds uint32
	CreatedAt      time.Time
}

type jobInstanceRecord struct {
	ID     int64
	Status string
}

type triggerMode struct {
	Source                     string
	MarkFailedOnDispatchError  bool
	AllowExistingPendingReplay bool
}

func main() {
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
		MasterGRPCAddr:         envOrDefault("MASTER_GRPC_ADDR", ":50052"),
		MasterHTTPAddr:         envOrDefault("MASTER_HTTP_ADDR", ":8080"),
		WorkerGRPCAddr:         envOrDefault("WORKER_GRPC_ADDR", "worker:50051"),
		SchedulerTickInterval:  schedulerTickInterval,
		SchedulerCatchupWindow: schedulerCatchupWindow,
		SchedulerMaxCatchup:    schedulerMaxCatchup,
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

	srv := &masterServer{db: db, cfg: cfg, logger: logger}
	errCh := make(chan error, 2)

	go func() {
		errCh <- srv.serveGRPC(cfg.MasterGRPCAddr)
	}()

	go func() {
		errCh <- srv.serveHTTP(cfg.MasterHTTPAddr)
	}()

	go srv.runSchedulerLoop(context.Background())

	logger.Printf(
		"master started: grpc=%s http=%s worker=%s scheduler_tick=%s catchup_window=%s max_catchup=%d",
		cfg.MasterGRPCAddr,
		cfg.MasterHTTPAddr,
		cfg.WorkerGRPCAddr,
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
	mux.HandleFunc("/job-instances/", s.handleGetJobInstance)

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
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
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
		`INSERT INTO jobs(name, cron_expr, command, args_json, timeout_seconds, enabled)
		 VALUES(?, ?, ?, ?, ?, ?)`,
		req.Name,
		req.CronExpr,
		req.Command,
		argsJSON,
		req.TimeoutSeconds,
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

	s.logger.Printf("create job: id=%d name=%s command=%s", jobID, req.Name, req.Command)
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

func (s *masterServer) handleGetJobInstance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 2 || parts[0] != "job-instances" {
		http.NotFound(w, r)
		return
	}

	instanceID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || instanceID <= 0 {
		http.Error(w, "invalid job instance id", http.StatusBadRequest)
		return
	}

	var (
		jobID      int64
		status     string
		scheduled  time.Time
		workerID   sql.NullString
		startedAt  sql.NullTime
		finishedAt sql.NullTime
		exitCode   sql.NullInt64
		errMsg     sql.NullString
	)

	err = s.db.QueryRowContext(
		r.Context(),
		`SELECT job_id, status, scheduled_at, worker_id, started_at, finished_at, exit_code, error_message
		 FROM job_instances WHERE id=?`,
		instanceID,
	).Scan(&jobID, &status, &scheduled, &workerID, &startedAt, &finishedAt, &exitCode, &errMsg)
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
		"job_id":       jobID,
		"status":       status,
		"scheduled_at": scheduled.UTC().Format(time.RFC3339Nano),
	}
	if workerID.Valid {
		resp["worker_id"] = workerID.String
	}
	if startedAt.Valid {
		resp["started_at"] = startedAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if finishedAt.Valid {
		resp["finished_at"] = finishedAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if exitCode.Valid {
		resp["exit_code"] = exitCode.Int64
	}
	if errMsg.Valid {
		resp["error_message"] = errMsg.String
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *masterServer) loadJob(ctx context.Context, jobID int64) (jobRecord, error) {
	var (
		job      jobRecord
		argsJSON []byte
	)
	job.ID = jobID
	err := s.db.QueryRowContext(
		ctx,
		`SELECT name, cron_expr, command, args_json, timeout_seconds, created_at FROM jobs WHERE id=?`,
		jobID,
	).Scan(&job.Name, &job.CronExpr, &job.Command, &argsJSON, &job.TimeoutSeconds, &job.CreatedAt)
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
		`SELECT id, name, cron_expr, command, args_json, timeout_seconds, created_at
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
		if err := rows.Scan(&job.ID, &job.Name, &job.CronExpr, &job.Command, &argsJSON, &job.TimeoutSeconds, &job.CreatedAt); err != nil {
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
	jobs, err := s.loadSchedulableJobs(ctx)
	if err != nil {
		s.logger.Printf("scheduler load jobs failed: %v", err)
		return
	}
	if len(jobs) == 0 {
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
}

func (s *masterServer) triggerJob(
	ctx context.Context,
	job jobRecord,
	scheduledAt time.Time,
	mode triggerMode,
) (jobInstanceRecord, error) {
	normalizedScheduledAt := normalizeScheduledAt(scheduledAt)

	instance, created, err := s.ensureJobInstance(ctx, job.ID, normalizedScheduledAt)
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

	runReq := &scheduler.RunJobRequest{
		JobId:          job.ID,
		JobInstanceId:  instance.ID,
		Command:        job.Command,
		Args:           job.Args,
		TimeoutSeconds: job.TimeoutSeconds,
		ScheduledAt:    timestamppb.New(normalizedScheduledAt),
	}

	if err := s.dispatchRunJob(ctx, runReq); err != nil {
		if mode.MarkFailedOnDispatchError {
			s.markDispatchFailed(ctx, instance.ID, err)
		}
		return instance, err
	}

	action := "trigger job"
	if !created {
		action = "redispatch pending job"
	}
	s.logger.Printf(
		"%s: source=%s job_id=%d instance_id=%d scheduled_at=%s worker=%s",
		action,
		mode.Source,
		job.ID,
		instance.ID,
		normalizedScheduledAt.Format(time.RFC3339Nano),
		s.cfg.WorkerGRPCAddr,
	)
	return instance, nil
}

func (s *masterServer) ensureJobInstance(ctx context.Context, jobID int64, scheduledAt time.Time) (jobInstanceRecord, bool, error) {
	res, err := s.db.ExecContext(
		ctx,
		`INSERT INTO job_instances(job_id, scheduled_at, status) VALUES (?, ?, 'PENDING')`,
		jobID,
		scheduledAt,
	)
	if err == nil {
		instanceID, readErr := res.LastInsertId()
		if readErr != nil {
			return jobInstanceRecord{}, false, fmt.Errorf("read job instance id failed: %w", readErr)
		}
		return jobInstanceRecord{ID: instanceID, Status: "PENDING"}, true, nil
	}
	if !isDuplicateKeyError(err) {
		return jobInstanceRecord{}, false, fmt.Errorf("insert job instance failed: %w", err)
	}

	instance, loadErr := s.getJobInstanceBySlot(ctx, jobID, scheduledAt)
	if loadErr != nil {
		return jobInstanceRecord{}, false, fmt.Errorf("load existing job instance failed: %w", loadErr)
	}
	return instance, false, nil
}

func (s *masterServer) getJobInstanceBySlot(ctx context.Context, jobID int64, scheduledAt time.Time) (jobInstanceRecord, error) {
	var instance jobInstanceRecord
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, status FROM job_instances WHERE job_id=? AND scheduled_at=?`,
		jobID,
		scheduledAt,
	).Scan(&instance.ID, &instance.Status)
	if err != nil {
		return jobInstanceRecord{}, err
	}
	return instance, nil
}

func (s *masterServer) markDispatchFailed(ctx context.Context, instanceID int64, dispatchErr error) {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE job_instances
		 SET status='FAILED', finished_at=?, error_message=?, updated_at=CURRENT_TIMESTAMP
		 WHERE id=? AND status='PENDING'`,
		time.Now().UTC(),
		"dispatch run job failed: "+dispatchErr.Error(),
		instanceID,
	)
	if err != nil {
		s.logger.Printf("mark dispatch failed: instance_id=%d err=%v", instanceID, err)
	}
}

func (s *masterServer) dispatchRunJob(parent context.Context, req *scheduler.RunJobRequest) error {
	ctx, cancel := context.WithTimeout(parent, 6*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, s.cfg.WorkerGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("dial worker failed: %w", err)
	}
	defer conn.Close()

	client := scheduler.NewWorkerClient(conn)
	resp, err := client.RunJob(ctx, req)
	if err != nil {
		return fmt.Errorf("worker RunJob failed: %w", err)
	}
	if !resp.GetAccepted() {
		return fmt.Errorf("worker rejected run job: %s", resp.GetMessage())
	}
	return nil
}

func (s *masterServer) ReportResult(ctx context.Context, req *scheduler.ReportResultRequest) (*scheduler.ReportResultResponse, error) {
	nextStatus, ok := protoStatusToDB(req.GetStatus())
	if !ok {
		return &scheduler.ReportResultResponse{Accepted: false, Message: "unsupported status"}, nil
	}

	var current string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM job_instances WHERE id=?`, req.GetJobInstanceId()).Scan(&current)
	if err != nil {
		return &scheduler.ReportResultResponse{Accepted: false, Message: fmt.Sprintf("query status failed: %v", err)}, nil
	}

	if !isValidTransition(current, nextStatus) {
		return &scheduler.ReportResultResponse{
			Accepted: false,
			Message:  fmt.Sprintf("invalid transition: %s -> %s", current, nextStatus),
		}, nil
	}

	if nextStatus == "RUNNING" {
		_, err = s.db.ExecContext(
			ctx,
			`UPDATE job_instances
			 SET status=?, worker_id=?, started_at=COALESCE(?, started_at), updated_at=CURRENT_TIMESTAMP
			 WHERE id=?`,
			nextStatus,
			req.GetWorkerId(),
			optionalTime(req.GetStartedAt()),
			req.GetJobInstanceId(),
		)
	} else {
		_, err = s.db.ExecContext(
			ctx,
			`UPDATE job_instances
			 SET status=?, worker_id=?, started_at=COALESCE(?, started_at), finished_at=COALESCE(?, finished_at),
			     exit_code=?, error_message=?, updated_at=CURRENT_TIMESTAMP
			 WHERE id=?`,
			nextStatus,
			req.GetWorkerId(),
			optionalTime(req.GetStartedAt()),
			optionalTime(req.GetFinishedAt()),
			req.GetExitCode(),
			req.GetErrorMessage(),
			req.GetJobInstanceId(),
		)
	}
	if err != nil {
		return &scheduler.ReportResultResponse{Accepted: false, Message: fmt.Sprintf("update job instance failed: %v", err)}, nil
	}

	s.logger.Printf(
		"report result: instance_id=%d status=%s worker=%s exit_code=%d",
		req.GetJobInstanceId(),
		nextStatus,
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
		return to == "RUNNING" || to == "KILLED"
	case "RUNNING":
		return to == "SUCCESS" || to == "FAILED" || to == "KILLED"
	default:
		return false
	}
}

func optionalTime(ts *timestamppb.Timestamp) interface{} {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	if t.IsZero() {
		return nil
	}
	return t.UTC()
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

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
