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
	"strings"
	"sync"
	"time"

	scheduler "github.com/HITGerhman/distributed-job-scheduler/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type config struct {
	WorkerGRPCAddr string
	MasterGRPCAddr string
	WorkerID       string
	LogDir         string
}

type workerServer struct {
	scheduler.UnimplementedWorkerServer
	cfg     config
	logger  *log.Logger
	mu      sync.Mutex
	running map[int64]context.CancelFunc
}

func main() {
	cfg := config{
		WorkerGRPCAddr: envOrDefault("WORKER_GRPC_ADDR", ":50051"),
		MasterGRPCAddr: envOrDefault("MASTER_GRPC_ADDR", "master:50052"),
		WorkerID:       envOrDefault("WORKER_ID", "worker-1"),
		LogDir:         envOrDefault("LOG_DIR", "/app/logs"),
	}

	if err := os.MkdirAll(cfg.LogDir, 0o755); err != nil {
		log.Fatalf("create log dir failed: %v", err)
	}

	logger := log.New(os.Stdout, "[worker] ", log.LstdFlags|log.Lmicroseconds)
	srv := &workerServer{
		cfg:     cfg,
		logger:  logger,
		running: make(map[int64]context.CancelFunc),
	}

	lis, err := net.Listen("tcp", cfg.WorkerGRPCAddr)
	if err != nil {
		logger.Fatalf("listen worker grpc failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	scheduler.RegisterWorkerServer(grpcServer, srv)

	logger.Printf("worker started: grpc=%s master=%s", cfg.WorkerGRPCAddr, cfg.MasterGRPCAddr)
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatalf("worker grpc serve failed: %v", err)
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

	s.mu.Lock()
	if _, exists := s.running[instanceID]; exists {
		s.mu.Unlock()
		return &scheduler.RunJobResponse{Accepted: false, Message: "job instance already running"}, nil
	}

	baseCtx := context.Background()
	var cancel context.CancelFunc
	if req.GetTimeoutSeconds() > 0 {
		baseCtx, cancel = context.WithTimeout(baseCtx, time.Duration(req.GetTimeoutSeconds())*time.Second)
	} else {
		baseCtx, cancel = context.WithCancel(baseCtx)
	}
	s.running[instanceID] = cancel
	s.mu.Unlock()

	go s.execute(baseCtx, req)

	s.logger.Printf("accept job: instance_id=%d command=%s args=%v", instanceID, req.GetCommand(), req.GetArgs())
	return &scheduler.RunJobResponse{Accepted: true, Message: "accepted"}, nil
}

func (s *workerServer) KillJob(ctx context.Context, req *scheduler.KillJobRequest) (*scheduler.KillJobResponse, error) {
	instanceID := req.GetJobInstanceId()
	if instanceID <= 0 {
		return &scheduler.KillJobResponse{Accepted: false, Message: "job_instance_id must be > 0"}, nil
	}

	s.mu.Lock()
	cancel, ok := s.running[instanceID]
	s.mu.Unlock()
	if !ok {
		return &scheduler.KillJobResponse{Accepted: false, Message: "job instance not running"}, nil
	}

	cancel()
	s.logger.Printf("kill requested: instance_id=%d reason=%s", instanceID, req.GetReason())
	return &scheduler.KillJobResponse{Accepted: true, Message: "kill signal sent"}, nil
}

func (s *workerServer) execute(runCtx context.Context, req *scheduler.RunJobRequest) {
	instanceID := req.GetJobInstanceId()
	defer s.unregister(instanceID)

	startedAt := time.Now().UTC()
	logPath := filepath.Join(s.cfg.LogDir, fmt.Sprintf("job_%d_instance_%d.log", req.GetJobId(), instanceID))
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		s.logger.Printf("open log file failed: instance_id=%d err=%v", instanceID, err)
		s.reportResult(req, scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_FAILED, -1, err.Error(), startedAt, time.Now().UTC())
		return
	}
	defer logFile.Close()

	_, _ = fmt.Fprintf(logFile, "[%s] start worker=%s command=%s args=%v\n", startedAt.Format(time.RFC3339Nano), s.cfg.WorkerID, req.GetCommand(), req.GetArgs())

	cmd := exec.CommandContext(runCtx, req.GetCommand(), req.GetArgs()...)
	cmd.Env = append(os.Environ(), flattenEnv(req.GetEnv())...)
	cmd.Stdout = io.MultiWriter(logFile)
	cmd.Stderr = io.MultiWriter(logFile)

	if err := cmd.Start(); err != nil {
		finished := time.Now().UTC()
		s.logger.Printf("start command failed: instance_id=%d err=%v", instanceID, err)
		_, _ = fmt.Fprintf(logFile, "[%s] start failed: %v\n", finished.Format(time.RFC3339Nano), err)
		s.reportResult(req, scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_FAILED, -1, err.Error(), startedAt, finished)
		return
	}

	s.reportResult(req, scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_RUNNING, 0, "", startedAt, time.Time{})

	err = cmd.Wait()
	finishedAt := time.Now().UTC()

	status := scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_SUCCESS
	exitCode := int32(0)
	errMsg := ""

	if err != nil {
		if errors.Is(runCtx.Err(), context.Canceled) || errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			status = scheduler.JobInstanceStatus_JOB_INSTANCE_STATUS_KILLED
			errMsg = runCtx.Err().Error()
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

	_, _ = fmt.Fprintf(logFile, "[%s] finished status=%s exit_code=%d error=%s\n", finishedAt.Format(time.RFC3339Nano), status.String(), exitCode, errMsg)
	s.reportResult(req, status, exitCode, errMsg, startedAt, finishedAt)
}

func (s *workerServer) unregister(instanceID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cancel, ok := s.running[instanceID]; ok {
		cancel()
		delete(s.running, instanceID)
	}
}

func (s *workerServer) reportResult(
	req *scheduler.RunJobRequest,
	status scheduler.JobInstanceStatus,
	exitCode int32,
	errMessage string,
	startedAt time.Time,
	finishedAt time.Time,
) {
	report := &scheduler.ReportResultRequest{
		JobId:         req.GetJobId(),
		JobInstanceId: req.GetJobInstanceId(),
		WorkerId:      s.cfg.WorkerID,
		Status:        status,
		ExitCode:      exitCode,
		ErrorMessage:  errMessage,
		StartedAt:     timestamppb.New(startedAt),
	}
	if !finishedAt.IsZero() {
		report.FinishedAt = timestamppb.New(finishedAt)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		s.cfg.MasterGRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		s.logger.Printf("report dial master failed: instance_id=%d status=%s err=%v", req.GetJobInstanceId(), status.String(), err)
		return
	}
	defer conn.Close()

	client := scheduler.NewMasterClient(conn)
	resp, err := client.ReportResult(ctx, report)
	if err != nil {
		s.logger.Printf("report result failed: instance_id=%d status=%s err=%v", req.GetJobInstanceId(), status.String(), err)
		return
	}
	if !resp.GetAccepted() {
		s.logger.Printf("report rejected: instance_id=%d status=%s msg=%s", req.GetJobInstanceId(), status.String(), resp.GetMessage())
		return
	}
	s.logger.Printf("report success: instance_id=%d status=%s", req.GetJobInstanceId(), status.String())
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
