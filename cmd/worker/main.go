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
	"time"

	scheduler "github.com/HITGerhman/distributed-job-scheduler/api/proto"
	"github.com/HITGerhman/distributed-job-scheduler/internal/discovery"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type config struct {
	WorkerGRPCAddr        string
	WorkerAdvertiseAddr   string
	MasterGRPCAddr        string
	WorkerID              string
	LogDir                string
	EtcdEndpoints         []string
	EtcdDialTimeout       time.Duration
	WorkerDiscoveryPrefix string
	WorkerLeaseTTL        int64
}

type workerServer struct {
	scheduler.UnimplementedWorkerServer
	cfg     config
	logger  *log.Logger
	etcd    *clientv3.Client
	mu      sync.Mutex
	running map[int64]context.CancelFunc
}

func main() {
	etcdDialTimeout, err := durationEnvOrDefault("ETCD_DIAL_TIMEOUT", discovery.DefaultEtcdDialTimeout)
	if err != nil {
		log.Fatalf("invalid ETCD_DIAL_TIMEOUT: %v", err)
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
		WorkerAdvertiseAddr:   envOrDefault("WORKER_ADVERTISE_ADDR", envOrDefault("WORKER_GRPC_ADDR", ":50051")),
		MasterGRPCAddr:        envOrDefault("MASTER_GRPC_ADDR", "master:50052"),
		WorkerID:              envOrDefault("WORKER_ID", "worker-1"),
		LogDir:                envOrDefault("LOG_DIR", "/app/logs"),
		EtcdEndpoints:         discovery.ParseEtcdEndpoints(envOrDefault("ETCD_ENDPOINTS", discovery.DefaultEtcdEndpointsRaw)),
		EtcdDialTimeout:       etcdDialTimeout,
		WorkerDiscoveryPrefix: discovery.NormalizeWorkerPrefix(envOrDefault("WORKER_DISCOVERY_PREFIX", discovery.DefaultWorkerPrefix)),
		WorkerLeaseTTL:        workerLeaseTTL,
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

	srv := &workerServer{
		cfg:     cfg,
		logger:  logger,
		etcd:    etcdClient,
		running: make(map[int64]context.CancelFunc),
	}

	lis, err := net.Listen("tcp", cfg.WorkerGRPCAddr)
	if err != nil {
		logger.Fatalf("listen worker grpc failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	scheduler.RegisterWorkerServer(grpcServer, srv)

	go srv.runRegistrationLoop(context.Background())

	logger.Printf(
		"worker started: grpc=%s advertise=%s master=%s etcd=%v lease_ttl=%d",
		cfg.WorkerGRPCAddr,
		cfg.WorkerAdvertiseAddr,
		cfg.MasterGRPCAddr,
		cfg.EtcdEndpoints,
		cfg.WorkerLeaseTTL,
	)
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatalf("worker grpc serve failed: %v", err)
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
