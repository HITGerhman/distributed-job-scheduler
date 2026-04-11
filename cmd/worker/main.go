package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	workerapp "djs/internal/app/worker"
	"djs/internal/config"
	loggerinfra "djs/internal/infra/logger"
	traceinfra "djs/internal/infra/tracing"
	"djs/internal/observability"
	registryetcd "djs/internal/registry/etcd"
	transportgrpc "djs/internal/transport/grpc"
)

func main() {
	var configPath string
	var instanceID string
	var listen string
	var advertise string
	var httpListen string

	flag.StringVar(&configPath, "config", "configs/local.yaml", "config file path")
	flag.StringVar(&instanceID, "id", "", "worker instance id override")
	flag.StringVar(&listen, "listen", "", "worker gRPC listen address override")
	flag.StringVar(&advertise, "advertise", "", "worker gRPC advertise address override")
	flag.StringVar(&httpListen, "http-listen", "", "worker observability HTTP listen address override")
	flag.Parse()

	bootstrap := loggerinfra.NewCommandLogger("djs", "worker", "bootstrap", os.Stderr)
	cfg, err := config.Load(configPath)
	if err != nil {
		fatal(bootstrap, "load_config_failed", "load config failed", err)
	}
	cfg.App.Role = "worker"
	if instanceID != "" {
		cfg.App.ID = instanceID
	}
	if listen != "" {
		cfg.GRPC.WorkerListen = listen
	}
	if advertise != "" {
		cfg.GRPC.WorkerAdvertise = advertise
	}
	if httpListen != "" {
		cfg.Observability.WorkerHTTPListen = httpListen
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serviceName := cfg.Observability.ServiceName
	if serviceName == "" {
		serviceName = cfg.App.Name
	}
	logger, err := loggerinfra.NewProcessLogger(serviceName, "worker", cfg.App.ID, cfg.Observability.LogDir, fmt.Sprintf("worker-%s.log", safeFileName(cfg.App.ID)))
	if err != nil {
		fatal(bootstrap, "logger_init_failed", "initialize process logger failed", err)
	}
	defer logger.Close()

	traceShutdown, err := traceinfra.Setup(ctx, cfg.Tracing, fmt.Sprintf("%s-worker", cfg.Tracing.ServiceName), cfg.App.ID)
	if err != nil {
		fatal(logger, "tracing_init_failed", "initialize tracing failed", err)
	}
	defer traceShutdown(context.Background())

	readiness := observability.NewReadiness("grpc_listener", "etcd_registration")
	metrics := observability.NewWorkerMetrics(cfg.Observability.MetricsNamespace)

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.ETCD.Endpoints,
		DialTimeout: cfg.ETCD.DialTimeout,
	})
	if err != nil {
		fatal(logger, "etcd_open_failed", "open etcd failed", err)
	}
	defer etcdClient.Close()

	connPool := transportgrpc.NewConnPool()
	defer connPool.Close()

	service := workerapp.NewService(
		cfg,
		registryetcd.NewWorkerRegistry(etcdClient, cfg.ETCD.WorkerPrefix, cfg.ETCD.LeaseTTL),
		registryetcd.NewLeaderResolver(etcdClient, cfg.ETCD.ElectionPrefix),
		transportgrpc.NewMasterClient(connPool),
		logger,
		metrics,
		readiness,
	)

	server := transportgrpc.NewWorkerServer(service)
	listener, err := transportgrpc.Listen(cfg.GRPC.WorkerListen)
	if err != nil {
		fatal(logger, "grpc_listen_failed", "listen worker gRPC failed", err)
	}
	defer listener.Close()
	readiness.Set("grpc_listener", true)

	obsServer := observability.NewHTTPServer(
		cfg.Observability.WorkerHTTPListen,
		metrics.Registry(),
		readiness,
		serviceName,
		"worker",
		cfg.App.ID,
	)

	go func() {
		<-ctx.Done()
		server.GracefulStop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := obsServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("observability_shutdown_failed", "shutdown observability server failed", err, nil)
		}
	}()
	go func() {
		if err := server.Serve(listener); err != nil && ctx.Err() == nil {
			logger.Error("grpc_server_stopped", "worker gRPC server stopped", err, nil)
			stop()
		}
	}()
	go func() {
		if err := obsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("observability_server_stopped", "worker observability server stopped", err, nil)
			stop()
		}
	}()

	logger.Info("startup", "worker started", loggerinfra.Fields{
		"worker_id": cfg.App.ID,
	})
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fatal(logger, "worker_run_failed", "run worker failed", err)
	}
}

func safeFileName(value string) string {
	replacer := strings.NewReplacer("/", "-", "\\", "-", ":", "-", " ", "-")
	return replacer.Replace(value)
}

func fatal(logger *loggerinfra.Logger, event string, message string, err error) {
	if logger != nil {
		logger.Error(event, message, err, nil)
	}
	os.Exit(1)
}
