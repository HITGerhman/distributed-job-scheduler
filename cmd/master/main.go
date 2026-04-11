package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	clientv3 "go.etcd.io/etcd/client/v3"

	masterapp "djs/internal/app/master"
	"djs/internal/config"
	cacheinfra "djs/internal/infra/cache"
	loggerinfra "djs/internal/infra/logger"
	messaginginfra "djs/internal/infra/messaging"
	traceinfra "djs/internal/infra/tracing"
	"djs/internal/observability"
	registryetcd "djs/internal/registry/etcd"
	mysqlrepo "djs/internal/repository/mysql"
	transportgrpc "djs/internal/transport/grpc"
)

func main() {
	var configPath string
	var instanceID string
	var listen string
	var advertise string
	var httpListen string

	flag.StringVar(&configPath, "config", "configs/local.yaml", "config file path")
	flag.StringVar(&instanceID, "id", "", "master instance id override")
	flag.StringVar(&listen, "listen", "", "master gRPC listen address override")
	flag.StringVar(&advertise, "advertise", "", "master gRPC advertise address override")
	flag.StringVar(&httpListen, "http-listen", "", "master observability HTTP listen address override")
	flag.Parse()

	bootstrap := loggerinfra.NewCommandLogger("djs", "master", "bootstrap", os.Stderr)
	cfg, err := config.Load(configPath)
	if err != nil {
		fatal(bootstrap, "load_config_failed", "load config failed", err)
	}
	cfg.App.Role = "master"
	if instanceID != "" {
		cfg.App.ID = instanceID
	}
	if listen != "" {
		cfg.GRPC.MasterListen = listen
	}
	if advertise != "" {
		cfg.GRPC.MasterAdvertise = advertise
	}
	if httpListen != "" {
		cfg.Observability.MasterHTTPListen = httpListen
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serviceName := cfg.Observability.ServiceName
	if serviceName == "" {
		serviceName = cfg.App.Name
	}
	logger, err := loggerinfra.NewProcessLogger(serviceName, "master", cfg.App.ID, cfg.Observability.LogDir, fmt.Sprintf("master-%s.log", safeFileName(cfg.App.ID)))
	if err != nil {
		fatal(bootstrap, "logger_init_failed", "initialize process logger failed", err)
	}
	defer logger.Close()

	traceShutdown, err := traceinfra.Setup(ctx, cfg.Tracing, fmt.Sprintf("%s-master", cfg.Tracing.ServiceName), cfg.App.ID)
	if err != nil {
		fatal(logger, "tracing_init_failed", "initialize tracing failed", err)
	}
	defer traceShutdown(context.Background())

	readiness := observability.NewReadiness("mysql", "etcd", "grpc_listener", "worker_watcher", "election_loop")
	metrics := observability.NewMasterMetrics(cfg.Observability.MetricsNamespace)

	db, err := openDB(ctx, cfg.MySQL.DSN)
	if err != nil {
		fatal(logger, "mysql_open_failed", "open mysql failed", err)
	}
	defer db.Close()
	readiness.Set("mysql", true)

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.ETCD.Endpoints,
		DialTimeout: cfg.ETCD.DialTimeout,
	})
	if err != nil {
		fatal(logger, "etcd_open_failed", "open etcd failed", err)
	}
	defer etcdClient.Close()
	readiness.Set("etcd", true)

	store := mysqlrepo.NewStore(db)
	connPool := transportgrpc.NewConnPool()
	defer connPool.Close()

	cache := cacheinfra.NewRedisCache(cfg.Redis)
	if cache != nil {
		defer cache.Close()
		if err := cache.Ping(ctx); err != nil {
			logger.Warn("redis_cache_unavailable", "redis cache unavailable, fallback to mysql", loggerinfra.Fields{
				"error": err.Error(),
			})
			cache = nil
		}
	}

	producer, err := messaginginfra.NewKafkaProducer(cfg.Messaging.Brokers, cfg.Messaging.ProducerBatchTimeout)
	if err != nil {
		fatal(logger, "kafka_producer_init_failed", "initialize kafka producer failed", err)
	}
	defer producer.Close()

	workerClient := transportgrpc.NewWorkerClient(connPool)
	workers := registryetcd.NewWorkerRegistry(etcdClient, cfg.ETCD.WorkerPrefix, cfg.ETCD.LeaseTTL)
	election := registryetcd.NewElection(etcdClient, cfg.ETCD.ElectionPrefix, registryetcd.LeaderInfo{
		MasterID: cfg.App.ID,
		GRPCAddr: cfg.GRPC.MasterAdvertise,
	}, cfg.ETCD.LeaseTTL)

	service := masterapp.NewService(cfg, store, workers, election, workerClient, logger, metrics, readiness, producer, cache)
	server := transportgrpc.NewMasterServer(service)
	listener, err := transportgrpc.Listen(cfg.GRPC.MasterListen)
	if err != nil {
		fatal(logger, "grpc_listen_failed", "listen master gRPC failed", err)
	}
	defer listener.Close()
	readiness.Set("grpc_listener", true)

	obsServer := observability.NewHTTPServer(
		cfg.Observability.MasterHTTPListen,
		metrics.Registry(),
		readiness,
		serviceName,
		"master",
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
			logger.Error("grpc_server_stopped", "master gRPC server stopped", err, nil)
			stop()
		}
	}()
	go func() {
		if err := obsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("observability_server_stopped", "master observability server stopped", err, nil)
			stop()
		}
	}()

	logger.Info("startup", "master started", loggerinfra.Fields{
		"leader": false,
		"msg":    "master started",
	})
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fatal(logger, "master_run_failed", "run master failed", err)
	}
}

func openDB(ctx context.Context, dsn string) (*sql.DB, error) {
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ParseTime = true
	cfg.Loc = timeUTC()

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func timeUTC() *time.Location {
	return time.UTC
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
