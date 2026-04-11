package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	App           AppConfig
	MySQL         MySQLConfig
	ETCD          ETCDConfig
	GRPC          GRPCConfig
	Scheduling    SchedulingConfig
	Worker        WorkerConfig
	Messaging     MessagingConfig
	Redis         RedisConfig
	Tracing       TracingConfig
	Observability ObservabilityConfig
}

type AppConfig struct {
	Name string
	Role string
	ID   string
}

type MySQLConfig struct {
	DSN string
}

type ETCDConfig struct {
	Endpoints      []string
	DialTimeout    time.Duration
	LeaseTTL       time.Duration
	ElectionPrefix string
	WorkerPrefix   string
}

type GRPCConfig struct {
	MasterListen    string
	MasterAdvertise string
	WorkerListen    string
	WorkerAdvertise string
	RequestTimeout  time.Duration
}

type SchedulingConfig struct {
	CreateInterval     time.Duration
	DispatchInterval   time.Duration
	ReconcileInterval  time.Duration
	Lookback           time.Duration
	Lookahead          time.Duration
	DispatchAckTimeout time.Duration
	HeartbeatTimeout   time.Duration
	BatchSize          int
}

type WorkerConfig struct {
	HeartbeatInterval time.Duration
	KillGrace         time.Duration
}

type MessagingConfig struct {
	Brokers              []string
	TopicLifecycle       string
	ProducerBatchTimeout time.Duration
	RelayInterval        time.Duration
	RelayBatchSize       int
	ConsumerGroup        string
}

type RedisConfig struct {
	Addr        string
	Password    string
	DB          int
	SnapshotTTL time.Duration
	CacheTTL    time.Duration
}

type TracingConfig struct {
	Enabled      bool
	ServiceName  string
	OTLPEndpoint string
	SampleRatio  float64
}

type ObservabilityConfig struct {
	MasterHTTPListen string
	WorkerHTTPListen string
	MetricsNamespace string
	LogFormat        string
	LogDir           string
	ServiceName      string
}

type rawConfig struct {
	App struct {
		Name string `yaml:"name"`
		Role string `yaml:"role"`
		ID   string `yaml:"id"`
	} `yaml:"app"`
	MySQL struct {
		DSN string `yaml:"dsn"`
	} `yaml:"mysql"`
	ETCD struct {
		Endpoints      []string `yaml:"endpoints"`
		DialTimeout    string   `yaml:"dial_timeout"`
		LeaseTTL       string   `yaml:"lease_ttl"`
		ElectionPrefix string   `yaml:"election_prefix"`
		WorkerPrefix   string   `yaml:"worker_prefix"`
	} `yaml:"etcd"`
	GRPC struct {
		MasterListen    string `yaml:"master_listen"`
		MasterAdvertise string `yaml:"master_advertise"`
		WorkerListen    string `yaml:"worker_listen"`
		WorkerAdvertise string `yaml:"worker_advertise"`
		RequestTimeout  string `yaml:"request_timeout"`
	} `yaml:"grpc"`
	Scheduling struct {
		CreateInterval     string `yaml:"create_interval"`
		DispatchInterval   string `yaml:"dispatch_interval"`
		ReconcileInterval  string `yaml:"reconcile_interval"`
		Lookback           string `yaml:"lookback"`
		Lookahead          string `yaml:"lookahead"`
		DispatchAckTimeout string `yaml:"dispatch_ack_timeout"`
		HeartbeatTimeout   string `yaml:"heartbeat_timeout"`
		BatchSize          int    `yaml:"batch_size"`
	} `yaml:"scheduling"`
	Worker struct {
		HeartbeatInterval string `yaml:"heartbeat_interval"`
		KillGrace         string `yaml:"kill_grace"`
	} `yaml:"worker"`
	Messaging struct {
		Brokers              []string `yaml:"brokers"`
		TopicLifecycle       string   `yaml:"topic_lifecycle"`
		ProducerBatchTimeout string   `yaml:"producer_batch_timeout"`
		RelayInterval        string   `yaml:"relay_interval"`
		RelayBatchSize       int      `yaml:"relay_batch_size"`
		ConsumerGroup        string   `yaml:"consumer_group"`
	} `yaml:"messaging"`
	Redis struct {
		Addr        string `yaml:"addr"`
		Password    string `yaml:"password"`
		DB          int    `yaml:"db"`
		SnapshotTTL string `yaml:"snapshot_ttl"`
		CacheTTL    string `yaml:"cache_ttl"`
	} `yaml:"redis"`
	Tracing struct {
		Enabled      bool    `yaml:"enabled"`
		ServiceName  string  `yaml:"service_name"`
		OTLPEndpoint string  `yaml:"otlp_endpoint"`
		SampleRatio  float64 `yaml:"sample_ratio"`
	} `yaml:"tracing"`
	Observability struct {
		MasterHTTPListen string `yaml:"master_http_listen"`
		WorkerHTTPListen string `yaml:"worker_http_listen"`
		MetricsNamespace string `yaml:"metrics_namespace"`
		LogFormat        string `yaml:"log_format"`
		LogDir           string `yaml:"log_dir"`
		ServiceName      string `yaml:"service_name"`
	} `yaml:"observability"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config failed: %w", err)
	}

	var raw rawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal config failed: %w", err)
	}

	cfg := &Config{
		App: AppConfig{
			Name: defaultString(raw.App.Name, "djs"),
			Role: defaultString(raw.App.Role, "master"),
			ID:   raw.App.ID,
		},
		MySQL: MySQLConfig{
			DSN: strings.TrimSpace(raw.MySQL.DSN),
		},
		ETCD: ETCDConfig{
			Endpoints:      raw.ETCD.Endpoints,
			ElectionPrefix: defaultString(raw.ETCD.ElectionPrefix, "/djs/leader"),
			WorkerPrefix:   defaultString(raw.ETCD.WorkerPrefix, "/djs/workers"),
		},
		GRPC: GRPCConfig{
			MasterListen:    defaultString(raw.GRPC.MasterListen, "127.0.0.1:8080"),
			MasterAdvertise: raw.GRPC.MasterAdvertise,
			WorkerListen:    defaultString(raw.GRPC.WorkerListen, "127.0.0.1:9090"),
			WorkerAdvertise: raw.GRPC.WorkerAdvertise,
		},
		Scheduling: SchedulingConfig{
			BatchSize: raw.Scheduling.BatchSize,
		},
		Messaging: MessagingConfig{
			Brokers:        raw.Messaging.Brokers,
			TopicLifecycle: defaultString(raw.Messaging.TopicLifecycle, "djs.lifecycle.v1"),
			RelayBatchSize: raw.Messaging.RelayBatchSize,
			ConsumerGroup:  defaultString(raw.Messaging.ConsumerGroup, "djs-audit-consumer"),
		},
		Redis: RedisConfig{
			Addr:     defaultString(raw.Redis.Addr, "127.0.0.1:16379"),
			Password: raw.Redis.Password,
			DB:       raw.Redis.DB,
		},
		Tracing: TracingConfig{
			Enabled:      raw.Tracing.Enabled,
			ServiceName:  defaultString(raw.Tracing.ServiceName, "djs"),
			OTLPEndpoint: defaultString(raw.Tracing.OTLPEndpoint, "127.0.0.1:14317"),
			SampleRatio:  raw.Tracing.SampleRatio,
		},
		Observability: ObservabilityConfig{
			MasterHTTPListen: defaultString(raw.Observability.MasterHTTPListen, "127.0.0.1:18080"),
			WorkerHTTPListen: defaultString(raw.Observability.WorkerHTTPListen, "127.0.0.1:19080"),
			MetricsNamespace: defaultString(raw.Observability.MetricsNamespace, "djs"),
			LogFormat:        defaultString(raw.Observability.LogFormat, "json"),
			LogDir:           defaultString(raw.Observability.LogDir, "runtime/logs"),
			ServiceName:      defaultString(raw.Observability.ServiceName, "djs"),
		},
	}

	if cfg.MySQL.DSN == "" {
		return nil, fmt.Errorf("mysql.dsn is required")
	}
	if len(cfg.ETCD.Endpoints) == 0 {
		cfg.ETCD.Endpoints = []string{"127.0.0.1:2379"}
	}

	if cfg.ETCD.DialTimeout, err = parseDuration(raw.ETCD.DialTimeout, 5*time.Second); err != nil {
		return nil, fmt.Errorf("parse etcd.dial_timeout failed: %w", err)
	}
	if cfg.ETCD.LeaseTTL, err = parseDuration(raw.ETCD.LeaseTTL, 10*time.Second); err != nil {
		return nil, fmt.Errorf("parse etcd.lease_ttl failed: %w", err)
	}
	if cfg.GRPC.RequestTimeout, err = parseDuration(raw.GRPC.RequestTimeout, 5*time.Second); err != nil {
		return nil, fmt.Errorf("parse grpc.request_timeout failed: %w", err)
	}
	if cfg.Scheduling.CreateInterval, err = parseDuration(raw.Scheduling.CreateInterval, 5*time.Second); err != nil {
		return nil, fmt.Errorf("parse scheduling.create_interval failed: %w", err)
	}
	if cfg.Scheduling.DispatchInterval, err = parseDuration(raw.Scheduling.DispatchInterval, 2*time.Second); err != nil {
		return nil, fmt.Errorf("parse scheduling.dispatch_interval failed: %w", err)
	}
	if cfg.Scheduling.ReconcileInterval, err = parseDuration(raw.Scheduling.ReconcileInterval, 5*time.Second); err != nil {
		return nil, fmt.Errorf("parse scheduling.reconcile_interval failed: %w", err)
	}
	if cfg.Scheduling.Lookback, err = parseDuration(raw.Scheduling.Lookback, 2*time.Minute); err != nil {
		return nil, fmt.Errorf("parse scheduling.lookback failed: %w", err)
	}
	if cfg.Scheduling.Lookahead, err = parseDuration(raw.Scheduling.Lookahead, 30*time.Second); err != nil {
		return nil, fmt.Errorf("parse scheduling.lookahead failed: %w", err)
	}
	if cfg.Scheduling.DispatchAckTimeout, err = parseDuration(raw.Scheduling.DispatchAckTimeout, 10*time.Second); err != nil {
		return nil, fmt.Errorf("parse scheduling.dispatch_ack_timeout failed: %w", err)
	}
	if cfg.Scheduling.HeartbeatTimeout, err = parseDuration(raw.Scheduling.HeartbeatTimeout, 15*time.Second); err != nil {
		return nil, fmt.Errorf("parse scheduling.heartbeat_timeout failed: %w", err)
	}
	if cfg.Worker.HeartbeatInterval, err = parseDuration(raw.Worker.HeartbeatInterval, 3*time.Second); err != nil {
		return nil, fmt.Errorf("parse worker.heartbeat_interval failed: %w", err)
	}
	if cfg.Worker.KillGrace, err = parseDuration(raw.Worker.KillGrace, 5*time.Second); err != nil {
		return nil, fmt.Errorf("parse worker.kill_grace failed: %w", err)
	}
	if cfg.Messaging.ProducerBatchTimeout, err = parseDuration(raw.Messaging.ProducerBatchTimeout, 5*time.Second); err != nil {
		return nil, fmt.Errorf("parse messaging.producer_batch_timeout failed: %w", err)
	}
	if cfg.Messaging.RelayInterval, err = parseDuration(raw.Messaging.RelayInterval, 3*time.Second); err != nil {
		return nil, fmt.Errorf("parse messaging.relay_interval failed: %w", err)
	}
	if cfg.Redis.SnapshotTTL, err = parseDuration(raw.Redis.SnapshotTTL, 30*time.Second); err != nil {
		return nil, fmt.Errorf("parse redis.snapshot_ttl failed: %w", err)
	}
	if cfg.Redis.CacheTTL, err = parseDuration(raw.Redis.CacheTTL, 15*time.Second); err != nil {
		return nil, fmt.Errorf("parse redis.cache_ttl failed: %w", err)
	}
	if cfg.Scheduling.BatchSize <= 0 {
		cfg.Scheduling.BatchSize = 100
	}
	if len(cfg.Messaging.Brokers) == 0 {
		cfg.Messaging.Brokers = []string{"127.0.0.1:19092"}
	}
	if cfg.Messaging.RelayBatchSize <= 0 {
		cfg.Messaging.RelayBatchSize = 100
	}
	if cfg.Tracing.SampleRatio <= 0 || cfg.Tracing.SampleRatio > 1 {
		cfg.Tracing.SampleRatio = 1
	}

	if cfg.App.ID == "" {
		host, _ := os.Hostname()
		cfg.App.ID = fmt.Sprintf("%s-%s-%d", cfg.App.Role, host, os.Getpid())
	}
	if cfg.GRPC.MasterAdvertise == "" {
		cfg.GRPC.MasterAdvertise = cfg.GRPC.MasterListen
	}
	if cfg.GRPC.WorkerAdvertise == "" {
		cfg.GRPC.WorkerAdvertise = cfg.GRPC.WorkerListen
	}

	return cfg, nil
}

func parseDuration(raw string, fallback time.Duration) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	return time.ParseDuration(raw)
}

func defaultString(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
