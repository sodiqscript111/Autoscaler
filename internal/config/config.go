package config

import (
	"fmt"
	"os"
	"time"

	yaml "gopkg.in/yaml.v3"
)

type Config struct {
	API        APIConfig        `yaml:"api"`
	Kafka      KafkaConfig      `yaml:"kafka"`
	Workers    WorkersConfig    `yaml:"workers"`
	Processing ProcessingConfig `yaml:"processing"`
	MongoDB    MongoDBConfig    `yaml:"mongodb"`
	Redis      RedisConfig      `yaml:"redis"`
	Downstream DownstreamConfig `yaml:"downstream"`
	Scaling    ScalingConfig    `yaml:"scaling"`
	Nomad      NomadConfig      `yaml:"nomad"`
}

type NomadConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Address   string `yaml:"address"`
	JobName   string `yaml:"job_name"`
	GroupName string `yaml:"group_name"`
	MaxScale  int    `yaml:"max_scale"`
}

type APIConfig struct {
	Address string `yaml:"address"`
}

type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
	GroupID string   `yaml:"group_id"`
}

type WorkersConfig struct {
	InitialWorkers   int `yaml:"initial_workers"`
	MinWorkers       int `yaml:"min_workers"`
	MaxWorkers       int `yaml:"max_workers"`
	InitialBatchSize int `yaml:"initial_batch_size"`
	MinBatchSize     int `yaml:"min_batch_size"`
	MaxBatchSize     int `yaml:"max_batch_size"`
	BatchStep        int `yaml:"batch_step"`
}

type ProcessingConfig struct {
	RedisCacheTTL  time.Duration `yaml:"redis_cache_ttl"`
	RedisKeyPrefix string        `yaml:"redis_key_prefix"`
}

type MongoDBConfig struct {
	URI                 string        `yaml:"uri"`
	Database            string        `yaml:"database"`
	Collection          string        `yaml:"collection"`
	ConnectTimeout      time.Duration `yaml:"connect_timeout"`
	HealthCheckEnabled  bool          `yaml:"health_check_enabled"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
	Policy              string        `yaml:"policy"`
}

type RedisConfig struct {
	Addr                string        `yaml:"addr"`
	Password            string        `yaml:"password"`
	DB                  int           `yaml:"db"`
	ConnectTimeout      time.Duration `yaml:"connect_timeout"`
	HealthCheckEnabled  bool          `yaml:"health_check_enabled"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
	HealthCheckTimeout  time.Duration `yaml:"health_check_timeout"`
	Policy              string        `yaml:"policy"`
}

type DownstreamConfig struct {
	Enabled                     bool          `yaml:"enabled"`
	ObserveOnly                 bool          `yaml:"observe_only"`
	DegradedLatency             time.Duration `yaml:"degraded_latency"`
	UnhealthyLatency            time.Duration `yaml:"unhealthy_latency"`
	DegradedErrorRate           float64       `yaml:"degraded_error_rate"`
	UnhealthyErrorRate          float64       `yaml:"unhealthy_error_rate"`
	MinimumSamplesForState      int           `yaml:"minimum_samples_for_state"`
	DegradedConsecutiveWindows  int           `yaml:"degraded_consecutive_windows"`
	UnhealthyConsecutiveWindows int           `yaml:"unhealthy_consecutive_windows"`
	HealthyConsecutiveWindows   int           `yaml:"healthy_consecutive_windows"`
	DecisionCooldown            time.Duration `yaml:"decision_cooldown"`
}

type ScalingConfig struct {
	TickInterval             time.Duration `yaml:"tick_interval"`
	ThroughputWindowSize     int           `yaml:"throughput_window_size"`
	ThroughputInterval       time.Duration `yaml:"throughput_interval"`
	ScaleUpLagThreshold      int64         `yaml:"scale_up_lag_threshold"`
	BackpressureLagThreshold int64         `yaml:"backpressure_lag_threshold"`
	ScaleDownLagThreshold    int64         `yaml:"scale_down_lag_threshold"`
	CPUScaleUpThreshold      float64       `yaml:"cpu_scale_up_threshold"`
	CPUBackpressureThreshold float64       `yaml:"cpu_backpressure_threshold"`
	QueueGrowthWindow        int           `yaml:"queue_growth_window"`
	QueueGrowthIncreaseCount int           `yaml:"queue_growth_increase_count"`
}

func Default() Config {
	return Config{
		API: APIConfig{
			Address: ":8080",
		},
		Kafka: KafkaConfig{
			Brokers: []string{"localhost:9094"},
			Topic:   "events",
			GroupID: "autoscaler-group",
		},
		Workers: WorkersConfig{
			InitialWorkers:   1,
			MinWorkers:       1,
			MaxWorkers:       8,
			InitialBatchSize: 100,
			MinBatchSize:     10,
			MaxBatchSize:     500,
			BatchStep:        25,
		},
		Processing: ProcessingConfig{
			RedisCacheTTL:  15 * time.Minute,
			RedisKeyPrefix: "autoscaler:event:",
		},
		MongoDB: MongoDBConfig{
			URI:                 "mongodb://localhost:27017",
			Database:            "autoscaler",
			Collection:          "events",
			ConnectTimeout:      5 * time.Second,
			HealthCheckEnabled:  true,
			HealthCheckInterval: time.Second,
			Policy:              "critical",
		},
		Redis: RedisConfig{
			Addr:                "localhost:6379",
			DB:                  0,
			ConnectTimeout:      500 * time.Millisecond,
			HealthCheckEnabled:  true,
			HealthCheckInterval: time.Second,
			HealthCheckTimeout:  500 * time.Millisecond,
			Policy:              "protective",
		},
		Downstream: DownstreamConfig{
			Enabled:                     true,
			ObserveOnly:                 false,
			DegradedLatency:             250 * time.Millisecond,
			UnhealthyLatency:            time.Second,
			DegradedErrorRate:           0.05,
			UnhealthyErrorRate:          0.20,
			MinimumSamplesForState:      3,
			DegradedConsecutiveWindows:  2,
			UnhealthyConsecutiveWindows: 2,
			HealthyConsecutiveWindows:   3,
			DecisionCooldown:            30 * time.Second,
		},
		Scaling: ScalingConfig{
			TickInterval:             time.Second,
			ThroughputWindowSize:     5,
			ThroughputInterval:       time.Second,
			ScaleUpLagThreshold:      70,
			BackpressureLagThreshold: 100,
			ScaleDownLagThreshold:    20,
			CPUScaleUpThreshold:      75,
			CPUBackpressureThreshold: 85,
			QueueGrowthWindow:        5,
			QueueGrowthIncreaseCount: 3,
		},
		Nomad: NomadConfig{
			Enabled:   false,
			Address:   "http://localhost:4646",
			JobName:   "autoscaler",
			GroupName: "worker-group",
			MaxScale:  10,
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()

	resolved, hasFile := resolvePath(path)
	if !hasFile {
		return cfg, nil
	}

	content, err := os.ReadFile(resolved)
	if err != nil {
		return Config{}, fmt.Errorf("read config file %s: %w", resolved, err)
	}

	if err := yaml.Unmarshal(content, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config file %s: %w", resolved, err)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) Validate() error {
	switch {
	case c.API.Address == "":
		return fmt.Errorf("api.address is required")
	case len(c.Kafka.Brokers) == 0:
		return fmt.Errorf("kafka.brokers must contain at least one broker")
	case c.Kafka.Topic == "":
		return fmt.Errorf("kafka.topic is required")
	case c.Kafka.GroupID == "":
		return fmt.Errorf("kafka.group_id is required")
	case c.MongoDB.URI == "":
		return fmt.Errorf("mongodb.uri is required")
	case c.MongoDB.Database == "":
		return fmt.Errorf("mongodb.database is required")
	case c.MongoDB.Collection == "":
		return fmt.Errorf("mongodb.collection is required")
	case c.Redis.Addr == "":
		return fmt.Errorf("redis.addr is required")
	case c.Workers.MaxWorkers < c.Workers.MinWorkers:
		return fmt.Errorf("workers.max_workers must be >= workers.min_workers")
	case c.Workers.MaxBatchSize < c.Workers.MinBatchSize:
		return fmt.Errorf("workers.max_batch_size must be >= workers.min_batch_size")
	case c.Downstream.UnhealthyLatency < c.Downstream.DegradedLatency:
		return fmt.Errorf("downstream.unhealthy_latency must be >= downstream.degraded_latency")
	case c.Downstream.UnhealthyErrorRate < c.Downstream.DegradedErrorRate:
		return fmt.Errorf("downstream.unhealthy_error_rate must be >= downstream.degraded_error_rate")
	case c.Scaling.BackpressureLagThreshold < c.Scaling.ScaleUpLagThreshold:
		return fmt.Errorf("scaling.backpressure_lag_threshold must be >= scaling.scale_up_lag_threshold")
	case c.Scaling.QueueGrowthWindow <= 1:
		return fmt.Errorf("scaling.queue_growth_window must be > 1")
	case c.Scaling.QueueGrowthIncreaseCount <= 0:
		return fmt.Errorf("scaling.queue_growth_increase_count must be > 0")
	}

	return nil
}

func resolvePath(path string) (string, bool) {
	if path != "" {
		return path, true
	}

	if envPath := os.Getenv("AUTOSCALER_CONFIG"); envPath != "" {
		return envPath, true
	}

	const defaultPath = "config.yaml"
	if _, err := os.Stat(defaultPath); err == nil {
		return defaultPath, true
	}

	return "", false
}
