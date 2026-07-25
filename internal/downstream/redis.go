package downstream

import (
	"context"
	"time"

	"autoscaler/internal/redisx"
)

type RedisConfig struct {
	Name     string
	Addr     string
	Password string
	DB       int
	Policy   Policy
	Interval time.Duration
	Timeout  time.Duration
}

type RedisChecker struct {
	monitor *Monitor
	config  RedisConfig
}

func NewRedisChecker(monitor *Monitor, config RedisConfig) *RedisChecker {
	if config.Name == "" {
		config.Name = "redis"
	}
	if config.Addr == "" {
		config.Addr = "localhost:6379"
	}
	if config.Interval <= 0 {
		config.Interval = time.Second
	}
	if config.Timeout <= 0 {
		config.Timeout = 500 * time.Millisecond
	}
	if config.Policy == "" {
		config.Policy = PolicyProtective
	}

	return &RedisChecker{
		monitor: monitor,
		config:  config,
	}
}

func (c *RedisChecker) Start(ctx context.Context) {
	if c == nil || c.monitor == nil {
		return
	}

	runCheckerLoop(ctx, c.config.Interval, c.checkOnce)
}

func (c *RedisChecker) checkOnce(ctx context.Context) {
	started := time.Now()
	client := redisx.NewClient(redisx.Config{
		Addr:     c.config.Addr,
		Password: c.config.Password,
		DB:       c.config.DB,
		Timeout:  c.config.Timeout,
	})
	err := client.Ping(ctx)

	c.monitor.RecordPing(c.config.Name, KindRedis, c.config.Policy, time.Since(started), err)
}
