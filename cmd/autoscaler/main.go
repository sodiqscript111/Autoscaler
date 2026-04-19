package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"autoscaler/internal/api"
	"autoscaler/internal/downstream"
	"autoscaler/internal/kafka"
	"autoscaler/internal/scaler"
	"autoscaler/internal/workers"

	"github.com/gin-gonic/gin"
)

func main() {
	kafka.InitKafkaWriter()
	kafka.InitKafkaReader()
	defer kafka.CloseKafkaWriter()
	defer kafka.CloseKafkaReader()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := scaler.InitProcessMonitor(); err != nil {
		fmt.Println("process monitor unavailable:", err)
	}

	throughput := scaler.NewThroughputWindow(5, time.Second)
	api.ConfigureRuntime(throughput)

	downstreamMonitor := newDownstreamMonitor()
	if downstreamMonitor != nil {
		fmt.Println("[downstream] monitoring enabled")
	}
	downstreamDecisionCooldown := envDurationMS("DOWNSTREAM_DECISION_COOLDOWN_MS", 30*time.Second)

	redisChecker := newRedisChecker(downstreamMonitor)
	if redisChecker != nil {
		fmt.Println("[downstream] redis monitoring enabled")
		go redisChecker.Start(ctx)
	}

	stopThroughput := make(chan struct{})
	go throughput.Start(stopThroughput)
	defer close(stopThroughput)

	manager := workers.NewManager(ctx, throughput, workers.Config{
		Downstream: downstreamMonitor,
	})
	defer manager.Stop()

	r := gin.Default()
	r.POST("/events", api.Injectionpoint)
	go func() {
		if err := r.Run(":8080"); err != nil {
			fmt.Println("api server error:", err)
			stop()
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cpuUsage := currentCPUUsage()
			lag := kafka.CurrentConsumerLag()
			if lag < 0 {
				lag = 0
			}
			downstreamStatus := currentDecisionDownstreamStatus(downstreamMonitor)
			decision := scaler.CalculateDecisionWithContext(scaler.DecisionContext{
				CPUUsage:           cpuUsage,
				Throughput:         throughput,
				QueueSize:          lag,
				Downstream:         downstreamStatus,
				DownstreamCooldown: downstreamDecisionCooldown,
			})
			manager.ApplyDecision(decision)
			printStatus(manager, throughput, decision, cpuUsage, downstreamStatus)
		case <-ctx.Done():
			return
		}
	}
}

func printStatus(manager *workers.Manager, throughput *scaler.ThroughputWindow, decision scaler.Decision, cpuUsage float64, downstreamStatus downstream.Status) {
	snapshot := throughput.LatestSnapshot()
	lag := kafka.CurrentConsumerLag()
	if lag < 0 {
		lag = 0
	}

	state := manager.State()
	fmt.Printf(
		"[brain] action=%s incoming=%d/s processed=%d/s lag=%d workers=%d batchSize=%d backpressure=%v cpu=%s downstream=%s/%s/%s policy=%s state=%s raw=%s actionable=%v p95=%s error=%.1f%% reason=%q\n",
		decision.Action(),
		snapshot.IncomingRate,
		snapshot.ProcessedRate,
		lag,
		state.Workers,
		state.BatchSize,
		state.BackpressureEnabled,
		formatCPUUsage(cpuUsage),
		downstreamStatus.Kind,
		downstreamStatus.Name,
		downstreamStatus.Operation,
		downstreamStatus.Policy,
		downstreamStatus.State,
		downstreamStatus.RawState,
		downstreamStatus.Actionable,
		formatLatency(downstreamStatus),
		downstreamStatus.ErrorRate*100,
		decision.Reason,
	)
}

func currentCPUUsage() float64 {
	metrics, err := scaler.CurrentProcessCPUAndMemory()
	if err != nil {
		fmt.Println("process metrics error:", err)
		return -1
	}

	return metrics.CPUUsage
}

func formatCPUUsage(cpuUsage float64) string {
	if cpuUsage < 0 {
		return "unknown"
	}

	return fmt.Sprintf("%.1f%%", cpuUsage)
}

func newDownstreamMonitor() *downstream.Monitor {
	if !envBool("DOWNSTREAM_HEALTH_ENABLED", true) {
		return nil
	}

	return downstream.NewMonitor(downstream.Thresholds{
		DegradedLatency:             envDurationMS("DOWNSTREAM_DEGRADED_LATENCY_MS", 250*time.Millisecond),
		UnhealthyLatency:            envDurationMS("DOWNSTREAM_UNHEALTHY_LATENCY_MS", time.Second),
		DegradedErrorRate:           envFloat("DOWNSTREAM_DEGRADED_ERROR_RATE", 0.05),
		UnhealthyErrorRate:          envFloat("DOWNSTREAM_UNHEALTHY_ERROR_RATE", 0.20),
		MinimumSamplesForState:      envInt("DOWNSTREAM_MIN_SAMPLES", 3),
		DegradedConsecutiveWindows:  envInt("DOWNSTREAM_DEGRADED_WINDOWS", 2),
		UnhealthyConsecutiveWindows: envInt("DOWNSTREAM_UNHEALTHY_WINDOWS", 2),
		HealthyConsecutiveWindows:   envInt("DOWNSTREAM_HEALTHY_WINDOWS", 3),
		ObserveOnly:                 envBool("DOWNSTREAM_OBSERVE_ONLY", false),
	})
}

func newRedisChecker(monitor *downstream.Monitor) *downstream.RedisChecker {
	if monitor == nil || !envBool("REDIS_HEALTH_ENABLED", false) {
		return nil
	}

	return downstream.NewRedisChecker(monitor, downstream.RedisConfig{
		Addr:     envString("REDIS_ADDR", "localhost:6379"),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       envInt("REDIS_DB", 0),
		Policy:   envPolicy("REDIS_DOWNSTREAM_POLICY", downstream.PolicyProtective),
		Interval: envDurationMS("REDIS_HEALTH_INTERVAL_MS", time.Second),
		Timeout:  envDurationMS("REDIS_HEALTH_TIMEOUT_MS", 500*time.Millisecond),
	})
}

func currentDecisionDownstreamStatus(monitor *downstream.Monitor) downstream.Status {
	if monitor == nil {
		return downstream.Status{
			State:     downstream.StateUnknown,
			RawState:  downstream.StateUnknown,
			Reason:    "downstream monitoring disabled",
			Timestamp: time.Now(),
		}
	}

	return monitor.DecisionStatus()
}

func formatLatency(status downstream.Status) string {
	if status.SampleCount == 0 {
		return "unknown"
	}

	return status.P95Latency.String()
}

func envString(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

func envBool(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}

	switch value {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func envFloat(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}

	return parsed
}

func envDurationMS(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}

	return time.Duration(parsed) * time.Millisecond
}

func envPolicy(key string, fallback downstream.Policy) downstream.Policy {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch value {
	case string(downstream.PolicyCritical):
		return downstream.PolicyCritical
	case string(downstream.PolicyProtective):
		return downstream.PolicyProtective
	case string(downstream.PolicyObserveOnly):
		return downstream.PolicyObserveOnly
	default:
		return fallback
	}
}
