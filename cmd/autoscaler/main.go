package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"autoscaler/internal/api"
	"autoscaler/internal/config"
	"autoscaler/internal/downstream"
	"autoscaler/internal/kafka"
	"autoscaler/internal/processing"
	"autoscaler/internal/redisx"
	"autoscaler/internal/scaler"
	"autoscaler/internal/workers"

	"github.com/gin-gonic/gin"
)

func main() {
	cfg, err := config.Load("")
	if err != nil {
		fmt.Println("config error:", err)
		os.Exit(1)
	}

	kafka.InitKafkaWriter(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	kafka.InitKafkaReader(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID)
	defer kafka.CloseKafkaWriter()
	defer kafka.CloseKafkaReader()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := scaler.InitProcessMonitor(); err != nil {
		fmt.Println("process monitor unavailable:", err)
	}

	throughput := scaler.NewThroughputWindow(cfg.Scaling.ThroughputWindowSize, cfg.Scaling.ThroughputInterval)
	api.ConfigureRuntime(throughput)

	downstreamMonitor := newDownstreamMonitor(cfg)
	if downstreamMonitor != nil {
		fmt.Println("[downstream] monitoring enabled")
	}

	processor, err := processing.NewMongoRedisProcessor(ctx, cfg, downstreamMonitor)
	if err != nil {
		fmt.Println("processor startup error:", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := processor.Close(shutdownCtx); err != nil {
			fmt.Println("processor shutdown error:", err)
		}
	}()

	redisClient := redisx.NewClient(redisx.Config{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		Timeout:  cfg.Redis.ConnectTimeout,
	})

	nomadScaler, err := scaler.NewNomadScaler(cfg.Nomad, redisClient)
	if err != nil {
		fmt.Println("nomad scaler startup error:", err)
		os.Exit(1)
	}

	startBackgroundCheckers(ctx, cfg, downstreamMonitor, processor)

	stopThroughput, cancelThroughput := context.WithCancel(ctx)
	go throughput.Start(stopThroughput)
	defer cancelThroughput()

	manager := workers.NewManager(ctx, throughput, workers.Config{
		InitialWorkers:   cfg.Workers.InitialWorkers,
		MinWorkers:       cfg.Workers.MinWorkers,
		MaxWorkers:       cfg.Workers.MaxWorkers,
		InitialBatchSize: cfg.Workers.InitialBatchSize,
		MinBatchSize:     cfg.Workers.MinBatchSize,
		MaxBatchSize:     cfg.Workers.MaxBatchSize,
		BatchStep:        cfg.Workers.BatchStep,
		Processor:        processor,
		Downstream:       downstreamMonitor,
	})
	defer manager.Shutdown()

	r := gin.Default()
	r.POST("/events", api.ReceiveEvent)
	r.GET("/healthz", api.Healthz)
	r.GET("/internal/status", api.RuntimeStatus)
	go func() {
		if err := r.Run(cfg.API.Address); err != nil {
			fmt.Println("api server error:", err)
			stop()
		}
	}()

	ticker := time.NewTicker(cfg.Scaling.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cpuUsage := currentCPUUsage()
			lag := kafka.CurrentConsumerLag()
			if lag < 0 {
				lag = 0
			}

			decisionDownstreamStatus := currentDecisionDownstreamStatus(downstreamMonitor)
			allDownstreamStatuses := currentAllDownstreamStatuses(downstreamMonitor)
			decision := scaler.CalculateDecisionWithContext(scaler.DecisionContext{
				CPUUsage:           cpuUsage,
				Throughput:         throughput,
				QueueSize:          lag,
				Downstream:         decisionDownstreamStatus,
				DownstreamCooldown: cfg.Downstream.DecisionCooldown,
				Policy: scaler.Policy{
					ScaleUpLagThreshold:      cfg.Scaling.ScaleUpLagThreshold,
					BackpressureLagThreshold: cfg.Scaling.BackpressureLagThreshold,
					ScaleDownLagThreshold:    cfg.Scaling.ScaleDownLagThreshold,
					CPUScaleUpThreshold:      cfg.Scaling.CPUScaleUpThreshold,
					CPUBackpressureThreshold: cfg.Scaling.CPUBackpressureThreshold,
					QueueGrowthWindow:        cfg.Scaling.QueueGrowthWindow,
					QueueGrowthIncreaseCount: cfg.Scaling.QueueGrowthIncreaseCount,
				},
			})

			maxedOut, minOut := manager.ApplyDecision(decision)
			nomadScaler.ApplyNomadScaling(ctx, decision, maxedOut, minOut)
			state := manager.State()
			snapshot := throughput.LatestSnapshot()
			printStatus(state, snapshot, decision, cpuUsage, lag, decisionDownstreamStatus)
			api.ReportRuntime(api.RuntimeSnapshot{
				Timestamp:           time.Now().UTC(),
				QueueLag:            lag,
				CPUUsage:            cpuUsage,
				BackpressureEnabled: state.BackpressureEnabled,
				Workers:             state.Workers,
				BatchSize:           state.BatchSize,
				DecisionAction:      decision.Action(),
				DecisionReason:      decision.Reason,
				Throughput:          snapshot,
				Downstream:          toSnapshot(decisionDownstreamStatus),
				Downstreams:         toSnapshots(allDownstreamStatuses),
			})
		case <-ctx.Done():
			return
		}
	}
}

func printStatus(state workers.State, snapshot scaler.ThroughputSnapshot, decision scaler.Decision, cpuUsage float64, lag int64, downstreamStatus downstream.Status) {
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

func newDownstreamMonitor(cfg config.Config) *downstream.Monitor {
	if !cfg.Downstream.Enabled {
		return nil
	}

	return downstream.NewMonitor(downstream.Thresholds{
		DegradedLatency:             cfg.Downstream.DegradedLatency,
		UnhealthyLatency:            cfg.Downstream.UnhealthyLatency,
		DegradedErrorRate:           cfg.Downstream.DegradedErrorRate,
		UnhealthyErrorRate:          cfg.Downstream.UnhealthyErrorRate,
		MinimumSamplesForState:      cfg.Downstream.MinimumSamplesForState,
		DegradedConsecutiveWindows:  cfg.Downstream.DegradedConsecutiveWindows,
		UnhealthyConsecutiveWindows: cfg.Downstream.UnhealthyConsecutiveWindows,
		HealthyConsecutiveWindows:   cfg.Downstream.HealthyConsecutiveWindows,
		ObserveOnly:                 cfg.Downstream.ObserveOnly,
	})
}

func startBackgroundCheckers(ctx context.Context, cfg config.Config, monitor *downstream.Monitor, processor *processing.MongoRedisProcessor) {
	if monitor == nil {
		return
	}

	if cfg.Redis.HealthCheckEnabled {
		redisChecker := downstream.NewRedisChecker(monitor, downstream.RedisConfig{
			Name:     "redis",
			Addr:     cfg.Redis.Addr,
			Password: cfg.Redis.Password,
			DB:       cfg.Redis.DB,
			Policy:   downstream.ParsePolicy(cfg.Redis.Policy, downstream.PolicyProtective),
			Interval: cfg.Redis.HealthCheckInterval,
			Timeout:  cfg.Redis.HealthCheckTimeout,
		})
		go redisChecker.Start(ctx)
	}

	if cfg.MongoDB.HealthCheckEnabled {
		mongoChecker := downstream.NewMongoDBChecker(
			monitor,
			processor.MongoClient(),
			downstream.MongoDBConfig{
				Name:     "mongodb",
				Policy:   downstream.ParsePolicy(cfg.MongoDB.Policy, downstream.PolicyCritical),
				Interval: cfg.MongoDB.HealthCheckInterval,
			},
		)
		go mongoChecker.Start(ctx)
	}
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

func currentAllDownstreamStatuses(monitor *downstream.Monitor) []downstream.Status {
	if monitor == nil {
		return nil
	}

	return monitor.Statuses()
}

func formatLatency(status downstream.Status) string {
	if status.SampleCount == 0 {
		return "unknown"
	}

	return status.P95Latency.String()
}

func toSnapshot(status downstream.Status) api.DownstreamStatusSnapshot {
	return api.DownstreamStatusSnapshot{
		Name:        status.Name,
		Kind:        string(status.Kind),
		Operation:   status.Operation,
		Policy:      string(status.Policy),
		State:       string(status.State),
		RawState:    string(status.RawState),
		Actionable:  status.Actionable,
		SampleCount: status.SampleCount,
		ErrorRate:   status.ErrorRate,
		P95Latency:  formatLatency(status),
		Reason:      status.Reason,
		LastError:   status.LastError,
	}
}

func toSnapshots(statuses []downstream.Status) []api.DownstreamStatusSnapshot {
	result := make([]api.DownstreamStatusSnapshot, 0, len(statuses))
	for _, status := range statuses {
		result = append(result, toSnapshot(status))
	}
	return result
}
