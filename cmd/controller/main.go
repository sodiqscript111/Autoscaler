package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"autoscaler/internal/config"
	"autoscaler/internal/downstream"
	"autoscaler/internal/rabbitmq"
	"autoscaler/internal/scaler"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func main() {
	cfg, err := config.Load("")
	if err != nil {
		fmt.Println("config error:", err)
		os.Exit(1)
	}

	if err := rabbitmq.InitRabbitMQ(cfg.RabbitMQ.URL, cfg.RabbitMQ.QueueName); err != nil {
		fmt.Println("rabbitmq init error:", err)
		os.Exit(1)
	}
	defer rabbitmq.CloseRabbitMQ()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nomadScaler, err := scaler.NewNomadScaler(cfg.Nomad, nil) // no redis client needed
	if err != nil {
		fmt.Println("nomad scaler startup error:", err)
		os.Exit(1)
	}

	downstreamMonitor := newDownstreamMonitor(cfg)
	if downstreamMonitor != nil {
		fmt.Println("[downstream] monitoring enabled")
		startBackgroundCheckers(ctx, cfg, downstreamMonitor)
	}

	ticker := time.NewTicker(cfg.Scaling.TickInterval)
	defer ticker.Stop()

	fmt.Println("Controller started. Monitoring queue...")

	// The throughput window will just be fake since we aren't processing events here.
	// But the scaler needs it to check if "fallingBehind".
	// Since workers don't report throughput locally to the controller yet, we will just pass a dummy window that always returns fallingBehind=true if queue > 0.
	// Wait, the scaling rule checks `fallingBehind := incomingRate > processedRate`.
	// Since this is the new architecture, let's just make the decision context.
	// For now we will rely entirely on the queue growth rule.

	for {
		select {
		case <-ticker.C:
			lag := rabbitmq.CurrentQueueDepth()
			if lag < 0 {
				lag = 0
			}

			decisionDownstreamStatus := currentDecisionDownstreamStatus(downstreamMonitor)
			
			// We need a dummy throughput to pass to CalculateDecisionWithContext
			dummyThroughput := scaler.NewThroughputWindow(5, 1*time.Second)
			dummyThroughput.AddIncoming(lag) // fake incoming to trigger fallingBehind if queue is non-zero
			
			decision := scaler.CalculateDecisionWithContext(scaler.DecisionContext{
				CPUUsage:           0,
				Throughput:         dummyThroughput,
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

			// maxedOut=true, minOut=true means the controller forces Nomad to handle scaling directly
			nomadScaler.ApplyNomadScaling(ctx, decision, true, true)
			
			if decision.Action() != "none" {
				fmt.Printf("[controller] action=%s lag=%d downstream=%s policy=%s reason=%q\n",
					decision.Action(),
					lag,
					decisionDownstreamStatus.State,
					decisionDownstreamStatus.Policy,
					decision.Reason,
				)
			}
		case <-ctx.Done():
			return
		}
	}
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

func startBackgroundCheckers(ctx context.Context, cfg config.Config, monitor *downstream.Monitor) {
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
		clientOptions := options.Client().ApplyURI(cfg.MongoDB.URI)
		client, err := mongo.Connect(clientOptions)
		if err == nil {
			err = client.Ping(ctx, readpref.Primary())
			if err == nil {
				mongoChecker := downstream.NewMongoDBChecker(
					monitor,
					client,
					downstream.MongoDBConfig{
						Name:     "mongodb",
						Policy:   downstream.ParsePolicy(cfg.MongoDB.Policy, downstream.PolicyCritical),
						Interval: cfg.MongoDB.HealthCheckInterval,
					},
				)
				go mongoChecker.Start(ctx)
			}
		}
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
