package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"autoscaler/internal/api"
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

	stopThroughput := make(chan struct{})
	go throughput.Start(stopThroughput)
	defer close(stopThroughput)

	manager := workers.NewManager(ctx, throughput, workers.Config{})
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
			decision := scaler.CalculateDecision(cpuUsage, throughput)
			manager.ApplyDecision(decision)
			printStatus(manager, throughput, decision, cpuUsage)
		case <-ctx.Done():
			return
		}
	}
}

func printStatus(manager *workers.Manager, throughput *scaler.ThroughputWindow, decision scaler.Decision, cpuUsage float64) {
	snapshot := throughput.LatestSnapshot()
	lag := kafka.CurrentConsumerLag()
	if lag < 0 {
		lag = 0
	}

	state := manager.State()
	fmt.Printf(
		"incoming=%d/s processed=%d/s lag=%d workers=%d batchSize=%d backpressure=%v cpu=%s decision=%q\n",
		snapshot.IncomingRate,
		snapshot.ProcessedRate,
		lag,
		state.Workers,
		state.BatchSize,
		state.BackpressureEnabled,
		formatCPUUsage(cpuUsage),
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
