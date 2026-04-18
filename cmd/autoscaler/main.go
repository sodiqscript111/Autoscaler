package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"autoscaler/internal/api"
	"autoscaler/internal/kafka"
	"autoscaler/internal/scaler"

	"github.com/gin-gonic/gin"
	kafkago "github.com/segmentio/kafka-go"
)

const (
	initialWorkers = 1
	minWorkers     = 1
	maxWorkers     = 8

	initialBatchSize = 100
	minBatchSize     = 10
	maxBatchSize     = 500
	batchStep        = 25
)

var currentBatchSize atomic.Int64

func main() {
	currentBatchSize.Store(initialBatchSize)

	kafka.InitKafkaWriter()
	kafka.InitKafkaReader()
	defer kafka.CloseKafkaWriter()
	defer kafka.CloseKafkaReader()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	throughput := scaler.NewThroughputWindow(5, time.Second)
	api.ConfigureRuntime(throughput)

	stopThroughput := make(chan struct{})
	go throughput.Start(stopThroughput)
	defer close(stopThroughput)

	workers := newWorkerPool(ctx, throughput)
	workers.ScaleTo(initialWorkers)
	defer workers.Stop()

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
			applyDecision(workers, decision)
			printStatus(workers, throughput, decision, cpuUsage)
		case <-ctx.Done():
			return
		}
	}
}

type workerPool struct {
	rootCtx    context.Context
	throughput *scaler.ThroughputWindow

	mu      sync.Mutex
	cancels []context.CancelFunc
}

func newWorkerPool(rootCtx context.Context, throughput *scaler.ThroughputWindow) *workerPool {
	return &workerPool{
		rootCtx:    rootCtx,
		throughput: throughput,
	}
}

func (p *workerPool) ScaleTo(target int) {
	target = clamp(target, minWorkers, maxWorkers)

	p.mu.Lock()
	defer p.mu.Unlock()

	for len(p.cancels) < target {
		workerCtx, cancel := context.WithCancel(p.rootCtx)
		p.cancels = append(p.cancels, cancel)
		workerID := len(p.cancels)
		go p.runWorker(workerCtx, workerID)
	}

	for len(p.cancels) > target {
		last := len(p.cancels) - 1
		p.cancels[last]()
		p.cancels = p.cancels[:last]
	}
}

func (p *workerPool) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.cancels)
}

func (p *workerPool) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, cancel := range p.cancels {
		cancel()
	}
	p.cancels = nil
}

func (p *workerPool) runWorker(ctx context.Context, workerID int) {
	for {
		batch := make([]kafkago.Message, 0, int(currentBatchSize.Load()))

		item, err := kafka.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		batch = append(batch, item)

		fillCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		for len(batch) < int(currentBatchSize.Load()) {
			item, err := kafka.FetchMessage(fillCtx)
			if err != nil {
				break
			}
			batch = append(batch, item)
		}
		cancel()

		time.Sleep(50 * time.Millisecond)

		commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = kafka.CommitMessages(commitCtx, batch...)
		cancel()
		if err != nil {
			fmt.Printf("worker=%d failed to commit messages: %v\n", workerID, err)
			continue
		}

		if p.throughput != nil {
			p.throughput.AddProcessed(int64(len(batch)))
		}
	}
}

func applyDecision(workers *workerPool, decision scaler.Decision) {
	api.SetBackpressureEnabled(decision.EnableBackpressure)

	switch {
	case decision.ScaleUp:
		if workers.Count() >= maxWorkers && currentBatchSize.Load() >= maxBatchSize {
			api.SetBackpressureEnabled(true)
			return
		}
		workers.ScaleTo(workers.Count() + 1)
		adjustBatchSize(batchStep)
	case decision.ScaleDown:
		workers.ScaleTo(workers.Count() - 1)
		adjustBatchSize(-batchStep)
	}
}

func adjustBatchSize(delta int64) {
	next := currentBatchSize.Load() + delta
	currentBatchSize.Store(int64(clamp(int(next), minBatchSize, maxBatchSize)))
}

func printStatus(workers *workerPool, throughput *scaler.ThroughputWindow, decision scaler.Decision, cpuUsage float64) {
	snapshot := throughput.LatestSnapshot()
	lag := kafka.CurrentConsumerLag()
	if lag < 0 {
		lag = 0
	}

	fmt.Printf(
		"incoming=%d/s processed=%d/s lag=%d workers=%d batchSize=%d backpressure=%v cpu=%s decision=%q\n",
		snapshot.IncomingRate,
		snapshot.ProcessedRate,
		lag,
		workers.Count(),
		currentBatchSize.Load(),
		api.BackpressureEnabled(),
		formatCPUUsage(cpuUsage),
		decision.Reason,
	)
}

func currentCPUUsage() float64 {
	return -1
}

func formatCPUUsage(cpuUsage float64) string {
	if cpuUsage < 0 {
		return "unknown"
	}

	return fmt.Sprintf("%.1f%%", cpuUsage)
}

func clamp(value, min, max int) int {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}
