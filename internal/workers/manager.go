package workers

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"autoscaler/internal/api"
	"autoscaler/internal/downstream"
	"autoscaler/internal/kafka"
	"autoscaler/internal/scaler"

	kafkago "github.com/segmentio/kafka-go"
)

type Config struct {
	InitialWorkers   int
	MinWorkers       int
	MaxWorkers       int
	InitialBatchSize int
	MinBatchSize     int
	MaxBatchSize     int
	BatchStep        int
	Processor        BatchProcessor
	Downstream       *downstream.Monitor
}

type BatchProcessor interface {
	ProcessBatch(ctx context.Context, batch []kafkago.Message) error
}

type SleepProcessor struct {
	Latency time.Duration
}

type State struct {
	Workers             int
	BatchSize           int64
	BackpressureEnabled bool
}

type Manager struct {
	rootCtx    context.Context
	throughput *scaler.ThroughputWindow
	config     Config
	processor  BatchProcessor
	downstream *downstream.Monitor

	currentBatchSize atomic.Int64

	mu      sync.Mutex
	cancels []context.CancelFunc
}

func NewManager(rootCtx context.Context, throughput *scaler.ThroughputWindow, config Config) *Manager {
	config = withDefaults(config)

	manager := &Manager{
		rootCtx:    rootCtx,
		throughput: throughput,
		config:     config,
		processor:  config.Processor,
		downstream: config.Downstream,
	}
	manager.currentBatchSize.Store(int64(config.InitialBatchSize))
	manager.ScaleTo(config.InitialWorkers)
	fmt.Printf("[manager] started workers=%d batchSize=%d\n", manager.Count(), manager.BatchSize())

	return manager
}

func (m *Manager) ApplyDecision(decision scaler.Decision) {
	before := m.State()

	if decision.EnableBackpressure {
		api.SetBackpressureEnabled(true)
		if !before.BackpressureEnabled {
			fmt.Printf("[manager] backpressure enabled reason=%q\n", decision.Reason)
		}
		return
	}

	if before.BackpressureEnabled {
		fmt.Printf("[manager] backpressure disabled reason=%q\n", decision.Reason)
	}
	api.SetBackpressureEnabled(false)

	switch {
	case decision.ScaleUp:
		if m.Count() >= m.config.MaxWorkers && m.BatchSize() >= int64(m.config.MaxBatchSize) {
			api.SetBackpressureEnabled(true)
			fmt.Printf("[manager] backpressure enabled reason=%q workers=%d batchSize=%d\n", "scale-up requested but worker and batch limits are maxed out", m.Count(), m.BatchSize())
			return
		}

		m.ScaleTo(m.Count() + 1)
		m.adjustBatchSize(m.config.BatchStep)
		after := m.State()
		fmt.Printf("[manager] scale_up workers=%d->%d batchSize=%d->%d reason=%q\n", before.Workers, after.Workers, before.BatchSize, after.BatchSize, decision.Reason)
	case decision.ScaleDown:
		m.ScaleTo(m.Count() - 1)
		m.adjustBatchSize(-m.config.BatchStep)
		after := m.State()
		fmt.Printf("[manager] scale_down workers=%d->%d batchSize=%d->%d reason=%q\n", before.Workers, after.Workers, before.BatchSize, after.BatchSize, decision.Reason)
	}
}

func (m *Manager) ScaleTo(target int) {
	target = clamp(target, m.config.MinWorkers, m.config.MaxWorkers)

	m.mu.Lock()
	defer m.mu.Unlock()

	for len(m.cancels) < target {
		workerCtx, cancel := context.WithCancel(m.rootCtx)
		m.cancels = append(m.cancels, cancel)
		workerID := len(m.cancels)
		go m.runWorker(workerCtx, workerID)
	}

	for len(m.cancels) > target {
		last := len(m.cancels) - 1
		m.cancels[last]()
		m.cancels = m.cancels[:last]
	}
}

func (m *Manager) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.cancels)
}

func (m *Manager) BatchSize() int64 {
	return m.currentBatchSize.Load()
}

func (m *Manager) State() State {
	return State{
		Workers:             m.Count(),
		BatchSize:           m.BatchSize(),
		BackpressureEnabled: api.BackpressureEnabled(),
	}
}

func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, cancel := range m.cancels {
		cancel()
	}
	m.cancels = nil
}

func (m *Manager) runWorker(ctx context.Context, workerID int) {
	for {
		batch := make([]kafkago.Message, 0, int(m.BatchSize()))

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
		for len(batch) < int(m.BatchSize()) {
			item, err := kafka.FetchMessage(fillCtx)
			if err != nil {
				break
			}
			batch = append(batch, item)
		}
		cancel()

		if err := m.processBatch(ctx, batch); err != nil {
			fmt.Printf("worker=%d failed to process messages: %v\n", workerID, err)
			continue
		}

		commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = kafka.CommitMessages(commitCtx, batch...)
		cancel()
		if err != nil {
			fmt.Printf("worker=%d failed to commit messages: %v\n", workerID, err)
			continue
		}

		if m.throughput != nil {
			m.throughput.AddProcessed(int64(len(batch)))
		}
	}
}

func (m *Manager) processBatch(ctx context.Context, batch []kafkago.Message) error {
	processor := m.processor
	if processor == nil {
		processor = SleepProcessor{Latency: 50 * time.Millisecond}
	}

	started := time.Now()
	err := processor.ProcessBatch(ctx, batch)

	if m.downstream != nil {
		sample := downstream.Sample{
			Name:      "worker-processor",
			Kind:      downstream.KindWorker,
			Operation: "process_batch",
			Policy:    downstream.PolicyProtective,
			Duration:  time.Since(started),
			Success:   err == nil,
			Timestamp: time.Now(),
		}
		if err != nil {
			sample.Error = err.Error()
		}
		m.downstream.Record(sample)
	}

	return err
}

func (p SleepProcessor) ProcessBatch(ctx context.Context, batch []kafkago.Message) error {
	latency := p.Latency
	if latency <= 0 {
		latency = 50 * time.Millisecond
	}

	timer := time.NewTimer(latency)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) adjustBatchSize(delta int) {
	next := int(m.currentBatchSize.Load()) + delta
	next = clamp(next, m.config.MinBatchSize, m.config.MaxBatchSize)
	m.currentBatchSize.Store(int64(next))
}

func withDefaults(config Config) Config {
	if config.InitialWorkers <= 0 {
		config.InitialWorkers = 1
	}
	if config.MinWorkers <= 0 {
		config.MinWorkers = 1
	}
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = 8
	}
	if config.InitialBatchSize <= 0 {
		config.InitialBatchSize = 100
	}
	if config.MinBatchSize <= 0 {
		config.MinBatchSize = 10
	}
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 500
	}
	if config.BatchStep <= 0 {
		config.BatchStep = 25
	}
	if config.Processor == nil {
		config.Processor = SleepProcessor{Latency: 50 * time.Millisecond}
	}

	config.InitialWorkers = clamp(config.InitialWorkers, config.MinWorkers, config.MaxWorkers)
	config.InitialBatchSize = clamp(config.InitialBatchSize, config.MinBatchSize, config.MaxBatchSize)

	return config
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
