package workers

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"autoscaler/internal/api"
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
	}
	manager.currentBatchSize.Store(int64(config.InitialBatchSize))
	manager.ScaleTo(config.InitialWorkers)

	return manager
}

func (m *Manager) ApplyDecision(decision scaler.Decision) {
	if decision.EnableBackpressure {
		api.SetBackpressureEnabled(true)
		return
	}

	api.SetBackpressureEnabled(false)

	switch {
	case decision.ScaleUp:
		if m.Count() >= m.config.MaxWorkers && m.BatchSize() >= int64(m.config.MaxBatchSize) {
			api.SetBackpressureEnabled(true)
			return
		}

		m.ScaleTo(m.Count() + 1)
		m.adjustBatchSize(m.config.BatchStep)
	case decision.ScaleDown:
		m.ScaleTo(m.Count() - 1)
		m.adjustBatchSize(-m.config.BatchStep)
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

		time.Sleep(50 * time.Millisecond)

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
