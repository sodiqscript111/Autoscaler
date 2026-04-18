package scaler

import (
	"sync"
	"sync/atomic"
	"time"
)

type ThroughputSnapshot struct {
	IncomingRate  int64
	ProcessedRate int64
	Timestamp     time.Time
}

type ThroughputWindow struct {
	incomingCounter  atomic.Int64
	processedCounter atomic.Int64

	mu       sync.RWMutex
	history  []ThroughputSnapshot
	window   int
	interval time.Duration
}

func NewThroughputWindow(window int, interval time.Duration) *ThroughputWindow {
	if window <= 0 {
		window = 5
	}
	if interval <= 0 {
		interval = time.Second
	}

	return &ThroughputWindow{
		history:  make([]ThroughputSnapshot, 0, window),
		window:   window,
		interval: interval,
	}
}

func (tw *ThroughputWindow) IncrementIncoming() {
	tw.AddIncoming(1)
}

func (tw *ThroughputWindow) AddIncoming(count int64) {
	if count <= 0 {
		return
	}

	tw.incomingCounter.Add(count)
}

func (tw *ThroughputWindow) IncrementProcessed() {
	tw.AddProcessed(1)
}

func (tw *ThroughputWindow) AddProcessed(count int64) {
	if count <= 0 {
		return
	}

	tw.processedCounter.Add(count)
}

func (tw *ThroughputWindow) Start(stopCh <-chan struct{}) {
	ticker := time.NewTicker(tw.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tw.captureSnapshot()
		case <-stopCh:
			return
		}
	}
}

func (tw *ThroughputWindow) captureSnapshot() {
	incoming := tw.incomingCounter.Swap(0)
	processed := tw.processedCounter.Swap(0)

	snapshot := ThroughputSnapshot{
		IncomingRate:  incoming,
		ProcessedRate: processed,
		Timestamp:     time.Now(),
	}

	tw.mu.Lock()
	defer tw.mu.Unlock()

	tw.history = append(tw.history, snapshot)
	if len(tw.history) > tw.window {
		tw.history = tw.history[len(tw.history)-tw.window:]
	}
}

func (tw *ThroughputWindow) LatestSnapshot() ThroughputSnapshot {
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	if len(tw.history) == 0 {
		return ThroughputSnapshot{}
	}

	return tw.history[len(tw.history)-1]
}

func (tw *ThroughputWindow) AverageIncomingRate() float64 {
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	if len(tw.history) == 0 {
		return 0
	}

	var total int64
	for _, snapshot := range tw.history {
		total += snapshot.IncomingRate
	}

	return float64(total) / float64(len(tw.history))
}

func (tw *ThroughputWindow) AverageProcessedRate() float64 {
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	if len(tw.history) == 0 {
		return 0
	}

	var total int64
	for _, snapshot := range tw.history {
		total += snapshot.ProcessedRate
	}

	return float64(total) / float64(len(tw.history))
}

func (tw *ThroughputWindow) FallingBehind() bool {
	return tw.AverageIncomingRate() > tw.AverageProcessedRate()
}

func (tw *ThroughputWindow) History() []ThroughputSnapshot {
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	result := make([]ThroughputSnapshot, len(tw.history))
	copy(result, tw.history)
	return result
}
