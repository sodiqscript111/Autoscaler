package api

import (
	"net/http"
	"sync"
	"time"

	"autoscaler/internal/scaler"

	"github.com/gin-gonic/gin"
)

type RuntimeSnapshot struct {
	Timestamp           time.Time                  `json:"timestamp"`
	QueueLag            int64                      `json:"queue_lag"`
	CPUUsage            float64                    `json:"cpu_usage"`
	BackpressureEnabled bool                       `json:"backpressure_enabled"`
	Workers             int                        `json:"workers"`
	BatchSize           int64                      `json:"batch_size"`
	DecisionAction      string                     `json:"decision_action"`
	DecisionReason      string                     `json:"decision_reason"`
	Throughput          scaler.ThroughputSnapshot  `json:"throughput"`
	Downstream          DownstreamStatusSnapshot   `json:"downstream"`
	Downstreams         []DownstreamStatusSnapshot `json:"downstreams"`
}

type DownstreamStatusSnapshot struct {
	Name        string  `json:"name"`
	Kind        string  `json:"kind"`
	Operation   string  `json:"operation"`
	Policy      string  `json:"policy"`
	State       string  `json:"state"`
	RawState    string  `json:"raw_state"`
	Actionable  bool    `json:"actionable"`
	SampleCount int     `json:"sample_count"`
	ErrorRate   float64 `json:"error_rate"`
	P95Latency  string  `json:"p95_latency"`
	Reason      string  `json:"reason"`
	LastError   string  `json:"last_error,omitempty"`
}

var runtimeSnapshotStore struct {
	mu       sync.RWMutex
	snapshot RuntimeSnapshot
}

func ReportRuntime(snapshot RuntimeSnapshot) {
	runtimeSnapshotStore.mu.Lock()
	defer runtimeSnapshotStore.mu.Unlock()

	if snapshot.Timestamp.IsZero() {
		snapshot.Timestamp = time.Now()
	}
	runtimeSnapshotStore.snapshot = snapshot
}

func CurrentRuntime() RuntimeSnapshot {
	runtimeSnapshotStore.mu.RLock()
	defer runtimeSnapshotStore.mu.RUnlock()

	return runtimeSnapshotStore.snapshot
}

func RuntimeStatus(c *gin.Context) {
	c.JSON(http.StatusOK, CurrentRuntime())
}

func Healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":               "ok",
		"backpressure_enabled": BackpressureEnabled(),
		"timestamp":            time.Now().UTC(),
	})
}
