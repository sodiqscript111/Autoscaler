package workers

import (
	"context"
	"errors"
	"testing"
	"time"

	"autoscaler/internal/downstream"

	kafkago "github.com/segmentio/kafka-go"
)

func TestProcessBatchRecordsSuccessfulDownstreamSample(t *testing.T) {
	monitor := downstream.NewMonitor(workerTestThresholds())
	manager := &Manager{
		processor:  fakeProcessor{},
		downstream: monitor,
	}

	err := manager.processBatch(context.Background(), []kafkago.Message{{Value: []byte("ok")}})
	if err != nil {
		t.Fatalf("process batch failed: %v", err)
	}

	status := monitor.Status()
	if status.State != downstream.StateHealthy {
		t.Fatalf("expected healthy downstream status, got %s reason=%q", status.State, status.Reason)
	}
	if status.SuccessCount != 1 {
		t.Fatalf("expected one successful sample, got %d", status.SuccessCount)
	}
}

func TestProcessBatchRecordsFailedDownstreamSample(t *testing.T) {
	monitor := downstream.NewMonitor(workerTestThresholds())
	expectedErr := errors.New("downstream unavailable")
	manager := &Manager{
		processor:  fakeProcessor{err: expectedErr},
		downstream: monitor,
	}

	err := manager.processBatch(context.Background(), []kafkago.Message{{Value: []byte("fail")}})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected processor error, got %v", err)
	}

	status := monitor.Status()
	if status.State != downstream.StateUnhealthy {
		t.Fatalf("expected unhealthy downstream status, got %s reason=%q", status.State, status.Reason)
	}
	if status.LastError != expectedErr.Error() {
		t.Fatalf("expected last error %q, got %q", expectedErr.Error(), status.LastError)
	}
}

type fakeProcessor struct {
	err error
}

func (p fakeProcessor) ProcessBatch(ctx context.Context, batch []kafkago.Message) error {
	return p.err
}

func workerTestThresholds() downstream.Thresholds {
	return downstream.Thresholds{
		Window:                      3,
		DegradedLatency:             50 * time.Millisecond,
		UnhealthyLatency:            100 * time.Millisecond,
		DegradedErrorRate:           0.10,
		UnhealthyErrorRate:          0.50,
		MinimumSamplesForState:      1,
		DegradedConsecutiveWindows:  1,
		UnhealthyConsecutiveWindows: 1,
		HealthyConsecutiveWindows:   1,
	}
}
