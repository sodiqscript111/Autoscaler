package downstream

import (
	"context"
	"testing"
	"time"
)

func TestMonitorStatusUnknownWithoutSamples(t *testing.T) {
	monitor := NewMonitor(testThresholds())

	status := monitor.Status()

	if status.State != StateUnknown {
		t.Fatalf("expected unknown status, got %s", status.State)
	}
}

func TestMonitorStatusHealthy(t *testing.T) {
	monitor := NewMonitor(testThresholds())

	recordSamples(monitor, 3, 20*time.Millisecond, true)

	status := monitor.Status()
	if status.State != StateHealthy {
		t.Fatalf("expected healthy status, got %s reason=%q", status.State, status.Reason)
	}
}

func TestMonitorStatusDegradedByLatency(t *testing.T) {
	monitor := NewMonitor(testThresholds())

	recordSamples(monitor, 3, 80*time.Millisecond, true)

	status := monitor.Status()
	if status.State != StateDegraded {
		t.Fatalf("expected degraded status, got %s reason=%q", status.State, status.Reason)
	}
}

func TestMonitorStatusUnhealthyByErrorRate(t *testing.T) {
	monitor := NewMonitor(testThresholds())

	recordSamples(monitor, 3, 20*time.Millisecond, false)

	status := monitor.Status()
	if status.State != StateUnhealthy {
		t.Fatalf("expected unhealthy status, got %s reason=%q", status.State, status.Reason)
	}
	if status.ErrorRate != 1 {
		t.Fatalf("expected error rate 1, got %f", status.ErrorRate)
	}
}

func TestMonitorWaitsForMinimumSamples(t *testing.T) {
	monitor := NewMonitor(testThresholds())

	recordSamples(monitor, 2, 80*time.Millisecond, true)

	status := monitor.Status()
	if status.State != StateUnknown {
		t.Fatalf("expected unknown status before minimum samples, got %s", status.State)
	}
}

func TestMonitorReturnsWorstIndependentDownstreamStatus(t *testing.T) {
	monitor := NewMonitor(testThresholds())

	for i := 0; i < 10; i++ {
		monitor.Record(Sample{
			Name:      "worker",
			Kind:      KindWorker,
			Operation: "process_batch",
			Policy:    PolicyProtective,
			Duration:  20 * time.Millisecond,
			Success:   true,
		})
	}
	for i := 0; i < 3; i++ {
		monitor.Record(Sample{
			Name:      "redis",
			Kind:      KindRedis,
			Operation: "ping",
			Policy:    PolicyProtective,
			Duration:  10 * time.Millisecond,
			Success:   false,
			Error:     "connection refused",
		})
	}

	status := monitor.Status()
	if status.State != StateUnhealthy {
		t.Fatalf("expected unhealthy status, got %s reason=%q", status.State, status.Reason)
	}
	if status.Name != "redis" {
		t.Fatalf("expected redis status to be selected, got %q", status.Name)
	}
}

func TestMonitorDecisionStatusIgnoresObserveOnlyDependency(t *testing.T) {
	monitor := NewMonitor(testThresholds())

	for i := 0; i < 3; i++ {
		monitor.Record(Sample{
			Name:      "stripe",
			Kind:      KindHTTP,
			Operation: "charge",
			Policy:    PolicyObserveOnly,
			Duration:  10 * time.Millisecond,
			Success:   false,
		})
		monitor.Record(Sample{
			Name:      "postgres",
			Kind:      KindSQL,
			Operation: "insert",
			Policy:    PolicyCritical,
			Duration:  20 * time.Millisecond,
			Success:   true,
		})
	}

	status := monitor.DecisionStatus()
	if status.Name != "postgres" {
		t.Fatalf("expected actionable postgres status, got %s/%s", status.Kind, status.Name)
	}
	if status.State != StateHealthy {
		t.Fatalf("expected healthy actionable status, got %s", status.State)
	}
}

func TestMonitorGlobalObserveOnlyMakesStatusesNonActionable(t *testing.T) {
	thresholds := testThresholds()
	thresholds.ObserveOnly = true
	monitor := NewMonitor(thresholds)

	recordSamples(monitor, 3, 20*time.Millisecond, false)

	visible := monitor.Status()
	if visible.State != StateUnhealthy {
		t.Fatalf("expected visible unhealthy status, got %s", visible.State)
	}

	decision := monitor.DecisionStatus()
	if decision.State != StateUnknown {
		t.Fatalf("expected no actionable decision status, got %s", decision.State)
	}
}

func TestMonitorHysteresisRequiresRepeatedBadWindows(t *testing.T) {
	thresholds := testThresholds()
	thresholds.MinimumSamplesForState = 1
	thresholds.UnhealthyConsecutiveWindows = 2
	monitor := NewMonitor(thresholds)

	monitor.Record(Sample{
		Name:      "postgres",
		Kind:      KindSQL,
		Operation: "insert",
		Policy:    PolicyCritical,
		Duration:  20 * time.Millisecond,
		Success:   false,
	})

	status := monitor.Status()
	if status.State != StateUnknown {
		t.Fatalf("expected first bad window to be held as unknown, got %s", status.State)
	}

	monitor.Record(Sample{
		Name:      "postgres",
		Kind:      KindSQL,
		Operation: "insert",
		Policy:    PolicyCritical,
		Duration:  20 * time.Millisecond,
		Success:   false,
	})

	status = monitor.Status()
	if status.State != StateUnhealthy {
		t.Fatalf("expected repeated bad windows to become unhealthy, got %s", status.State)
	}
}

func TestTrackRecordsDependencySample(t *testing.T) {
	thresholds := testThresholds()
	thresholds.MinimumSamplesForState = 1
	monitor := NewMonitor(thresholds)
	dependency := Dependency{
		Name:      "stripe",
		Kind:      KindHTTP,
		Operation: "charge",
		Policy:    PolicyCritical,
	}

	err := Track(context.Background(), monitor, dependency, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("track returned unexpected error: %v", err)
	}

	status := monitor.Status()
	if status.Name != "stripe" || status.Kind != KindHTTP || status.Operation != "charge" {
		t.Fatalf("unexpected dependency identity: %+v", status)
	}
	if status.State != StateHealthy {
		t.Fatalf("expected healthy status, got %s", status.State)
	}
}

func recordSamples(monitor *Monitor, count int, duration time.Duration, success bool) {
	for i := 0; i < count; i++ {
		monitor.Record(Sample{
			Name:      "test",
			Kind:      KindGeneric,
			Operation: "test",
			Policy:    PolicyCritical,
			Duration:  duration,
			Success:   success,
		})
	}
}

func testThresholds() Thresholds {
	return Thresholds{
		Window:                      10,
		DegradedLatency:             50 * time.Millisecond,
		UnhealthyLatency:            100 * time.Millisecond,
		DegradedErrorRate:           0.10,
		UnhealthyErrorRate:          0.50,
		MinimumSamplesForState:      3,
		DegradedConsecutiveWindows:  1,
		UnhealthyConsecutiveWindows: 1,
		HealthyConsecutiveWindows:   1,
	}
}
