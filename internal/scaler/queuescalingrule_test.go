package scaler

import (
	"strings"
	"testing"
	"time"

	"autoscaler/internal/downstream"
)

func TestCalculateDecisionScalesUpWhenDownstreamUnknown(t *testing.T) {
	downstreamProtectionUntil = time.Time{}
	queueSizeHistory = []int64{10, 20, 30, 40}
	throughput := throughputWithRates(100, 10)

	decision := CalculateDecisionWithContext(DecisionContext{
		CPUUsage:   50,
		Throughput: throughput,
		QueueSize:  80,
		Downstream: downstream.Status{State: downstream.StateUnknown, RawState: downstream.StateUnknown},
		Now:        time.Unix(10, 0),
	})

	if !decision.ScaleUp {
		t.Fatalf("expected scale up decision, got %+v", decision)
	}
}

func TestCalculateDecisionSuppressesScaleUpWhenDownstreamDegraded(t *testing.T) {
	downstreamProtectionUntil = time.Time{}
	queueSizeHistory = []int64{10, 20, 30, 40}
	throughput := throughputWithRates(100, 10)

	decision := CalculateDecisionWithContext(DecisionContext{
		CPUUsage:   50,
		Throughput: throughput,
		QueueSize:  80,
		Downstream: downstream.Status{
			Name:       "redis",
			Kind:       downstream.KindRedis,
			Operation:  "ping",
			Policy:     downstream.PolicyProtective,
			State:      downstream.StateDegraded,
			RawState:   downstream.StateDegraded,
			Actionable: true,
		},
		Now: time.Unix(10, 0),
	})

	if decision.ScaleUp || decision.EnableBackpressure || decision.ScaleDown {
		t.Fatalf("expected no scaling action, got %+v", decision)
	}
	if !strings.Contains(decision.Reason, "downstream") {
		t.Fatalf("expected downstream reason, got %q", decision.Reason)
	}
}

func TestCalculateDecisionBackpressuresWhenDownstreamUnhealthy(t *testing.T) {
	downstreamProtectionUntil = time.Time{}
	queueSizeHistory = []int64{10, 20, 30, 40}
	throughput := throughputWithRates(100, 10)

	decision := CalculateDecisionWithContext(DecisionContext{
		CPUUsage:   50,
		Throughput: throughput,
		QueueSize:  120,
		Downstream: downstream.Status{
			Name:       "postgres",
			Kind:       downstream.KindSQL,
			Operation:  "insert",
			Policy:     downstream.PolicyCritical,
			State:      downstream.StateUnhealthy,
			RawState:   downstream.StateUnhealthy,
			Actionable: true,
		},
		Now: time.Unix(10, 0),
	})

	if !decision.EnableBackpressure {
		t.Fatalf("expected backpressure decision, got %+v", decision)
	}
}

func TestCalculateDecisionProtectiveDownstreamDoesNotBackpressure(t *testing.T) {
	downstreamProtectionUntil = time.Time{}
	queueSizeHistory = []int64{10, 20, 30, 40}
	throughput := throughputWithRates(100, 10)

	decision := CalculateDecisionWithContext(DecisionContext{
		CPUUsage:   50,
		Throughput: throughput,
		QueueSize:  120,
		Downstream: downstream.Status{
			Name:       "redis",
			Kind:       downstream.KindRedis,
			Operation:  "ping",
			Policy:     downstream.PolicyProtective,
			State:      downstream.StateUnhealthy,
			RawState:   downstream.StateUnhealthy,
			Actionable: true,
		},
		Now: time.Unix(10, 0),
	})

	if decision.EnableBackpressure {
		t.Fatalf("expected protective downstream not to backpressure, got %+v", decision)
	}
	if decision.ScaleUp {
		t.Fatalf("expected protective downstream to suppress scale-up, got %+v", decision)
	}
}

func TestCalculateDecisionObserveOnlyDownstreamDoesNotAffectScaling(t *testing.T) {
	downstreamProtectionUntil = time.Time{}
	queueSizeHistory = []int64{10, 20, 30, 40}
	throughput := throughputWithRates(100, 10)

	decision := CalculateDecisionWithContext(DecisionContext{
		CPUUsage:   50,
		Throughput: throughput,
		QueueSize:  120,
		Downstream: downstream.Status{
			Name:       "stripe",
			Kind:       downstream.KindHTTP,
			Operation:  "charge",
			Policy:     downstream.PolicyObserveOnly,
			State:      downstream.StateUnhealthy,
			RawState:   downstream.StateUnhealthy,
			Actionable: false,
		},
		Now: time.Unix(10, 0),
	})

	if !decision.ScaleUp {
		t.Fatalf("expected observe-only downstream to preserve scale-up behavior, got %+v", decision)
	}
}

func TestCalculateDecisionScalesDownWithLowLag(t *testing.T) {
	downstreamProtectionUntil = time.Time{}
	queueSizeHistory = []int64{40, 30, 25, 20}
	throughput := throughputWithRates(10, 100)

	decision := CalculateDecisionWithContext(DecisionContext{
		CPUUsage:   50,
		Throughput: throughput,
		QueueSize:  10,
		Downstream: downstream.Status{
			Name:       "postgres",
			Kind:       downstream.KindSQL,
			Operation:  "insert",
			Policy:     downstream.PolicyCritical,
			State:      downstream.StateUnhealthy,
			RawState:   downstream.StateUnhealthy,
			Actionable: true,
		},
		Now: time.Unix(10, 0),
	})

	if !decision.ScaleDown {
		t.Fatalf("expected scale down decision, got %+v", decision)
	}
}

func throughputWithRates(incoming, processed int64) *ThroughputWindow {
	throughput := NewThroughputWindow(5, time.Second)
	throughput.AddIncoming(incoming)
	throughput.AddProcessed(processed)
	throughput.captureSnapshot()
	return throughput
}
