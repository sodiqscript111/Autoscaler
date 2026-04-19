package scaler

import (
	"time"

	"autoscaler/internal/downstream"
	"autoscaler/internal/kafka"
)

type Decision struct {
	ScaleUp            bool
	ScaleDown          bool
	EnableBackpressure bool
	Reason             string
}

func (d Decision) Action() string {
	switch {
	case d.EnableBackpressure:
		return "backpressure"
	case d.ScaleUp:
		return "scale_up"
	case d.ScaleDown:
		return "scale_down"
	default:
		return "none"
	}
}

var queueSizeHistory []int64
var downstreamProtectionUntil time.Time

type DecisionContext struct {
	CPUUsage           float64
	Throughput         *ThroughputWindow
	QueueSize          int64
	Downstream         downstream.Status
	DownstreamCooldown time.Duration
	Now                time.Time
}

func CalculateDecision(cpuUsage float64, throughput *ThroughputWindow) Decision {
	return CalculateDecisionWithContext(DecisionContext{
		CPUUsage:   cpuUsage,
		Throughput: throughput,
		QueueSize:  int64(kafka.CurrentConsumerLag()),
		Downstream: downstream.Status{
			State: downstream.StateUnknown,
		},
	})
}

func CalculateDecisionWithContext(input DecisionContext) Decision {
	if input.Throughput == nil {
		return Decision{
			Reason: "throughput window is not initialized",
		}
	}
	if input.Now.IsZero() {
		input.Now = time.Now()
	}
	if input.DownstreamCooldown <= 0 {
		input.DownstreamCooldown = 30 * time.Second
	}

	queueSize := input.QueueSize

	queueSizeHistory = append(queueSizeHistory, queueSize)
	if len(queueSizeHistory) > 5 {
		queueSizeHistory = queueSizeHistory[len(queueSizeHistory)-5:]
	}

	growing := isQueueGrowing(queueSizeHistory)

	incomingRate := input.Throughput.AverageIncomingRate()
	processedRate := input.Throughput.AverageProcessedRate()
	fallingBehind := incomingRate > processedRate
	cpuUnknown := input.CPUUsage < 0
	downstreamActionable := actionableDownstream(input.Downstream)
	downstreamPolicy := downstreamPolicy(input.Downstream)
	downstreamState := input.Downstream.State
	downstreamDegraded := downstreamActionable && downstreamState == downstream.StateDegraded
	downstreamUnhealthy := downstreamActionable && downstreamState == downstream.StateUnhealthy
	downstreamProtecting := downstreamDegraded || downstreamUnhealthy
	downstreamCritical := downstreamPolicy == downstream.PolicyCritical

	if queueSize >= 100 && growing && fallingBehind && downstreamUnhealthy && downstreamCritical {
		markDownstreamProtection(input.Now, input.DownstreamCooldown)
		return Decision{
			EnableBackpressure: true,
			Reason:             downstreamDecisionReason("lag is very high, queue is growing, workers are falling behind, and critical downstream is unhealthy", input.Downstream),
		}
	}

	if queueSize >= 100 && growing && fallingBehind && input.CPUUsage > 85 {
		return Decision{
			EnableBackpressure: true,
			Reason:             "lag is very high, queue is growing, workers are falling behind, and CPU is unhealthy",
		}
	}

	if queueSize >= 70 && growing && fallingBehind && downstreamProtecting {
		reason := "lag is high and workers are falling behind, but downstream is not healthy so scale-up is suppressed"
		if input.Now.Before(downstreamProtectionUntil) {
			reason = "downstream protection cooldown is active; scale-up remains suppressed"
		}
		markDownstreamProtection(input.Now, input.DownstreamCooldown)

		return Decision{
			Reason: downstreamDecisionReason(reason, input.Downstream),
		}
	}

	if queueSize >= 70 && growing && fallingBehind && (cpuUnknown || input.CPUUsage < 75) {
		reason := "lag is high, queue is growing, workers are falling behind, and CPU is healthy enough to scale up"
		if cpuUnknown {
			reason = "lag is high, queue is growing, workers are falling behind, and CPU usage is unknown"
		}

		return Decision{
			ScaleUp: true,
			Reason:  reason,
		}
	}

	if queueSize <= 20 && !growing && incomingRate <= processedRate {
		return Decision{
			ScaleDown: true,
			Reason:    "lag is low, queue is stable, and workers are keeping up",
		}
	}

	return Decision{
		Reason: "no scaling action needed",
	}
}

func actionableDownstream(status downstream.Status) bool {
	if status.Policy == downstream.PolicyObserveOnly {
		return false
	}
	if status.Actionable {
		return true
	}

	return status.Policy == "" && (status.State == downstream.StateDegraded || status.State == downstream.StateUnhealthy)
}

func downstreamPolicy(status downstream.Status) downstream.Policy {
	if status.Policy == "" {
		return downstream.PolicyCritical
	}

	return status.Policy
}

func markDownstreamProtection(now time.Time, cooldown time.Duration) {
	downstreamProtectionUntil = now.Add(cooldown)
}

func downstreamDecisionReason(reason string, status downstream.Status) string {
	if status.Name == "" {
		return reason
	}

	return reason + ": " + status.Kind + "/" + status.Name + "/" + status.Operation
}

func isQueueGrowing(history []int64) bool {
	if len(history) < 5 {
		return false
	}

	increaseCount := 0

	for i := 1; i < len(history); i++ {
		if history[i] > history[i-1] {
			increaseCount++
		}
	}

	return increaseCount >= 3
}
