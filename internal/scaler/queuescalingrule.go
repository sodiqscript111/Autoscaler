package scaler

import (
	"time"

	"autoscaler/internal/downstream"
	"autoscaler/internal/rabbitmq"
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

type Policy struct {
	ScaleUpLagThreshold      int64
	BackpressureLagThreshold int64
	ScaleDownLagThreshold    int64
	CPUScaleUpThreshold      float64
	CPUBackpressureThreshold float64
	QueueGrowthWindow        int
	QueueGrowthIncreaseCount int
}

type DecisionContext struct {
	CPUUsage           float64
	Throughput         *ThroughputWindow
	QueueSize          int64
	Downstream         downstream.Status
	DownstreamCooldown time.Duration
	Now                time.Time
	Policy             Policy
}

func CalculateDecision(cpuUsage float64, throughput *ThroughputWindow) Decision {
	return CalculateDecisionWithContext(DecisionContext{
		CPUUsage:   cpuUsage,
		Throughput: throughput,
		QueueSize:  int64(rabbitmq.CurrentQueueDepth()),
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
	input.Policy = withPolicyDefaults(input.Policy)

	queueSize := input.QueueSize

	queueSizeHistory = append(queueSizeHistory, queueSize)
	if len(queueSizeHistory) > input.Policy.QueueGrowthWindow {
		queueSizeHistory = queueSizeHistory[len(queueSizeHistory)-input.Policy.QueueGrowthWindow:]
	}

	growing := isQueueGrowing(queueSizeHistory, input.Policy.QueueGrowthIncreaseCount)

	incomingRate := input.Throughput.AverageIncomingRate()
	processedRate := input.Throughput.AverageProcessedRate()
	fallingBehind := incomingRate > processedRate
	downstreamActionable := actionableDownstream(input.Downstream)
	downstreamPolicy := downstreamPolicy(input.Downstream)
	downstreamState := input.Downstream.State
	downstreamDegraded := downstreamActionable && downstreamState == downstream.StateDegraded
	downstreamUnhealthy := downstreamActionable && downstreamState == downstream.StateUnhealthy
	downstreamProtecting := downstreamDegraded || downstreamUnhealthy
	downstreamCritical := downstreamPolicy == downstream.PolicyCritical

	if queueSize >= input.Policy.BackpressureLagThreshold && growing && fallingBehind && downstreamUnhealthy && downstreamCritical {
		markDownstreamProtection(input.Now, input.DownstreamCooldown)
		return Decision{
			EnableBackpressure: true,
			Reason:             downstreamDecisionReason("lag is very high, queue is growing, workers are falling behind, and critical downstream is unhealthy", input.Downstream),
		}
	}



	if queueSize >= input.Policy.ScaleUpLagThreshold && growing && fallingBehind && downstreamProtecting {
		reason := "lag is high and workers are falling behind, but downstream is not healthy so scale-up is suppressed"
		if input.Now.Before(downstreamProtectionUntil) {
			reason = "downstream protection cooldown is active; scale-up remains suppressed"
		}
		markDownstreamProtection(input.Now, input.DownstreamCooldown)

		return Decision{
			Reason: downstreamDecisionReason(reason, input.Downstream),
		}
	}

	if queueSize >= input.Policy.ScaleUpLagThreshold && growing && fallingBehind {
		return Decision{
			ScaleUp: true,
			Reason:  "lag is high, queue is growing, and workers are falling behind",
		}
	}

	if queueSize <= input.Policy.ScaleDownLagThreshold && !growing && incomingRate <= processedRate {
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

	return reason + ": " + string(status.Kind) + "/" + status.Name + "/" + status.Operation
}

func isQueueGrowing(history []int64, increaseCountThreshold int) bool {
	if len(history) < 2 {
		return false
	}

	increaseCount := 0

	for i := 1; i < len(history); i++ {
		if history[i] > history[i-1] {
			increaseCount++
		}
	}

	return increaseCount >= increaseCountThreshold
}

func withPolicyDefaults(policy Policy) Policy {
	if policy.ScaleUpLagThreshold <= 0 {
		policy.ScaleUpLagThreshold = 70
	}
	if policy.BackpressureLagThreshold <= 0 {
		policy.BackpressureLagThreshold = 100
	}
	if policy.ScaleDownLagThreshold <= 0 {
		policy.ScaleDownLagThreshold = 20
	}
	if policy.CPUScaleUpThreshold <= 0 {
		policy.CPUScaleUpThreshold = 75
	}
	if policy.CPUBackpressureThreshold <= 0 {
		policy.CPUBackpressureThreshold = 85
	}
	if policy.QueueGrowthWindow <= 1 {
		policy.QueueGrowthWindow = 5
	}
	if policy.QueueGrowthIncreaseCount <= 0 {
		policy.QueueGrowthIncreaseCount = 3
	}

	return policy
}
