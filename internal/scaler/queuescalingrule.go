package scaler

import "autoscaler/internal/kafka"

type Decision struct {
	ScaleUp            bool
	ScaleDown          bool
	EnableBackpressure bool
	Reason             string
}

var queueSizeHistory []int64

func CalculateDecision(cpuUsage float64, throughput *ThroughputWindow) Decision {
	if throughput == nil {
		return Decision{
			Reason: "throughput window is not initialized",
		}
	}

	queueSize := int64(kafka.CurrentConsumerLag())

	queueSizeHistory = append(queueSizeHistory, queueSize)
	if len(queueSizeHistory) > 5 {
		queueSizeHistory = queueSizeHistory[len(queueSizeHistory)-5:]
	}

	growing := isQueueGrowing(queueSizeHistory)

	incomingRate := throughput.AverageIncomingRate()
	processedRate := throughput.AverageProcessedRate()
	fallingBehind := incomingRate > processedRate
	cpuUnknown := cpuUsage < 0

	if queueSize >= 100 && growing && fallingBehind && cpuUsage > 85 {
		return Decision{
			EnableBackpressure: true,
			Reason:             "lag is very high, queue is growing, workers are falling behind, and CPU is unhealthy",
		}
	}

	if queueSize >= 70 && growing && fallingBehind && (cpuUnknown || cpuUsage < 75) {
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
