package scaler

import "autoscaler/internal/kafka"

type Decision struct {
	ScaleUp            bool
	ScaleDown          bool
	EnableBackpressure bool
	Reason             string
}

var queueSizeHistory []int64

func CalculateDecision(cpuUsage float64) Decision {
	queueSize := int64(kafka.CurrentConsumerLag())

	queueSizeHistory = append(queueSizeHistory, queueSize)
	if len(queueSizeHistory) > 5 {
		queueSizeHistory = queueSizeHistory[len(queueSizeHistory)-5:]
	}

	growing := isQueueGrowing(queueSizeHistory)

	if queueSize >= 100 && growing && cpuUsage > 85 {
		return Decision{
			EnableBackpressure: true,
			Reason:"lag is very high, queue is growing, and CPU is unhealthy",
		}
	}

	if queueSize >= 70 && growing && cpuUsage < 75 {
		return Decision{
			ScaleUp: true,
			Reason:  "lag is high, queue is growing, and CPU is healthy enough to scale up",
		}
	}

	if queueSize <= 20 && !growing {
		return Decision{
			ScaleDown: true,
			Reason:    "lag is low and queue is stable",
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