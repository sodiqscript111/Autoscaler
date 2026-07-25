package downstream

import (
	"math"
	"sort"
	"strings"
	"time"
)

func buildStatus(samples []Sample, thresholds Thresholds) Status {
	status := Status{
		Name:        samples[0].Name,
		Kind:        samples[0].Kind,
		Operation:   samples[0].Operation,
		Policy:      samples[0].Policy,
		State:       StateHealthy,
		RawState:    StateHealthy,
		Actionable:  samples[0].Policy != PolicyObserveOnly,
		SampleCount: len(samples),
		Timestamp:   samples[len(samples)-1].Timestamp,
	}

	durations := make([]time.Duration, 0, len(samples))
	var total time.Duration

	for _, sample := range samples {
		durations = append(durations, sample.Duration)
		total += sample.Duration
		if sample.Success {
			status.SuccessCount++
			continue
		}

		status.FailureCount++
		if sample.Error != "" {
			status.LastError = sample.Error
		}
	}

	sort.Slice(durations, func(i, j int) bool {
		return durations[i] < durations[j]
	})

	status.AverageLatency = total / time.Duration(len(samples))
	status.P95Latency = percentileDuration(durations, 0.95)
	status.ErrorRate = float64(status.FailureCount) / float64(len(samples))

	if len(samples) < thresholds.MinimumSamplesForState {
		status.State = StateUnknown
		status.RawState = StateUnknown
		status.Reason = "waiting for more downstream samples"
		return status
	}

	switch {
	case status.ErrorRate >= thresholds.UnhealthyErrorRate:
		status.RawState = StateUnhealthy
		status.Reason = "downstream error rate is unhealthy"
	case status.P95Latency >= thresholds.UnhealthyLatency:
		status.RawState = StateUnhealthy
		status.Reason = "downstream latency is unhealthy"
	case status.ErrorRate >= thresholds.DegradedErrorRate:
		status.RawState = StateDegraded
		status.Reason = "downstream error rate is degraded"
	case status.P95Latency >= thresholds.DegradedLatency:
		status.RawState = StateDegraded
		status.Reason = "downstream latency is degraded"
	default:
		status.RawState = StateHealthy
		status.Reason = "downstream is healthy"
	}
	status.State = status.RawState

	return status
}

func applyHysteresis(raw Status, window *dependencyWindow, thresholds Thresholds, globalObserveOnly bool) Status {
	effective := raw
	effective.Actionable = !globalObserveOnly && raw.Policy != PolicyObserveOnly

	if raw.RawState == StateUnknown {
		effective.State = StateUnknown
		window.pendingState = StateUnknown
		window.pendingCount = 0
		return effective
	}

	previous := window.status.State
	if previous == "" || previous == StateUnknown {
		if raw.RawState == StateHealthy {
			effective.State = StateHealthy
			return effective
		}

		required := requiredConsecutiveWindows(raw.RawState, thresholds)
		if bumpPending(window, raw.RawState) >= required {
			effective.State = raw.RawState
			window.pendingState = ""
			window.pendingCount = 0
			return effective
		}

		effective.State = StateUnknown
		effective.Reason = raw.Reason + "; waiting for repeated signal"
		return effective
	}

	if raw.RawState == previous {
		effective.State = previous
		window.pendingState = ""
		window.pendingCount = 0
		return effective
	}

	required := requiredConsecutiveWindows(raw.RawState, thresholds)
	if bumpPending(window, raw.RawState) >= required {
		effective.State = raw.RawState
		window.pendingState = ""
		window.pendingCount = 0
		return effective
	}

	effective.State = previous
	effective.Reason = raw.Reason + "; holding previous state until signal is stable"
	return effective
}

func requiredConsecutiveWindows(state State, thresholds Thresholds) int {
	switch state {
	case StateUnhealthy:
		return thresholds.UnhealthyConsecutiveWindows
	case StateDegraded:
		return thresholds.DegradedConsecutiveWindows
	case StateHealthy:
		return thresholds.HealthyConsecutiveWindows
	default:
		return 1
	}
}

func bumpPending(window *dependencyWindow, state State) int {
	if window.pendingState == state {
		window.pendingCount++
		return window.pendingCount
	}

	window.pendingState = state
	window.pendingCount = 1
	return window.pendingCount
}

func statusRank(status Status) int {
	return stateRank(status.State)
}

func stateRank(state State) int {
	switch state {
	case StateUnhealthy:
		return 3
	case StateDegraded:
		return 2
	case StateHealthy:
		return 1
	default:
		return 0
	}
}

func sampleKey(sample Sample) string {
	return strings.Join([]string{string(sample.Kind), sample.Name, sample.Operation}, ":")
}

func normalizeSample(sample Sample) Sample {
	if sample.Name == "" {
		sample.Name = "default"
	}
	if sample.Kind == "" {
		sample.Kind = KindGeneric
	}
	if sample.Operation == "" {
		sample.Operation = "unknown"
	}
	if sample.Policy == "" {
		sample.Policy = PolicyCritical
	}
	if sample.Duration < 0 {
		sample.Duration = 0
	}
	if sample.Timestamp.IsZero() {
		sample.Timestamp = time.Now()
	}

	return sample
}

func percentileDuration(sortedDurations []time.Duration, percentile float64) time.Duration {
	if len(sortedDurations) == 0 {
		return 0
	}
	if len(sortedDurations) == 1 {
		return sortedDurations[0]
	}
	if percentile <= 0 {
		return sortedDurations[0]
	}
	if percentile >= 1 {
		return sortedDurations[len(sortedDurations)-1]
	}

	index := int(math.Ceil(float64(len(sortedDurations)-1) * percentile))
	return sortedDurations[index]
}

func DefaultThresholds() Thresholds {
	return Thresholds{
		Window:                      20,
		DegradedLatency:             250 * time.Millisecond,
		UnhealthyLatency:            time.Second,
		DegradedErrorRate:           0.05,
		UnhealthyErrorRate:          0.20,
		MinimumSamplesForState:      3,
		DegradedConsecutiveWindows:  2,
		UnhealthyConsecutiveWindows: 2,
		HealthyConsecutiveWindows:   3,
	}
}

func withThresholdDefaults(thresholds Thresholds) Thresholds {
	defaults := DefaultThresholds()

	if thresholds.Window <= 0 {
		thresholds.Window = defaults.Window
	}
	if thresholds.DegradedLatency <= 0 {
		thresholds.DegradedLatency = defaults.DegradedLatency
	}
	if thresholds.UnhealthyLatency <= 0 {
		thresholds.UnhealthyLatency = defaults.UnhealthyLatency
	}
	if thresholds.UnhealthyLatency < thresholds.DegradedLatency {
		thresholds.UnhealthyLatency = thresholds.DegradedLatency
	}
	if thresholds.DegradedErrorRate <= 0 {
		thresholds.DegradedErrorRate = defaults.DegradedErrorRate
	}
	if thresholds.UnhealthyErrorRate <= 0 {
		thresholds.UnhealthyErrorRate = defaults.UnhealthyErrorRate
	}
	if thresholds.UnhealthyErrorRate < thresholds.DegradedErrorRate {
		thresholds.UnhealthyErrorRate = thresholds.DegradedErrorRate
	}
	if thresholds.MinimumSamplesForState <= 0 {
		thresholds.MinimumSamplesForState = defaults.MinimumSamplesForState
	}
	if thresholds.MinimumSamplesForState > thresholds.Window {
		thresholds.MinimumSamplesForState = thresholds.Window
	}
	if thresholds.DegradedConsecutiveWindows <= 0 {
		thresholds.DegradedConsecutiveWindows = defaults.DegradedConsecutiveWindows
	}
	if thresholds.UnhealthyConsecutiveWindows <= 0 {
		thresholds.UnhealthyConsecutiveWindows = defaults.UnhealthyConsecutiveWindows
	}
	if thresholds.HealthyConsecutiveWindows <= 0 {
		thresholds.HealthyConsecutiveWindows = defaults.HealthyConsecutiveWindows
	}

	return thresholds
}
