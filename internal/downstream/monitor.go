package downstream

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type State string

const (
	StateUnknown   State = "unknown"
	StateHealthy   State = "healthy"
	StateDegraded  State = "degraded"
	StateUnhealthy State = "unhealthy"
)

type Policy string

const (
	PolicyCritical    Policy = "critical"
	PolicyProtective  Policy = "protective"
	PolicyObserveOnly Policy = "observe_only"
)

const (
	KindGeneric = "generic"
	KindHTTP    = "http"
	KindRedis   = "redis"
	KindSQL     = "sql"
	KindWorker  = "worker"
)

type Dependency struct {
	Name      string
	Kind      string
	Operation string
	Policy    Policy
}

type Thresholds struct {
	Window                      int
	DegradedLatency             time.Duration
	UnhealthyLatency            time.Duration
	DegradedErrorRate           float64
	UnhealthyErrorRate          float64
	MinimumSamplesForState      int
	DegradedConsecutiveWindows  int
	UnhealthyConsecutiveWindows int
	HealthyConsecutiveWindows   int
	ObserveOnly                 bool
}

type Sample struct {
	Name      string
	Kind      string
	Operation string
	Policy    Policy
	Duration  time.Duration
	Success   bool
	Error     string
	Timestamp time.Time
}

type Status struct {
	Name           string
	Kind           string
	Operation      string
	Policy         Policy
	State          State
	RawState       State
	Actionable     bool
	SampleCount    int
	SuccessCount   int
	FailureCount   int
	ErrorRate      float64
	AverageLatency time.Duration
	P95Latency     time.Duration
	LastError      string
	Reason         string
	Timestamp      time.Time
}

type Monitor struct {
	mu          sync.RWMutex
	thresholds  Thresholds
	observeOnly bool
	windows     map[string]*dependencyWindow
	order       []string
}

type dependencyWindow struct {
	samples      []Sample
	status       Status
	pendingState State
	pendingCount int
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

func NewMonitor(thresholds Thresholds) *Monitor {
	thresholds = withThresholdDefaults(thresholds)

	return &Monitor{
		thresholds:  thresholds,
		observeOnly: thresholds.ObserveOnly,
		windows:     make(map[string]*dependencyWindow),
	}
}

func Track(ctx context.Context, monitor *Monitor, dependency Dependency, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}

	started := time.Now()
	err := fn(ctx)

	if monitor != nil {
		sample := dependency.Sample(time.Since(started), err)
		monitor.Record(sample)
	}

	return err
}

func (d Dependency) Sample(duration time.Duration, err error) Sample {
	sample := Sample{
		Name:      d.Name,
		Kind:      d.Kind,
		Operation: d.Operation,
		Policy:    d.Policy,
		Duration:  duration,
		Success:   err == nil,
		Timestamp: time.Now(),
	}
	if err != nil {
		sample.Error = err.Error()
	}

	return sample
}

func (m *Monitor) Record(sample Sample) {
	if m == nil {
		return
	}

	sample = normalizeSample(sample)
	key := sampleKey(sample)

	m.mu.Lock()
	defer m.mu.Unlock()

	window, ok := m.windows[key]
	if !ok {
		window = &dependencyWindow{
			samples: make([]Sample, 0, m.thresholds.Window),
		}
		m.windows[key] = window
		m.order = append(m.order, key)
	}

	window.samples = append(window.samples, sample)
	if len(window.samples) > m.thresholds.Window {
		window.samples = window.samples[len(window.samples)-m.thresholds.Window:]
	}

	raw := buildStatus(window.samples, m.thresholds)
	window.status = applyHysteresis(raw, window, m.thresholds, m.observeOnly)
}

func (m *Monitor) Status() Status {
	return m.selectStatus(false)
}

func (m *Monitor) DecisionStatus() Status {
	return m.selectStatus(true)
}

func (m *Monitor) Statuses() []Status {
	if m == nil {
		return []Status{{
			State:     StateUnknown,
			RawState:  StateUnknown,
			Reason:    "downstream monitor is not configured",
			Timestamp: time.Now(),
		}}
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.order) == 0 {
		return nil
	}

	statuses := make([]Status, 0, len(m.order))
	for _, key := range m.order {
		statuses = append(statuses, m.windows[key].status)
	}

	return statuses
}

func (m *Monitor) selectStatus(actionableOnly bool) Status {
	statuses := m.Statuses()
	if len(statuses) == 0 {
		return Status{
			State:     StateUnknown,
			RawState:  StateUnknown,
			Reason:    "no downstream samples recorded",
			Timestamp: time.Now(),
		}
	}

	selected := Status{
		State:     StateUnknown,
		RawState:  StateUnknown,
		Reason:    "no actionable downstream samples recorded",
		Timestamp: time.Now(),
	}
	found := false

	for _, status := range statuses {
		if actionableOnly && !status.Actionable {
			continue
		}
		if !found || statusRank(status) > statusRank(selected) {
			selected = status
			found = true
			continue
		}
		if statusRank(status) == statusRank(selected) && status.Timestamp.After(selected.Timestamp) {
			selected = status
			found = true
		}
	}

	if !found {
		return selected
	}

	return selected
}

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
	return strings.Join([]string{sample.Kind, sample.Name, sample.Operation}, ":")
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
