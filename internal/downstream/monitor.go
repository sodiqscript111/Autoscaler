package downstream

import (
	"context"
	"sync"
	"time"
)

type Monitor struct {
	mu          sync.RWMutex
	thresholds  Thresholds
	observeOnly bool
	windows     map[string]*dependencyWindow
	order       []string
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

	return selected
}
