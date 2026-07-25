package downstream

import (
	"context"
	"time"
)

func runCheckerLoop(ctx context.Context, interval time.Duration, checkOnce func(context.Context)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	checkOnce(ctx)

	for {
		select {
		case <-ticker.C:
			checkOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (m *Monitor) RecordPing(name string, kind Kind, policy Policy, duration time.Duration, err error) {
	if m == nil {
		return
	}

	sample := Sample{
		Name:      name,
		Kind:      kind,
		Operation: "ping",
		Policy:    policy,
		Duration:  duration,
		Success:   err == nil,
		Timestamp: time.Now(),
	}
	if err != nil {
		sample.Error = err.Error()
	}

	m.Record(sample)
}
