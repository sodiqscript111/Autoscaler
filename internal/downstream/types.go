package downstream

import (
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

func ParsePolicy(value string, fallback Policy) Policy {
	switch value {
	case string(PolicyCritical):
		return PolicyCritical
	case string(PolicyProtective):
		return PolicyProtective
	case string(PolicyObserveOnly):
		return PolicyObserveOnly
	default:
		return fallback
	}
}

type Kind string

const (
	KindGeneric Kind = "generic"
	KindHTTP    Kind = "http"
	KindMongoDB Kind = "mongodb"
	KindRedis   Kind = "redis"
	KindSQL     Kind = "sql"
	KindWorker  Kind = "worker"
)

type Dependency struct {
	Name      string
	Kind      Kind
	Operation string
	Policy    Policy
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
	Kind      Kind
	Operation string
	Policy    Policy
	Duration  time.Duration
	Success   bool
	Error     string
	Timestamp time.Time
}

type Status struct {
	Name           string
	Kind           Kind
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

type dependencyWindow struct {
	samples      []Sample
	status       Status
	pendingState State
	pendingCount int
}
