package querytrace

import (
	"fmt"
	"strings"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

const (
	CategoryQuery     = "query"
	CategoryQueryPerf = "query-perf"
	CategoryDebug     = "debug-query"
)

type Tracer struct {
	enabled   bool
	startTime time.Time
	endpoint  string
	subject   string
	steps     []Step
	metadata  map[string]any
}

type Step struct {
	component string
	action    string
	duration  time.Duration
	count     int
	details   string
}

type StepTimer struct {
	tracer    *Tracer
	component string
	action    string
	startTime time.Time
	count     int
	details   string
}

// TraceOutput is a JSON-serializable representation of trace data
type TraceOutput struct {
	TotalMs   float64           `json:"total_ms"`
	IndexUsed string            `json:"index_used,omitempty"`
	Steps     []TraceStepOutput `json:"steps"`
}

type TraceStepOutput struct {
	Component  string  `json:"component"`
	Action     string  `json:"action"`
	DurationMs float64 `json:"duration_ms"`
	Count      int     `json:"count,omitempty"`
	Details    string  `json:"details,omitempty"`
}

func New(endpoint, subject string) *Tracer {
	enabled := logger.IsCategoryEnabled(CategoryDebug) ||
		logger.IsCategoryEnabled(CategoryQuery) ||
		logger.IsCategoryEnabled(CategoryQueryPerf)
	if !enabled {
		return &Tracer{enabled: false}
	}
	return &Tracer{
		enabled:   true,
		startTime: time.Now(),
		endpoint:  endpoint,
		subject:   subject,
		steps:     make([]Step, 0, 8),
		metadata:  make(map[string]any),
	}
}

// IsClientTraceAllowed returns true if the operator has enabled query tracing,
// which allows clients to request trace output via ?trace=true
func IsClientTraceAllowed() bool {
	return logger.IsCategoryEnabled(CategoryDebug) ||
		logger.IsCategoryEnabled(CategoryQuery) ||
		logger.IsCategoryEnabled(CategoryQueryPerf)
}

func (t *Tracer) Enabled() bool {
	return t.enabled
}

func (t *Tracer) Step(component, action string) *StepTimer {
	if !t.enabled {
		return &StepTimer{tracer: nil}
	}
	return &StepTimer{
		tracer:    t,
		component: component,
		action:    action,
		startTime: time.Now(),
	}
}

func (st *StepTimer) WithCount(count int) *StepTimer {
	if st.tracer == nil {
		return st
	}
	st.count = count
	return st
}

func (st *StepTimer) WithDetails(details string) *StepTimer {
	if st.tracer == nil {
		return st
	}
	st.details = details
	return st
}

func (st *StepTimer) End() {
	if st.tracer == nil {
		return
	}
	st.tracer.steps = append(st.tracer.steps, Step{
		component: st.component,
		action:    st.action,
		duration:  time.Since(st.startTime),
		count:     st.count,
		details:   st.details,
	})
}

func (t *Tracer) AddStep(component, action string, duration time.Duration, details string) {
	if !t.enabled {
		return
	}
	t.steps = append(t.steps, Step{
		component: component,
		action:    action,
		duration:  duration,
		details:   details,
	})
}

func (t *Tracer) AddStepWithCount(component, action string, duration time.Duration, count int, details string) {
	if !t.enabled {
		return
	}
	t.steps = append(t.steps, Step{
		component: component,
		action:    action,
		duration:  duration,
		count:     count,
		details:   details,
	})
}

func (t *Tracer) SetMetadata(key string, value any) {
	if !t.enabled {
		return
	}
	t.metadata[key] = value
}

func (t *Tracer) SetResult(count int, indexUsed string) {
	if !t.enabled {
		return
	}
	t.metadata["results"] = count
	if indexUsed != "" {
		t.metadata["index_used"] = indexUsed
	}
}

func (t *Tracer) Log() {
	if !t.enabled {
		return
	}

	total := time.Since(t.startTime)
	var sb strings.Builder

	results, hasResults := t.metadata["results"]
	if hasResults {
		sb.WriteString(fmt.Sprintf("query_path endpoint=%s subject=%s total=%v results=%v\n",
			t.endpoint, t.subject, total, results))
	} else {
		sb.WriteString(fmt.Sprintf("query_path endpoint=%s subject=%s total=%v\n",
			t.endpoint, t.subject, total))
	}

	for i, step := range t.steps {
		prefix := "├─"
		if i == len(t.steps)-1 {
			prefix = "└─"
		}
		sb.WriteString(fmt.Sprintf("  %s [%s] %s: %v",
			prefix, step.component, step.action, step.duration))
		if step.count > 0 {
			sb.WriteString(fmt.Sprintf(" count=%d", step.count))
		}
		if step.details != "" {
			sb.WriteString(fmt.Sprintf(" (%s)", step.details))
		}
		sb.WriteString("\n")
	}

	indexUsed, hasIndex := t.metadata["index_used"]
	if hasIndex {
		sb.WriteString(fmt.Sprintf("  index_used=%v", indexUsed))
	}

	logger.Printf(CategoryDebug, "%s", sb.String())
}

// ToJSON returns a JSON-serializable representation of the trace data
func (t *Tracer) ToJSON() *TraceOutput {
	if t == nil || !t.enabled {
		return nil
	}

	output := &TraceOutput{
		TotalMs: float64(time.Since(t.startTime).Microseconds()) / 1000.0,
		Steps:   make([]TraceStepOutput, 0, len(t.steps)),
	}

	if indexUsed, ok := t.metadata["index_used"].(string); ok {
		output.IndexUsed = indexUsed
	}

	for _, step := range t.steps {
		stepOutput := TraceStepOutput{
			Component:  step.component,
			Action:     step.action,
			DurationMs: float64(step.duration.Microseconds()) / 1000.0,
		}
		if step.count > 0 {
			stepOutput.Count = step.count
		}
		if step.details != "" {
			stepOutput.Details = step.details
		}
		output.Steps = append(output.Steps, stepOutput)
	}

	return output
}
