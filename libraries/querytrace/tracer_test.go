package querytrace

import (
	"strings"
	"testing"
	"time"

	"github.com/greymass/roborovski/libraries/logger"
)

func newWithCategory(endpoint, subject, category string) *Tracer {
	if !logger.IsCategoryEnabled(category) {
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

func TestNew_Disabled(t *testing.T) {
	logger.SetCategoryFilter([]string{"unrelated-category"})

	tracer := New("test-endpoint", "test-subject")
	if tracer.Enabled() {
		t.Error("Tracer should be disabled when query categories not enabled")
	}
}

func TestNew_EnabledDebugQuery(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("test-endpoint", "test-subject")
	if !tracer.Enabled() {
		t.Error("Tracer should be enabled when debug-query category enabled")
	}
}

func TestNew_EnabledQuery(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryQuery})

	tracer := New("test-endpoint", "test-subject")
	if !tracer.Enabled() {
		t.Error("Tracer should be enabled when query category enabled")
	}
}

func TestNew_EnabledQueryPerf(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryQueryPerf})

	tracer := New("test-endpoint", "test-subject")
	if !tracer.Enabled() {
		t.Error("Tracer should be enabled when query-perf category enabled")
	}
}

func TestNewWithCategory_Enabled(t *testing.T) {
	logger.SetCategoryFilter([]string{"custom-category"})

	tracer := newWithCategory("endpoint", "subject", "custom-category")
	if !tracer.Enabled() {
		t.Error("Tracer should be enabled for custom category")
	}
}

func TestNewWithCategory_Disabled(t *testing.T) {
	logger.SetCategoryFilter([]string{"other-category"})

	tracer := newWithCategory("endpoint", "subject", "custom-category")
	if tracer.Enabled() {
		t.Error("Tracer should be disabled when custom category not enabled")
	}
}

func TestStep_Disabled(t *testing.T) {
	logger.SetCategoryFilter([]string{"unrelated-category"})

	tracer := New("endpoint", "subject")
	step := tracer.Step("component", "action")

	if step.tracer != nil {
		t.Error("Step should have nil tracer when disabled")
	}

	step.WithCount(10).WithDetails("details").End()
}

func TestStep_Enabled(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("endpoint", "subject")
	step := tracer.Step("component", "action")

	if step.tracer == nil {
		t.Error("Step should have non-nil tracer when enabled")
	}

	step.WithCount(10).WithDetails("test details").End()

	if len(tracer.steps) != 1 {
		t.Fatalf("Expected 1 step, got %d", len(tracer.steps))
	}

	s := tracer.steps[0]
	if s.component != "component" {
		t.Errorf("component = %q, want %q", s.component, "component")
	}
	if s.action != "action" {
		t.Errorf("action = %q, want %q", s.action, "action")
	}
	if s.count != 10 {
		t.Errorf("count = %d, want %d", s.count, 10)
	}
	if s.details != "test details" {
		t.Errorf("details = %q, want %q", s.details, "test details")
	}
	if s.duration <= 0 {
		t.Error("duration should be positive")
	}
}

func TestAddStep_Disabled(t *testing.T) {
	logger.SetCategoryFilter([]string{"unrelated-category"})

	tracer := New("endpoint", "subject")
	tracer.AddStep("component", "action", time.Millisecond, "details")

	if len(tracer.steps) != 0 {
		t.Error("AddStep should be no-op when disabled")
	}
}

func TestAddStep_Enabled(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("endpoint", "subject")
	tracer.AddStep("component", "action", 5*time.Millisecond, "details")

	if len(tracer.steps) != 1 {
		t.Fatalf("Expected 1 step, got %d", len(tracer.steps))
	}

	s := tracer.steps[0]
	if s.duration != 5*time.Millisecond {
		t.Errorf("duration = %v, want %v", s.duration, 5*time.Millisecond)
	}
}

func TestAddStepWithCount_Disabled(t *testing.T) {
	logger.SetCategoryFilter([]string{"unrelated-category"})

	tracer := New("endpoint", "subject")
	tracer.AddStepWithCount("component", "action", time.Millisecond, 100, "details")

	if len(tracer.steps) != 0 {
		t.Error("AddStepWithCount should be no-op when disabled")
	}
}

func TestAddStepWithCount_Enabled(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("endpoint", "subject")
	tracer.AddStepWithCount("component", "action", 5*time.Millisecond, 100, "details")

	if len(tracer.steps) != 1 {
		t.Fatalf("Expected 1 step, got %d", len(tracer.steps))
	}

	s := tracer.steps[0]
	if s.count != 100 {
		t.Errorf("count = %d, want %d", s.count, 100)
	}
}

func TestSetMetadata_Disabled(t *testing.T) {
	logger.SetCategoryFilter([]string{"unrelated-category"})

	tracer := New("endpoint", "subject")
	tracer.SetMetadata("key", "value")

	if len(tracer.metadata) != 0 {
		t.Error("SetMetadata should be no-op when disabled")
	}
}

func TestSetMetadata_Enabled(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("endpoint", "subject")
	tracer.SetMetadata("key", "value")
	tracer.SetMetadata("count", 42)

	if tracer.metadata["key"] != "value" {
		t.Errorf("metadata[key] = %v, want %v", tracer.metadata["key"], "value")
	}
	if tracer.metadata["count"] != 42 {
		t.Errorf("metadata[count] = %v, want %v", tracer.metadata["count"], 42)
	}
}

func TestSetResult_Disabled(t *testing.T) {
	logger.SetCategoryFilter([]string{"unrelated-category"})

	tracer := New("endpoint", "subject")
	tracer.SetResult(10, "idx1")

	if len(tracer.metadata) != 0 {
		t.Error("SetResult should be no-op when disabled")
	}
}

func TestSetResult_Enabled(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("endpoint", "subject")
	tracer.SetResult(10, "idx1")

	if tracer.metadata["results"] != 10 {
		t.Errorf("metadata[results] = %v, want %v", tracer.metadata["results"], 10)
	}
	if tracer.metadata["index_used"] != "idx1" {
		t.Errorf("metadata[index_used] = %v, want %v", tracer.metadata["index_used"], "idx1")
	}
}

func TestSetResult_EmptyIndex(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("endpoint", "subject")
	tracer.SetResult(10, "")

	if tracer.metadata["results"] != 10 {
		t.Errorf("metadata[results] = %v, want %v", tracer.metadata["results"], 10)
	}
	if _, exists := tracer.metadata["index_used"]; exists {
		t.Error("index_used should not be set for empty string")
	}
}

func TestLog_Disabled(t *testing.T) {
	logger.SetCategoryFilter([]string{"unrelated-category"})

	tracer := New("endpoint", "subject")
	tracer.Log()
}

func TestLog_Enabled(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("get_actions", "eosio")
	tracer.Step("index", "scan").WithCount(100).WithDetails("idx1").End()
	tracer.Step("storage", "fetch").WithCount(10).End()
	tracer.SetResult(10, "idx_account")
	tracer.Log()
}

func TestLog_NoResults(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("health", "check")
	tracer.Step("db", "ping").End()
	tracer.Log()
}

func TestLog_NoSteps(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("simple", "query")
	tracer.SetResult(1, "")
	tracer.Log()
}

func TestStepTimer_ChainedCalls(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("endpoint", "subject")
	tracer.Step("c", "a").WithCount(5).WithDetails("d").End()

	s := tracer.steps[0]
	if s.count != 5 || s.details != "d" {
		t.Errorf("Chained calls failed: count=%d details=%q", s.count, s.details)
	}
}

func TestStepTimer_DisabledChainedCalls(t *testing.T) {
	logger.SetCategoryFilter([]string{})

	tracer := New("endpoint", "subject")
	step := tracer.Step("c", "a")
	result := step.WithCount(5).WithDetails("d")

	if result != step {
		t.Error("Chained calls should return same step when disabled")
	}
}

func TestMultipleSteps(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("endpoint", "subject")
	tracer.Step("step1", "action1").End()
	tracer.Step("step2", "action2").End()
	tracer.Step("step3", "action3").End()

	if len(tracer.steps) != 3 {
		t.Errorf("Expected 3 steps, got %d", len(tracer.steps))
	}
}

func TestCategories(t *testing.T) {
	if CategoryQuery != "query" {
		t.Errorf("CategoryQuery = %q, want %q", CategoryQuery, "query")
	}
	if CategoryQueryPerf != "query-perf" {
		t.Errorf("CategoryQueryPerf = %q, want %q", CategoryQueryPerf, "query-perf")
	}
	if CategoryDebug != "debug-query" {
		t.Errorf("CategoryDebug = %q, want %q", CategoryDebug, "debug-query")
	}
}

func TestStepDuration(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("endpoint", "subject")
	step := tracer.Step("component", "action")
	time.Sleep(10 * time.Millisecond)
	step.End()

	if tracer.steps[0].duration < 10*time.Millisecond {
		t.Errorf("duration = %v, expected >= 10ms", tracer.steps[0].duration)
	}
}

func BenchmarkNew_Disabled(b *testing.B) {
	logger.SetCategoryFilter([]string{"unrelated-category"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = New("endpoint", "subject")
	}
}

func BenchmarkNew_Enabled(b *testing.B) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = New("endpoint", "subject")
	}
}

func BenchmarkStep_Disabled(b *testing.B) {
	logger.SetCategoryFilter([]string{"unrelated-category"})
	tracer := New("endpoint", "subject")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracer.Step("component", "action").End()
	}
}

func BenchmarkStep_Enabled(b *testing.B) {
	logger.SetCategoryFilter([]string{CategoryDebug})
	tracer := New("endpoint", "subject")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracer.Step("component", "action").End()
	}
}

func BenchmarkFullTrace_Disabled(b *testing.B) {
	logger.SetCategoryFilter([]string{"unrelated-category"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracer := New("get_actions", "eosio")
		tracer.Step("index", "scan").WithCount(100).End()
		tracer.Step("storage", "fetch").WithCount(10).End()
		tracer.SetResult(10, "idx1")
		tracer.Log()
	}
}

func BenchmarkFullTrace_Enabled(b *testing.B) {
	logger.SetCategoryFilter([]string{CategoryDebug})
	var sb strings.Builder

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracer := New("get_actions", "eosio")
		tracer.Step("index", "scan").WithCount(100).End()
		tracer.Step("storage", "fetch").WithCount(10).End()
		tracer.SetResult(10, "idx1")
		sb.Reset()
	}
}

func TestIsClientTraceAllowed_Disabled(t *testing.T) {
	logger.SetCategoryFilter([]string{"unrelated-category"})

	if IsClientTraceAllowed() {
		t.Error("IsClientTraceAllowed should return false when no query categories enabled")
	}
}

func TestIsClientTraceAllowed_EnabledDebug(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	if !IsClientTraceAllowed() {
		t.Error("IsClientTraceAllowed should return true when debug-query enabled")
	}
}

func TestIsClientTraceAllowed_EnabledQuery(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryQuery})

	if !IsClientTraceAllowed() {
		t.Error("IsClientTraceAllowed should return true when query enabled")
	}
}

func TestIsClientTraceAllowed_EnabledQueryPerf(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryQueryPerf})

	if !IsClientTraceAllowed() {
		t.Error("IsClientTraceAllowed should return true when query-perf enabled")
	}
}

func TestToJSON_NilTracer(t *testing.T) {
	var tracer *Tracer = nil
	result := tracer.ToJSON()
	if result != nil {
		t.Error("ToJSON should return nil for nil tracer")
	}
}

func TestToJSON_DisabledTracer(t *testing.T) {
	logger.SetCategoryFilter([]string{"unrelated-category"})

	tracer := New("endpoint", "subject")
	result := tracer.ToJSON()
	if result != nil {
		t.Error("ToJSON should return nil for disabled tracer")
	}
}

func TestToJSON_EnabledTracer(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("get_actions", "eosio")
	tracer.Step("index", "scan").WithCount(100).WithDetails("scanning accounts").End()
	tracer.Step("storage", "fetch").WithCount(10).End()
	tracer.SetResult(10, "idx_account")

	result := tracer.ToJSON()
	if result == nil {
		t.Fatal("ToJSON should return non-nil for enabled tracer")
	}

	if result.TotalMs <= 0 {
		t.Error("TotalMs should be positive")
	}

	if result.IndexUsed != "idx_account" {
		t.Errorf("IndexUsed = %q, want %q", result.IndexUsed, "idx_account")
	}

	if len(result.Steps) != 2 {
		t.Fatalf("Expected 2 steps, got %d", len(result.Steps))
	}

	step1 := result.Steps[0]
	if step1.Component != "index" {
		t.Errorf("step1.Component = %q, want %q", step1.Component, "index")
	}
	if step1.Action != "scan" {
		t.Errorf("step1.Action = %q, want %q", step1.Action, "scan")
	}
	if step1.Count != 100 {
		t.Errorf("step1.Count = %d, want %d", step1.Count, 100)
	}
	if step1.Details != "scanning accounts" {
		t.Errorf("step1.Details = %q, want %q", step1.Details, "scanning accounts")
	}
	if step1.DurationMs < 0 {
		t.Error("step1.DurationMs should be non-negative")
	}

	step2 := result.Steps[1]
	if step2.Component != "storage" {
		t.Errorf("step2.Component = %q, want %q", step2.Component, "storage")
	}
	if step2.Count != 10 {
		t.Errorf("step2.Count = %d, want %d", step2.Count, 10)
	}
	if step2.Details != "" {
		t.Errorf("step2.Details = %q, want empty", step2.Details)
	}
}

func TestToJSON_NoIndexUsed(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("health", "check")
	tracer.Step("db", "ping").End()

	result := tracer.ToJSON()
	if result == nil {
		t.Fatal("ToJSON should return non-nil for enabled tracer")
	}

	if result.IndexUsed != "" {
		t.Errorf("IndexUsed = %q, want empty", result.IndexUsed)
	}
}

func TestToJSON_NoSteps(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("simple", "query")

	result := tracer.ToJSON()
	if result == nil {
		t.Fatal("ToJSON should return non-nil for enabled tracer")
	}

	if len(result.Steps) != 0 {
		t.Errorf("Expected 0 steps, got %d", len(result.Steps))
	}
}

func TestToJSON_ZeroCountAndEmptyDetails(t *testing.T) {
	logger.SetCategoryFilter([]string{CategoryDebug})

	tracer := New("endpoint", "subject")
	tracer.Step("component", "action").End()

	result := tracer.ToJSON()
	step := result.Steps[0]

	if step.Count != 0 {
		t.Errorf("Count = %d, want 0", step.Count)
	}
	if step.Details != "" {
		t.Errorf("Details = %q, want empty", step.Details)
	}
}

func BenchmarkToJSON(b *testing.B) {
	logger.SetCategoryFilter([]string{CategoryDebug})
	tracer := New("get_actions", "eosio")
	tracer.Step("index", "scan").WithCount(100).WithDetails("idx1").End()
	tracer.Step("storage", "fetch").WithCount(10).End()
	tracer.SetResult(10, "idx_account")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tracer.ToJSON()
	}
}
