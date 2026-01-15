package enforce

import (
	"errors"
	"testing"
)

// =============================================================================
// ENFORCE with Bool Tests
// =============================================================================

func TestENFORCEBoolTrue(t *testing.T) {
	// Should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("ENFORCE(true) panicked unexpectedly: %v", r)
		}
	}()

	ENFORCE(true, "this should not panic")
}

func TestENFORCEBoolFalse(t *testing.T) {
	// Should panic
	defer func() {
		r := recover()
		if r == nil {
			t.Error("ENFORCE(false) did not panic")
			return
		}

		// Check that it panics with 0
		if r != 0 {
			t.Errorf("ENFORCE(false) panicked with %v; want 0", r)
		}
	}()

	ENFORCE(false, "this should panic")
}

func TestENFORCEBoolFalseWithArgs(t *testing.T) {
	// Should panic with arguments logged
	defer func() {
		r := recover()
		if r == nil {
			t.Error("ENFORCE(false, args...) did not panic")
			return
		}

		if r != 0 {
			t.Errorf("ENFORCE(false, args...) panicked with %v; want 0", r)
		}
	}()

	ENFORCE(false, "test message", 123, "more args")
}

// =============================================================================
// ENFORCE with Error Tests
// =============================================================================

func TestENFORCEErrorNil(t *testing.T) {
	// Should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("ENFORCE(nil error) panicked unexpectedly: %v", r)
		}
	}()

	var err error = nil
	ENFORCE(err, "this should not panic")
}

func TestENFORCEErrorNonNil(t *testing.T) {
	// Should panic
	testErr := errors.New("test error")

	defer func() {
		r := recover()
		if r == nil {
			t.Error("ENFORCE(error) did not panic")
			return
		}

		// Should panic with the error itself
		if r != testErr {
			t.Errorf("ENFORCE(error) panicked with %v; want %v", r, testErr)
		}
	}()

	ENFORCE(testErr, "this should panic")
}

func TestENFORCEErrorNonNilWithArgs(t *testing.T) {
	// Should panic with arguments logged
	testErr := errors.New("test error")

	defer func() {
		r := recover()
		if r == nil {
			t.Error("ENFORCE(error, args...) did not panic")
			return
		}

		if r != testErr {
			t.Errorf("ENFORCE(error, args...) panicked with %v; want %v", r, testErr)
		}
	}()

	ENFORCE(testErr, "error occurred", "additional context")
}

// =============================================================================
// ENFORCE with Other Types (Should Not Panic)
// =============================================================================

func TestENFORCEWithString(t *testing.T) {
	// String is neither bool nor error, should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("ENFORCE(string) panicked unexpectedly: %v", r)
		}
	}()

	ENFORCE("some string", "should not panic")
}

func TestENFORCEWithInt(t *testing.T) {
	// Int is neither bool nor error, should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("ENFORCE(int) panicked unexpectedly: %v", r)
		}
	}()

	ENFORCE(42, "should not panic")
}

func TestENFORCEWithNil(t *testing.T) {
	// Nil interface (not explicitly error type) should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("ENFORCE(nil interface) panicked unexpectedly: %v", r)
		}
	}()

	var nilInterface interface{} = nil
	ENFORCE(nilInterface, "should not panic")
}

// =============================================================================
// CheckCompiler Tests
// =============================================================================

func TestCheckCompiler(t *testing.T) {
	// Should not panic on 64-bit system
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("CheckCompiler() panicked: %v", r)
			t.Error("This test is running on a 32-bit system")
		}
	}()

	CheckCompiler()
}

func TestCheckCompilerCalledInInit(t *testing.T) {
	// CheckCompiler is called in init(), so if we got this far,
	// it must have succeeded
	t.Log("CheckCompiler() was called successfully in init()")
}

// =============================================================================
// Edge Case Tests
// =============================================================================

func TestENFORCEBoolWithNoArgs(t *testing.T) {
	// ENFORCE(false) with no additional arguments
	defer func() {
		r := recover()
		if r == nil {
			t.Error("ENFORCE(false) with no args did not panic")
			return
		}

		if r != 0 {
			t.Errorf("ENFORCE(false) panicked with %v; want 0", r)
		}
	}()

	ENFORCE(false)
}

func TestENFORCEErrorWithNoArgs(t *testing.T) {
	// ENFORCE(error) with no additional arguments
	testErr := errors.New("test error")

	defer func() {
		r := recover()
		if r == nil {
			t.Error("ENFORCE(error) with no args did not panic")
			return
		}

		if r != testErr {
			t.Errorf("ENFORCE(error) panicked with %v; want %v", r, testErr)
		}
	}()

	ENFORCE(testErr)
}

func TestENFORCEBoolTrueWithManyArgs(t *testing.T) {
	// Should not panic even with many arguments
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("ENFORCE(true) with many args panicked unexpectedly: %v", r)
		}
	}()

	ENFORCE(true, "arg1", 2, "arg3", 4.5, true, []int{1, 2, 3})
}

// =============================================================================
// Type Assertion Coverage
// =============================================================================

func TestENFORCETypeSwitchCoverage(t *testing.T) {
	tests := []struct {
		name        string
		query       interface{}
		shouldPanic bool
	}{
		{"true bool", true, false},
		{"false bool", false, true},
		{"nil error", error(nil), false},
		{"non-nil error", errors.New("test"), true},
		{"string", "test", false},
		{"int", 123, false},
		{"float", 3.14, false},
		{"slice", []int{1, 2, 3}, false},
		{"map", map[string]int{"a": 1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			panicked := false
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicked = true
					}
				}()

				ENFORCE(tt.query, "test")
			}()

			if panicked != tt.shouldPanic {
				t.Errorf("ENFORCE(%v) panicked=%v; want %v", tt.query, panicked, tt.shouldPanic)
			}
		})
	}
}

// =============================================================================
// Practical Usage Examples (Documentation Tests)
// =============================================================================

func ExampleENFORCE_bool() {
	// Example: Using ENFORCE with boolean condition
	value := 42

	// This will not panic because the condition is true
	ENFORCE(value == 42, "value should be 42")

	// This would panic (commented out to avoid test failure):
	// ENFORCE(value == 0, "value should not be zero")
}

func ExampleENFORCE_error() {
	// Example: Using ENFORCE with error checking
	var err error = nil

	// This will not panic because err is nil
	ENFORCE(err, "operation should not fail")

	// This would panic (commented out to avoid test failure):
	// err = errors.New("something went wrong")
	// ENFORCE(err, "this will panic")
}

func ExampleCheckCompiler() {
	// Example: CheckCompiler is called automatically in init()
	// but can be called manually if needed
	CheckCompiler()

	// If this doesn't panic, we're on a 64-bit system
}

// =============================================================================
// Benchmarks
// =============================================================================

func BenchmarkENFORCEBoolTrue(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ENFORCE(true, "benchmark")
	}
}

func BenchmarkENFORCEBoolTrueNoArgs(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ENFORCE(true)
	}
}

func BenchmarkENFORCEErrorNil(b *testing.B) {
	var err error = nil

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ENFORCE(err, "benchmark")
	}
}

func BenchmarkENFORCEErrorNilNoArgs(b *testing.B) {
	var err error = nil

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ENFORCE(err)
	}
}
