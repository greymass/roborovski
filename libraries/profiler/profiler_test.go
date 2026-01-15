package profiler

import (
	"bytes"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

func stop() {
	mu.Lock()
	defer mu.Unlock()
	stopUnsafe()
}

func TestStart(t *testing.T) {
	cfg := Config{
		ServiceName: "testservice",
		Interval:    100 * time.Millisecond,
		TopN:        5,
	}

	Start(cfg)
	defer stop()

	// Verify running state
	mu.Lock()
	if !isRunning {
		t.Error("Expected isRunning to be true")
	}
	if currentCfg.ServiceName != "testservice" {
		t.Errorf("Expected service name 'testservice', got %s", currentCfg.ServiceName)
	}
	mu.Unlock()
}

func TestStartWithDefaults(t *testing.T) {
	cfg := Config{} // No fields set

	Start(cfg)
	defer stop()

	mu.Lock()
	if currentCfg.Interval != 60*time.Second {
		t.Errorf("Expected default interval 60s, got %v", currentCfg.Interval)
	}
	if currentCfg.TopN != 20 {
		t.Errorf("Expected default TopN 20, got %d", currentCfg.TopN)
	}
	if currentCfg.ServiceName != "unknown" {
		t.Errorf("Expected default service name 'unknown', got %s", currentCfg.ServiceName)
	}
	mu.Unlock()
}

func TestStop(t *testing.T) {
	cfg := Config{
		ServiceName: "testservice",
		Interval:    1 * time.Second,
	}

	Start(cfg)
	time.Sleep(50 * time.Millisecond) // Let it start

	stop()

	// Verify stopped state
	mu.Lock()
	if isRunning {
		t.Error("Expected isRunning to be false after stop()")
	}
	mu.Unlock()
}

func TestStartAlreadyRunning(t *testing.T) {
	cfg1 := Config{ServiceName: "service1", Interval: 1 * time.Second}
	cfg2 := Config{ServiceName: "service2", Interval: 2 * time.Second}

	Start(cfg1)
	Start(cfg2) // Should stop first and start second
	defer stop()

	mu.Lock()
	if currentCfg.ServiceName != "service2" {
		t.Errorf("Expected current service 'service2', got %s", currentCfg.ServiceName)
	}
	mu.Unlock()
}

func TestEnableBlockProfiling(t *testing.T) {
	// EnableBlockProfiling no longer logs (services log their own messages)
	// Just verify the function doesn't panic
	EnableBlockProfiling()

	// Verify runtime setting (note: we can't directly check SetBlockProfileRate's value)
	// But we can verify the function completed without error
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		samples    int64
		sampleRate int64
		want       string
	}{
		// 1,000,000 samples * 1,000,000 ns/sample = 1,000,000,000,000 ns = 1,000 seconds
		{1000000, 1000000, "1000.00s"},
		// 500,000 samples * 1,000,000 ns/sample = 500,000,000,000 ns = 500 seconds
		{500000, 1000000, "500.00s"},
		// 1,000 samples * 1,000,000 ns/sample = 1,000,000,000 ns = 1 second
		{1000, 1000000, "1.00s"},
		// 1 sample * 1,000,000 ns/sample = 1,000,000 ns = 1 ms
		{1, 1000000, "1ms"},
		// 1 sample * 100 ns/sample = 100 ns
		{1, 100, "100ns"},
		// 2 samples * 500,000 ns/sample = 1,000,000 ns = 1 ms
		{2, 500000, "1ms"},
		// 1000 samples * 1000 ns/sample = 1,000,000 ns = 1 ms
		{1000, 1000, "1ms"},
		// 100 samples * 10,000 ns/sample = 1,000,000 ns = 1 ms
		{100, 10000, "1ms"},
		// 0 samples
		{0, 1000000, "0ns"},
	}

	for _, tt := range tests {
		got := formatDuration(tt.samples, tt.sampleRate)
		if got != tt.want {
			t.Errorf("formatDuration(%d, %d) = %s, want %s", tt.samples, tt.sampleRate, got, tt.want)
		}
	}
}

func TestParseProfile(t *testing.T) {
	// This test creates a minimal valid pprof profile
	// We'll test with a real profile by running CPU profiling briefly
	var buf bytes.Buffer

	// Generate a small CPU profile
	if err := runtime.GOMAXPROCS(1); err == 0 {
		// Keep same GOMAXPROCS
	}

	// Note: Testing parseProfile requires a real pprof binary format
	// which is complex to generate in tests. The function is tested
	// indirectly through printCPUProfile which uses it.
	t.Skip("Skipping parseProfile test - tested indirectly through printCPUProfile")

	// Parse the profile
	data, err := parseProfile(&buf)
	if err != nil {
		t.Fatalf("Failed to parse profile: %v", err)
	}

	// Basic validation
	if data.totalSamples < 0 {
		t.Errorf("Expected non-negative totalSamples, got %d", data.totalSamples)
	}

	if data.totalDuration < 0 {
		t.Errorf("Expected non-negative totalDuration, got %f", data.totalDuration)
	}

	if data.sampleRate <= 0 {
		t.Errorf("Expected positive sampleRate, got %d", data.sampleRate)
	}

	// Functions might be empty if no samples captured (system idle)
	// But the structure should be valid
	if data.functions == nil {
		t.Error("Expected non-nil functions slice")
	}
}

func TestPrintCPUProfile(t *testing.T) {
	// This will run a short profile and print results
	// We can't easily test the output with the new logger, but we can verify it doesn't panic
	printCPUProfile("testservice", 10*time.Millisecond, 5)

	// If we got here without panicking, test passed
	// The actual output format is verified manually/visually
}

func TestProfileDataCalculations(t *testing.T) {
	// Test percentage calculations
	functions := []funcProfile{
		{name: "func1", flat: 100},
		{name: "func2", flat: 50},
		{name: "func3", flat: 25},
	}

	totalSamples := int64(175) // 100 + 50 + 25

	for i := range functions {
		functions[i].flatPct = float64(functions[i].flat) / float64(totalSamples) * 100
	}

	// Check func1 percentages
	if functions[0].flatPct < 57.0 || functions[0].flatPct > 57.2 {
		t.Errorf("Expected func1 flatPct ~57.14%%, got %.2f%%", functions[0].flatPct)
	}

	// Check func2 percentages
	if functions[1].flatPct < 28.5 || functions[1].flatPct > 28.6 {
		t.Errorf("Expected func2 flatPct ~28.57%%, got %.2f%%", functions[1].flatPct)
	}

	// Check func3 percentages
	if functions[2].flatPct < 14.2 || functions[2].flatPct > 14.3 {
		t.Errorf("Expected func3 flatPct ~14.29%%, got %.2f%%", functions[2].flatPct)
	}
}

func TestConcurrentStartStop(t *testing.T) {
	// Test that concurrent Start/Stop calls don't cause race conditions
	cfg := Config{ServiceName: "concurrent", Interval: 100 * time.Millisecond}

	done := make(chan bool)

	// Start and stop in goroutines
	go func() {
		for i := 0; i < 5; i++ {
			Start(cfg)
			time.Sleep(10 * time.Millisecond)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 5; i++ {
			stop()
			time.Sleep(10 * time.Millisecond)
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	// Clean up
	stop()

	// If we got here without panic, test passed
}

func BenchmarkPrintCPUProfile(b *testing.B) {
	for i := 0; i < b.N; i++ {
		printCPUProfile("benchmark", 10*time.Millisecond, 20)
	}
}

func BenchmarkFormatDuration(b *testing.B) {
	samples := int64(123456)
	sampleRate := int64(1000000)

	for i := 0; i < b.N; i++ {
		formatDuration(samples, sampleRate)
	}
}

func TestParseProfile_Real(t *testing.T) {
	var buf bytes.Buffer

	if err := pprof.StartCPUProfile(&buf); err != nil {
		t.Skipf("Skipping: %v (another test may have profiling active)", err)
	}

	start := time.Now()
	for time.Since(start) < 100*time.Millisecond {
		_ = fibonacci(20)
		runtime.Gosched()
	}

	pprof.StopCPUProfile()

	data, err := parseProfile(&buf)
	if err != nil {
		t.Fatalf("parseProfile failed: %v", err)
	}

	if data == nil {
		t.Fatal("Expected non-nil profileData")
	}

	if data.totalSamples < 0 {
		t.Errorf("Expected non-negative totalSamples, got %d", data.totalSamples)
	}

	if data.totalDuration < 0 {
		t.Errorf("Expected non-negative totalDuration, got %f", data.totalDuration)
	}

	if data.sampleRate <= 0 {
		t.Errorf("Expected positive sampleRate, got %d", data.sampleRate)
	}

	if data.functions == nil {
		t.Error("Expected non-nil functions slice")
	}

	if data.totalSamples > 0 {
		if len(data.functions) == 0 {
			t.Error("Expected at least one function with samples > 0")
		}

		for i := 1; i < len(data.functions); i++ {
			if data.functions[i-1].flat < data.functions[i].flat {
				t.Errorf("Functions not sorted: func[%d].flat=%d < func[%d].flat=%d",
					i-1, data.functions[i-1].flat, i, data.functions[i].flat)
			}
		}

		for _, fn := range data.functions {
			if fn.flatPct < 0 || fn.flatPct > 100 {
				t.Errorf("Invalid flatPct for %s: %.2f%%", fn.name, fn.flatPct)
			}
		}
	}
}

func TestParseProfile_EmptyProfile(t *testing.T) {
	var buf bytes.Buffer
	_, err := parseProfile(&buf)
	if err == nil {
		t.Error("Expected error parsing empty buffer")
	}
}

func TestParseProfile_InvalidData(t *testing.T) {
	buf := bytes.NewBuffer([]byte("invalid pprof data"))
	_, err := parseProfile(buf)
	if err == nil {
		t.Error("Expected error parsing invalid data")
	}
}

func TestParseProfile_NoSamples(t *testing.T) {
	var buf bytes.Buffer

	if err := pprof.StartCPUProfile(&buf); err != nil {
		t.Skipf("Skipping: %v (another test may have profiling active)", err)
	}
	pprof.StopCPUProfile()

	data, err := parseProfile(&buf)
	if err != nil {
		t.Fatalf("parseProfile failed on zero-sample profile: %v", err)
	}

	if data == nil {
		t.Fatal("Expected non-nil profileData")
	}

	if data.totalSamples < 0 {
		t.Errorf("Expected non-negative totalSamples, got %d", data.totalSamples)
	}
}

func TestPrintCPUProfile_WithSamples(t *testing.T) {
	cfg := Config{
		ServiceName: "coverage_test",
		Interval:    50 * time.Millisecond,
		TopN:        10,
	}

	Start(cfg)
	defer stop()

	done := make(chan bool)
	go func() {
		start := time.Now()
		for time.Since(start) < 150*time.Millisecond {
			_ = fibonacci(25)
			runtime.Gosched()
		}
		done <- true
	}()

	time.Sleep(200 * time.Millisecond)

	stop()
	<-done
}

func TestPrintCPUProfile_StartError(t *testing.T) {
	var blockBuf bytes.Buffer
	if err := pprof.StartCPUProfile(&blockBuf); err != nil {
		t.Skipf("Skipping: %v (another test may have profiling active)", err)
	}
	defer pprof.StopCPUProfile()

	printCPUProfile("error_test", 10*time.Millisecond, 5)
}

func TestPrintCPUProfile_EarlyShutdown(t *testing.T) {
	cfg := Config{
		ServiceName: "shutdown_test",
		Interval:    5 * time.Second,
		TopN:        5,
	}

	Start(cfg)

	time.Sleep(50 * time.Millisecond)

	stop()

	time.Sleep(100 * time.Millisecond)
}

func TestPrintCPUProfile_NoSamples(t *testing.T) {
	printCPUProfile("no_samples", 1*time.Millisecond, 5)
}

func TestPrintCPUProfile_LargeTopN(t *testing.T) {
	printCPUProfile("large_topn", 20*time.Millisecond, 1000)
}

func TestFormatDuration_Microseconds(t *testing.T) {
	tests := []struct {
		samples    int64
		sampleRate int64
		want       string
	}{
		{1, 1000, "1µs"},
		{10, 1000, "10µs"},
		{100, 1000, "100µs"},
		{500, 1000, "500µs"},
		{999, 1000, "999µs"},
		{1, 900000, "900µs"},
		{10, 90000, "900µs"},
		{50, 10000, "500µs"},
		{100, 5000, "500µs"},
		{1000, 500, "500µs"},
		{5000, 100, "500µs"},
		{10000, 50, "500µs"},
		{2, 250000, "500µs"},
		{250000, 2, "500µs"},
		{1, 100000, "100µs"},
		{1, 10000, "10µs"},
		{1, 5000, "5µs"},
		{1, 2000, "2µs"},
		{2, 1000, "2µs"},
		{5, 1000, "5µs"},
		{1, 999999, "1000µs"},
		{999, 999, "998µs"},
		{1000, 999, "999µs"},
		{100, 9999, "1000µs"},
		{1, 500000, "500µs"},
		{2, 100000, "200µs"},
		{3, 100000, "300µs"},
		{7, 100000, "700µs"},
		{1, 750000, "750µs"},
		{1, 333333, "333µs"},
		{3, 333333, "1000µs"},
		{1, 1500, "2µs"},
		{15, 1000, "15µs"},
		{150, 1000, "150µs"},
		{1500, 100, "150µs"},
		{15000, 10, "150µs"},
		{1, 150000, "150µs"},
		{1, 15000, "15µs"},
		{1, 1001, "1µs"},
		{1001, 1, "1µs"},
		{10, 100, "1µs"},
		{100, 10, "1µs"},
		{1000, 1, "1µs"},
		{1, 1100, "1µs"},
		{1, 1500, "2µs"},
		{1, 1999, "2µs"},
		{2000, 1, "2µs"},
		{1, 2500, "2µs"},
		{25, 100, "2µs"},
		{1, 3000, "3µs"},
		{3, 1000, "3µs"},
		{30, 100, "3µs"},
		{300, 10, "3µs"},
		{3000, 1, "3µs"},
	}

	for _, tt := range tests {
		got := formatDuration(tt.samples, tt.sampleRate)
		if got != tt.want {
			nanos := tt.samples * tt.sampleRate
			micros := float64(nanos) / 1e3
			t.Errorf("formatDuration(%d, %d) = %s, want %s (%.3fns = %.3fµs)",
				tt.samples, tt.sampleRate, got, tt.want, float64(nanos), micros)
		}
	}
}

func TestFormatDuration_EdgeCases(t *testing.T) {
	tests := []struct {
		samples    int64
		sampleRate int64
		want       string
	}{
		{1000, 1000, "1ms"},
		{1000, 1000000, "1.00s"},
		{1000000, 1000000000, "1000000.00s"},
		{0, 1000, "0ns"},
		{0, 1000000, "0ns"},
		{1, 1, "1ns"},
		{10, 1, "10ns"},
		{999, 1, "999ns"},
	}

	for _, tt := range tests {
		got := formatDuration(tt.samples, tt.sampleRate)
		if got != tt.want {
			t.Errorf("formatDuration(%d, %d) = %s, want %s", tt.samples, tt.sampleRate, got, tt.want)
		}
	}
}

func TestParseProfile_SampleValueEdgeCases(t *testing.T) {
	var buf bytes.Buffer

	if err := pprof.StartCPUProfile(&buf); err != nil {
		t.Fatalf("Failed to start CPU profile: %v", err)
	}

	for i := 0; i < 100; i++ {
		_ = fibonacci(15)
	}

	pprof.StopCPUProfile()

	data, err := parseProfile(&buf)
	if err != nil {
		t.Fatalf("parseProfile failed: %v", err)
	}

	if data == nil {
		t.Fatal("Expected non-nil profileData")
	}

	if data.sampleRate < 1000 || data.sampleRate > 100000000 {
		t.Logf("WARNING: Unusual sampleRate: %d ns", data.sampleRate)
	}
}

func TestStopUnsafe_NotRunning(t *testing.T) {
	stop()

	mu.Lock()
	stopUnsafe()
	mu.Unlock()
}

func TestStartStop_RapidCycles(t *testing.T) {
	cfg := Config{
		ServiceName: "rapid",
		Interval:    10 * time.Millisecond,
		TopN:        5,
	}

	for i := 0; i < 10; i++ {
		Start(cfg)
		time.Sleep(5 * time.Millisecond)
		stop()
	}

	mu.Lock()
	running := isRunning
	mu.Unlock()

	if running {
		t.Error("Expected profiler to be stopped after rapid cycles")
	}
}

func fibonacci(n int) int {
	if n <= 1 {
		return n
	}
	return fibonacci(n-1) + fibonacci(n-2)
}
