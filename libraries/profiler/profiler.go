package profiler

import (
	"bytes"
	"fmt"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"time"

	"github.com/google/pprof/profile"
	"github.com/greymass/roborovski/libraries/logger"
)

type Config struct {
	ServiceName string        // Service name for log output (e.g., "coreindex")
	Interval    time.Duration // Profiling interval (default: 60s)
	TopN        int           // Number of functions to show (default: 20)
}

var (
	ticker     *time.Ticker
	stopChan   chan struct{}
	mu         sync.Mutex
	isRunning  bool
	currentCfg Config
)

func Start(cfg Config) {
	mu.Lock()
	defer mu.Unlock()

	if isRunning {
		logger.Printf("profiler", "WARNING: Profiler already running, stopping previous instance")
		stopUnsafe()
	}

	if cfg.Interval == 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.TopN == 0 {
		cfg.TopN = 20
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "unknown"
	}

	currentCfg = cfg
	stopChan = make(chan struct{})
	ticker = time.NewTicker(cfg.Interval)

	logger.Printf("profiler", "Starting periodic CPU profiling: every %v (profile duration: %v)", cfg.Interval, cfg.Interval)

	go func() {
		printCPUProfile(cfg.ServiceName, cfg.Interval, cfg.TopN)

		for {
			select {
			case <-ticker.C:
				printCPUProfile(cfg.ServiceName, cfg.Interval, cfg.TopN)
			case <-stopChan:
				logger.Printf("profiler", "Profiler stopped due to shutdown signal")
				return
			}
		}
	}()

	isRunning = true
}

func Stop() {
	mu.Lock()
	defer mu.Unlock()
	stopUnsafe()
}

func stopUnsafe() {
	if !isRunning {
		return
	}

	if ticker != nil {
		ticker.Stop()
	}
	if stopChan != nil {
		close(stopChan)
	}

	isRunning = false
	logger.Printf("profiler", "Stopped periodic CPU profiling")
}

func EnableBlockProfiling() {
	runtime.SetBlockProfileRate(1) // 1 = record every blocking event
}

func printCPUProfile(serviceName string, duration time.Duration, topN int) {
	startTime := time.Now()
	var buf bytes.Buffer

	if err := pprof.StartCPUProfile(&buf); err != nil {
		logger.Printf("profiler", "PROFILE ERROR: Could not start CPU profile: %v", err)
		return
	}

	profileTimer := time.NewTimer(duration)
	checkTicker := time.NewTicker(100 * time.Millisecond)
	defer checkTicker.Stop()
	defer profileTimer.Stop()

	select {
	case <-profileTimer.C:
	case <-stopChan:
		logger.Printf("profiler", "Profiling interrupted by shutdown (captured %.1fs of %.1fs)",
			time.Since(startTime).Seconds(), duration.Seconds())
	case <-checkTicker.C:
		select {
		case <-stopChan:
			logger.Printf("profiler", "Profiling interrupted by shutdown (captured %.1fs of %.1fs)",
				time.Since(startTime).Seconds(), duration.Seconds())
		case <-profileTimer.C:
		}
	}

	pprof.StopCPUProfile()

	goroutineCount := runtime.NumGoroutine()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	if buf.Len() == 0 {
		logger.Printf("profiler", "PROFILE: No CPU samples captured (duration may be too short or system idle)")
		return
	}

	profileData, err := parseProfile(&buf)
	if err != nil {
		logger.Printf("profiler", "PROFILE ERROR: Could not parse profile: %v", err)
		return
	}

	logger.Printf("profiler", "======================================================================")
	logger.Printf("profiler", "File: %s", serviceName)
	logger.Printf("profiler", "Type: cpu")
	logger.Printf("profiler", "Time: %s", startTime.Format("2006-01-02 15:04:05 MST"))
	logger.Printf("profiler", "Duration: %.1fs, Total samples = %.2fs (%5.2f%%)",
		duration.Seconds(),
		profileData.totalDuration,
		(profileData.totalDuration/duration.Seconds())*100)
	logger.Printf("profiler", "Showing nodes accounting for %.2fs, %.0f%% of %.2fs total",
		profileData.totalDuration, 100.0, profileData.totalDuration)
	logger.Printf("profiler", "Goroutines: %d | Heap: %d MB | Sys: %d MB | NumGC: %d",
		goroutineCount, m.Alloc/1024/1024, m.HeapSys/1024/1024, m.NumGC)
	logger.Printf("profiler", "      flat  flat%%   sum%%")

	cumSum := int64(0)
	for i := 0; i < topN && i < len(profileData.functions); i++ {
		fn := profileData.functions[i]
		cumSum += fn.flat
		sumPct := float64(cumSum) / float64(profileData.totalSamples) * 100

		logger.Printf("profiler", "%10s %5.2f%% %5.2f%%  %s",
			formatDuration(fn.flat, profileData.sampleRate),
			fn.flatPct,
			sumPct,
			fn.name)
	}
	logger.Printf("profiler", "======================================================================")
}

type profileData struct {
	totalSamples  int64
	totalDuration float64 // in seconds
	sampleRate    int64   // nanoseconds per sample
	functions     []funcProfile
}

type funcProfile struct {
	name    string
	flat    int64   // self samples
	flatPct float64 // self percentage
}

func parseProfile(r *bytes.Buffer) (*profileData, error) {
	prof, err := profile.Parse(r)
	if err != nil {
		return nil, err
	}

	sampleRate := int64(1000000) // default: 1ms per sample (10^6 ns)
	if len(prof.SampleType) > 0 && prof.SampleType[0].Unit == "nanoseconds" {
		if prof.Period > 0 {
			sampleRate = prof.Period
		}
	}

	funcStats := make(map[string]*funcProfile)
	totalSamples := int64(0)

	for _, sample := range prof.Sample {
		if len(sample.Value) == 0 {
			continue
		}

		flat := sample.Value[0] // CPU samples (self)
		totalSamples += flat

		if len(sample.Location) > 0 && len(sample.Location[0].Line) > 0 {
			line := sample.Location[0].Line[0]
			if line.Function != nil {
				name := line.Function.Name
				if stat, exists := funcStats[name]; exists {
					stat.flat += flat
				} else {
					funcStats[name] = &funcProfile{
						name: name,
						flat: flat,
					}
				}
			}
		}
	}

	for _, stat := range funcStats {
		if totalSamples > 0 {
			stat.flatPct = float64(stat.flat) / float64(totalSamples) * 100
		}
	}

	functions := make([]funcProfile, 0, len(funcStats))
	for _, stat := range funcStats {
		functions = append(functions, *stat)
	}

	sort.Slice(functions, func(i, j int) bool {
		return functions[i].flat > functions[j].flat
	})

	totalDuration := float64(totalSamples*sampleRate) / 1e9 // convert to seconds

	return &profileData{
		totalSamples:  totalSamples,
		totalDuration: totalDuration,
		sampleRate:    sampleRate,
		functions:     functions,
	}, nil
}

func formatDuration(samples int64, sampleRate int64) string {
	nanos := samples * sampleRate
	seconds := float64(nanos) / 1e9
	if seconds >= 1.0 {
		return fmt.Sprintf("%.2fs", seconds)
	} else if seconds >= 0.001 {
		return fmt.Sprintf("%.0fms", seconds*1000)
	} else if seconds >= 0.000001 {
		return fmt.Sprintf("%.0fµs", seconds*1e6)
	} else {
		return fmt.Sprintf("%.0fns", seconds*1e9)
	}
}
