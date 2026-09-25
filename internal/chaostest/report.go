package chaostest

import (
	"fmt"
	"strings"
	"time"
)

type ScenarioResult struct {
	Name     string
	Duration time.Duration
	Passed   bool
	Steps    []Step
}

type Step struct {
	OK      bool
	Message string
}

func (sr *ScenarioResult) AddStep(ok bool, msg string) {
	sr.Steps = append(sr.Steps, Step{OK: ok, Message: msg})
}

func (r *Report) Print() {
	total := len(r.Results)
	passed := 0
	for _, s := range r.Results {
		if s.Passed {
			passed++
		}
	}

	fmt.Println()
	fmt.Println(strings.Repeat("=", 68))
	fmt.Printf("  HydraCache Chaos Test Harness\n")
	fmt.Println(strings.Repeat("=", 68))
	fmt.Printf("  Cluster: %d nodes (%s)\n", r.NodeCount, r.ClusterSource)
	fmt.Println(strings.Repeat("=", 68))
	fmt.Println()

	for i, s := range r.Results {
		status := "PASS"
		if !s.Passed {
			status = "FAIL"
		}
		fmt.Printf("[%d/%d] %-50s %s  (%.1fs)\n", i+1, total, s.Name, status, s.Duration.Seconds())
		for _, step := range s.Steps {
			if step.OK {
				fmt.Printf("  \u2713 %s\n", step.Message)
			} else {
				fmt.Printf("  \u2717 %s\n", step.Message)
			}
		}
		fmt.Println()
	}

	fmt.Println(strings.Repeat("-", 68))
	fmt.Printf("Results: %d passed, %d failed (%d total)\n", passed, total-passed, total)
	fmt.Printf("Duration: %s\n", r.TotalDuration.Round(time.Millisecond*10))
	if passed == total {
		fmt.Println("Exit code: 0")
	} else {
		fmt.Println("Exit code: 1")
	}
	fmt.Println(strings.Repeat("-", 68))
	fmt.Println()
}

type Report struct {
	NodeCount     int
	ClusterSource string
	Results       []ScenarioResult
	TotalDuration time.Duration
}

func NewReport(nodeCount int, reused bool) *Report {
	src := "started by harness"
	if reused {
		src = "reused existing"
	}
	return &Report{
		NodeCount:     nodeCount,
		ClusterSource: src,
	}
}

func (r *Report) AddResult(sr ScenarioResult) {
	r.Results = append(r.Results, sr)
	r.TotalDuration += sr.Duration
}

func (r *Report) ExitCode() int {
	for _, s := range r.Results {
		if !s.Passed {
			return 1
		}
	}
	return 0
}
