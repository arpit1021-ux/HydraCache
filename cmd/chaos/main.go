package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hydracache/hydracache/internal/chaostest"
)

func main() {
	composeDir := flag.String("compose-dir", "deploy", "Path to directory containing docker-compose.yml")
	scenarios := flag.String("scenarios", "all", "Comma-separated list of scenarios to run (kill-primary, rolling-restart, node-join, partition, concurrent, or all)")
	flag.Parse()

	h := chaostest.NewHarness(*composeDir)

	fmt.Println("HydraCache Chaos Test Harness")
	fmt.Println()

	// Always start fresh: tear down any existing cluster, clean volumes, rebuild.
	fmt.Println("[setup] Tearing down any existing cluster and cleaning volumes...")
	h.FreshStart()

	// Build the list of scenarios to run.
	var allScenarios []chaostest.Scenario
	allScenarios = append(allScenarios, chaostest.KillPrimary{})
	allScenarios = append(allScenarios, chaostest.RollingRestart{})
	allScenarios = append(allScenarios, chaostest.NodeJoinMigration{})
	allScenarios = append(allScenarios, chaostest.PartitionAndHeal{})
	allScenarios = append(allScenarios, chaostest.ConcurrentChaos{})

	toRun := selectScenarios(*scenarios, allScenarios)

	// Run scenarios.
	report := chaostest.NewReport(len(h.Nodes), false)
	for i, scenario := range toRun {
		fmt.Printf("[scenario %d/%d] %s\n", i+1, len(toRun), scenario.Name())
		start := time.Now()
		result := scenario.Run(h)
		result.Duration = time.Since(start)
		report.AddResult(result)
		fmt.Printf("[scenario %d/%d] %s — %s (%.1fs)\n\n", i+1, len(toRun), scenario.Name(), passFail(result.Passed), result.Duration.Seconds())

		// Restore cluster to full health between scenarios.
		if i < len(toRun)-1 {
			h.ForceCleanup()
		}
	}

	// Final cleanup.
	h.Cleanup()

	// Print report.
	report.Print()
	os.Exit(report.ExitCode())
}

func selectScenarios(flag string, all []chaostest.Scenario) []chaostest.Scenario {
	if flag == "all" {
		return all
	}

	names := map[string]string{
		"kill-primary":    "Kill Primary → Verify Promotion",
		"rolling-restart": "Rolling Restart",
		"node-join":       "Node Join → Triggers Real Migration",
		"partition":       "Partition and Heal",
		"concurrent":      "Concurrent Chaos",
	}

	var selected []chaostest.Scenario
	for _, want := range splitCSV(flag) {
		if target, ok := names[want]; ok {
			for _, s := range all {
				if s.Name() == target {
					selected = append(selected, s)
					break
				}
			}
		} else {
			fmt.Fprintf(os.Stderr, "WARNING: unknown scenario %q, skipping\n", want)
		}
	}
	if len(selected) == 0 {
		fmt.Fprintf(os.Stderr, "WARNING: no valid scenarios selected, running all\n")
		return all
	}
	return selected
}

func splitCSV(s string) []string {
	var parts []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

func passFail(passed bool) string {
	if passed {
		return "PASS"
	}
	return "FAIL"
}
