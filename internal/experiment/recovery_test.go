package experiment

import (
	"testing"
	"time"
)

func TestRecoveryDetectors(t *testing.T) {
	t.Parallel()
	if !InfrastructureRecovered(InfrastructureSample{Running: true, Ready: true}) {
		t.Fatal("infrastructure should be recovered")
	}
	if !ClusterRecovered(ClusterSample{ISRSize: 3, ExpectedISRSize: 3}) {
		t.Fatal("cluster should be recovered")
	}
	now := time.Unix(100, 0)
	app := []ApplicationSample{{At: now, Acknowledgements: 1, Consumed: 1}, {At: now.Add(10 * time.Second), Acknowledgements: 2, Consumed: 2}}
	if !ApplicationsRecovered(app, 10*time.Second) {
		t.Fatal("applications should be recovered")
	}
	perf := []PerformanceSample{
		{At: now, Throughput: 90, LatencyP95: 1.1, Lag: 5},
		{At: now.Add(60 * time.Second), Throughput: 95, LatencyP95: 1, Lag: 4},
	}
	if !PerformanceRecovered(perf, Baseline{MedianThroughput: 100, LatencyP95: 1, LagP95: 5}, 60*time.Second) {
		t.Fatal("performance should be recovered")
	}
}

func TestPerformanceRequiresContinuousThresholds(t *testing.T) {
	t.Parallel()
	now := time.Unix(100, 0)
	samples := []PerformanceSample{{At: now, Throughput: 89}, {At: now.Add(60 * time.Second), Throughput: 100}}
	if PerformanceRecovered(samples, Baseline{MedianThroughput: 100, LatencyP95: 1, LagP95: 1}, 60*time.Second) {
		t.Fatal("threshold violation must fail recovery")
	}
}
