package experiment

import "time"

type InfrastructureSample struct {
	Running bool
	Ready   bool
}

func InfrastructureRecovered(sample InfrastructureSample) bool {
	return sample.Running && sample.Ready
}

type ClusterSample struct {
	LeaderlessPartitions int
	UnderReplicated      int
	ISRSize              int
	ExpectedISRSize      int
}

func ClusterRecovered(sample ClusterSample) bool {
	return sample.LeaderlessPartitions == 0 && sample.UnderReplicated == 0 && sample.ISRSize == sample.ExpectedISRSize
}

type ApplicationSample struct {
	At               time.Time
	Acknowledgements uint64
	Consumed         uint64
}

func ApplicationsRecovered(samples []ApplicationSample, stableFor time.Duration) bool {
	if len(samples) < 2 || stableFor <= 0 || samples[len(samples)-1].At.Sub(samples[0].At) < stableFor {
		return false
	}
	for i := 1; i < len(samples); i++ {
		if samples[i].Acknowledgements <= samples[i-1].Acknowledgements || samples[i].Consumed <= samples[i-1].Consumed {
			return false
		}
	}
	return true
}

type PerformanceSample struct {
	At         time.Time
	Throughput float64
	LatencyP95 float64
	Lag        float64
}

type Baseline struct {
	MedianThroughput float64
	LatencyP95       float64
	LagP95           float64
}

func PerformanceRecovered(samples []PerformanceSample, baseline Baseline, window time.Duration) bool {
	if len(samples) == 0 || window != 60*time.Second || samples[len(samples)-1].At.Sub(samples[0].At) < window {
		return false
	}
	cutoff := samples[len(samples)-1].At.Add(-window)
	for _, sample := range samples {
		if sample.At.Before(cutoff) {
			continue
		}
		if sample.Throughput < .9*baseline.MedianThroughput || sample.LatencyP95 > 1.1*baseline.LatencyP95 || sample.Lag > baseline.LagP95 {
			return false
		}
	}
	return true
}
