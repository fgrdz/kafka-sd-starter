package integrity

import (
	"fmt"
	"sync"
	"testing"
)

func TestInterleavedPartitionsAndTopics(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	observations := []struct {
		topic string
		part  int32
		off   int64
		seq   uint64
	}{{"a", 0, 0, 1}, {"a", 1, 0, 1}, {"b", 0, 0, 1}, {"a", 0, 1, 2}, {"a", 1, 1, 2}}
	for index, item := range observations {
		got := tracker.Observe(item.topic, item.part, item.off, fmt.Sprintf("m%d", index), item.seq)
		if got.OutOfOrder || got.SequenceGap {
			t.Fatalf("observation %d produced a false positive: %+v", index, got)
		}
	}
}

func TestIncreasingOffsetsWithinPartition(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	for index := range 3 {
		got := tracker.Observe("topic", 2, int64(index), fmt.Sprintf("p2-%d", index+1), uint64(index+1))
		if got.OutOfOrder || got.SequenceGap {
			t.Fatalf("unexpected result at offset %d: %+v", index, got)
		}
	}
}

func TestSequenceRegression(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	tracker.Observe("topic", 0, 10, "a", 10)
	got := tracker.Observe("topic", 0, 11, "b", 9)
	if !got.SequenceRegression || !got.OutOfOrder || got.SequenceGap {
		t.Fatalf("expected sequence regression: %+v", got)
	}
}

func TestDuplicate(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	tracker.Observe("topic", 0, 0, "a", 1)
	got := tracker.Observe("topic", 0, 1, "a", 1)
	if !got.Duplicate || got.OutOfOrder {
		t.Fatalf("unexpected duplicate result: %+v", got)
	}
}

func TestSequenceGap(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	tracker.Observe("topic", 0, 0, "a", 1)
	got := tracker.Observe("topic", 0, 1, "c", 3)
	if !got.SequenceGap || got.OutOfOrder {
		t.Fatalf("expected gap without regression: %+v", got)
	}
}

func TestConcurrentObserve(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	var wg sync.WaitGroup
	for partition := range int32(4) {
		for sequence := range uint64(100) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id := fmt.Sprintf("p%d-%d", partition, sequence)
				tracker.Observe("topic", partition, int64(sequence), id, sequence+1)
			}()
		}
	}
	wg.Wait()
	if len(tracker.seen) != 400 {
		t.Fatalf("got %d observations, want 400", len(tracker.seen))
	}
}

func TestConfirmedMissing(t *testing.T) {
	t.Parallel()
	got := ConfirmedMissing([]string{"c", "a", "b"}, []string{"b", "c"})
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("unexpected missing: %v", got)
	}
}
