package integrity

import (
	"sort"
	"sync"
)

type Result struct {
	Duplicate          bool
	SequenceRegression bool
	SequenceGap        bool
	OutOfOrder         bool
}

type streamKey struct {
	topic     string
	partition int32
}

type position struct {
	offset   int64
	sequence uint64
}

type Tracker struct {
	mu       sync.Mutex
	seen     map[string]struct{}
	last     map[streamKey]position
	consumed map[string]struct{}
}

func NewTracker() *Tracker {
	return &Tracker{seen: make(map[string]struct{}), last: make(map[streamKey]position), consumed: make(map[string]struct{})}
}

func (t *Tracker) Observe(topic string, partition int32, offset int64, id string, sequence uint64) Result {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, duplicate := t.seen[id]
	key := streamKey{topic: topic, partition: partition}
	var regression, gap bool
	if previous, ok := t.last[key]; ok && !duplicate && offset > previous.offset {
		regression = sequence <= previous.sequence
		gap = sequence > previous.sequence+1
	}
	t.seen[id] = struct{}{}
	t.consumed[id] = struct{}{}
	if !duplicate {
		previous, ok := t.last[key]
		if !ok || offset > previous.offset {
			t.last[key] = position{offset: offset, sequence: sequence}
		}
	}
	return Result{
		Duplicate: duplicate, SequenceRegression: regression, SequenceGap: gap,
		OutOfOrder: regression,
	}
}

func ConfirmedMissing(confirmed, consumed []string) []string {
	seen := make(map[string]struct{}, len(consumed))
	for _, id := range consumed {
		seen[id] = struct{}{}
	}
	var missing []string
	for _, id := range confirmed {
		if _, ok := seen[id]; !ok {
			missing = append(missing, id)
		}
	}
	sort.Strings(missing)
	return missing
}
