package message

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Event struct {
	RunID             string `json:"run_id"`
	MessageID         string `json:"message_id"`
	GlobalSequence    uint64 `json:"global_sequence"`
	Partition         int32  `json:"partition"`
	PartitionSequence uint64 `json:"partition_sequence"`
	ProducedAtUnixNS  int64  `json:"produced_at_unix_ns"`
	Payload           string `json:"payload"`
}

type Generator struct {
	runID      string
	partitions int32
	payload    string
	global     atomic.Uint64
	mu         sync.Mutex
	perPart    map[int32]uint64
	now        func() time.Time
}

func NewGenerator(runID string, partitions int32, payloadBytes int) (*Generator, error) {
	if runID == "" || partitions <= 0 || payloadBytes < 0 {
		return nil, fmt.Errorf("invalid generator parameters")
	}
	return &Generator{runID: runID, partitions: partitions, payload: string(make([]byte, payloadBytes)), perPart: make(map[int32]uint64), now: time.Now}, nil
}

func (g *Generator) Next() Event {
	global := g.global.Add(1)
	partition := int32((global - 1) % uint64(g.partitions))
	g.mu.Lock()
	g.perPart[partition]++
	partSequence := g.perPart[partition]
	g.mu.Unlock()
	return Event{
		RunID: g.runID, MessageID: fmt.Sprintf("%s-p%d-%08d", g.runID, partition, partSequence),
		GlobalSequence: global, Partition: partition, PartitionSequence: partSequence,
		ProducedAtUnixNS: g.now().UnixNano(), Payload: g.payload,
	}
}

func Encode(event Event) ([]byte, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("encode message: %w", err)
	}
	return data, nil
}

func Decode(data []byte) (Event, error) {
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return Event{}, fmt.Errorf("decode message: %w", err)
	}
	return event, nil
}

func NewRunID(prefix string) (string, error) {
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generate run id: %w", err)
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(suffix[:])), nil
}
