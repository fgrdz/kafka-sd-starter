package message

import (
	"strings"
	"testing"
)

func TestGeneratorSequencesByPartition(t *testing.T) {
	t.Parallel()
	g, err := NewGenerator("run", 2, 8)
	if err != nil {
		t.Fatal(err)
	}
	got := []Event{g.Next(), g.Next(), g.Next()}
	if got[0].Partition != 0 || got[1].Partition != 1 || got[2].PartitionSequence != 2 {
		t.Fatalf("unexpected sequence: %+v", got)
	}
	if len(got[0].Payload) != 8 || got[0].MessageID == got[2].MessageID {
		t.Fatal("payload or ID invariant failed")
	}
}

func TestGeneratorMessageIDEncodesPartitionAndSequence(t *testing.T) {
	t.Parallel()
	g, err := NewGenerator("smoke-20260727204312-3d82", 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	g.Next()
	g.Next()
	got := g.Next()
	if got.Partition != 2 || got.PartitionSequence != 1 ||
		got.MessageID != "smoke-20260727204312-3d82-p2-00000001" {
		t.Fatalf("unexpected partition identity: %+v", got)
	}
}

func TestNewRunID(t *testing.T) {
	t.Parallel()
	a, err := NewRunID("a")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewRunID("a")
	if a == b || !strings.HasPrefix(a, "a-") {
		t.Fatalf("invalid IDs %q %q", a, b)
	}
}
