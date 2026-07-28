package output

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestJSONLWriter(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "timeline.jsonl")
	writer, err := NewJSONLWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(map[string]string{"event": "start"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	if !scanner.Scan() || scanner.Text() != `{"event":"start"}` {
		t.Fatalf("unexpected JSONL: %q", scanner.Text())
	}
}

func TestJSONLIsReadOnlyAfterWriterClose(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "producer.jsonl")
	writer, err := NewJSONLWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write(map[string]string{"value": "pending in buffered writer"}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("test precondition failed: buffered data was persisted before Close: size=%d", info.Size())
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	assertValidJSONLLines(t, path, 1)
	if err := writer.Write(map[string]string{"value": "late"}); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("Write after Close error = %v, want ErrWriterClosed", err)
	}
}

func TestJSONLWriterConcurrentRecordsAndEscapes(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "producer.jsonl")
	writer, err := NewJSONLWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	values := []string{
		`\u `,
		`C:\results\producer.jsonl`,
		"Unicode: ação, café, 日本語, 🚀",
		"quotes: \"value\"; newline:\n; tab:\t",
	}
	const goroutines = 16
	const recordsPerGoroutine = 500
	var wait sync.WaitGroup
	errorsFound := make(chan error, goroutines)
	for worker := 0; worker < goroutines; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for index := 0; index < recordsPerGoroutine; index++ {
				value := values[index%len(values)]
				if err := writer.Write(map[string]any{"worker": worker, "index": index, "value": value}); err != nil {
					errorsFound <- fmt.Errorf("worker %d record %d: %w", worker, index, err)
					return
				}
			}
		}(worker)
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	assertValidJSONLLines(t, path, goroutines*recordsPerGoroutine)
}

func assertValidJSONLLines(t *testing.T, path string, expected int) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("line %d is not encoding/json output: %v; %q", count+1, err, scanner.Text())
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("line count = %d, want %d", count, expected)
	}
}
