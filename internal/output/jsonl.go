package output

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

var ErrWriterClosed = errors.New("JSONL writer is closed")

type writeRequest struct {
	value any
	done  chan error
}

type JSONLWriter struct {
	mu       sync.RWMutex
	file     *os.File
	requests chan writeRequest
	done     chan struct{}
	closed   bool
	err      error
}

func NewJSONLWriter(path string) (*JSONLWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open JSONL %q: %w", path, err)
	}
	writer := &JSONLWriter{
		file:     file,
		requests: make(chan writeRequest, 256),
		done:     make(chan struct{}),
	}
	go writer.run()
	return writer, nil
}

func (w *JSONLWriter) Write(value any) error {
	request := writeRequest{value: value, done: make(chan error, 1)}
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return ErrWriterClosed
	}
	w.requests <- request
	if err := <-request.done; err != nil {
		return fmt.Errorf("write JSONL: %w", err)
	}
	return nil
}

func (w *JSONLWriter) Close() error {
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.requests)
	}
	w.mu.Unlock()

	<-w.done
	return w.err
}

func (w *JSONLWriter) run() {
	defer close(w.done)
	buffer := bufio.NewWriter(w.file)
	encoder := json.NewEncoder(buffer)
	var writeErr error
	for request := range w.requests {
		if writeErr == nil {
			writeErr = encoder.Encode(request.value)
		}
		request.done <- writeErr
	}

	w.err = errors.Join(
		wrapError(buffer.Flush(), "flush JSONL"),
		wrapError(w.file.Sync(), "sync JSONL"),
		wrapError(w.file.Close(), "close JSONL"),
	)
	if writeErr != nil {
		w.err = errors.Join(fmt.Errorf("write JSONL: %w", writeErr), w.err)
	}
}

func wrapError(err error, operation string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}
