package watcher

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/everpan/idig/pkg/event"
)

// MockEventBus is a mock implementation of the EventBus interface
type MockEventBus struct {
	publishedEvents    []event.Event
	subscribedHandlers map[string][]func(*event.Event) error
}

// Publish implements the Publish method of EventBus
func (m *MockEventBus) Publish(ctx context.Context, topic string, event *event.Event) error {
	m.publishedEvents = append(m.publishedEvents, *event)
	return nil
}

// Subscribe implements the Subscribe method of EventBus
func (m *MockEventBus) Subscribe(ctx context.Context, topic string, handler func(*event.Event) error) error {
	if m.subscribedHandlers == nil {
		m.subscribedHandlers = make(map[string][]func(*event.Event) error)
	}
	m.subscribedHandlers[topic] = append(m.subscribedHandlers[topic], handler)
	return nil
}

// Unsubscribe implements the Unsubscribe method of EventBus
func (m *MockEventBus) Unsubscribe(topic string) error {
	// Mock implementation
	return nil
}

// Close implements the Close method of EventBus
func (m *MockEventBus) Close() error {
	return nil
}

// GetPublishedEvents returns the published events
func (m *MockEventBus) GetPublishedEvents() []event.Event {
	return m.publishedEvents
}

// TestNewFileWatcher tests the NewFileWatcher function
func TestNewFileWatcher(t *testing.T) {
	bus := &MockEventBus{}
	fw, err := NewFileWatcher(bus)
	if err != nil {
		t.Fatal("Expected no error, got", err)
	}
	if fw == nil {
		t.Fatal("Expected FileWatcher, got nil")
	}
}

// TestWatch tests the Watch method
func TestWatch(t *testing.T) {
	bus := &MockEventBus{}
	fw, _ := NewFileWatcher(bus)
	testFile := filepath.Join(os.TempDir(), "testfile.txt")
	os.WriteFile(testFile, []byte("test content"), 0644)
	defer os.Remove(testFile)

	if err := fw.Watch(testFile); err != nil {
		t.Fatal("Expected no error, got", err)
	}

	// Simulate a write to the file
	os.WriteFile(testFile, []byte("new content"), 0644)
	time.Sleep(100 * time.Millisecond) // Wait for event to be processed
}

// TestHandleEvents tests the handleEvents method
func TestHandleEvents(t *testing.T) {
	bus := &MockEventBus{}
	fw, _ := NewFileWatcher(bus)
	defer fw.Close()
	testFile := filepath.Join(os.TempDir(), "testfile.txt")
	os.WriteFile(testFile, []byte("test content"), 0644)
	fw.Watch(testFile)

	// Simulate a write event
	os.WriteFile(testFile, []byte("new content"), 0644)
	time.Sleep(100 * time.Millisecond) // Wait for event to be processed

	// Verify that the event was published
	if len(bus.GetPublishedEvents()) != 1 {
		t.Errorf("Expected 1 published event, got %d", len(bus.GetPublishedEvents()))
	}
}

// TestFileCreation tests the FileWatcher behavior when a file is created.
func TestFileCreation(t *testing.T) {
	bus := &MockEventBus{}
	fw, _ := NewFileWatcher(bus)
	testFile := filepath.Join(os.TempDir(), "testfile_create.txt")
	defer os.Remove(testFile)

	// Create the file before watching
	os.WriteFile(testFile, []byte("initial content"), 0644)

	if err := fw.Watch(testFile); err != nil {
		t.Fatal("Expected no error, got", err)
	}

	// Simulate file creation
	os.WriteFile(testFile, []byte("test content"), 0644)
	time.Sleep(100 * time.Millisecond) // Wait for event to be processed

	if len(bus.publishedEvents) == 0 {
		t.Fatal("Expected at least one published event")
	}
}

// TestFileDeletion tests the FileWatcher behavior when a file is deleted.
func TestFileDeletion(t *testing.T) {
	bus := &MockEventBus{}
	fw, _ := NewFileWatcher(bus)
	testFile := filepath.Join(os.TempDir(), "testfile_delete.txt")
	os.WriteFile(testFile, []byte("test content"), 0644)
	defer os.Remove(testFile)

	if err := fw.Watch(testFile); err != nil {
		t.Fatal("Expected no error, got", err)
	}

	// Simulate file deletion
	os.Remove(testFile)
	time.Sleep(100 * time.Millisecond) // Wait for event to be processed

	if len(bus.publishedEvents) == 0 {
		t.Fatal("Expected at least one published event")
	}
}

// TestConcurrentWrites tests the FileWatcher handling of concurrent writes.
func TestConcurrentWrites(t *testing.T) {
	bus := &MockEventBus{}
	fw, _ := NewFileWatcher(bus)
	testFile := filepath.Join(os.TempDir(), "testfile_concurrent.txt")
	defer os.Remove(testFile)

	// Create the file before watching
	os.WriteFile(testFile, []byte("initial content"), 0644)

	if err := fw.Watch(testFile); err != nil {
		t.Fatal("Expected no error, got", err)
	}

	// Simulate concurrent writes
	go os.WriteFile(testFile, []byte("first write"), 0644)
	go os.WriteFile(testFile, []byte("second write"), 0644)
	time.Sleep(100 * time.Millisecond) // Wait for events to be processed

	if len(bus.publishedEvents) < 2 {
		t.Fatal("Expected at least two published events")
	}
}

// TestWatchNonExistentFile tests the FileWatcher behavior when trying to watch a non-existent file.
func TestWatchNonExistentFile(t *testing.T) {
	bus := &MockEventBus{}
	fw, _ := NewFileWatcher(bus)

	// Attempt to watch a non-existent file
	err := fw.Watch("non_existent_file.txt")
	if err == nil {
		t.Fatal("Expected an error when watching a non-existent file")
	}
}

// TestClose tests the Close method of FileWatcher.
func TestClose(t *testing.T) {
	bus := &MockEventBus{}
	fw, _ := NewFileWatcher(bus)
	testFile := filepath.Join(os.TempDir(), "testfile_close.txt")
	defer os.Remove(testFile)

	if err := fw.Watch(testFile); err != nil {
		t.Fatal("Expected no error, got", err)
	}

	// Close the watcher
	if err := fw.Close(); err != nil {
		t.Fatal("Expected no error on close, got", err)
	}
}

// TestMultipleEvents tests the FileWatcher handling of multiple events.
func TestMultipleEvents(t *testing.T) {
	bus := &MockEventBus{}
	fw, _ := NewFileWatcher(bus)
	testFile := filepath.Join(os.TempDir(), "testfile_multiple_events.txt")
	defer os.Remove(testFile)

	if err := fw.Watch(testFile); err != nil {
		t.Fatal("Expected no error, got", err)
	}

	// Simulate multiple write events
	os.WriteFile(testFile, []byte("first write"), 0644)
	os.WriteFile(testFile, []byte("second write"), 0644)
	time.Sleep(100 * time.Millisecond) // Wait for events to be processed

	if len(bus.publishedEvents) < 2 {
		t.Fatal("Expected at least two published events")
	}
}
