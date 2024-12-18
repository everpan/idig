package watcher

import (
	"context"
	"fmt"
	"time"

	"github.com/everpan/idig/pkg/event" // Adjust the import path accordingly
	"github.com/fsnotify/fsnotify"
)

// FileWatcher monitors file changes and triggers events
type FileWatcher struct {
	watcher *fsnotify.Watcher
	bus     event.EventBus
}

// NewFileWatcher creates a new FileWatcher
func NewFileWatcher(bus event.EventBus) (*FileWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	return &FileWatcher{watcher: watcher, bus: bus}, nil
}

// Watch starts watching the specified file
func (fw *FileWatcher) Watch(filePath string) error {
	if err := fw.watcher.Add(filePath); err != nil {
		return err
	}
	go fw.handleEvents()
	return nil
}

// handleEvents listens for file system events and triggers corresponding events
func (fw *FileWatcher) handleEvents() {
	for {
		select {
		case ev, ok := <-fw.watcher.Events:
			if !ok {
				return
			}
			if ev.Op&fsnotify.Write == fsnotify.Write {
				// Trigger an event when the file is written to
				fw.bus.Publish(context.Background(), "file.write", &event.Event{
					Type:      "file.write",
					Source:    ev.Name,
					Timestamp: time.Now(),
				})
			}
		case err, ok := <-fw.watcher.Errors:
			if !ok {
				return
			}
			fmt.Println("Error watching file:", err)
		}
	}
}

// Close stops watching and closes the watcher
func (fw *FileWatcher) Close() error {
	return fw.watcher.Close()
}
