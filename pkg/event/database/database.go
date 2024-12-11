package database

import (
	"context"
	"fmt"
	"github.com/everpan/idig/pkg/event"
	_ "github.com/mattn/go-sqlite3"
	"sync"
	"time"
	"xorm.io/xorm"
)

type DBEventBus struct {
	engine   *xorm.Engine
	handlers map[string][]func(*event.Event) error
	mu       sync.RWMutex
}

// NewDBEventBus creates a new database event bus
func NewDBEventBus(engine *xorm.Engine) (*DBEventBus, error) {
	err := engine.Sync2(new(event.Event))
	if err != nil {
		return nil, err
	}
	return &DBEventBus{
		engine:   engine,
		handlers: make(map[string][]func(*event.Event) error),
	}, nil
}
func (d *DBEventBus) Publish(ctx context.Context, topic string, evt event.Event) error {
	if err := evt.Validate(); err != nil {
		return fmt.Errorf("invalid event: %w", err)
	}
	evt.Topic = topic
	_, err := d.engine.Insert(&evt)
	return err
}

func (d *DBEventBus) Subscribe(ctx context.Context, topic string, handler func(*event.Event) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.handlers[topic] == nil {
		d.handlers[topic] = make([]func(*event.Event) error, 0)
	}
	d.handlers[topic] = append(d.handlers[topic], handler)

	// Start a goroutine to poll for new events
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var evs []*event.Event
				d.engine.Find(&evs, event.Event{Topic: topic, Processed: false})
				for _, evt := range evs {
					d.mu.RLock()
					handlers := d.handlers[topic]
					d.mu.RUnlock()
					for _, h := range handlers {
						if err := h(evt); err != nil {
							// Log the error and continue
							continue
						} else {
							evt.Processed = true
							d.engine.MustCols("processed").Update(evt)
						}
					}
				}
			}
		}
	}()

	return nil
}

func (d *DBEventBus) Unsubscribe(topic string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.handlers, topic)
	return nil
}

func (d *DBEventBus) Close() error {
	return d.engine.Close()
}
