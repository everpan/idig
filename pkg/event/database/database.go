package database

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/everpan/idig/pkg/event"
	_ "github.com/mattn/go-sqlite3"
	"xorm.io/xorm"
)

type DBEventBus struct {
	engine   *xorm.Engine
	handlers map[string][]func(*event.Event) error
	mu       sync.RWMutex
}

// NewDBEventBus creates a new database event bus
func NewDBEventBus(engine *xorm.Engine) (*DBEventBus, error) {
	// 确保事件表存在
	err := engine.Sync2(new(event.Event))
	if err != nil {
		return nil, fmt.Errorf("failed to sync database schema: %w", err)
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
				err := d.engine.Where("topic = ? AND processed = ?", topic, false).Find(&evs)
				if err != nil {
					continue
				}

				for _, evt := range evs {
					d.mu.RLock()
					handlers := d.handlers[topic]
					d.mu.RUnlock()

					// 只有所有 handler 都成功时才标记为已处理
					allSuccess := true
					for _, h := range handlers {
						if err := h(evt); err != nil {
							allSuccess = false
							break
						}
					}

					if allSuccess {
						evt.Processed = true
						_, err := d.engine.ID(evt.ID).Cols("processed").Update(evt)
						if err != nil {
							evt.Processed = false
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
