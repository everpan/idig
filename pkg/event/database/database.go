package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/everpan/idig/pkg/event"
	"sync"
	"time"
)

type DBEventBus struct {
	db       *sql.DB
	handlers map[string][]func(event.Event) error
	mu       sync.RWMutex
}

// NewDBEventBus creates a new database event bus
func NewDBEventBus(db *sql.DB) (*DBEventBus, error) {
	// Create events table if it doesn't exist
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id VARCHAR(255) PRIMARY KEY,
			type VARCHAR(255),
			source VARCHAR(255),
			topic VARCHAR(255),
			data JSON,
			timestamp TIMESTAMP,
			processed BOOLEAN DEFAULT FALSE
		)
	`)
	if err != nil {
		return nil, err
	}

	return &DBEventBus{
		db:       db,
		handlers: make(map[string][]func(event.Event) error),
	}, nil
}

func (d *DBEventBus) Publish(ctx context.Context, topic string, evt event.Event) error {
	if err := evt.Validate(); err != nil {
		return fmt.Errorf("invalid event: %w", err)
	}

	data, err := json.Marshal(evt.Data)
	if err != nil {
		return err
	}

	_, err = d.db.ExecContext(ctx, `
		INSERT INTO events (id, type, source, topic, data, timestamp)
		VALUES (?, ?, ?, ?, ?, ?)
	`, evt.ID, evt.Type, evt.Source, topic, data, evt.Timestamp)

	return err
}

func (d *DBEventBus) Subscribe(ctx context.Context, topic string, handler func(event.Event) error) error {
	d.mu.Lock()
	if d.handlers[topic] == nil {
		d.handlers[topic] = make([]func(event.Event) error, 0)
	}
	d.handlers[topic] = append(d.handlers[topic], handler)
	d.mu.Unlock()

	// Start a goroutine to poll for new events
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rows, err := d.db.QueryContext(ctx, `
					SELECT id, type, source, data, timestamp
					FROM events
					WHERE topic = ? AND processed = FALSE
					ORDER BY timestamp ASC
				`, topic)
				if err != nil {
					continue
				}
				defer rows.Close()

				for rows.Next() {
					var evt event.Event
					var dataStr string
					if err := rows.Scan(&evt.ID, &evt.Type, &evt.Source, &dataStr, &evt.Timestamp); err != nil {
						// Log the error and continue
						continue
					}

					if err := json.Unmarshal([]byte(dataStr), &evt.Data); err != nil {
						// Log the error and continue
						continue
					}

					d.mu.RLock()
					handlers := d.handlers[topic]
					d.mu.RUnlock()

					for _, h := range handlers {
						if err := h(evt); err != nil {
							// Log the error and continue
							continue
						}
					}

					// Mark event as processed
					if _, err = d.db.ExecContext(ctx, `
						UPDATE events SET processed = TRUE WHERE id = ?
					`, evt.ID); err != nil {
						continue
					}
				}

				// Check for errors from iterating over rows
				if err = rows.Err(); err != nil {
					continue
				}
			}
		}
	}()

	return nil
}

func (d *DBEventBus) Unsubscribe(topic string) error {
	d.mu.Lock()
	delete(d.handlers, topic)
	d.mu.Unlock()
	return nil
}

func (d *DBEventBus) Close() error {
	return d.db.Close()
}
