package event

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrRetry indicates that the event handling should be retried
var ErrRetry = errors.New("retry event handling")

// Event represents a generic event in the system
type Event struct {
	ID        uint64                 `json:"id" xorm:"id pk autoincr"`
	Type      string                 `json:"type" xorm:"varchar(255) notnull index"`
	Source    string                 `json:"source" xorm:"varchar(255) notnull index"`
	Topic     string                 `json:"topic" xorm:"varchar(255) notnull index"`
	Data      map[string]interface{} `json:"data" xorm:"text"`
	Timestamp time.Time              `json:"timestamp" xorm:"uptime bigint notnull index"`
	Processed bool                   `json:"processed" xorm:"bool"`
}

// Validate checks if the event is valid
func (e *Event) Validate() error {
	if e.ID == 0 {
		return fmt.Errorf("event ID cannot be zero")
	}
	if e.Type == "" {
		return fmt.Errorf("event Type cannot be empty")
	}
	if e.Source == "" {
		return fmt.Errorf("event Source cannot be empty")
	}
	if e.Data == nil {
		return fmt.Errorf("event Data cannot be nil")
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("event Timestamp cannot be zero")
	}
	return nil
}

// Publisher defines the interface for publishing events
type Publisher interface {
	// Publish publishes an event to the specified topic
	Publish(ctx context.Context, topic string, event Event) error
	// Close closes the publisher
	Close() error
}

// Subscriber defines the interface for subscribing to events
type Subscriber interface {
	// Subscribe subscribes to events from the specified topic
	Subscribe(ctx context.Context, topic string, handler func(*Event) error) error
	// Unsubscribe removes the subscription for the specified topic
	Unsubscribe(topic string) error
	// Close closes the subscriber
	Close() error
}

// EventBus represents the main event bus that manages publishers and subscribers
type EventBus interface {
	Publisher
	Subscriber
}
