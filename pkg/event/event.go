package event

import (
	"context"
	"time"
)

// Event represents a generic event in the system
type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Source    string                 `json:"source"`
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time             `json:"timestamp"`
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
	Subscribe(ctx context.Context, topic string, handler func(Event) error) error
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
