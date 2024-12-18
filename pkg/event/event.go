package event

import (
	"context"
	"errors"
	"fmt"
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/core"
	"github.com/spf13/viper"
	"time"
	"xorm.io/xorm"
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

func (e *Event) TableName() string {
	return "idig_events"
}

const defaultProvider = "database"

var eventProvider = defaultProvider

func InitEventTable(engine *xorm.Engine) error {
	return engine.Sync2(new(Event))
}

func reloadEventConfig() error {
	eventProvider = viper.GetString("event.provider")
	if eventProvider == "" {
		eventProvider = defaultProvider
	}
	return nil
}

func init() {
	viper.SetDefault("event.provider", eventProvider)
	config.RegisterReloadConfigFunc(reloadEventConfig)
	reloadEventConfig()
	if eventProvider == defaultProvider {
		core.RegisterInitTableFunction(InitEventTable)
	}
}

// NewEvent creates a new event with the given parameters and sets the timestamp
func NewEvent(id uint64, eventType, source string, data map[string]interface{}) *Event {
	return &Event{
		ID:        id,
		Type:      eventType,
		Source:    source,
		Data:      data,
		Timestamp: time.Now(),
	}
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
	Publish(ctx context.Context, topic string, event *Event) error
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
