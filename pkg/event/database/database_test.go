package database

import (
	"context"
	"errors"
	"fmt"
	"github.com/everpan/idig/pkg/event"
	et "github.com/everpan/idig/pkg/event/testing"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/suite"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
	"xorm.io/xorm"
)

const (
	schemaVersion = 1
	testDBPath    = "test.db"
)

type DBEventBusTestSuite struct {
	et.EventBusTestSuite
	engine     *xorm.Engine
	backupPath string
}

func (suite *DBEventBusTestSuite) SetupSuite() {
	// Create temporary directory for test files
	tempDir := os.TempDir()
	suite.backupPath = filepath.Join(tempDir, "backup.db")
}

func (suite *DBEventBusTestSuite) TearDownSuite() {
	// Cleanup temporary directory
	if suite.backupPath != "" {
		os.RemoveAll(filepath.Dir(suite.backupPath))
	}
}

func (suite *DBEventBusTestSuite) SetupTest() {
	var err error
	suite.engine, err = xorm.NewEngine("sqlite3", ":memory:")
	suite.Require().NoError(err)
	suite.engine.ShowSQL(true)
	eventBus, err := NewDBEventBus(suite.engine)
	suite.Require().NoError(err)
	suite.EventBus = eventBus
}

func (suite *DBEventBusTestSuite) backupDatabase() error {
	// Backup the current database
	backup, err := os.Create(suite.backupPath)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer backup.Close()

	// Export database to backup file
	_, err = suite.engine.Exec(fmt.Sprintf("VACUUM INTO '%s'", suite.backupPath))
	return err
}

func (suite *DBEventBusTestSuite) restoreDatabase() error {
	// Close current database connection
	if err := suite.engine.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	// Create new database connection
	var err error
	suite.engine, err = xorm.NewEngine("sqlite3", ":memory:")
	if err != nil {
		return fmt.Errorf("failed to open new database: %w", err)
	}

	// Restore from backup
	_, err = suite.engine.Exec(fmt.Sprintf(".restore '%s'", suite.backupPath))
	return err
}

func (suite *DBEventBusTestSuite) TestDatabaseFeatures() {
	ctx := context.Background()
	topic := "test.db.specific"

	// Test event persistence with topic
	suite.Run("Event Persistence", func() {
		testEvent := et.NewTestEvent(1, "test.persistence", map[string]interface{}{
			"message": "persistence test",
			"nested": map[string]interface{}{
				"key": "value",
			},
		})
		testEvent.Event.Topic = topic
		// Publish event
		err := suite.EventBus.Publish(ctx, topic, testEvent.Event)
		suite.NoError(err)
		storeEvent := &event.Event{ID: 1}
		ok, err := suite.engine.Get(storeEvent)

		suite.NoError(err)
		suite.Equal(true, ok)

		// Verify stored data
		suite.Equal(testEvent.ID, storeEvent.ID)
		suite.Equal(testEvent.Type, storeEvent.Type)
		suite.Equal(testEvent.Source, storeEvent.Source)
		suite.Equal(topic, storeEvent.Topic)
		suite.False(storeEvent.Processed)

		// suite.Equal(testEvent.Event, *storeEvent)
	})

	// Test subscription and event processing
	suite.Run("Subscription and Processing", func() {
		processed := make(chan *event.Event, 1)
		err := suite.EventBus.Subscribe(ctx, topic, func(e *event.Event) error {
			// e.Processed = true
			processed <- e
			return nil
		})
		suite.NoError(err)

		// Publish test event
		testEvent := et.NewTestEvent(2, "test.subscription", map[string]interface{}{
			"message": "subscription test",
		})
		err = suite.EventBus.Publish(ctx, topic, testEvent.Event)
		suite.NoError(err)

		// Wait for event processing
		select {
		case receivedEvent := <-processed:
			suite.Equal(testEvent.ID, receivedEvent.ID)
			suite.Equal(testEvent.Type, receivedEvent.Type)
			suite.Equal(testEvent.Source, receivedEvent.Source)
			suite.Equal(testEvent.Data, receivedEvent.Data)
			storeEvent := &event.Event{ID: 2}
			_, err := suite.engine.Get(storeEvent)
			// Verify event is marked as processed
			suite.NoError(err)
			suite.True(storeEvent.Processed)
		case <-time.After(2 * time.Second):
			suite.Fail("Timeout waiting for event processing")
		}
	})

	// Test concurrent event publishing and processing
	suite.Run("Concurrent Operations", func() {
		eventCount := 3
		processed := make(chan *event.Event, eventCount)
		var wg sync.WaitGroup
		time.Sleep(2 * time.Second)
		// Subscribe to events
		err := suite.EventBus.Subscribe(ctx, topic, func(e *event.Event) error {
			processed <- e
			return nil
		})
		suite.NoError(err)

		// Publish events concurrently
		for i := 0; i < eventCount; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				testEvent := et.NewTestEvent(uint64(i+3), "test.concurrent", map[string]interface{}{
					"index": index,
				})
				err1 := suite.EventBus.Publish(ctx, topic, testEvent.Event)
				suite.NoError(err1)
			}(i)
		}

		// Wait for all events to be published
		wg.Wait()

		// Wait for all events to be processed
		processedEvents := make(map[uint64]bool)
		for i := 0; i < eventCount; i++ {
			select {
			case e := <-processed:
				processedEvents[e.ID] = true
			case <-time.After(5 * time.Second):
				suite.Fail("Timeout waiting for event processing")
			}
		}

		// Verify all events were processed
		suite.Len(processedEvents, eventCount)

		// Verify database state
		storeEvent := &event.Event{Type: "test.concurrent", Processed: true}
		count, err := suite.engine.Count(&storeEvent)
		suite.NoError(err)
		suite.Equal(eventCount, count)
	})

	// Test error handling
	suite.Run("Error Handling", func() {
		// Test invalid event
		invalidEvent := event.Event{} // Empty event
		err := suite.EventBus.Publish(ctx, topic, invalidEvent)
		suite.Error(err)

		// Test invalid JSON data
		//_, err = suite.db.Exec(`
		//	INSERT INTO events (id, type, source, topic, data, timestamp)
		//	VALUES (?, ?, ?, ?, ?, ?)
		//`, uint64(time.Now().UnixNano()), "test.error", "test", topic, "invalid json", time.Now())
		//suite.Error(err)

		// Test subscription error handling
		// errorChan := make(chan error, 1)
		err = suite.EventBus.Subscribe(ctx, topic, func(e *event.Event) error {
			return errors.New("handler error")
		})
		suite.NoError(err)

		testEvent := et.NewTestEvent(uint64(time.Now().UnixNano()), "test.error", nil)
		err = suite.EventBus.Publish(ctx, topic, testEvent.Event)
		suite.ErrorContains(err, "event Data cannot be nil")

		// Wait a bit for error processing
		time.Sleep(2 * time.Second)

		// Verify event is not marked as processed due to error
		storeEvent := &event.Event{ID: testEvent.ID}
		_, err = suite.engine.Get(storeEvent)
		suite.NoError(err)
		suite.False(storeEvent.Processed)
	})
}

func (suite *DBEventBusTestSuite) TestEventBus() {
	suite.RunEventBusTests()
}

func (suite *DBEventBusTestSuite) TearDownTest() {
	if suite.EventBus != nil {
		_ = suite.EventBus.Close()
	}
	if suite.engine != nil {
		_ = suite.engine.Close()
	}
}

func TestDBEventBusSuite(t *testing.T) {
	suite.Run(t, new(DBEventBusTestSuite))
}
