package database

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/everpan/idig/pkg/event"
	et "github.com/everpan/idig/pkg/event/testing"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/suite"
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
	suite.engine, err = xorm.NewEngine("sqlite3", "/tmp/test.db")
	suite.Require().NoError(err)
	suite.engine.ShowSQL(true)

	// 确保表被正确创建
	err = suite.engine.Sync2(new(event.Event))
	suite.Require().NoError(err)

	// 清理所有现有数据
	_, err = suite.engine.Exec("DELETE FROM event")
	suite.Require().NoError(err)

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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		processed := make(chan *event.Event, 1)
		err := suite.EventBus.Subscribe(ctx, topic, func(e *event.Event) error {
			processed <- e
			return nil
		})
		suite.NoError(err)

		// Publish events
		for i := 0; i < 3; i++ {
			testEvent := et.NewTestEvent(uint64(i), "test.concurrent", map[string]interface{}{})
			testEvent.Event.Topic = topic
			err = suite.EventBus.Publish(ctx, topic, testEvent.Event)
			suite.NoError(err)
		}

		// Wait for events to be processed
		select {
		case evt := <-processed:
			suite.Equal("test.concurrent", evt.Type)
		case <-ctx.Done():
			suite.Fail("Timeout waiting for event processing")
		}
	})

	// Test concurrent event publishing and processing
	suite.Run("Concurrent Operations", func() {
		eventCount := 3
		processed := make(chan *event.Event, eventCount)
		var wg sync.WaitGroup

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
				// 使用时间戳作为 ID 基础，避免冲突
				id := uint64(time.Now().UnixNano()) + uint64(index)
				testEvent := et.NewTestEvent(id%100000, "test.concurrent", map[string]interface{}{
					"index": index,
				})
				testEvent.Event.Topic = topic
				err1 := suite.EventBus.Publish(ctx, topic, testEvent.Event)
				suite.NoError(err1)
			}(i)
		}

		// Wait for all events to be published
		wg.Wait()
		time.Sleep(2 * time.Second)
		// Wait for all events to be processed
		processedEvents := make(map[uint64]bool)
		timeout := time.After(20 * time.Second)
		for i := 0; i < eventCount; i++ {
			select {
			case e := <-processed:
				processedEvents[e.ID] = true
			case <-timeout:
				suite.Fail("Timeout waiting for event processing")
				goto checkResults
			}
		}

	checkResults:
		// Verify all events were processed
		suite.Len(processedEvents, eventCount)

		// Verify database state
		var events []*event.Event
		err = suite.engine.Where("type = ? AND processed = ?", "test.concurrent", true).Find(&events)
		suite.NoError(err)
		suite.Equal(eventCount, len(events))
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
