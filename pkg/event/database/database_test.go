package database

import (
	"context"
	"database/sql"
	"github.com/everpan/idig/pkg/event"
	eventesting "github.com/everpan/idig/pkg/event/testing"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type DBEventBusTestSuite struct {
	eventesting.EventBusTestSuite
	db *sql.DB
}

func (suite *DBEventBusTestSuite) SetupTest() {
	var err error
	suite.db, err = sql.Open("sqlite3", ":memory:")
	suite.Require().NoError(err)

	eventBus, err := NewDBEventBus(suite.db)
	suite.Require().NoError(err)
	suite.EventBus = eventBus
}

func (suite *DBEventBusTestSuite) TearDownTest() {
	if suite.EventBus != nil {
		suite.EventBus.Close()
	}
	if suite.db != nil {
		suite.db.Close()
	}
}

func TestDBEventBusSuite(t *testing.T) {
	suite.Run(t, new(DBEventBusTestSuite))
}

// TestDBSpecificFeatures tests database-specific features
func (suite *DBEventBusTestSuite) TestDBSpecificFeatures() {
	ctx := context.Background()
	topic := "test.db.specific"

	// Test event persistence
	suite.Run("Event Persistence", func() {
		testEvent := eventesting.NewTestEvent("test.persistence", map[string]interface{}{
			"message": "persistence test",
		})

		// Publish event
		err := suite.EventBus.Publish(ctx, topic, testEvent.Event)
		suite.NoError(err)

		// Verify event is stored in database
		var count int
		err = suite.db.QueryRow("SELECT COUNT(*) FROM events WHERE id = ?", testEvent.ID).Scan(&count)
		suite.NoError(err)
		suite.Equal(1, count)

		// Verify event data
		var (
			storedID        string
			storedType      string
			storedSource    string
			storedProcessed bool
		)
		err = suite.db.QueryRow(`
			SELECT id, type, source, processed 
			FROM events 
			WHERE id = ?`, testEvent.ID).Scan(&storedID, &storedType, &storedSource, &storedProcessed)
		suite.NoError(err)
		suite.Equal(testEvent.ID, storedID)
		suite.Equal(testEvent.Type, storedType)
		suite.Equal(testEvent.Source, storedSource)
		suite.False(storedProcessed)
	})

	// Test event processing status
	suite.Run("Event Processing Status", func() {
		testEvent := eventesting.NewTestEvent("test.processing", map[string]interface{}{
			"message": "processing test",
		})

		processed := make(chan bool)
		err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
			processed <- true
			return nil
		})
		suite.NoError(err)

		err = suite.EventBus.Publish(ctx, topic, testEvent.Event)
		suite.NoError(err)

		select {
		case <-processed:
			// Verify event is marked as processed
			var processedStatus bool
			err = suite.db.QueryRow("SELECT processed FROM events WHERE id = ?", testEvent.ID).Scan(&processedStatus)
			suite.NoError(err)
			suite.True(processedStatus)
		case <-time.After(5 * time.Second):
			suite.Fail("Timeout waiting for event processing")
		}
	})

	// Test concurrent event processing
	suite.Run("Concurrent Event Processing", func() {
		eventCount := 10
		processed := make(chan bool, eventCount)

		err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
			processed <- true
			return nil
		})
		suite.NoError(err)

		// Publish events concurrently
		for i := 0; i < eventCount; i++ {
			testEvent := eventesting.NewTestEvent("test.concurrent", map[string]interface{}{
				"index": i,
			})
			err := suite.EventBus.Publish(ctx, topic, testEvent.Event)
			suite.NoError(err)
		}

		// Wait for all events to be processed
		for i := 0; i < eventCount; i++ {
			select {
			case <-processed:
				// Success
			case <-time.After(5 * time.Second):
				suite.Fail("Timeout waiting for concurrent event processing")
			}
		}

		// Verify all events are marked as processed
		var count int
		err = suite.db.QueryRow("SELECT COUNT(*) FROM events WHERE processed = true").Scan(&count)
		suite.NoError(err)
		suite.Equal(eventCount, count)
	})
}

func (suite *DBEventBusTestSuite) TestEventBus() {
	suite.RunEventBusTests()
}
