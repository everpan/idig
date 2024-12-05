package testing

import (
	"context"
	"github.com/ever/idig/pkg/event"
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"
	"time"
)

// EventBusTestSuite 定义了事件总线的通用测试套件
type EventBusTestSuite struct {
	suite.Suite
	EventBus event.EventBus
}

// TestEvent 是用于测试的事件数据
type TestEvent struct {
	event.Event
	ExpectedData map[string]interface{}
}

// NewTestEvent 创建一个测试事件
func NewTestEvent(eventType string, data map[string]interface{}) TestEvent {
	return TestEvent{
		Event: event.Event{
			ID:        generateID(),
			Type:      eventType,
			Source:    "test_service",
			Data:      data,
			Timestamp: time.Now(),
		},
		ExpectedData: data,
	}
}

var idCounter int
var idMutex sync.Mutex

func generateID() string {
	idMutex.Lock()
	defer idMutex.Unlock()
	idCounter++
	return fmt.Sprintf("test-event-%d", idCounter)
}

// RunEventBusTests 运行所有事件总线测试
func (suite *EventBusTestSuite) RunEventBusTests() {
	suite.Run("Basic Publish Subscribe", suite.TestBasicPublishSubscribe)
	suite.Run("Multiple Subscribers", suite.TestMultipleSubscribers)
	suite.Run("Unsubscribe", suite.TestUnsubscribe)
	suite.Run("Concurrent Publishing", suite.TestConcurrentPublishing)
	suite.Run("Error Handling", suite.TestErrorHandling)
	suite.Run("Large Event Data", suite.TestLargeEventData)
	suite.Run("Multiple Topics", suite.TestMultipleTopics)
}

// TestBasicPublishSubscribe 测试基本的发布订阅功能
func (suite *EventBusTestSuite) TestBasicPublishSubscribe() {
	ctx := context.Background()
	topic := "test.basic"
	received := make(chan event.Event, 1)

	testEvent := NewTestEvent("test.basic", map[string]interface{}{
		"message": "hello world",
	})

	err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
		received <- e
		return nil
	})
	suite.NoError(err)

	err = suite.EventBus.Publish(ctx, topic, testEvent.Event)
	suite.NoError(err)

	select {
	case evt := <-received:
		suite.Equal(testEvent.ID, evt.ID)
		suite.Equal(testEvent.Type, evt.Type)
		suite.Equal(testEvent.ExpectedData, evt.Data)
	case <-time.After(5 * time.Second):
		suite.Fail("Timeout waiting for event")
	}
}

// TestMultipleSubscribers 测试多个订阅者
func (suite *EventBusTestSuite) TestMultipleSubscribers() {
	ctx := context.Background()
	topic := "test.multiple"
	received1 := make(chan event.Event, 1)
	received2 := make(chan event.Event, 1)

	testEvent := NewTestEvent("test.multiple", map[string]interface{}{
		"message": "multiple subscribers",
	})

	err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
		received1 <- e
		return nil
	})
	suite.NoError(err)

	err = suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
		received2 <- e
		return nil
	})
	suite.NoError(err)

	err = suite.EventBus.Publish(ctx, topic, testEvent.Event)
	suite.NoError(err)

	for i, ch := range []chan event.Event{received1, received2} {
		select {
		case evt := <-ch:
			suite.Equal(testEvent.ID, evt.ID, "Subscriber %d", i+1)
			suite.Equal(testEvent.Type, evt.Type, "Subscriber %d", i+1)
			suite.Equal(testEvent.ExpectedData, evt.Data, "Subscriber %d", i+1)
		case <-time.After(5 * time.Second):
			suite.Fail("Timeout waiting for subscriber", "Subscriber %d", i+1)
		}
	}
}

// TestUnsubscribe 测试取消订阅
func (suite *EventBusTestSuite) TestUnsubscribe() {
	ctx := context.Background()
	topic := "test.unsubscribe"
	received := make(chan event.Event, 1)

	testEvent := NewTestEvent("test.unsubscribe", map[string]interface{}{
		"message": "unsubscribe test",
	})

	err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
		received <- e
		return nil
	})
	suite.NoError(err)

	err = suite.EventBus.Unsubscribe(topic)
	suite.NoError(err)

	err = suite.EventBus.Publish(ctx, topic, testEvent.Event)
	suite.NoError(err)

	select {
	case <-received:
		suite.Fail("Should not receive event after unsubscribe")
	case <-time.After(2 * time.Second):
		// Success: no event received
	}
}

// TestConcurrentPublishing 测试并发发布
func (suite *EventBusTestSuite) TestConcurrentPublishing() {
	ctx := context.Background()
	topic := "test.concurrent"
	receivedCount := 0
	var mu sync.Mutex
	eventCount := 100

	err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		return nil
	})
	suite.NoError(err)

	var wg sync.WaitGroup
	for i := 0; i < eventCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			testEvent := NewTestEvent("test.concurrent", map[string]interface{}{
				"counter": i,
			})
			err := suite.EventBus.Publish(ctx, topic, testEvent.Event)
			suite.NoError(err)
		}(i)
	}

	wg.Wait()
	time.Sleep(2 * time.Second) // Wait for all events to be processed

	mu.Lock()
	suite.Equal(eventCount, receivedCount)
	mu.Unlock()
}

// TestErrorHandling 测试错误处理
func (suite *EventBusTestSuite) TestErrorHandling() {
	ctx := context.Background()
	topic := "test.error"
	errorMessage := "test error"

	err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
		return fmt.Errorf(errorMessage)
	})
	suite.NoError(err)

	testEvent := NewTestEvent("test.error", map[string]interface{}{
		"message": "error test",
	})

	err = suite.EventBus.Publish(ctx, topic, testEvent.Event)
	suite.NoError(err) // Publishing should succeed even if handler returns error
}

// TestLargeEventData 测试大数据量事件
func (suite *EventBusTestSuite) TestLargeEventData() {
	ctx := context.Background()
	topic := "test.large"
	received := make(chan event.Event, 1)

	// 创建大量数据
	largeData := make(map[string]interface{})
	for i := 0; i < 1000; i++ {
		largeData[fmt.Sprintf("key-%d", i)] = fmt.Sprintf("value-%d", i)
	}

	testEvent := NewTestEvent("test.large", largeData)

	err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
		received <- e
		return nil
	})
	suite.NoError(err)

	err = suite.EventBus.Publish(ctx, topic, testEvent.Event)
	suite.NoError(err)

	select {
	case evt := <-received:
		suite.Equal(testEvent.ID, evt.ID)
		suite.Equal(testEvent.ExpectedData, evt.Data)
	case <-time.After(5 * time.Second):
		suite.Fail("Timeout waiting for large event")
	}
}

// TestMultipleTopics 测试多个主题
func (suite *EventBusTestSuite) TestMultipleTopics() {
	ctx := context.Background()
	topics := []string{"test.topic1", "test.topic2", "test.topic3"}
	received := make(map[string]chan event.Event)
	
	for _, topic := range topics {
		received[topic] = make(chan event.Event, 1)
		topicCopy := topic // Capture topic in closure
		
		err := suite.EventBus.Subscribe(ctx, topicCopy, func(e event.Event) error {
			received[topicCopy] <- e
			return nil
		})
		suite.NoError(err)
	}

	for _, topic := range topics {
		testEvent := NewTestEvent(topic, map[string]interface{}{
			"topic": topic,
		})
		err := suite.EventBus.Publish(ctx, topic, testEvent.Event)
		suite.NoError(err)
	}

	for _, topic := range topics {
		select {
		case evt := <-received[topic]:
			suite.Equal(topic, evt.Type)
			suite.Equal(topic, evt.Data["topic"])
		case <-time.After(5 * time.Second):
			suite.Fail("Timeout waiting for event on topic", topic)
		}
	}
}
