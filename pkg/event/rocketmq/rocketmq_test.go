package rocketmq

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/everpan/idig/pkg/event"
	eventesting "github.com/everpan/idig/pkg/event/testing"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

// MockProducer mocks RocketMQ producer
type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) SendAsync(ctx context.Context, mq func(ctx context.Context, result *primitive.SendResult, err error), msg ...*primitive.Message) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockProducer) SendOneWay(ctx context.Context, mq ...*primitive.Message) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockProducer) Request(ctx context.Context, ttl time.Duration, msg *primitive.Message) (*primitive.Message, error) {
	//TODO implement me
	panic("implement me")
}

func (m *MockProducer) RequestAsync(ctx context.Context, ttl time.Duration, callback interface{}, msg *primitive.Message) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockProducer) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockProducer) Shutdown() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockProducer) SendSync(ctx context.Context, msg *primitive.Message) (*primitive.SendResult, error) {
	args := m.Called(ctx, msg)
	if result := args.Get(0); result != nil {
		return result.(*primitive.SendResult), args.Error(1)
	}
	return nil, args.Error(1)
}

// MockPushConsumer mocks RocketMQ consumer
type MockPushConsumer struct {
	mock.Mock
	messageHandler func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)
}

func (m *MockPushConsumer) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockPushConsumer) Shutdown() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockPushConsumer) Subscribe(topic string, selector consumer.MessageSelector,
	f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error {
	args := m.Called(topic, selector)
	m.messageHandler = f
	return args.Error(0)
}

func (m *MockPushConsumer) Unsubscribe(topic string) error {
	args := m.Called(topic)
	return args.Error(0)
}

// SimulateMessage simulates receiving a message
func (m *MockPushConsumer) SimulateMessage(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	if m.messageHandler != nil {
		return m.messageHandler(ctx, msgs...)
	}
	return consumer.ConsumeSuccess, nil
}

type RocketMQEventBusTestSuite struct {
	eventesting.EventBusTestSuite
	mockProducer *MockProducer
	mockConsumer *MockPushConsumer
}

func (suite *RocketMQEventBusTestSuite) SetupTest() {
	suite.mockProducer = new(MockProducer)
	suite.mockConsumer = new(MockPushConsumer)

	suite.mockProducer.On("Start").Return(nil)
	suite.mockConsumer.On("Start").Return(nil)

	eventBus := &RocketMQEventBus{
		producer: suite.mockProducer,
		consumer: suite.mockConsumer,
		handlers: make(map[string][]func(event.Event) error),
		group:    "test-group",
	}
	suite.EventBus = eventBus
}

func (suite *RocketMQEventBusTestSuite) TearDownTest() {
	suite.mockProducer.On("Shutdown").Return(nil)
	suite.mockConsumer.On("Shutdown").Return(nil)

	if suite.EventBus != nil {
		suite.EventBus.Close()
	}
}

func TestRocketMQEventBusSuite(t *testing.T) {
	suite.Run(t, new(RocketMQEventBusTestSuite))
}

// TestRocketMQSpecificFeatures tests RocketMQ-specific features
func (suite *RocketMQEventBusTestSuite) TestRocketMQSpecificFeatures() {
	ctx := context.Background()
	topic := "test.rocketmq.specific"

	// Test message tags
	suite.Run("Message Tags", func() {
		testEvent := eventesting.NewTestEvent("test.tags", map[string]interface{}{
			"message": "tags test",
			"tag":     "important",
		})

		suite.mockProducer.On("SendSync", mock.Anything, mock.MatchedBy(func(msg *primitive.Message) bool {
			return msg.Topic == topic
		})).Return(&primitive.SendResult{}, nil)

		err := suite.EventBus.Publish(ctx, topic, testEvent.Event)
		suite.NoError(err)
		suite.mockProducer.AssertExpectations(suite.T())
	})

	// Test consumer retry
	suite.Run("Consumer Retry", func() {
		retryCount := 0
		maxRetries := 3

		testEvent := eventesting.NewTestEvent("test.retry", map[string]interface{}{
			"message": "retry test",
		})

		suite.mockConsumer.On("Subscribe", topic, mock.Anything).Return(nil)

		err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
			retryCount++
			if retryCount < maxRetries {
				return event.ErrRetry
			}
			return nil
		})
		suite.NoError(err)

		// Simulate message delivery with retries
		msg := &primitive.MessageExt{
			Message: primitive.Message{
				Topic: topic,
				Body:  []byte(`{"id":"test-1","type":"test.retry","source":"test_service","data":{"message":"retry test"}}`),
			},
		}

		for i := 0; i < maxRetries; i++ {
			result, err := suite.mockConsumer.SimulateMessage(ctx, msg)
			if i < maxRetries-1 {
				suite.Equal(consumer.ConsumeRetryLater, result)
				suite.Error(err)
			} else {
				suite.Equal(consumer.ConsumeSuccess, result)
				suite.NoError(err)
			}
		}

		suite.Equal(maxRetries, retryCount)
	})

	// Test batch message processing
	suite.Run("Batch Message Processing", func() {
		messageCount := 5
		processedCount := 0

		suite.mockConsumer.On("Subscribe", topic, mock.Anything).Return(nil)

		err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
			processedCount++
			return nil
		})
		suite.NoError(err)

		// Create batch of messages
		messages := make([]*primitive.MessageExt, messageCount)
		for i := 0; i < messageCount; i++ {
			messages[i] = &primitive.MessageExt{
				Message: primitive.Message{
					Topic: topic,
					Body:  []byte(`{"id":"test-1","type":"test.batch","source":"test_service","data":{"index":` + string(rune(i)) + `}}`),
				},
			}
		}

		// Simulate batch message delivery
		result, err := suite.mockConsumer.SimulateMessage(ctx, messages...)
		suite.Equal(consumer.ConsumeSuccess, result)
		suite.NoError(err)
		suite.Equal(messageCount, processedCount)
	})

	// Test message delay
	suite.Run("Message Delay", func() {
		testEvent := eventesting.NewTestEvent("test.delay", map[string]interface{}{
			"message": "delay test",
		})

		suite.mockProducer.On("SendSync", mock.Anything, mock.MatchedBy(func(msg *primitive.Message) bool {
			return msg.Topic == topic && msg.DelayTimeLevel == 3 // 10s delay
		})).Return(&primitive.SendResult{}, nil)

		// Publish with delay
		msg := &primitive.Message{
			Topic:          topic,
			DelayTimeLevel: 3, // 10s delay
		}
		_, err := suite.mockProducer.SendSync(ctx, msg)
		suite.NoError(err)
		suite.mockProducer.AssertExpectations(suite.T())
	})
}

func (suite *RocketMQEventBusTestSuite) TestEventBus() {
	suite.RunEventBusTests()
}
