package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/ever/idig/pkg/event"
	eventesting "github.com/ever/idig/pkg/event/testing"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type KafkaEventBusTestSuite struct {
	eventesting.EventBusTestSuite
	mockProducer *mocks.SyncProducer
	mockConsumer *mocks.Consumer
}

func (suite *KafkaEventBusTestSuite) SetupTest() {
	suite.mockProducer = mocks.NewSyncProducer(suite.T(), nil)
	suite.mockConsumer = mocks.NewConsumer(suite.T(), nil)

	eventBus := &KafkaEventBus{
		producer: suite.mockProducer,
		consumer: suite.mockConsumer,
		handlers: make(map[string][]func(event.Event) error),
	}
	suite.EventBus = eventBus
}

func (suite *KafkaEventBusTestSuite) TearDownTest() {
	if suite.EventBus != nil {
		suite.EventBus.Close()
	}
}

func TestKafkaEventBusSuite(t *testing.T) {
	suite.Run(t, new(KafkaEventBusTestSuite))
}

// TestKafkaSpecificFeatures tests Kafka-specific features
func (suite *KafkaEventBusTestSuite) TestKafkaSpecificFeatures() {
	ctx := context.Background()
	topic := "test.kafka.specific"

	// Test message acknowledgment
	suite.Run("Message Acknowledgment", func() {
		testEvent := eventesting.NewTestEvent("test.ack", map[string]interface{}{
			"message": "ack test",
		})

		// Setup producer expectations
		suite.mockProducer.ExpectSendMessageWithCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
			suite.Equal(topic, msg.Topic)
			return nil
		})

		err := suite.EventBus.Publish(ctx, topic, testEvent.Event)
		suite.NoError(err)
	})

	// Test consumer group behavior
	suite.Run("Consumer Group Behavior", func() {
		testEvent := eventesting.NewTestEvent("test.consumer", map[string]interface{}{
			"message": "consumer test",
		})

		processed := make(chan bool)
		err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
			processed <- true
			return nil
		})
		suite.NoError(err)

		// Create mock partition consumer
		mockPartitionConsumer := suite.mockConsumer.ExpectConsumePartition(topic, 0, sarama.OffsetNewest)

		// Simulate message arrival
		go func() {
			mockPartitionConsumer.YieldMessage(&sarama.ConsumerMessage{
				Topic: topic,
				Value: []byte(fmt.Sprintf(`{"id":"test-1","type":"test.consumer","source":"test_service","data":{"message":"consumer test"}}`)),
			})
		}()

		select {
		case <-processed:
			// Success
		case <-time.After(5 * time.Second):
			suite.Fail("Timeout waiting for message processing")
		}
	})

	// Test message retry
	suite.Run("Message Retry", func() {
		retryCount := 0
		maxRetries := 3

		err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
			retryCount++
			if retryCount < maxRetries {
				return event.ErrRetry
			}
			return nil
		})
		suite.NoError(err)

		testEvent := eventesting.NewTestEvent("test.retry", map[string]interface{}{
			"message": "retry test",
		})

		// Setup producer expectations for retries
		for i := 0; i < maxRetries; i++ {
			suite.mockProducer.ExpectSendMessageWithCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
				suite.Equal(topic, msg.Topic)
				return nil
			})
		}

		err = suite.EventBus.Publish(ctx, topic, testEvent.Event)
		suite.NoError(err)

		// Verify retry count
		suite.Equal(maxRetries, retryCount)
	})

	// Test message ordering
	suite.Run("Message Ordering", func() {
		messageCount := 5
		receivedOrder := make([]int, 0, messageCount)

		err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
			if order, ok := e.Data["order"].(float64); ok {
				receivedOrder = append(receivedOrder, int(order))
			}
			return nil
		})
		suite.NoError(err)

		// Setup mock partition consumer
		mockPartitionConsumer := suite.mockConsumer.ExpectConsumePartition(topic, 0, sarama.OffsetNewest)

		// Send messages in order
		go func() {
			for i := 0; i < messageCount; i++ {
				mockPartitionConsumer.YieldMessage(&sarama.ConsumerMessage{
					Topic: topic,
					Value: []byte(fmt.Sprintf(`{"id":"test-%d","type":"test.order","source":"test_service","data":{"order":%d}}`, i, i)),
				})
			}
		}()

		// Wait for all messages
		time.Sleep(2 * time.Second)

		// Verify message order
		suite.Equal(messageCount, len(receivedOrder))
		for i := 0; i < messageCount; i++ {
			suite.Equal(i, receivedOrder[i])
		}
	})
}

func (suite *KafkaEventBusTestSuite) TestEventBus() {
	suite.RunEventBusTests()
}
