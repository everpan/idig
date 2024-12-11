package kafka

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/everpan/idig/pkg/event"
	eventesting "github.com/everpan/idig/pkg/event/testing"
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
		_ = suite.EventBus.Close()
	}
	suite.mockProducer.Close()
	suite.mockConsumer.Close()
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
		// func(val []byte) error
		// Setup producer expectations
		suite.mockProducer.ExpectSendMessageWithCheckerFunctionAndSucceed(func(val []byte) error {
			//msg, ok := val.(*sarama.ProducerMessage)
			//if !ok {
			//	return false
			//}
			//return msg.Topic == topic
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

		processed := make(chan bool, 1) // 使用带缓冲的通道
		defer close(processed)

		err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
			select {
			case processed <- true:
			default:
			}
			return nil
		})
		suite.NoError(err)

		// Create mock partition consumer
		mockPartitionConsumer := suite.mockConsumer.ExpectConsumePartition(topic, 0, sarama.OffsetNewest)
		defer mockPartitionConsumer.Close()

		// Simulate message arrival
		testMessage := fmt.Sprintf(`{"id":"%s","type":"%s","source":"%s","data":%s,"timestamp":"%s"}`,
			testEvent.ID,
			testEvent.Type,
			testEvent.Source,
			`{"message":"consumer test"}`,
			testEvent.Timestamp.Format(time.RFC3339),
		)

		mockPartitionConsumer.YieldMessage(&sarama.ConsumerMessage{
			Topic: topic,
			Value: []byte(testMessage),
		})

		select {
		case <-processed:
			// Success
		case <-time.After(10 * time.Second): // 增加超时时间
			suite.Fail("Timeout waiting for message processing")
		}
	})

	// Test message retry
	suite.Run("Message Retry", func() {
		retryCount := 0
		maxRetries := 3
		processed := make(chan bool, 1)
		defer close(processed)

		err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
			retryCount++
			if retryCount < maxRetries {
				return event.ErrRetry
			}
			processed <- true
			return nil
		})
		suite.NoError(err)

		testEvent := eventesting.NewTestEvent("test.retry", map[string]interface{}{
			"message": "retry test",
		})

		// Create mock partition consumer
		mockPartitionConsumer := suite.mockConsumer.ExpectConsumePartition(topic, 0, sarama.OffsetNewest)
		defer mockPartitionConsumer.Close()

		// Simulate message arrival with retries
		testMessage := fmt.Sprintf(`{"id":"%s","type":"%s","source":"%s","data":%s,"timestamp":"%s"}`,
			testEvent.ID,
			testEvent.Type,
			testEvent.Source,
			`{"message":"retry test"}`,
			testEvent.Timestamp.Format(time.RFC3339),
		)

		// Send the message multiple times to simulate retries
		for i := 0; i < maxRetries; i++ {
			mockPartitionConsumer.YieldMessage(&sarama.ConsumerMessage{
				Topic: topic,
				Value: []byte(testMessage),
			})
		}

		select {
		case <-processed:
			suite.Equal(maxRetries, retryCount)
		case <-time.After(10 * time.Second):
			suite.Fail("Timeout waiting for message retries")
		}
	})

	// Test message ordering
	suite.Run("Message Ordering", func() {
		messageCount := 5
		receivedMessages := make(chan int, messageCount)
		defer close(receivedMessages)

		err := suite.EventBus.Subscribe(ctx, topic, func(e event.Event) error {
			if order, ok := e.Data["order"].(float64); ok {
				receivedMessages <- int(order)
			}
			return nil
		})
		suite.NoError(err)

		// Setup mock partition consumer
		mockPartitionConsumer := suite.mockConsumer.ExpectConsumePartition(topic, 0, sarama.OffsetNewest)
		defer mockPartitionConsumer.Close()

		// Send messages in order
		for i := 0; i < messageCount; i++ {
			testMessage := fmt.Sprintf(`{"id":"test-%d","type":"test.order","source":"test_service","data":{"order":%d},"timestamp":"%s"}`,
				i, i, time.Now().Format(time.RFC3339))
			mockPartitionConsumer.YieldMessage(&sarama.ConsumerMessage{
				Topic: topic,
				Value: []byte(testMessage),
			})
		}

		// Collect messages with timeout
		receivedOrder := make([]int, 0, messageCount)
		timeout := time.After(10 * time.Second)

		for i := 0; i < messageCount; i++ {
			select {
			case order := <-receivedMessages:
				receivedOrder = append(receivedOrder, order)
			case <-timeout:
				suite.Fail("Timeout waiting for messages")
				return
			}
		}

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
