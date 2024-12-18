package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/everpan/idig/pkg/event"
	"sync"
	"time"
)

type KafkaEventBus struct {
	producer sarama.SyncProducer
	consumer sarama.Consumer
	handlers map[string][]func(*event.Event) error
	mu       sync.RWMutex
}

// NewKafkaEventBus creates a new Kafka event bus
func NewKafkaEventBus(brokers []string) (*KafkaEventBus, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &KafkaEventBus{
		producer: producer,
		consumer: consumer,
		handlers: make(map[string][]func(*event.Event) error),
	}, nil
}

func (k *KafkaEventBus) Publish(ctx context.Context, topic string, evt *event.Event) error {
	if err := evt.Validate(); err != nil {
		return fmt.Errorf("invalid event: %w", err)
	}

	data, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(data),
	}

	_, _, err = k.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	return nil
}

func (k *KafkaEventBus) Subscribe(ctx context.Context, topic string, handler func(*event.Event) error) error {
	k.mu.Lock()
	if k.handlers[topic] == nil {
		k.handlers[topic] = make([]func(*event.Event) error, 0)
	}
	k.handlers[topic] = append(k.handlers[topic], handler)
	k.mu.Unlock()

	partitionConsumer, err := k.consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("failed to create partition consumer: %w", err)
	}

	go func() {
		defer partitionConsumer.Close()

		for {
			select {
			case msg := <-partitionConsumer.Messages():
				var evt event.Event
				if err := json.Unmarshal(msg.Value, &evt); err != nil {
					// Log error and continue
					continue
				}

				if err := evt.Validate(); err != nil {
					// Log error and continue
					continue
				}

				k.mu.RLock()
				handlers := k.handlers[topic]
				k.mu.RUnlock()

				for _, h := range handlers {
					maxRetries := 3
					retryCount := 0
					for {
						if err := h(&evt); err != nil {
							if err == event.ErrRetry {
								if retryCount < maxRetries {
									retryCount++
									time.Sleep(time.Duration(retryCount) * time.Second)
									continue
								}
							}
							// Log other errors and break retry loop
							break
						}
						break // Success, break retry loop
					}
				}
			case err := <-partitionConsumer.Errors():
				// Log consumer errors
				if err != nil {
					// Consider implementing a proper error handling strategy
					continue
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func (k *KafkaEventBus) Unsubscribe(topic string) error {
	k.mu.Lock()
	delete(k.handlers, topic)
	k.mu.Unlock()
	return nil
}

func (k *KafkaEventBus) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()

	var errs []error
	if err := k.producer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close producer: %w", err))
	}
	if err := k.consumer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close consumer: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to close kafka event bus: %v", errs)
	}
	return nil
}
