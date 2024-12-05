package kafka

import (
	"context"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/ever/idig/pkg/event"
	"sync"
)

type KafkaEventBus struct {
	producer sarama.SyncProducer
	consumer sarama.Consumer
	handlers map[string][]func(event.Event) error
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
		handlers: make(map[string][]func(event.Event) error),
	}, nil
}

func (k *KafkaEventBus) Publish(ctx context.Context, topic string, evt event.Event) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(data),
	}

	_, _, err = k.producer.SendMessage(msg)
	return err
}

func (k *KafkaEventBus) Subscribe(ctx context.Context, topic string, handler func(event.Event) error) error {
	k.mu.Lock()
	if k.handlers[topic] == nil {
		k.handlers[topic] = make([]func(event.Event) error, 0)
	}
	k.handlers[topic] = append(k.handlers[topic], handler)
	k.mu.Unlock()

	partitionConsumer, err := k.consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				var evt event.Event
				if err := json.Unmarshal(msg.Value, &evt); err != nil {
					continue
				}

				k.mu.RLock()
				handlers := k.handlers[topic]
				k.mu.RUnlock()

				for _, h := range handlers {
					if err := h(evt); err != nil {
						// Handle error (could log it or implement retry logic)
					}
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
	if err := k.producer.Close(); err != nil {
		return err
	}
	return k.consumer.Close()
}
