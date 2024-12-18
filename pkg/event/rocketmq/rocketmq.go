package rocketmq

import (
	"context"
	"encoding/json"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/everpan/idig/pkg/event"
	"sync"
	"time"
)

type RocketMQEventBus struct {
	producer rocketmq.Producer
	consumer rocketmq.PushConsumer
	handlers map[string][]func(*event.Event) error
	mu       sync.RWMutex
	group    string
}

type RocketMQConfig struct {
	Endpoints []string
	Group     string
}

// NewRocketMQEventBus creates a new RocketMQ event bus
func NewRocketMQEventBus(config RocketMQConfig) (*RocketMQEventBus, error) {
	p, err := rocketmq.NewProducer(
		producer.WithNameServer(config.Endpoints),
		producer.WithGroupName(config.Group),
		producer.WithRetry(2),
	)
	if err != nil {
		return nil, err
	}

	if err := p.Start(); err != nil {
		return nil, err
	}

	c, err := rocketmq.NewPushConsumer(
		consumer.WithNameServer(config.Endpoints),
		consumer.WithGroupName(config.Group),
	)
	if err != nil {
		p.Shutdown()
		return nil, err
	}

	return &RocketMQEventBus{
		producer: p,
		consumer: c,
		handlers: make(map[string][]func(*event.Event) error),
		group:    config.Group,
	}, nil
}

func (r *RocketMQEventBus) Publish(ctx context.Context, topic string, evt *event.Event) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	msg := &primitive.Message{
		Topic: topic,
		Body:  data,
	}

	_, err = r.producer.SendSync(ctx, msg)
	return err
}

func (r *RocketMQEventBus) Subscribe(ctx context.Context, topic string, handler func(*event.Event) error) error {
	r.mu.Lock()
	if r.handlers[topic] == nil {
		r.handlers[topic] = make([]func(*event.Event) error, 0)
	}
	r.handlers[topic] = append(r.handlers[topic], handler)
	r.mu.Unlock()

	selector := consumer.MessageSelector{
		Type:       consumer.TAG,
		Expression: "*",
	}

	err := r.consumer.Subscribe(topic, selector, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			var evt event.Event
			if err := json.Unmarshal(msg.Body, &evt); err != nil {
				continue
			}

			r.mu.RLock()
			handlers := r.handlers[topic]
			r.mu.RUnlock()

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
							return consumer.ConsumeRetryLater, err
						}
						// Other errors, don't retry
						return consumer.ConsumeRetryLater, err
					}
					break // Success, break retry loop
				}
			}
		}
		return consumer.ConsumeSuccess, nil
	})

	if err != nil {
		return err
	}

	return r.consumer.Start()
}

func (r *RocketMQEventBus) Unsubscribe(topic string) error {
	r.mu.Lock()
	delete(r.handlers, topic)
	r.mu.Unlock()

	return r.consumer.Unsubscribe(topic)
}

func (r *RocketMQEventBus) Close() error {
	r.producer.Shutdown()
	return r.consumer.Shutdown()
}
