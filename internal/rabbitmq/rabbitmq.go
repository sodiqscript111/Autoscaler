package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"autoscaler/internal/model"
)

var (
	conn    *amqp.Connection
	channel *amqp.Channel
	q       amqp.Queue
	mu      sync.Mutex
)

func InitRabbitMQ(url, queueName string) error {
	var err error
	conn, err = amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err = conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %w", err)
	}

	q, err = channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare a queue: %w", err)
	}

	return nil
}

func CloseRabbitMQ() {
	if channel != nil {
		channel.Close()
	}
	if conn != nil {
		conn.Close()
	}
}

func Publish(eventData model.Event) error {
	mu.Lock()
	defer mu.Unlock()
	
	if channel == nil {
		return fmt.Errorf("rabbitmq channel is not initialized")
	}

	body, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	return channel.PublishWithContext(context.Background(),
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
}

// FetchMessage retrieves a single message from the queue synchronously.
// This mimics the Kafka polling behavior for our batch workers.
func FetchMessage(ctx context.Context) (amqp.Delivery, error) {
	mu.Lock()
	defer mu.Unlock()

	if channel == nil {
		return amqp.Delivery{}, fmt.Errorf("rabbitmq channel is not initialized")
	}

	msg, ok, err := channel.Get(q.Name, false)
	if err != nil {
		return amqp.Delivery{}, err
	}
	if !ok {
		return amqp.Delivery{}, fmt.Errorf("no message available")
	}
	return msg, nil
}

// CurrentQueueDepth returns the number of messages in the queue.
func CurrentQueueDepth() int64 {
	mu.Lock()
	defer mu.Unlock()

	if channel == nil {
		return 0
	}
	// Re-declare the queue to get the latest message count.
	// Passive declare retrieves the current state without recreating.
	state, err := channel.QueueInspect(q.Name)
	if err != nil {
		return 0
	}
	return int64(state.Messages)
}

// CommitMessages acknowledges a slice of messages.
func CommitMessages(ctx context.Context, messages ...amqp.Delivery) error {
	mu.Lock()
	defer mu.Unlock()

	for _, msg := range messages {
		// Acknowledge the message
		if err := msg.Ack(false); err != nil {
			return fmt.Errorf("failed to ack message: %w", err)
		}
	}
	return nil
}
