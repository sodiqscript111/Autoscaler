package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"autoscaler/internal/model"

	kafkago "github.com/segmentio/kafka-go"
)

var writer *kafkago.Writer
var reader *kafkago.Reader

func InitKafkaWriter(brokers []string, configuredTopic string) {
	if len(brokers) == 0 {
		brokers = []string{"localhost:9094"}
	}
	if configuredTopic == "" {
		configuredTopic = "events"
	}
	writer = &kafkago.Writer{
		Addr:     kafkago.TCP(brokers...),
		Topic:    configuredTopic,
		Balancer: &kafkago.LeastBytes{},
	}
}

func InitKafkaReader(brokers []string, configuredTopic, groupID string) {
	if len(brokers) == 0 {
		brokers = []string{"localhost:9094"}
	}
	if configuredTopic == "" {
		configuredTopic = "events"
	}
	if groupID == "" {
		groupID = "autoscaler-group"
	}
	reader = kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   configuredTopic,
		GroupID: groupID,
	})
}

func CloseKafkaWriter() error {
	if writer == nil {
		return nil
	}

	return writer.Close()
}

func CloseKafkaReader() error {
	if reader == nil {
		return nil
	}
	return reader.Close()
}

func WriteToKafka(eventData model.Event) error {
	if writer == nil {
		return fmt.Errorf("kafka writer is not initialized")
	}

	value, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return writer.WriteMessages(ctx,
		kafkago.Message{
			Key:   []byte(strconv.FormatInt(eventData.ID, 10)),
			Value: value,
		},
	)
}

func ReaderStats() kafkago.ReaderStats {
	if reader == nil {
		return kafkago.ReaderStats{}
	}

	return reader.Stats()
}

func FetchMessage(ctx context.Context) (kafkago.Message, error) {
	if reader == nil {
		return kafkago.Message{}, fmt.Errorf("kafka reader is not initialized")
	}

	return reader.FetchMessage(ctx)
}

func CommitMessages(ctx context.Context, messages ...kafkago.Message) error {
	if reader == nil {
		return fmt.Errorf("kafka reader is not initialized")
	}

	return reader.CommitMessages(ctx, messages...)
}

func FetchAndProcess(ctx context.Context) error {
	msg, err := FetchMessage(ctx)
	if err != nil {
		return fmt.Errorf("fetch message: %w", err)
	}

	var event model.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("decode event: %w", err)
	}

	fmt.Printf("message: key=%s event=%+v\n", string(msg.Key), event)

	if err := CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("commit messages: %w", err)
	}

	return nil
}

func CurrentConsumerLag() int64 {
	if reader == nil {
		return 0
	}
	return ReaderStats().Lag
}
