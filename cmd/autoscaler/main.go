package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"autoscaler/internal/api"
	"autoscaler/internal/kafka"

	"github.com/gin-gonic/gin"
	kafkago "github.com/segmentio/kafka-go"
)

var (
	normalThroughput    = 1000
	throughputState     = 0 // -1 = below normal, 0 = normal, 1 = above normal
	batchSize           = 100
	idealQueueSize      = 100
	processedThisSecond atomic.Int64
)

func main() {
	kafka.InitKafkaWriter()
	kafka.InitKafkaReader()
	defer kafka.CloseKafkaWriter()
	defer kafka.CloseKafkaReader()

	go worker()

	r := gin.Default()
	r.POST("/events", api.Injectionpoint)
	go r.Run(":8080") // Important: Run gin asynchronously to avoid blocking main ticker

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		currentThroughput := processedThisSecond.Swap(0)

		stats := kafka.ReaderStats()
		queueSize := int(stats.Lag)
		if queueSize < 0 {
			queueSize = 0
		}

		queueLag := checkQueueLag(queueSize)

		increaseOrDecreaseThroughput(int(currentThroughput))

		fmt.Printf(
			"throughput=%d msg/s | queueSize=%d | queueLag=%v | state=%d | batchSize=%d\n",
			currentThroughput,
			queueSize,
			queueLag,
			throughputState,
			batchSize,
		)
	}
}

func checkQueueLag(queueSize int) bool {
	return queueSize > idealQueueSize
}

func increaseOrDecreaseThroughput(currentThroughput int) {
	upperThreshold := int(float64(normalThroughput) * 1.2) // 20% above normal
	lowerThreshold := int(float64(normalThroughput) * 0.8) // 20% below normal

	switch {
	case currentThroughput > upperThreshold:
		throughputState = 1
		// throughput is high, maybe increase workers if queue is also growing
	case currentThroughput < lowerThreshold:
		throughputState = -1
		// throughput is low, maybe decrease workers if backlog is low too
	default:
		throughputState = 0
	}
}

func worker() {
	for {
		batch := make([]kafkago.Message, 0, batchSize)
		ctx := context.Background()

		// block until at least one item is available
		item, err := kafka.FetchMessage(ctx)
		if err == nil {
			batch = append(batch, item)
		} else {
			time.Sleep(100 * time.Millisecond) // Sleep on error to avoid tight loop
			continue
		}

		ctxTimeout, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		// try to fill the rest of the batch without blocking forever
		for len(batch) < batchSize {
			item, err := kafka.FetchMessage(ctxTimeout)
			if err != nil {
				// timeout or error
				break
			}
			batch = append(batch, item)
		}
		cancel()

		if len(batch) > 0 {
			// simulate processing time
			time.Sleep(50 * time.Millisecond)

			// Commit messages in batch
			if err := kafka.CommitMessages(ctx, batch...); err != nil {
				fmt.Println("failed to commit messages:", err)
			}
			processedThisSecond.Add(int64(len(batch)))
		}
	}
}
