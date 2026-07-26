package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"autoscaler/internal/api"
	"autoscaler/internal/config"
	"autoscaler/internal/processing"
	"autoscaler/internal/rabbitmq"
	"autoscaler/internal/workers"

	"github.com/gin-gonic/gin"
)

func main() {
	cfg, err := config.Load("")
	if err != nil {
		fmt.Println("config error:", err)
		os.Exit(1)
	}

	if err := rabbitmq.InitRabbitMQ(cfg.RabbitMQ.URL, cfg.RabbitMQ.QueueName); err != nil {
		fmt.Println("rabbitmq init error:", err)
		os.Exit(1)
	}
	defer rabbitmq.CloseRabbitMQ()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	processor, err := processing.NewMongoRedisProcessor(ctx, cfg, nil)
	if err != nil {
		fmt.Println("processor startup error:", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := processor.Close(shutdownCtx); err != nil {
			fmt.Println("processor shutdown error:", err)
		}
	}()

	manager := workers.NewManager(ctx, nil, workers.Config{
		InitialWorkers:   5,
		MinWorkers:       5,
		MaxWorkers:       5,
		InitialBatchSize: cfg.Workers.MaxBatchSize, // Max efficiency
		MinBatchSize:     cfg.Workers.MaxBatchSize,
		MaxBatchSize:     cfg.Workers.MaxBatchSize,
		BatchStep:        0,
		Processor:        processor,
		Downstream:       nil,
	})
	defer manager.Shutdown()

	r := gin.Default()
	r.POST("/events", api.ReceiveEvent)
	r.GET("/healthz", api.Healthz)
	go func() {
		if err := r.Run(cfg.API.Address); err != nil {
			fmt.Println("api server error:", err)
			stop()
		}
	}()

	fmt.Println("Worker started with 5 goroutines. Waiting for events...")
	<-ctx.Done()
	fmt.Println("Shutting down worker...")
}
