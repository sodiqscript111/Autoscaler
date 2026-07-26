package processing

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"autoscaler/internal/config"
	"autoscaler/internal/downstream"
	"autoscaler/internal/model"
	"autoscaler/internal/redisx"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoRedisProcessor struct {
	monitor         *downstream.Monitor
	mongoClient     *mongo.Client
	mongoCollection *mongo.Collection
	redisClient     *redisx.Client
	redisCacheTTL   time.Duration
	redisKeyPrefix  string
	mongoDependency downstream.Dependency
	redisDependency downstream.Dependency
}

func NewMongoRedisProcessor(ctx context.Context, cfg config.Config, monitor *downstream.Monitor) (*MongoRedisProcessor, error) {
	mongoOptions := options.Client().ApplyURI(cfg.MongoDB.URI).SetTimeout(cfg.MongoDB.ConnectTimeout)
	mongoClient, err := mongo.Connect(mongoOptions)
	if err != nil {
		return nil, fmt.Errorf("connect mongodb: %w", err)
	}
	if err := mongoClient.Ping(ctx, nil); err != nil {
		_ = mongoClient.Disconnect(context.Background())
		return nil, fmt.Errorf("ping mongodb: %w", err)
	}

	redisClient := redisx.NewClient(redisx.Config{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
		Timeout:  cfg.Redis.ConnectTimeout,
	})
	if err := redisClient.Ping(ctx); err != nil {
		_ = mongoClient.Disconnect(context.Background())
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return &MongoRedisProcessor{
		monitor:         monitor,
		mongoClient:     mongoClient,
		mongoCollection: mongoClient.Database(cfg.MongoDB.Database).Collection(cfg.MongoDB.Collection),
		redisClient:     redisClient,
		redisCacheTTL:   cfg.Processing.RedisCacheTTL,
		redisKeyPrefix:  cfg.Processing.RedisKeyPrefix,
		mongoDependency: downstream.Dependency{
			Name:      "mongodb",
			Kind:      downstream.KindMongoDB,
			Operation: "insert_many",
			Policy:    downstream.ParsePolicy(cfg.MongoDB.Policy, downstream.PolicyCritical),
		},
		redisDependency: downstream.Dependency{
			Name:      "redis",
			Kind:      downstream.KindRedis,
			Operation: "set_batch",
			Policy:    downstream.ParsePolicy(cfg.Redis.Policy, downstream.PolicyProtective),
		},
	}, nil
}

func (p *MongoRedisProcessor) ProcessBatch(ctx context.Context, batch []amqp.Delivery) error {
	events, err := decodeEvents(batch)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	err = downstream.Track(ctx, p.monitor, p.mongoDependency, func(ctx context.Context) error {
		_, err := p.mongoCollection.InsertMany(ctx, events)
		return err
	})
	if err != nil {
		return fmt.Errorf("store batch in mongodb: %w", err)
	}

	err = downstream.Track(ctx, p.monitor, p.redisDependency, func(ctx context.Context) error {
		for _, event := range events {
			payload, err := json.Marshal(event)
			if err != nil {
				return fmt.Errorf("marshal event %d for redis: %w", event.ID, err)
			}

			if err := p.redisClient.Set(ctx, p.redisKey(event), string(payload), p.redisCacheTTL); err != nil {
				return fmt.Errorf("cache event %d in redis: %w", event.ID, err)
			}
		}
		return nil
	})

	return err
}

func (p *MongoRedisProcessor) MongoClient() *mongo.Client {
	return p.mongoClient
}

func (p *MongoRedisProcessor) Close(ctx context.Context) error {
	if p.mongoClient == nil {
		return nil
	}

	return p.mongoClient.Disconnect(ctx)
}

func (p *MongoRedisProcessor) redisKey(event model.Event) string {
	return fmt.Sprintf("%s%d", p.redisKeyPrefix, event.ID)
}

func decodeEvents(batch []amqp.Delivery) ([]model.Event, error) {
	events := make([]model.Event, 0, len(batch))

	for _, msg := range batch {
		var event model.Event
		if err := json.Unmarshal(msg.Body, &event); err != nil {
			return nil, fmt.Errorf("decode rabbitmq message: %w", err)
		}
		events = append(events, event)
	}

	return events, nil
}
