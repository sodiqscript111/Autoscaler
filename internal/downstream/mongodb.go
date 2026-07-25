package downstream

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type MongoDBConfig struct {
	Name     string
	Policy   Policy
	Interval time.Duration
}

type MongoDBChecker struct {
	monitor *Monitor
	client  *mongo.Client
	config  MongoDBConfig
}

func NewMongoDBChecker(monitor *Monitor, client *mongo.Client, config MongoDBConfig) *MongoDBChecker {
	if config.Name == "" {
		config.Name = "mongodb"
	}
	if config.Policy == "" {
		config.Policy = PolicyCritical
	}
	if config.Interval <= 0 {
		config.Interval = time.Second
	}

	return &MongoDBChecker{
		monitor: monitor,
		client:  client,
		config:  config,
	}
}

func (c *MongoDBChecker) Start(ctx context.Context) {
	if c == nil || c.monitor == nil || c.client == nil {
		return
	}

	runCheckerLoop(ctx, c.config.Interval, c.checkOnce)
}

func (c *MongoDBChecker) checkOnce(ctx context.Context) {
	started := time.Now()
	err := c.client.Ping(ctx, readpref.Primary())

	c.monitor.RecordPing(c.config.Name, KindMongoDB, c.config.Policy, time.Since(started), err)
}
