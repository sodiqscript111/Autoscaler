package downstream

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type MongoDBChecker struct {
	monitor  *Monitor
	client   *mongo.Client
	interval time.Duration
	name     string
	policy   Policy
}

func NewMongoDBChecker(monitor *Monitor, client *mongo.Client, name string, policy Policy, interval time.Duration) *MongoDBChecker {
	if name == "" {
		name = "mongodb"
	}
	if policy == "" {
		policy = PolicyCritical
	}
	if interval <= 0 {
		interval = time.Second
	}

	return &MongoDBChecker{
		monitor:  monitor,
		client:   client,
		interval: interval,
		name:     name,
		policy:   policy,
	}
}

func (c *MongoDBChecker) Start(ctx context.Context) {
	if c == nil || c.monitor == nil || c.client == nil {
		return
	}

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	c.checkOnce(ctx)
	for {
		select {
		case <-ticker.C:
			c.checkOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (c *MongoDBChecker) checkOnce(ctx context.Context) {
	started := time.Now()
	err := c.client.Ping(ctx, readpref.Primary())

	sample := Sample{
		Name:      c.name,
		Kind:      KindMongoDB,
		Operation: "ping",
		Policy:    c.policy,
		Duration:  time.Since(started),
		Success:   err == nil,
		Timestamp: time.Now(),
	}
	if err != nil {
		sample.Error = err.Error()
	}

	c.monitor.Record(sample)
}
