package api

import (
	"net/http"
	"sync/atomic"
	"time"

	"autoscaler/internal/kafka"
	"autoscaler/internal/model"
	"autoscaler/internal/scaler"

	"github.com/gin-gonic/gin"
)

var (
	backpressureEnabled atomic.Bool
	throughputWindow    *scaler.ThroughputWindow
)

func ConfigureRuntime(window *scaler.ThroughputWindow) {
	throughputWindow = window
}

func SetBackpressureEnabled(enabled bool) {
	backpressureEnabled.Store(enabled)
}

func BackpressureEnabled() bool {
	return backpressureEnabled.Load()
}

func Injectionpoint(c *gin.Context) {
	if backpressureEnabled.Load() {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "backpressure enabled; retry later"})
		return
	}

	var event model.Event

	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if event.ID == 0 {
		event.ID = time.Now().UnixNano()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}

	if err := kafka.WriteToKafka(event); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if throughputWindow != nil {
		throughputWindow.IncrementIncoming()
	}

	c.JSON(http.StatusOK, gin.H{"message": "event sent"})
}
