package api

import (
	"autoscaler/internal/kafka"
	"autoscaler/internal/model"

	"github.com/gin-gonic/gin"
)

func Injectionpoint(c *gin.Context) {
	var event model.Event

	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	if err := kafka.WriteToKafka(event); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, gin.H{"message": "event sent"})
}
