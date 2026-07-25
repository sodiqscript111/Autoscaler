package model

import "time"

type Event struct {
	ID        int64     `json:"id,omitempty" bson:"id"`
	UserID    string    `json:"user_id" bson:"user_id"`
	Action    string    `json:"action" bson:"action"`
	Element   string    `json:"element" bson:"element"`
	Duration  float64   `json:"duration" bson:"duration"`
	Timestamp time.Time `json:"timestamp" bson:"timestamp"`
}
