package model

import "time"

type Event struct {
	ID        int64     `json:"id,omitempty"`
	UserId    string    `json:"user_id"`
	Action    string    `json:"action"`
	Element   string    `json:"element"`
	Duration  float64   `json:"duration"`
	Timestamp time.Time `json:"timestamp"`
}
