package message

import (
	"time"
)

type Message struct {
	ID        string    `json:"id"`
	Topic     string    `json:"topic"`
	Payload   []byte    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}