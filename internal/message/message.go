package message

import (
	"time"
)

type Message struct {
	ID         string     `json:"id"`
	Topic      string     `json:"topic"`
	Payload    []byte     `json:"payload"`
	Timestamp  time.Time  `json:"timestamp"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	DeliverAt  *time.Time `json:"deliver_at,omitempty"`
	RetryCount int        `json:"retry_count"`
}
