package tests

import (
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/message"
)

// Validate that Publish does not deadlock when no wildcard consumers are active.
func TestPublishDoesNotDeadlock(t *testing.T) {
	b := broker.New(nil)

	err := b.Publish("orders", []byte("hello"), nil, "normal", nil, nil, false)
	if err != nil {
		t.Fatalf("Publish returned an error: %v", err)
	}

	if len(b.Topics["orders"].Messages) != 1 {
		t.Fatalf("Expected 1 message in topic, got %d", len(b.Topics["orders"].Messages))
	}
}

// Validate topic name regex rules and edge cases.
func TestIsValidTopicName(t *testing.T) {
	b := broker.New(nil)

	validNames := []string{"orders", "orders.eu", "orders-eu_123", "orders::subgroup", "orders/eu"}
	for _, name := range validNames {
		if !b.IsValidTopicName(name) {
			t.Errorf("Expected '%s' to be valid, but it was rejected", name)
		}
	}

	invalidNames := []string{"orders\\eu", "../orders", "orders#123", ""}
	for _, name := range invalidNames {
		if b.TopicExists(name) || (len(name) > 0 && b.IsValidTopicName(name)) {
			t.Errorf("Expected '%s' to be invalid, but it was accepted", name)
		}
	}
}

// Validate manual topic creation and the ring buffer policy initialization.
func TestCreateTopicAndLimit(t *testing.T) {
	b := broker.New(nil)

	err := b.CreateTopic("sensor.data", "reject", 0)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	if !b.TopicExists("sensor.data") {
		t.Errorf("Topic should exist after creation")
	}
}

// Validate FIFO message consumption and acknowledgment logic.
func TestConsumeAndAck(t *testing.T) {
	b := broker.New(nil)

	b.Publish("alerts", []byte("msg1"), nil, "normal", nil, nil, false)

	msgID := b.Topics["alerts"].Messages[0].ID

	success := b.Ack("alerts", msgID)
	if !success {
		t.Errorf("Ack failed for message ID %s", msgID)
	}

	if len(b.Topics["alerts"].Messages) != 0 {
		t.Errorf("Expected 0 messages remaining, got %d", len(b.Topics["alerts"].Messages))
	}

	notifyChan := make(chan message.Message, 1)
	_, ok := b.Consume("alerts", 1, notifyChan)
	if ok {
		t.Errorf("Expected Consume to return false on empty queue")
	}
}

// Validate idempotency keys to prevent duplicate network requests.
func TestIdempotency(t *testing.T) {
	b := broker.New(nil)
	key := "tx-req-12345"

	if b.IsIdempotent(key) {
		t.Errorf("First time key should not be considered idempotent")
	}

	if !b.IsIdempotent(key) {
		t.Errorf("Second time key should be blocked as idempotent")
	}
}

// Validate wildcard pattern routing for topic subscriptions.
func TestWildcardRouting(t *testing.T) {
	b := broker.New(nil)

	notifyChan := make(chan message.Message, 1)
	b.Consume("events.*", 1, notifyChan)

	err := b.Publish("events.login", []byte("user_logged_in"), nil, "normal", nil, nil, false)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	select {
	case msg := <-notifyChan:
		if string(msg.Payload) != "user_logged_in" {
			t.Errorf("Expected 'user_logged_in', got '%s'", string(msg.Payload))
		}
	default:
		t.Errorf("Wildcard consumer did not receive the message! Routing is broken.")
	}
}

// Validate that messages are routed to the Dead Letter Queue after 3 failed retries.
func TestDLQAfterThreeRetries(t *testing.T) {
	b := broker.New(nil)

	b.Publish("tasks", []byte("fail_me"), nil, "normal", nil, nil, false)

	notifyChan := make(chan message.Message, 1)
	msgs, _ := b.Consume("tasks", 1, notifyChan)
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}

	msg := msgs[0]
	msg.RetryCount = 2
	b.Requeue(msg)

	if len(b.Topics["tasks"].Messages) != 0 {
		t.Errorf("Message should have been removed from the original topic")
	}

	if len(b.Topics["tasks.dlq"].Messages) != 1 {
		t.Fatalf("Message was not routed to the DLQ successfully")
	}
}

// Validate that messages with an expired TTL are automatically purged from RAM.
func TestTTLExpiration(t *testing.T) {
	b := broker.New(nil)

	now := time.Now()
	expiredTime := now.Add(-1 * time.Minute)

	b.Publish("ephemeral", []byte("too_late"), nil, "normal", &expiredTime, nil, false)

	notifyChan := make(chan message.Message, 1)
	msgs, ok := b.Consume("ephemeral", 1, notifyChan)

	if ok || len(msgs) > 0 {
		t.Errorf("Broker delivered an expired message!")
	}

	if len(b.Topics["ephemeral"].Messages) != 0 {
		t.Errorf("Expired message should have been purged from RAM")
	}
}

// Validate Fan-Out broadcast delivery to multiple listening workers.
func TestBroadcastMode(t *testing.T) {
	b := broker.New(nil)

	ch1 := make(chan message.Message, 1)
	ch2 := make(chan message.Message, 1)

	b.Consume("notifications", 1, ch1)
	b.Consume("notifications", 1, ch2)

	err := b.Publish("notifications", []byte("system_update"), nil, "normal", nil, nil, true)
	if err != nil {
		t.Fatalf("Broadcast publish failed: %v", err)
	}

	select {
	case <-ch1:
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Worker 1 missed the broadcast message")
	}

	select {
	case <-ch2:
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Worker 2 missed the broadcast message")
	}
}
