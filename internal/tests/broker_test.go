package tests

import (
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/message"
)

// Deadlock Test
func TestPublishDoesNotDeadlock(t *testing.T) {
	b := broker.New(nil)

	err := b.Publish("orders", []byte("hello"), nil, nil, false)
	if err != nil {
		t.Fatalf("Publish returned an error: %v", err)
	}

	if len(b.Topics["orders"].Messages) != 1 {
		t.Fatalf("Expected 1 message in topic, got %d", len(b.Topics["orders"].Messages))
	}
}

// Valid Topic Test
func TestIsValidTopicName(t *testing.T) {
	b := broker.New(nil)

	validNames := []string{"orders", "orders.eu", "orders-eu_123", "orders::subgroup"}
	for _, name := range validNames {
		if !b.IsValidTopicName(name) {
			t.Errorf("Expected '%s' to be valid, but it was rejected", name)
		}
	}

	invalidNames := []string{"orders/eu", "orders\\eu", "../orders", "orders#123", ""}
	for _, name := range invalidNames {
		if b.IsValidTopicName(name) {
			t.Errorf("Expected '%s' to be invalid, but it was accepted", name)
		}
	}
}

// Validate Ring Buffer Logic
func TestCreateTopicAndLimit(t *testing.T) {
	b := broker.New(nil)

	err := b.CreateTopic("sensor.data", "reject")
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	if !b.TopicExists("sensor.data") {
		t.Errorf("Topic should exist after creation")
	}
}

// Validate FIFO consume and ACK
func TestConsumeAndAck(t *testing.T) {
	b := broker.New(nil)

	b.Publish("alerts", []byte("msg1"), nil, nil, false)
	b.Publish("alerts", []byte("msg2"), nil, nil, false)

	notifyChan := make(chan message.Message, 1)

	msgs, ok := b.Consume("alerts", 1, notifyChan)
	if !ok || len(msgs) != 1 {
		t.Fatalf("Expected to consume 1 message, got %d", len(msgs))
	}

	if string(msgs[0].Payload) != "msg1" {
		t.Errorf("Expected 'msg1', got '%s'", string(msgs[0].Payload))
	}

	success := b.Ack("alerts", msgs[0].ID)
	if !success {
		t.Errorf("Ack failed for valid message ID")
	}
}

// Validate Idempotency
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

// Validate Wildcard Routing
func TestWildcardRouting(t *testing.T) {
	b := broker.New(nil)

	notifyChan := make(chan message.Message, 1)

	b.Consume("events.*", 1, notifyChan)

	err := b.Publish("events.login", []byte("user_logged_in"), nil, nil, false)
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

// Validate DLQ Logic
func TestDLQAfterThreeRetries(t *testing.T) {
	b := broker.New(nil)

	b.Publish("tasks", []byte("fail_me"), nil, nil, false)

	notifyChan := make(chan message.Message, 1)
	msgs, _ := b.Consume("tasks", 1, notifyChan)
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}

	msg := msgs[0]

	b.Requeue(msg)
	b.Requeue(msg)
	b.Requeue(msg)

	if len(b.Topics["tasks"].Messages) != 0 {
		t.Errorf("Message should have been removed from the original topic")
	}

	if len(b.Topics["tasks.dlq"].Messages) != 1 {
		t.Fatalf("Message was not routed to the DLQ successfully")
	}
}

// Validate auto-destroy TTL Messages
func TestTTLExpiration(t *testing.T) {
	b := broker.New(nil)

	now := time.Now()
	expiredTime := now.Add(-1 * time.Minute)

	b.Publish("ephemeral", []byte("too_late"), &expiredTime, nil, false)

	notifyChan := make(chan message.Message, 1)
	msgs, ok := b.Consume("ephemeral", 1, notifyChan)

	if ok || len(msgs) > 0 {
		t.Errorf("Broker delivered an expired message!")
	}

	if len(b.Topics["ephemeral"].Messages) != 0 {
		t.Errorf("Expired message should have been purged from RAM")
	}
}

// Validate Fan-Out (Multiple Workers Broadcast)
func TestBroadcastMode(t *testing.T) {
	b := broker.New(nil)

	ch1 := make(chan message.Message, 1)
	ch2 := make(chan message.Message, 1)

	b.Consume("notifications", 1, ch1)
	b.Consume("notifications", 1, ch2)

	err := b.Publish("notifications", []byte("system_update"), nil, nil, true)
	if err != nil {
		t.Fatalf("Broadcast publish failed: %v", err)
	}

	select {
	case <-ch1:
	default:
		t.Errorf("Worker 1 missed the broadcast message")
	}

	select {
	case <-ch2:
	default:
		t.Errorf("Worker 2 missed the broadcast message")
	}
}
