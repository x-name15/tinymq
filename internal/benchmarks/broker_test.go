package benchmarks

import (
	"testing"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/message"
	"github.com/x-name15/tinymq/internal/storage"
)

func BenchmarkBrokerPublish(b *testing.B) {
	brk := broker.New(nil)
	topic := "bench"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte("hello world")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = brk.Publish(topic, payload, nil, "normal", nil, nil, false)
	}
}

func BenchmarkBrokerPublishWithStorage(b *testing.B) {
	store, err := storage.New(b.TempDir(), false)
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer store.CloseAll()

	brk := broker.New(store)
	topic := "bench"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte("hello world")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = brk.Publish(topic, payload, nil, "normal", nil, nil, false)
	}
}

func BenchmarkBrokerPublishPriority(b *testing.B) {
	brk := broker.New(nil)
	topic := "bench"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte("hello world")
	priorities := []string{"high", "normal", "low"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = brk.Publish(topic, payload, nil, priorities[i%3], nil, nil, false)
	}
}

func BenchmarkBrokerConsume(b *testing.B) {
	brk := broker.New(nil)
	topic := "bench"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte("hello world")

	// Pre-fill the queue with 1000 messages.
	for i := 0; i < 1000; i++ {
		_ = brk.Publish(topic, payload, nil, "normal", nil, nil, false)
	}

	notifyChan := make(chan message.Message, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs, _ := brk.Consume(topic, 1, notifyChan)
		if len(msgs) > 0 {
			brk.Ack(topic, msgs[0].ID)
		}
	}
}

func BenchmarkBrokerAck(b *testing.B) {
	brk := broker.New(nil)
	topic := "bench"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte("hello world")

	_ = brk.Publish(topic, payload, nil, "normal", nil, nil, false)
	notifyChan := make(chan message.Message, 1)
	msgs, _ := brk.Consume(topic, 1, notifyChan)
	if len(msgs) == 0 {
		b.Fatal("no message to ack")
	}
	msgID := msgs[0].ID

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i > 0 {
			_ = brk.Publish(topic, payload, nil, "normal", nil, nil, false)
			msgs, _ := brk.Consume(topic, 1, notifyChan)
			if len(msgs) > 0 {
				msgID = msgs[0].ID
			}
		}
		brk.Ack(topic, msgID)
	}
}

func BenchmarkBrokerAddSpy(b *testing.B) {
	brk := broker.New(nil)
	topic := "bench"
	brk.CreateTopic(topic, "reject", 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch, _ := brk.AddSpy(topic)
		brk.RemoveSpy(topic, ch)
	}
}

func BenchmarkBrokerPeek(b *testing.B) {
	brk := broker.New(nil)
	topic := "bench"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte("hello world")

	// Pre-fill with 100 messages.
	for i := 0; i < 100; i++ {
		_ = brk.Publish(topic, payload, nil, "normal", nil, nil, false)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brk.Peek(topic, 10)
	}
}

func BenchmarkBrokerCreateTopic(b *testing.B) {
	brk := broker.New(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := "bench_" + string(rune(i))
		_ = brk.CreateTopic(name, "reject", 0)
	}
}

func BenchmarkBrokerPublishBroadcast(b *testing.B) {
	brk := broker.New(nil)
	topic := "bench"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte("hello world")
	for i := 0; i < 10; i++ {
		notifyChan := make(chan message.Message, 1)
		brk.Consume(topic, 1, notifyChan)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = brk.Publish(topic, payload, nil, "normal", nil, nil, true)
	}
}
