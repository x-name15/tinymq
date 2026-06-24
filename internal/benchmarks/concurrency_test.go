package benchmarks

import (
	"sync"
	"testing"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/message"
	"github.com/x-name15/tinymq/internal/storage"
)

func BenchmarkConcurrentPublish(b *testing.B) {
	brk := broker.New(nil)
	topic := "bench/concurrent"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte("hello world")

	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = brk.Publish(topic, payload, nil, "normal", nil, nil, false)
		}
	})
}

func BenchmarkConcurrentPublishWithStorage(b *testing.B) {
	store, err := storage.New(b.TempDir(), false)
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	defer store.CloseAll()

	brk := broker.New(store)
	topic := "bench/concurrent"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte("hello world")

	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = brk.Publish(topic, payload, nil, "normal", nil, nil, false)
		}
	})
}

func BenchmarkConcurrentConsume(b *testing.B) {
	brk := broker.New(nil)
	topic := "bench/concurrent"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte("hello world")

	for i := 0; i < 1000; i++ {
		_ = brk.Publish(topic, payload, nil, "normal", nil, nil, false)
	}

	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		ch := make(chan message.Message, 1)
		for pb.Next() {
			msgs, _ := brk.Consume(topic, 1, ch)
			if len(msgs) > 0 {
				brk.Ack(topic, msgs[0].ID)
			}
		}
	})
}

func BenchmarkConcurrentPubSub(b *testing.B) {
	brk := broker.New(nil)
	topic := "bench/pubsub"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte("hello world")

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := make(chan message.Message, 1)
			for {
				select {
				case <-stop:
					return
				default:
					msgs, _ := brk.Consume(topic, 1, ch)
					if len(msgs) > 0 {
						brk.Ack(topic, msgs[0].ID)
					}
				}
			}
		}()
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = brk.Publish(topic, payload, nil, "normal", nil, nil, false)
		}
	})

	close(stop)
	wg.Wait()
}

func BenchmarkConcurrentBroadcast(b *testing.B) {
	brk := broker.New(nil)
	topic := "bench/broadcast"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte("hello world")

	for i := 0; i < 10; i++ {
		ch := make(chan message.Message, 1)
		brk.Consume(topic, 1, ch)
	}

	b.SetParallelism(10)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = brk.Publish(topic, payload, nil, "normal", nil, nil, true)
		}
	})
}
