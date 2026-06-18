package broker

import (
	"log"
	"sync"
	"time"
	"regexp"
	"strings"
	
	"tinymq/internal/message"
	"tinymq/internal/storage"
	"github.com/google/uuid"
)

type Topic struct {
	Name             string
	Messages         []message.Message
	mu               sync.Mutex
	waitingConsumers []chan message.Message
}

type Broker struct {
	mu            sync.RWMutex
	Topics        map[string]*Topic
	storage       *storage.DiskStorage
	compiledRegex map[string]*regexp.Regexp
}

type TopicStat struct {
	Name         string
	MessageCount int
}

func New(store *storage.DiskStorage) *Broker {
	return &Broker{
		Topics:        make(map[string]*Topic),
		storage:       store,
		compiledRegex: make(map[string]*regexp.Regexp),
	}
}

func (b *Broker) LoadExistingTopics(topicNames []string) {
	for _, name := range topicNames {
		msgs, err := b.storage.LoadMessages(name)
		if err != nil {
			log.Printf("Failed to recover topic %s: %v\n", name, err)
			continue
		}
		
		if len(msgs) > 0 {
			b.mu.Lock()
			b.Topics[name] = &Topic{
				Name:     name,
				Messages: msgs,
			}
			b.mu.Unlock()
			log.Printf("Recovered topic '%s' with %d unacknowledged messages from disk\n", name, len(msgs))
		}
	}
}

func (b *Broker) Publish(topicName string, payload []byte) {
	b.mu.Lock()
	if _, exists := b.Topics[topicName]; !exists {
		b.Topics[topicName] = &Topic{Name: topicName}
	}
	t := b.Topics[topicName]
	
	var matchingWildcards []*Topic
	for name, wildcardT := range b.Topics {
		if strings.Contains(name, "*") {
			reg, exists := b.compiledRegex[name]
			if !exists {
				regexPattern := "^" + strings.ReplaceAll(name, "*", ".*") + "$"
				reg, _ = regexp.Compile(regexPattern)
				b.compiledRegex[name] = reg
			}
			if reg != nil && reg.MatchString(topicName) {
				matchingWildcards = append(matchingWildcards, wildcardT)
			}
		}
	}
	b.mu.Unlock()

	msg := message.Message{
		ID:        uuid.New().String(),
		Topic:     topicName,
		Payload:   payload,
		Timestamp: time.Now(),
	}

	if b.storage != nil {
		if err := b.storage.AppendPut(topicName, msg); err != nil {
			log.Printf("Error persisting PUT record: %v\n", err)
		}
	}

	for _, wildcardT := range matchingWildcards {
		wildcardT.mu.Lock()
		if len(wildcardT.waitingConsumers) > 0 {
			consumerChan := wildcardT.waitingConsumers[0]
			wildcardT.waitingConsumers[0] = nil
			wildcardT.waitingConsumers = wildcardT.waitingConsumers[1:]
			wildcardT.mu.Unlock()
			consumerChan <- msg 
			return
		}
		wildcardT.mu.Unlock()
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	
	if len(t.waitingConsumers) > 0 {
		consumerChan := t.waitingConsumers[0]
		t.waitingConsumers[0] = nil
		t.waitingConsumers = t.waitingConsumers[1:]
		consumerChan <- msg
		return
	}

	t.Messages = append(t.Messages, msg)
}

func (b *Broker) Consume(topicName string, notifyChan chan message.Message) (*message.Message, bool) {
	b.mu.Lock()
	
	if !strings.Contains(topicName, "*") {
		if _, exists := b.Topics[topicName]; !exists {
			b.Topics[topicName] = &Topic{Name: topicName}
		}
		t := b.Topics[topicName]
		b.mu.Unlock()

		t.mu.Lock()
		defer t.mu.Unlock()

		if len(t.Messages) > 0 {
			msg := t.Messages[0]
			t.Messages[0] = message.Message{}
			t.Messages = t.Messages[1:]
			return &msg, true
		}

		t.waitingConsumers = append(t.waitingConsumers, notifyChan)
		return nil, false
	}

	reg, exists := b.compiledRegex[topicName]
	if !exists {
		regexPattern := "^" + strings.ReplaceAll(topicName, "*", ".*") + "$"
		var err error
		reg, err = regexp.Compile(regexPattern)
		if err != nil {
			b.mu.Unlock()
			return nil, false
		}
		b.compiledRegex[topicName] = reg
	}

	for name, t := range b.Topics {
		if reg.MatchString(name) {
			t.mu.Lock()
			if len(t.Messages) > 0 {
				msg := t.Messages[0]
				t.Messages[0] = message.Message{}
				t.Messages = t.Messages[1:]
				t.mu.Unlock()
				b.mu.Unlock()
				return &msg, true
			}
			t.mu.Unlock()
		}
	}

	if _, exists := b.Topics[topicName]; !exists {
		b.Topics[topicName] = &Topic{Name: topicName}
	}
	wildcardTopic := b.Topics[topicName]
	b.mu.Unlock()

	wildcardTopic.mu.Lock()
	wildcardTopic.waitingConsumers = append(wildcardTopic.waitingConsumers, notifyChan)
	wildcardTopic.mu.Unlock()

	return nil, false
}

func (b *Broker) RemoveWaitingConsumer(topicName string, notifyChan chan message.Message) {
	b.mu.RLock()
	t, exists := b.Topics[topicName]
	b.mu.RUnlock()
	if !exists {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	for i, ch := range t.waitingConsumers {
		if ch == notifyChan {
			t.waitingConsumers[i] = nil
			t.waitingConsumers = append(t.waitingConsumers[:i], t.waitingConsumers[i+1:]...)
			break
		}
	}
}

func (b *Broker) Ack(topicName string, msgID string) bool {
	b.mu.RLock()
	t, exists := b.Topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	foundIndex := -1
	for i, m := range t.Messages {
		if m.ID == msgID {
			foundIndex = i
			break
		}
	}

	if b.storage != nil {
		if err := b.storage.AppendAck(topicName, msgID); err != nil {
			log.Printf("Error persisting ACK record: %v\n", err)
		}
	}

	if foundIndex != -1 {
		t.Messages[foundIndex] = message.Message{}
		t.Messages = append(t.Messages[:foundIndex], t.Messages[foundIndex+1:]...)
	}
	return true
}

func (b *Broker) GetStats() []TopicStat {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := make([]TopicStat, 0, len(b.Topics))
	for name, t := range b.Topics {
		t.mu.Lock()
		stats = append(stats, TopicStat{
			Name:         name,
			MessageCount: len(t.Messages),
		})
		t.mu.Unlock()
	}
	return stats
}