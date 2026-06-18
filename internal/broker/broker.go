package broker

import (
	"log"
	"errors"
	"bytes"
	"net/http"
	"sync"
	"time"
	"regexp"
	"strings"
	
	"tinymq/internal/message"
	"tinymq/internal/storage"
	"tinymq/internal/helper"
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
	webhooks   	  map[string][]string
}

type TopicStat struct {
	Name         	 string
	MessageCount 	 int
	WaitingConsumers int
	IsDLQ 		 	 bool
	HasWebhooks 	 bool
	
}

var validTopicRegex = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

func New(store *storage.DiskStorage) *Broker {
	return &Broker{
		Topics:        make(map[string]*Topic),
		storage:       store,
		compiledRegex: make(map[string]*regexp.Regexp),
		webhooks: make(map[string][]string),
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

func (b *Broker) Publish(topicName string, payload []byte, expiresAt *time.Time, deliverAt *time.Time, isBroadcast bool) {
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
        ID:        helper.NewUUID(),
        Topic:     topicName,
        Payload:   payload,
        Timestamp: time.Now(),
        ExpiresAt: expiresAt,
        DeliverAt: deliverAt,
    }

    b.mu.RLock()
    urls := b.webhooks[topicName]
    b.mu.RUnlock()

    if len(urls) > 0 {
        go func(endpoints []string, data []byte) {
            for _, u := range endpoints {
                resp, err := http.Post(u, "application/json", bytes.NewBuffer(data))
                if err == nil {
                    resp.Body.Close()
                }
            }
        }(urls, payload)
    }

    if isBroadcast {
        var broadcastChannels []chan message.Message

        for _, wildcardT := range matchingWildcards {
            wildcardT.mu.Lock()
            broadcastChannels = append(broadcastChannels, wildcardT.waitingConsumers...)
            wildcardT.waitingConsumers = nil 
            wildcardT.mu.Unlock()
        }

        t.mu.Lock()
        broadcastChannels = append(broadcastChannels, t.waitingConsumers...)
        t.waitingConsumers = nil 
        t.mu.Unlock()

        go func(channels []chan message.Message, m message.Message) {
            for _, ch := range channels {
                ch <- m
            }
        }(broadcastChannels, msg)

        return 
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

func (b *Broker) extractMessages(t *Topic, limit int) []message.Message {
	var results []message.Message
	
	for i := 0; i < len(t.Messages) && len(results) < limit; i++ {
		msg := t.Messages[i]
		
		if msg.ExpiresAt != nil && time.Now().After(*msg.ExpiresAt) {
			t.Messages = append(t.Messages[:i], t.Messages[i+1:]...)
			if b.storage != nil {
				b.storage.AppendAck(t.Name, msg.ID)
			}
			i--
			continue
		}

		if msg.DeliverAt != nil && time.Now().Before(*msg.DeliverAt) {
			continue
		}

		results = append(results, msg)
		t.Messages = append(t.Messages[:i], t.Messages[i+1:]...)
		i--
	}
	return results
}

func (b *Broker) Consume(topicName string, limit int, notifyChan chan message.Message) ([]message.Message, bool) {
	b.mu.Lock()
	
	if !strings.Contains(topicName, "*") {
		if _, exists := b.Topics[topicName]; !exists {
			b.Topics[topicName] = &Topic{Name: topicName}
		}
		t := b.Topics[topicName]
		b.mu.Unlock()

		t.mu.Lock()
		defer t.mu.Unlock()

		results := b.extractMessages(t, limit)
		if len(results) > 0 {
			return results, true
		}

		t.waitingConsumers = append(t.waitingConsumers, notifyChan)
		return nil, false
	}

	reg, exists := b.compiledRegex[topicName]
	if !exists {
		regexPattern := "^" + strings.ReplaceAll(topicName, "*", ".*") + "$"
		reg, _ = regexp.Compile(regexPattern)
		b.compiledRegex[topicName] = reg
	}

	for name, t := range b.Topics {
		if reg.MatchString(name) {
			t.mu.Lock()
			results := b.extractMessages(t, limit)
			t.mu.Unlock()
			
			if len(results) > 0 {
				b.mu.Unlock()
				return results, true
			}
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

func (b *Broker) GetStats() ([]TopicStat, int) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	totalWebhooks := 0
	for _, urls := range b.webhooks {
		totalWebhooks += len(urls)
	}

	stats := make([]TopicStat, 0, len(b.Topics))
	for name, t := range b.Topics {
		t.mu.Lock()
		_, hasWebhook := b.webhooks[name]
		
		stats = append(stats, TopicStat{
			Name:             name,
			MessageCount:     len(t.Messages),
			WaitingConsumers: len(t.waitingConsumers),
			IsDLQ:            strings.HasSuffix(name, ".dlq"),
			HasWebhooks:      hasWebhook, 
		})
		t.mu.Unlock()
	}

	return stats, totalWebhooks
}

func (b *Broker) Requeue(msg message.Message) {
	msg.RetryCount++
	
	targetTopic := msg.Topic
	if msg.RetryCount >= 3 {
		targetTopic = msg.Topic + ".dlq"
		msg.Topic = targetTopic 
		log.Printf("Message %s move to DLQ: %s\n", msg.ID, targetTopic)
	}

	b.mu.Lock()
	if _, exists := b.Topics[targetTopic]; !exists {
		b.Topics[targetTopic] = &Topic{Name: targetTopic}
	}
	t := b.Topics[targetTopic]
	b.mu.Unlock()

	if b.storage != nil {
		if err := b.storage.AppendPut(targetTopic, msg); err != nil {
			log.Printf("Error persisting requeue record: %v\n", err)
		}
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

func (b *Broker) RegisterWebhook(topic, callbackURL string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.webhooks[topic] = append(b.webhooks[topic], callbackURL)
	log.Printf("Registred Webhook for topic '%s' -> %s\n", topic, callbackURL)
}

func (b *Broker) CreateTopic(topicName string) error {
	if len(topicName) == 0 || len(topicName) > 255 {
		return errors.New("Invalid name length (1-255 characters)")
	}

	if !validTopicRegex.MatchString(topicName) {
		return errors.New("The name can only contain letters, numbers, '.', '-' and '_'")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.Topics[topicName]; exists {
		return errors.New("The Topic already exists")
	}

	b.Topics[topicName] = &Topic{Name: topicName}
	log.Printf("Created Topic '%s' manually vía API/Dashboard\n", topicName)
	return nil
}