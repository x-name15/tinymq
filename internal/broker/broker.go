package broker

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/x-name15/tinymq/internal/helper"
	"github.com/x-name15/tinymq/internal/message"
	"github.com/x-name15/tinymq/internal/storage"
)

type Topic struct {
	Name             string
	Messages         []message.Message
	waitingConsumers []chan message.Message
	spies            []chan message.Message
	Policy 			 string
	mu               sync.Mutex
}

type Broker struct {
	mu            	sync.RWMutex
	Topics        	map[string]*Topic
	storage       	*storage.DiskStorage
	compiledRegex 	map[string]*regexp.Regexp
	webhooks      	map[string][]string
	webhookClient 	*http.Client
	idempotencyKeys map[string]time.Time
	bindings        map[string]map[string]bool
}

type TopicStat struct {
	Name            string
	MessageCount    int
	WaitingConsumers int
	IsDLQ           bool
	HasWebhooks     bool
}

var validTopicRegex = regexp.MustCompile(`^[a-zA-Z0-9._:-]+$`)

func New(store *storage.DiskStorage) *Broker {
	return &Broker{
		Topics:          make(map[string]*Topic),
		storage:         store,
		compiledRegex:   make(map[string]*regexp.Regexp),
		webhooks:      	 make(map[string][]string),
		webhookClient: 	 &http.Client{Timeout: 10 * time.Second},
		idempotencyKeys: make(map[string]time.Time),
		bindings:        make(map[string]map[string]bool),
	}
}

func getMaxMessages() int {
    val := os.Getenv("TINYMQ_MAX_MESSAGES")
    if limit, err := strconv.Atoi(val); err == nil && limit > 0 {
        return limit
    }
    return 100000 
}

func (b *Broker) LoadExistingTopics(topicNames []string) {
	defaultPolicy := os.Getenv("TINYMQ_DEFAULT_POLICY")
	if defaultPolicy != "drop-oldest" {
		defaultPolicy = "reject"
	}

	for _, name := range topicNames {
		if strings.Contains(name, "::") {
			parts := strings.SplitN(name, "::", 2)
			sourceTopic := parts[0]
			
			b.mu.Lock()
			if b.bindings[sourceTopic] == nil {
				b.bindings[sourceTopic] = make(map[string]bool)
			}
			b.bindings[sourceTopic][name] = true
			b.mu.Unlock()
		}
		
		msgs, err := b.storage.LoadMessages(name)
		if err != nil {
			log.Printf("Failed to recover topic %s: %v\n", name, err)
			continue
		}

		b.mu.Lock()
		b.Topics[name] = &Topic{
			Name:     name,
			Messages: msgs, 
			Policy:   defaultPolicy,
		}
		b.mu.Unlock()

		if len(msgs) > 0 {
			log.Printf("Recovered topic '%s' with %d unacknowledged messages from disk\n", name, len(msgs))
		} else {
			log.Printf("Recovered empty topic '%s' from disk\n", name)
		}
	}
}

func (b *Broker) Publish(topicName string, payload []byte, expiresAt *time.Time, deliverAt *time.Time, isBroadcast bool) error {
	if !validTopicRegex.MatchString(topicName) {
		log.Printf("Rejected publish to invalid topic name: %s\n", topicName)
		return errors.New("invalid topic name")
	}

	if !strings.Contains(topicName, "::") {
		b.mu.RLock()
		var boundTopics []string
		if b.bindings != nil && b.bindings[topicName] != nil {
			for vTopic := range b.bindings[topicName] {
				boundTopics = append(boundTopics, vTopic)
			}
		}
		b.mu.RUnlock()

		for _, dest := range boundTopics {
			b.Publish(dest, payload, expiresAt, deliverAt, isBroadcast)
		}
	}

	b.mu.Lock()
	if _, exists := b.Topics[topicName]; !exists {
		policy := os.Getenv("TINYMQ_DEFAULT_POLICY")
		if policy != "drop-oldest" {
			policy = "reject"
		}
		b.Topics[topicName] = &Topic{Name: topicName, Policy: policy}
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
				resp, err := b.webhookClient.Post(u, "application/json", bytes.NewBuffer(data))
				if err == nil {
					resp.Body.Close()
				} else {
					log.Printf("Webhook delivery failed to %s: %v\n", u, err)
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

		return nil
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
			return nil
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
		return nil
	}

	if len(t.Messages) >= getMaxMessages() {
		if t.Policy == "drop-oldest" {
			oldestMsg := t.Messages[0]
			t.Messages = t.Messages[1:]
			
			if b.storage != nil {
				if err := b.storage.AppendAck(topicName, oldestMsg.ID); err != nil {
					log.Printf("Eviction ACK logging failed: %v\n", err)
				}
			}
			log.Printf("[RingBuffer] Queue '%s' full. Evicted oldest message: %s\n", topicName, oldestMsg.ID)
		} else {
			return errors.New("queue capacity reached (max 100,000 messages)")
		}
	}

	t.Messages = append(t.Messages, msg)

	for _, spy := range t.spies {
		select {
		case spy <- msg:
		default:
		}
	}

	return nil
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
	topicsCopy := make(map[string]*Topic, len(b.Topics))
	for k, v := range b.Topics {
		topicsCopy[k] = v
	}
	webhooksCopy := make(map[string][]string, len(b.webhooks))
	for k, v := range b.webhooks {
		webhooksCopy[k] = v
	}
	b.mu.RUnlock()

	totalWebhooks := 0
	for _, urls := range webhooksCopy {
		totalWebhooks += len(urls)
	}

	stats := make([]TopicStat, 0, len(topicsCopy))
	for name, t := range topicsCopy {
		t.mu.Lock()
		_, hasWebhook := webhooksCopy[name]
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
		log.Printf("Message %s moved to DLQ: %s\n", msg.ID, targetTopic)
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
	log.Printf("Registered Webhook for topic '%s' -> %s\n", topic, callbackURL)
}

func (b *Broker) CreateTopic(topicName string, policy string) error {
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

	if policy != "drop-oldest" {
		policy = "reject"
	}

	b.Topics[topicName] = &Topic{Name: topicName, Policy: policy}
	log.Printf("Created Topic '%s' manually via API/Dashboard with policy '%s'\n", topicName, policy)
	return nil
}

func (b *Broker) Peek(topicName string, limit int) []message.Message {
	b.mu.RLock()
	t, exists := b.Topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	count := limit
	if len(t.Messages) < count {
		count = len(t.Messages)
	}

	results := make([]message.Message, count)
	copy(results, t.Messages[:count])
	return results
}

func (b *Broker) IsValidTopicName(name string) bool {
	return validTopicRegex.MatchString(name)
}

func (b *Broker) TopicExists(name string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, exists := b.Topics[name]
	return exists
}

func (b *Broker) Purge(topicName string) error {
	b.mu.RLock()
	t, exists := b.Topics[topicName]
	b.mu.RUnlock()

	if !exists {
		return errors.New("queue not found")
	}

	t.mu.Lock()
	t.Messages = nil 
	t.mu.Unlock()

	if b.storage != nil {
		b.storage.ClearLog(topicName)
	}
	log.Printf("Queue '%s' purged completely.\n", topicName)
	return nil
}

func (b *Broker) DeleteTopic(topicName string) error {
	b.mu.Lock()
	if _, exists := b.Topics[topicName]; !exists {
		b.mu.Unlock()
		return errors.New("queue not found")
	}

	delete(b.Topics, topicName)
	delete(b.webhooks, topicName)
	delete(b.compiledRegex, topicName)
	b.mu.Unlock()

	if b.storage != nil {
		b.storage.DeleteLog(topicName)
	}
	log.Printf("Queue '%s' deleted.\n", topicName)
	return nil
}

func (b *Broker) GetWebhooks(topicName string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	
	urls := b.webhooks[topicName]
	result := make([]string, len(urls))
	copy(result, urls)
	return result
}

func (b *Broker) IsIdempotent(key string) bool {
	if key == "" {
		return false
	}
	
	b.mu.Lock()
	defer b.mu.Unlock()
	
	now := time.Now()
	
	if exp, exists := b.idempotencyKeys[key]; exists {
		if now.Before(exp) {
			return true 
		}
	}
	
	b.idempotencyKeys[key] = now.Add(5 * time.Minute)
	
	if len(b.idempotencyKeys) > 5000 {
		for k, v := range b.idempotencyKeys {
			if now.After(v) {
				delete(b.idempotencyKeys, k)
			}
		}
	}
	
	return false
}

func (b *Broker) AddSpy(topicName string) chan message.Message {
	b.mu.Lock()
	t, exists := b.Topics[topicName]
	if !exists {
		t = &Topic{Name: topicName}
		b.Topics[topicName] = t
	}
	b.mu.Unlock()

	t.mu.Lock()
	defer t.mu.Unlock()
	
	ch := make(chan message.Message, 50)
	t.spies = append(t.spies, ch)
	return ch
}

func (b *Broker) RemoveSpy(topicName string, ch chan message.Message) {
	b.mu.Lock()
	t, exists := b.Topics[topicName]
	b.mu.Unlock()

	if !exists {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	for i, spy := range t.spies {
		if spy == ch {
			t.spies = append(t.spies[:i], t.spies[i+1:]...)
			close(ch)
			break
		}
	}
}

func (b *Broker) CreateGroup(topicName, groupName string) string {
	virtualName := fmt.Sprintf("%s::%s", topicName, groupName)
	
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if b.bindings[topicName] == nil {
		b.bindings[topicName] = make(map[string]bool)
	}
	b.bindings[topicName][virtualName] = true

	if _, exists := b.Topics[virtualName]; !exists {
		b.Topics[virtualName] = &Topic{Name: virtualName}
	}
	
	return virtualName
}