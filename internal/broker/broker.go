package broker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
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

type WebhookConfig struct {
	URL    string
	Secret string
}

type Topic struct {
	Name             string
	Messages         []message.Message
	HighMessages     []message.Message
	LowMessages      []message.Message
	waitingConsumers []chan message.Message
	spies            []chan message.Message
	Policy           string
	Deleted          bool
	Retention        time.Duration
	mu               sync.Mutex
}

type Broker struct {
	mu              sync.RWMutex
	Topics          map[string]*Topic
	wildcards       map[string]*Topic
	storage         *storage.DiskStorage
	compiledRegex   map[string]*regexp.Regexp
	webhooks        map[string][]WebhookConfig
	webhookClient   *http.Client
	webhookSem      chan struct{}
	idempotencyKeys map[string]time.Time
	bindings        map[string]map[string]bool
	maxMessages     int
	maxTopics       int
	OnPublish       func(topic string, payload []byte) error
	OnGroupCreate   func(topic, group string) error
}

type TopicStat struct {
	Name             string
	MessageCount     int
	WaitingConsumers int
	IsDLQ            bool
	HasWebhooks      bool
}

var validTopicRegex = regexp.MustCompile(`^[a-zA-Z0-9._:\-/]+$`)
var validWildcardRegex = regexp.MustCompile(`^([a-zA-Z0-9._:\-/]+\*|\*)$`)

func New(store *storage.DiskStorage) *Broker {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	secureTransport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				host = addr
			}
			_ = port
			ips, err := net.LookupIP(host)
			if err != nil {
				return nil, err
			}
			for _, ip := range ips {
				if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() {
					return nil, errors.New("DNS rebinding blocked: target resolved to an internal IP at dial time")
				}
			}
			return dialer.DialContext(ctx, network, addr)
		},
	}

	maxMsgs := 100000
	if val := os.Getenv("TINYMQ_MAX_MESSAGES"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			maxMsgs = n
		}
	}
	maxTopics := 10000
	if val := os.Getenv("TINYMQ_MAX_TOPICS"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			maxTopics = n
		}
	}
	b := &Broker{
		Topics:          make(map[string]*Topic),
		wildcards:       make(map[string]*Topic),
		storage:         store,
		compiledRegex:   make(map[string]*regexp.Regexp),
		webhooks:        make(map[string][]WebhookConfig),
		webhookClient:   &http.Client{Timeout: 10 * time.Second, Transport: secureTransport},
		webhookSem:      make(chan struct{}, 64),
		idempotencyKeys: make(map[string]time.Time),
		bindings:        make(map[string]map[string]bool),
		maxMessages:     maxMsgs,
		maxTopics:       maxTopics,
	}

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			b.purgeExpiredMessages()
		}
	}()

	return b
}

func (b *Broker) compileWildcardRegex(name string) {
	if _, exists := b.compiledRegex[name]; !exists {
		regexPattern := "^" + strings.ReplaceAll(name, "*", ".*") + "$"
		if compiled, err := regexp.Compile(regexPattern); err == nil {
			b.compiledRegex[name] = compiled
		}
	}
}

func (b *Broker) getOrCreateTopic(name string) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok := b.Topics[name]; ok {
		return t
	}
	if len(b.Topics) >= b.maxTopics {
		return nil
	}
	policy := os.Getenv("TINYMQ_DEFAULT_POLICY")
	if policy != "drop-oldest" {
		policy = "reject"
	}
	t := &Topic{Name: name, Policy: policy}
	b.Topics[name] = t
	if strings.Contains(name, "*") {
		b.wildcards[name] = t
		b.compileWildcardRegex(name)
	}
	return t
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
		t := &Topic{
			Name:     name,
			Messages: msgs,
			Policy:   defaultPolicy,
		}
		b.Topics[name] = t
		if strings.Contains(name, "*") {
			b.wildcards[name] = t
			b.compileWildcardRegex(name)
		}
		b.mu.Unlock()
		if len(msgs) > 0 {
			log.Printf("Recovered topic '%s' with %d unacknowledged messages from disk\n", name, len(msgs))
		} else {
			log.Printf("Recovered empty topic '%s' from disk\n", name)
		}
	}
}

func (b *Broker) Publish(topicName string, payload []byte, headers map[string]string, priority string, expiresAt *time.Time, deliverAt *time.Time, isBroadcast bool) error {
	return b.publishCore(topicName, payload, headers, priority, expiresAt, deliverAt, isBroadcast, false, 0)
}

func (b *Broker) PublishReplicated(topicName string, payload []byte) error {
	return b.publishCore(topicName, payload, nil, "normal", nil, nil, false, true, 0)
}

func (b *Broker) publishCore(topicName string, payload []byte, headers map[string]string, priority string, expiresAt *time.Time, deliverAt *time.Time, isBroadcast bool, isReplication bool, depth int) error {
	if depth > 10 {
		log.Printf("[Broker] SEC-ALERT: Binding loop or max depth detected resolving topic '%s'", topicName)
		return errors.New("binding loop detected")
	}
	if len(topicName) == 0 || len(topicName) > 255 {
		return errors.New("invalid topic name length (must be between 1 and 255 characters)")
	}
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
			b.publishCore(dest, payload, headers, priority, expiresAt, deliverAt, isBroadcast, isReplication, depth+1)
		}
	}
	t := b.getOrCreateTopic(topicName)
	if t == nil {
		return errors.New("broker maximum topic limit reached")
	}
	var matchingWildcards []*Topic
	b.mu.RLock()
	for name, wildcardT := range b.wildcards {
		reg := b.compiledRegex[name]
		if reg != nil && reg.MatchString(topicName) {
			matchingWildcards = append(matchingWildcards, wildcardT)
		}
	}
	b.mu.RUnlock()
	if expiresAt == nil && t.Retention > 0 {
		exp := time.Now().Add(t.Retention)
		expiresAt = &exp
	}
	msg := message.Message{
		ID:        helper.NewUUID(),
		Topic:     topicName,
		Payload:   payload,
		Timestamp: time.Now(),
		ExpiresAt: expiresAt,
		DeliverAt: deliverAt,
		Headers:   headers,
	}
	if !isReplication && b.OnPublish != nil {
		if err := b.OnPublish(topicName, payload); err != nil {
			return err
		}
	}
	b.mu.RLock()
	configs := b.webhooks[topicName]
	b.mu.RUnlock()
	if len(configs) > 0 {
		// SEC-CRIT-01: Each webhook delivery runs in its own goroutine, bounded by webhookSem
		// (max 64 concurrent deliveries). Uses a per-request context so the HTTP timeout is
		// properly scoped and goroutines cannot accumulate unboundedly under high publish rates.
		for _, wh := range configs {
			select {
			case b.webhookSem <- struct{}{}:
			default:
				log.Printf("[Broker] Webhook semaphore full, dropping delivery to %s\n", wh.URL)
				continue
			}
			go func(endpoint WebhookConfig, data []byte) {
				defer func() { <-b.webhookSem }()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, "POST", endpoint.URL, bytes.NewBuffer(data))
				if err != nil {
					log.Printf("[Broker] Failed to build webhook request to %s: %v\n", endpoint.URL, err)
					return
				}
				req.Header.Set("Content-Type", "application/json")
				if endpoint.Secret != "" {
					mac := hmac.New(sha256.New, []byte(endpoint.Secret))
					mac.Write(data)
					signature := hex.EncodeToString(mac.Sum(nil))
					req.Header.Set("X-TinyMQ-Signature", "sha256="+signature)
				}
				resp, err := b.webhookClient.Do(req)
				if err == nil {
					resp.Body.Close()
				} else {
					log.Printf("[Broker] Webhook delivery failed to %s: %v\n", endpoint.URL, err)
				}
			}(wh, payload)
		}
	}
	t.mu.Lock()
	spyCount := len(t.spies)
	for _, spy := range t.spies {
		select {
		case spy <- msg:
		default:
			log.Printf("[Broker] Spy buffer full for topic '%s', message %s dropped\n", t.Name, msg.ID)
		}
	}
	t.mu.Unlock()
	if spyCount > 0 {
		log.Printf("[Broker] Delivered message %s to %d spies on topic '%s'\n", msg.ID, spyCount, t.Name)
	}
	for _, wildcardT := range matchingWildcards {
		wildcardT.mu.Lock()
		spyCountW := len(wildcardT.spies)
		for _, spy := range wildcardT.spies {
			select {
			case spy <- msg:
			default:
				log.Printf("[Broker] Spy buffer full for topic '%s', message %s dropped\n", wildcardT.Name, msg.ID)
			}
		}
		wildcardT.mu.Unlock()
		if spyCountW > 0 {
			log.Printf("[Broker] Delivered message %s to %d spies on wildcard topic '%s'\n", msg.ID, spyCountW, wildcardT.Name)
		}
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
				select {
				case ch <- m:
				default:
					log.Printf("[Broker] Broadcast consumer disappeared on topic '%s', message %s dropped\n", m.Topic, m.ID)
				}
			}
		}(broadcastChannels, msg)
		return nil
	}
	t.mu.Lock()
	if t.Deleted {
		t.mu.Unlock()
		return errors.New("topic was concurrently deleted")
	}
	// BUG-HIGH-01: WAL persistence is deferred until we confirm the message stays in the
	// queue (i.e. no waiting consumer takes it directly). This prevents WAL entries that
	// are never Ack'd on direct delivery, which would cause phantom messages after restart.
	// Check waiting consumers on matching wildcard topics first.
	for _, wildcardT := range matchingWildcards {
		wildcardT.mu.Lock()
		if len(wildcardT.waitingConsumers) > 0 {
			consumerChan := wildcardT.waitingConsumers[0]
			wildcardT.waitingConsumers[0] = nil
			wildcardT.waitingConsumers = wildcardT.waitingConsumers[1:]
			wildcardT.mu.Unlock()
			t.mu.Unlock()
			select {
			case consumerChan <- msg:
				log.Printf("[Broker] Delivered message %s to waiting consumer on wildcard topic '%s'\n", msg.ID, wildcardT.Name)
				return nil
			default:
				log.Printf("[Broker] Waiting consumer on wildcard topic '%s' disappeared, enqueuing message\n", wildcardT.Name)
				t.mu.Lock()
			}
		} else {
			wildcardT.mu.Unlock()
		}
	}
	var pendingConsumer chan message.Message
	if len(t.waitingConsumers) > 0 {
		pendingConsumer = t.waitingConsumers[0]
		t.waitingConsumers[0] = nil
		t.waitingConsumers = t.waitingConsumers[1:]
	}
	if pendingConsumer != nil {
		t.mu.Unlock()
		select {
		case pendingConsumer <- msg:
			log.Printf("[Broker] Delivered message %s to waiting consumer on topic '%s'\n", msg.ID, t.Name)
			return nil
		default:
			log.Printf("[Broker] Waiting consumer on topic '%s' disappeared, enqueuing message\n", t.Name)
			t.mu.Lock()
		}
	}
	// No waiting consumer took the message: persist to WAL and enqueue in memory.
	if !isBroadcast && b.storage != nil {
		if err := b.storage.AppendPut(topicName, msg); err != nil {
			log.Printf("Error persisting PUT record: %v\n", err)
		}
	}
	var targetQueue *[]message.Message
	switch priority {
	case "high":
		targetQueue = &t.HighMessages
	case "low":
		targetQueue = &t.LowMessages
	default:
		targetQueue = &t.Messages
	}
	totalActiveMessages := len(t.HighMessages) + len(t.Messages) + len(t.LowMessages)
	if totalActiveMessages >= b.maxMessages {
		if t.Policy == "drop-oldest" {
			var oldestMsg message.Message
			if len(t.LowMessages) > 0 {
				oldestMsg = t.LowMessages[0]
				t.LowMessages = t.LowMessages[1:]
			} else if len(t.Messages) > 0 {
				oldestMsg = t.Messages[0]
				t.Messages = t.Messages[1:]
			} else {
				oldestMsg = t.HighMessages[0]
				t.HighMessages = t.HighMessages[1:]
			}
			if b.storage != nil {
				b.storage.AppendAck(topicName, oldestMsg.ID)
			}
			log.Printf("[RingBuffer] Queue '%s' full. Evicted oldest message: %s\n", topicName, oldestMsg.ID)
		} else {
			t.mu.Unlock()
			// BUG-LOW-03: Use actual configured limit in error message instead of hardcoded value.
			return fmt.Errorf("queue capacity reached (max %d messages)", b.maxMessages)
		}
	}
	*targetQueue = append(*targetQueue, msg)
	log.Printf("[Broker] Message %s enqueued on topic '%s' (priority: %s)\n", msg.ID, t.Name, priority)
	t.mu.Unlock()
	return nil
}

func (b *Broker) extractMessages(t *Topic, limit int) ([]message.Message, []string) {
	var results []message.Message
	var expiredIDs []string
	now := time.Now()
	extractFrom := func(queue *[]message.Message) {
		var keep []message.Message
		for _, msg := range *queue {
			if len(results) >= limit {
				keep = append(keep, msg)
				continue
			}
			if msg.ExpiresAt != nil && now.After(*msg.ExpiresAt) {
				expiredIDs = append(expiredIDs, msg.ID)
				continue
			}
			if msg.DeliverAt != nil && now.Before(*msg.DeliverAt) {
				keep = append(keep, msg)
				continue
			}
			results = append(results, msg)
		}
		*queue = keep
	}
	extractFrom(&t.HighMessages)
	if len(results) < limit {
		extractFrom(&t.Messages)
	}
	if len(results) < limit {
		extractFrom(&t.LowMessages)
	}
	return results, expiredIDs
}

func (b *Broker) ackExpired(topicName string, expiredIDs []string) {
	if b.storage == nil {
		return
	}
	for _, id := range expiredIDs {
		b.storage.AppendAck(topicName, id)
	}
}

func (b *Broker) Consume(topicName string, limit int, notifyChan chan message.Message) ([]message.Message, bool) {
	if len(topicName) > 0 && !b.IsValidTopicName(topicName) {
		return nil, false
	}
	targetTopic := b.getOrCreateTopic(topicName)
	if targetTopic == nil {
		return nil, false
	}
	var matchingTopics []*Topic
	if strings.Contains(topicName, "*") {
		b.mu.RLock()
		reg := b.compiledRegex[topicName]
		if reg != nil {
			for name, t := range b.Topics {
				if reg.MatchString(name) {
					matchingTopics = append(matchingTopics, t)
				}
			}
		}
		b.mu.RUnlock()
	}
	for _, t := range matchingTopics {
		t.mu.Lock()
		results, expiredIDs := b.extractMessages(t, limit)
		t.mu.Unlock()
		b.ackExpired(t.Name, expiredIDs)
		if len(results) > 0 {
			return results, true
		}
	}
	targetTopic.mu.Lock()
	results, expiredIDs := b.extractMessages(targetTopic, limit)
	if len(results) == 0 {
		targetTopic.waitingConsumers = append(targetTopic.waitingConsumers, notifyChan)
	}
	targetTopic.mu.Unlock()
	b.ackExpired(targetTopic.Name, expiredIDs)
	if len(results) > 0 {
		return results, true
	}
	return nil, false
}

func (b *Broker) GetStats() ([]TopicStat, int) {
	b.mu.RLock()
	topicsCopy := make(map[string]*Topic, len(b.Topics))
	for k, v := range b.Topics {
		topicsCopy[k] = v
	}
	webhooksCopy := make(map[string][]string, len(b.webhooks))
	for k, v := range b.webhooks {
		var urls []string
		for _, wh := range v {
			urls = append(urls, wh.URL)
		}
		webhooksCopy[k] = urls
	}
	b.mu.RUnlock()
	totalWebhooks := 0
	for _, urls := range webhooksCopy {
		totalWebhooks += len(urls)
	}
	stats := make([]TopicStat, 0, len(topicsCopy))
	for name, t := range topicsCopy {
		_, hasWebhook := webhooksCopy[name]
		t.mu.Lock()
		msgCount := len(t.Messages) + len(t.HighMessages) + len(t.LowMessages)
		consumerCount := len(t.waitingConsumers)
		t.mu.Unlock()
		stats = append(stats, TopicStat{
			Name:             name,
			MessageCount:     msgCount,
			WaitingConsumers: consumerCount,
			IsDLQ:            strings.HasSuffix(name, ".dlq"),
			HasWebhooks:      hasWebhook,
		})
	}
	return stats, totalWebhooks
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
	ackFrom := func(queue *[]message.Message) bool {
		for i, m := range *queue {
			if m.ID == msgID {
				(*queue)[i] = message.Message{}
				*queue = append((*queue)[:i], (*queue)[i+1:]...)
				if b.storage != nil {
					b.storage.AppendAck(topicName, msgID)
				}
				return true
			}
		}
		return false
	}
	return ackFrom(&t.HighMessages) || ackFrom(&t.Messages) || ackFrom(&t.LowMessages)
}

func (b *Broker) Requeue(msg message.Message) {
	if msg.RetryCount < 0 {
		msg.RetryCount = 0
	}
	msg.RetryCount++
	targetTopic := msg.Topic
	if msg.RetryCount >= 3 {
		targetTopic = msg.Topic + ".dlq"
		msg.Topic = targetTopic
		log.Printf("Message %s moved to DLQ: %s\n", msg.ID, targetTopic)
	}
	t := b.getOrCreateTopic(targetTopic)
	if t == nil {
		log.Printf("[Broker] Requeue failed: max topics limit reached for %s\n", targetTopic)
		return
	}
	if b.storage != nil {
		b.storage.AppendPut(targetTopic, msg)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// SEC-CRIT-02: Fixed — previously had a blocking send (consumerChan <- msg) under the
	// mutex which could deadlock, plus unreachable dead code below it. Now uses select/default
	// to avoid blocking. Only one branch is needed.
	if len(t.waitingConsumers) > 0 {
		consumerChan := t.waitingConsumers[0]
		t.waitingConsumers[0] = nil
		t.waitingConsumers = t.waitingConsumers[1:]
		select {
		case consumerChan <- msg:
			return
		default:
			log.Printf("[Broker] Waiting consumer on topic '%s' disappeared during requeue, enqueuing message\n", targetTopic)
		}
	}
	t.Messages = append(t.Messages, msg)
}

func (b *Broker) purgeExpiredMessages() {
	b.mu.RLock()
	var topics []*Topic
	for _, t := range b.Topics {
		topics = append(topics, t)
	}
	b.mu.RUnlock()

	now := time.Now()
	for _, t := range topics {
		t.mu.Lock()

		purge := func(queue *[]message.Message) []string {
			var expiredIDs []string
			var active []message.Message
			for _, msg := range *queue {
				if msg.ExpiresAt != nil && msg.ExpiresAt.Before(now) {
					expiredIDs = append(expiredIDs, msg.ID)
				} else {
					active = append(active, msg)
				}
			}
			*queue = active
			return expiredIDs
		}

		var allExpired []string
		allExpired = append(allExpired, purge(&t.HighMessages)...)
		allExpired = append(allExpired, purge(&t.Messages)...)
		allExpired = append(allExpired, purge(&t.LowMessages)...)
		t.mu.Unlock()

		if len(allExpired) > 0 {
			b.ackExpired(t.Name, allExpired)
			log.Printf("[Broker] Active GC purged %d expired messages from topic '%s'\n", len(allExpired), t.Name)
		}
	}
}

func (b *Broker) RedriveDLQ(targetTopic string) (int, error) {
	dlqName := targetTopic
	if !strings.HasSuffix(dlqName, ".dlq") {
		dlqName = targetTopic + ".dlq"
	} else {
		targetTopic = strings.TrimSuffix(dlqName, ".dlq")
	}

	b.mu.RLock()
	dlqT, ok := b.Topics[dlqName]
	b.mu.RUnlock()

	if !ok {
		return 0, fmt.Errorf("DLQ topic '%s' not found", dlqName)
	}

	dlqT.mu.Lock()
	count := len(dlqT.Messages)
	if count == 0 {
		dlqT.mu.Unlock()
		return 0, nil
	}

	msgsToRedrive := make([]message.Message, count)
	copy(msgsToRedrive, dlqT.Messages)
	dlqT.Messages = nil
	dlqT.mu.Unlock()

	redriven := 0
	for _, msg := range msgsToRedrive {
		if b.storage != nil {
			b.storage.AppendAck(dlqName, msg.ID)
		}

		err := b.publishCore(targetTopic, msg.Payload, msg.Headers, "normal", msg.ExpiresAt, nil, false, false, 0)
		if err != nil {
			log.Printf("[Broker] Failed to redrive message %s: %v\n", msg.ID, err)
		} else {
			redriven++
		}
	}

	return redriven, nil
}

func (b *Broker) RegisterWebhook(topicName string, callbackURL string, secret string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, wh := range b.webhooks[topicName] {
		if wh.URL == callbackURL {
			return errors.New("webhook already registered for this topic")
		}
	}
	b.webhooks[topicName] = append(b.webhooks[topicName], WebhookConfig{
		URL:    callbackURL,
		Secret: secret,
	})
	return nil
}

func (b *Broker) CreateTopic(topicName string, policy string, retention time.Duration) error {
	if len(topicName) == 0 || len(topicName) > 255 {
		return errors.New("Invalid name length (1-255 characters)")
	}
	if !validTopicRegex.MatchString(topicName) {
		return errors.New("The name can only contain letters, numbers, '.', '-' and '_'")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.Topics) >= b.maxTopics {
		return errors.New("broker maximum topic limit reached")
	}
	if _, exists := b.Topics[topicName]; exists {
		return errors.New("the Topic already exists")
	}
	if policy != "drop-oldest" {
		policy = "reject"
	}
	t := &Topic{Name: topicName, Policy: policy, Retention: retention}
	b.Topics[topicName] = t
	if strings.Contains(topicName, "*") {
		b.wildcards[topicName] = t
		b.compileWildcardRegex(topicName)
	}
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
	var results []message.Message
	appendUpToLimit := func(source []message.Message) {
		for _, msg := range source {
			if len(results) >= limit {
				return
			}
			results = append(results, msg)
		}
	}
	appendUpToLimit(t.HighMessages)
	appendUpToLimit(t.Messages)
	appendUpToLimit(t.LowMessages)
	return results
}

func (b *Broker) IsValidTopicName(name string) bool {
	if strings.Contains(name, "..") {
		return false
	}
	return validTopicRegex.MatchString(name) || validWildcardRegex.MatchString(name)
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
	t.HighMessages = nil
	t.LowMessages = nil
	t.mu.Unlock()
	if b.storage != nil {
		b.storage.ClearLog(topicName)
	}
	log.Printf("Queue '%s' purged completely.\n", topicName)
	return nil
}

func (b *Broker) DeleteTopic(topicName string) error {
	b.mu.Lock()
	t, exists := b.Topics[topicName]
	if !exists {
		b.mu.Unlock()
		return errors.New("queue not found")
	}
	t.mu.Lock()
	t.Deleted = true
	t.mu.Unlock()
	delete(b.Topics, topicName)
	delete(b.wildcards, topicName)
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
	configs := b.webhooks[topicName]
	result := make([]string, 0, len(configs))
	for _, wh := range configs {
		result = append(result, wh.URL)
	}
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
	if len(b.idempotencyKeys) > 5000 {
		for k, v := range b.idempotencyKeys {
			if now.After(v) {
				delete(b.idempotencyKeys, k)
			}
		}
	}
	const maxIdempotencyKeys = 20000
	if len(b.idempotencyKeys) >= maxIdempotencyKeys {
		log.Printf("[Broker] WARNING: idempotency key cap (%d) reached, new keys are not being tracked — duplicate detection is degraded until keys expire\n", maxIdempotencyKeys)
		return false
	}
	b.idempotencyKeys[key] = now.Add(5 * time.Minute)
	return false
}

func (b *Broker) AddSpy(topicName string) (chan message.Message, error) {
	if !b.IsValidTopicName(topicName) {
		return nil, errors.New("invalid topic name")
	}
	t := b.getOrCreateTopic(topicName)
	if t == nil {
		return nil, errors.New("broker maximum topic limit reached")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	ch := make(chan message.Message, 1024)
	t.spies = append(t.spies, ch)
	log.Printf("[Broker] Added spy on topic '%s' (total spies: %d)\n", topicName, len(t.spies))
	return ch, nil
}

func (b *Broker) RemoveSpy(topicName string, ch chan message.Message) {
	b.mu.RLock()
	t, exists := b.Topics[topicName]
	b.mu.RUnlock()
	if !exists {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, spy := range t.spies {
		if spy == ch {
			t.spies = append(t.spies[:i], t.spies[i+1:]...)
			close(ch)
			log.Printf("[Broker] Removed spy from topic '%s' (remaining spies: %d)\n", topicName, len(t.spies))
			break
		}
	}
}

func (b *Broker) CreateGroup(topicName, groupName string) (string, error) {
	if !validTopicRegex.MatchString(groupName) {
		return "", errors.New("invalid group name format")
	}
	virtualName := fmt.Sprintf("%s::%s", topicName, groupName)
	b.mu.Lock()
	if b.bindings[topicName] == nil {
		b.bindings[topicName] = make(map[string]bool)
	}
	b.bindings[topicName][virtualName] = true
	b.mu.Unlock()
	t := b.getOrCreateTopic(virtualName)
	if t == nil {
		return "", errors.New("broker maximum topic limit reached")
	}
	if b.OnGroupCreate != nil {
		b.OnGroupCreate(topicName, groupName)
	}
	return virtualName, nil
}

func (b *Broker) GetGroups(topicName string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	bindings := b.bindings[topicName]
	groups := make([]string, 0, len(bindings))
	prefix := topicName + "::"
	for vt := range bindings {
		groups = append(groups, strings.TrimPrefix(vt, prefix))
	}
	return groups
}

func (b *Broker) GetStateSnapshot() []message.Message {
	b.mu.RLock()
	topics := make([]*Topic, 0, len(b.Topics))
	for _, t := range b.Topics {
		topics = append(topics, t)
	}
	b.mu.RUnlock()
	var allMessages []message.Message
	for _, t := range topics {
		t.mu.Lock()
		allMessages = append(allMessages, t.HighMessages...)
		allMessages = append(allMessages, t.Messages...)
		allMessages = append(allMessages, t.LowMessages...)
		t.mu.Unlock()
	}
	return allMessages
}
