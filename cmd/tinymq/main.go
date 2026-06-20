package main

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/helper"
	"github.com/x-name15/tinymq/internal/message"
	"github.com/x-name15/tinymq/internal/storage"
)

type Server struct {
	broker *broker.Broker
}

//go:embed dashboard.html
var dashboardFS embed.FS

var compiledDashboardTemplate = template.Must(template.ParseFS(dashboardFS, "dashboard.html"))

var startTime = time.Now()

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 || parts[2] == "" {
		http.Error(w, "Topic is required. Usage: POST /publish/{topic}", http.StatusBadRequest)
		return
	}
	topic := parts[2]

	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)

	body, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, "Payload too large or unreadable (Max 2MB)", http.StatusRequestEntityTooLarge)
		return
	}

	if len(body) == 0 {
		http.Error(w, "Payload is empty", http.StatusBadRequest)
		return
	}

	var expiresAt *time.Time
	ttlStr := r.URL.Query().Get("ttl")
	if ttlStr != "" {
		if duration, err := time.ParseDuration(ttlStr); err == nil {
			exp := time.Now().Add(duration)
			expiresAt = &exp
		}
	}

	var deliverAt *time.Time
	if delayStr := r.URL.Query().Get("delay"); delayStr != "" {
		if duration, err := time.ParseDuration(delayStr); err == nil {
			del := time.Now().Add(duration)
			deliverAt = &del
		}
	}

	iKey := r.Header.Get("Idempotency-Key")
	if iKey != "" && s.broker.IsIdempotent(iKey) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"status": "ignored", "reason": "idempotency_key_exists", "topic": "%s"}`, topic)))
		log.Printf("Ignored duplicate message on topic: %s (Key: %s)\n", topic, iKey)
		return
	}

	isBroadcast := r.URL.Query().Get("broadcast") == "true"

	s.broker.Publish(topic, body, expiresAt, deliverAt, isBroadcast)

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "{\"status\": \"accepted\", \"topic\": \"%s\"}\n", topic)
	log.Printf("Published message to topic: %s\n", topic)
}

func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 || parts[2] == "" {
		http.Error(w, "Topic is required. Usage: GET /consume/{topic}", http.StatusBadRequest)
		return
	}
	topic := parts[2]

	if group := r.URL.Query().Get("group"); group != "" {
		vt, err := s.broker.CreateGroup(topic, group)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		topic = vt
	}

	timeoutStr := r.URL.Query().Get("timeout")
	var timeout time.Duration
	if timeoutStr != "" {
		var err error
		timeout, err = time.ParseDuration(timeoutStr)
		if err != nil {
			http.Error(w, "Invalid timeout format. Example: 5s, 500ms", http.StatusBadRequest)
			return
		}
	}

	limit := 1
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	notifyChan := make(chan message.Message, 1)
	msgs, ok := s.broker.Consume(topic, limit, notifyChan)

	if !ok && timeout > 0 {
		log.Printf("Topic '%s' empty. Consumer waiting for up to %v...\n", topic, timeout)

		select {
		case receivedMsg := <-notifyChan:
			msgs = []message.Message{receivedMsg}
			ok = true
		case <-time.After(timeout):
			s.broker.RemoveWaitingConsumer(topic, notifyChan)
		case <-r.Context().Done():
			log.Printf("Consumer disconnected prematurely from topic '%s'. Cleaning up...\n", topic)
			s.broker.RemoveWaitingConsumer(topic, notifyChan)
			return
		}
	}

	if !ok || len(msgs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"status": "empty", "message": "No messages in topic"}`))
		return
	}

	autoAckParam := r.URL.Query().Get("auto_ack")
	if autoAckParam == "true" {
		for _, m := range msgs {
			s.broker.Ack(topic, m.ID)
			log.Printf("Auto-Acknowledged message %s in topic: %s\n", m.ID, topic)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if limit == 1 && len(msgs) == 1 {
		json.NewEncoder(w).Encode(msgs[0])
	} else {
		json.NewEncoder(w).Encode(msgs)
	}
	log.Printf("Consumed %d message(s) from topic: %s\n", len(msgs), topic)
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 || parts[2] == "" || parts[3] == "" {
		http.Error(w, "Topic and Message ID are required. Usage: POST /ack/{topic}/{id}", http.StatusBadRequest)
		return
	}
	topic := parts[2]
	msgID := parts[3]

	success := s.broker.Ack(topic, msgID)
	if !success {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"status": "error", "message": "Message not found or already acknowledged"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "success", "message": "Message acknowledged"}`))
	log.Printf("Acknowledged message %s in topic: %s\n", msgID, topic)
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	stats, totalWebhooks := s.broker.GetStats()

	totalMsgs := 0
	for _, stat := range stats {
		totalMsgs += stat.MessageCount
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	data := struct {
		Version         string
		TotalTopics     int
		TotalMessages   int
		MemoryAllocated string
		Uptime          string
		TotalWebhooks   int
		Topics          []broker.TopicStat
	}{
		Version:         Version,
		TotalTopics:     len(stats),
		TotalMessages:   totalMsgs,
		MemoryAllocated: fmt.Sprintf("%.2f MB", float64(m.Alloc)/1024/1024),
		Uptime:          time.Since(startTime).Round(time.Second).String(),
		TotalWebhooks:   totalWebhooks,
		Topics:          stats,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := compiledDashboardTemplate.Execute(w, data); err != nil {
		http.Error(w, "Failed to render dashboard", http.StatusInternalServerError)
	}
}

func (s *Server) handleRequeue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)

	var msg message.Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid message format", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if msg.ID == "" || msg.Topic == "" {
		http.Error(w, "Invalid message: id and topic are required", http.StatusBadRequest)
		return
	}

	if !s.broker.IsValidTopicName(msg.Topic) {
		http.Error(w, "Invalid topic name", http.StatusBadRequest)
		return
	}

	if !s.broker.TopicExists(msg.Topic) {
		http.Error(w, "Topic not found", http.StatusNotFound)
		return
	}

	s.broker.Requeue(msg)

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "requeued"}`))
}

func (s *Server) handleRegisterWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 || parts[2] == "" {
		http.Error(w, "Topic is required. Usage: POST /webhook/{topic}", http.StatusBadRequest)
		return
	}
	topic := parts[2]

	var payload struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.URL == "" {
		http.Error(w, "Invalid JSON body, expected {\"url\": \"http...\"}", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := validateWebhookURL(payload.URL); err != nil {
		http.Error(w, fmt.Sprintf("Security rejection: %v", err), http.StatusForbidden)
		return
	}

	s.broker.RegisterWebhook(topic, payload.URL)

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status": "webhook_registered"}`))
}

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		Name   string `json:"name"`
		Policy string `json:"policy"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if payload.Policy == "" {
		payload.Policy = os.Getenv("TINYMQ_DEFAULT_POLICY")
	}

	if err := s.broker.CreateTopic(payload.Name, payload.Policy); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status": "topic_created"}`))
}

func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats, _ := s.broker.GetStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleQueuePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)

	var req struct {
		Queue     string `json:"queue"`
		Payload   string `json:"payload"`
		TTL       string `json:"ttl,omitempty"`
		Delay     string `json:"delay,omitempty"`
		Broadcast bool   `json:"broadcast,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Queue == "" {
		http.Error(w, "Invalid payload. Expected {\"queue\": \"...\", \"payload\": \"...\"}", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var expiresAt *time.Time
	if req.TTL != "" {
		if d, err := time.ParseDuration(req.TTL); err == nil {
			exp := time.Now().Add(d)
			expiresAt = &exp
		}
	}

	var deliverAt *time.Time
	if req.Delay != "" {
		if d, err := time.ParseDuration(req.Delay); err == nil {
			del := time.Now().Add(d)
			deliverAt = &del
		}
	}

	err := s.broker.Publish(req.Queue, []byte(req.Payload), expiresAt, deliverAt, req.Broadcast)
	if err != nil {
		http.Error(w, err.Error(), http.StatusTooManyRequests)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(fmt.Sprintf(`{"status": "accepted", "queue": "%s"}`, req.Queue)))
	log.Printf("[API-QUEUES] Message published via UI in: %s\n", req.Queue)
}

func (s *Server) handleQueueConsume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Queue string `json:"queue"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Queue == "" {
		http.Error(w, "Invalid payload. Expected {\"queue\": \"...\"}", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	notifyChan := make(chan message.Message, 1)
	msgs, ok := s.broker.Consume(req.Queue, 1, notifyChan)

	if !ok || len(msgs) == 0 {
		s.broker.RemoveWaitingConsumer(req.Queue, notifyChan)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	s.broker.Ack(req.Queue, msgs[0].ID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs[0])
	log.Printf("[API-QUEUES] Message consumed from queue: %s\n", req.Queue)
}

func (s *Server) handleQueuePeek(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	queue := r.URL.Query().Get("queue")
	if queue == "" {
		http.Error(w, "Queue is required", http.StatusBadRequest)
		return
	}
	msgs := s.broker.Peek(queue, 10)
	if msgs == nil {
		msgs = []message.Message{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}

func (s *Server) handleQueuePurge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	queue := r.URL.Query().Get("queue")
	if err := s.broker.Purge(queue); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleQueueDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	queue := r.URL.Query().Get("queue")
	if err := s.broker.DeleteTopic(queue); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetWebhooks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	queue := r.URL.Query().Get("queue")
	urls := s.broker.GetWebhooks(queue)
	if urls == nil {
		urls = []string{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(urls)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	stats, totalWebhooks := s.broker.GetStats()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	fmt.Fprintf(w, "# HELP tinymq_topics_total Total Queue/Topics active on Memory\n")
	fmt.Fprintf(w, "# TYPE tinymq_topics_total gauge\n")
	fmt.Fprintf(w, "tinymq_topics_total %d\n\n", len(stats))

	fmt.Fprintf(w, "# HELP tinymq_webhooks_total Subscribed URLs\n")
	fmt.Fprintf(w, "# TYPE tinymq_webhooks_total gauge\n")
	fmt.Fprintf(w, "tinymq_webhooks_total %d\n\n", totalWebhooks)

	if len(stats) > 0 {
		fmt.Fprintf(w, "# HELP tinymq_topic_messages Messages held in RAM\n")
		fmt.Fprintf(w, "# TYPE tinymq_topic_messages gauge\n")
		for _, st := range stats {
			fmt.Fprintf(w, "tinymq_topic_messages{topic=\"%s\"} %d\n", st.Name, st.MessageCount)
		}
		fmt.Fprintf(w, "\n")

		fmt.Fprintf(w, "# HELP tinymq_topic_consumers Consumers waiting in Long-Polling\n")
		fmt.Fprintf(w, "# TYPE tinymq_topic_consumers gauge\n")
		for _, st := range stats {
			fmt.Fprintf(w, "tinymq_topic_consumers{topic=\"%s\"} %d\n", st.Name, st.WaitingConsumers)
		}
	}
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 || parts[2] == "" {
		http.Error(w, "Topic is required. Usage: GET /stream/{topic}", http.StatusBadRequest)
		return
	}
	topic := parts[2]

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	spyChan := s.broker.AddSpy(topic)

	ctx := r.Context()
	defer s.broker.RemoveSpy(topic, spyChan)

	fmt.Fprintf(w, "data: {\"status\":\"connected\",\"topic\":\"%s\"}\n\n", topic)
	flusher.Flush()
	log.Printf("[SSE] Client connected to live stream of '%s'\n", topic)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[SSE] Client disconnected from '%s'\n", topic)
			return
		case msg := <-spyChan:
			bytes, _ := json.Marshal(msg)
			fmt.Fprintf(w, "data: %s\n\n", string(bytes))
			flusher.Flush()
		}
	}
}

// Middleware de autenticación global (Soporta Bearer y Basic Auth)
func withAuth(next http.HandlerFunc) http.HandlerFunc {
	token := os.Getenv("TINYMQ_API_KEY")
	return func(w http.ResponseWriter, r *http.Request) {

		if token == "" {
			next(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")

		if strings.HasPrefix(authHeader, "Bearer ") {
			got := strings.TrimPrefix(authHeader, "Bearer ")
			if subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1 {
				next(w, r)
				return
			}
		}

		_, pwd, ok := r.BasicAuth()
		if ok && subtle.ConstantTimeCompare([]byte(pwd), []byte(token)) == 1 {
			next(w, r)
			return
		}

		w.Header().Set("WWW-Authenticate", `Basic realm="TinyMQ Secure Dashboard"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}
}

func validateWebhookURL(rawURL string) error {
	u, err := url.ParseRequestURI(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("only valid http/https URLs are allowed")
	}

	ips, err := net.LookupIP(u.Hostname())
	if err != nil {
		return errors.New("could not resolve webhook hostname")
	}

	for _, ip := range ips {
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() {
			return errors.New("webhook URL resolves to a forbidden private/internal network address")
		}
	}
	return nil
}

func main() {
	helper.LoadEnv()

	log.Printf("Starting TinyMQ v%s...\n", Version)

	dataDir := "./data"
	syncWrites := os.Getenv("TINYMQ_FSYNC") == "true"
	if syncWrites {
		log.Println("Strict disk durability (FSync) is ENABLED.")
	}

	store, err := storage.New(dataDir, syncWrites)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	b := broker.New(store)

	files, err := os.ReadDir(dataDir)
	if err == nil {
		var topicsToRecover []string
		for _, file := range files {
			if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
				topicName := strings.TrimSuffix(file.Name(), ".log")
				topicsToRecover = append(topicsToRecover, topicName)
			}
		}

		if len(topicsToRecover) > 0 {
			b.LoadExistingTopics(topicsToRecover)

			for _, name := range topicsToRecover {
				if err := store.CompactLog(name); err != nil {
					log.Printf("Failed to compact log for topic %s: %v\n", name, err)
				} else {
					log.Printf("Log for topic '%s' successfully compacted!\n", name)
				}
			}
		}
	}

	ctx, cancelGC := context.WithCancel(context.Background())
	defer cancelGC()

	go func() {
		compactionInterval := 10 * time.Minute
		if envInt := os.Getenv("TINYMQ_COMPACT_INTERVAL"); envInt != "" {
			if d, err := time.ParseDuration(envInt); err == nil {
				compactionInterval = d
			}
		}

		ticker := time.NewTicker(compactionInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				stats, _ := b.GetStats()
				compacted := 0
				for _, st := range stats {
					if err := store.CompactLog(st.Name); err == nil {
						compacted++
					}
				}
				if compacted > 0 {
					log.Printf("[GC] Auto-compacted %d WAL files to free disk space.\n", compacted)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	srv := &Server{broker: b}

	mux := http.NewServeMux()
	mux.HandleFunc("/publish/", withAuth(srv.handlePublish))
	mux.HandleFunc("/consume/", withAuth(srv.handleConsume))
	mux.HandleFunc("/ack/", withAuth(srv.handleAck))
	mux.HandleFunc("/requeue", withAuth(srv.handleRequeue))
	mux.HandleFunc("/webhook/", withAuth(srv.handleRegisterWebhook))
	mux.HandleFunc("/api/topics", withAuth(srv.handleCreateTopic))
	mux.HandleFunc("/stream/", withAuth(srv.handleStream))
	mux.HandleFunc("/dashboard", withAuth(srv.handleDashboard))
	mux.HandleFunc("/metrics", withAuth(srv.handleMetrics))

	mux.HandleFunc("/api/queues", withAuth(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			srv.handleListQueues(w, r)
		case http.MethodPost:
			srv.handleCreateTopic(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	mux.HandleFunc("/api/queues/publish", withAuth(srv.handleQueuePublish))
	mux.HandleFunc("/api/queues/consume", withAuth(srv.handleQueueConsume))
	mux.HandleFunc("/api/queues/peek", withAuth(srv.handleQueuePeek))
	mux.HandleFunc("/api/queues/purge", withAuth(srv.handleQueuePurge))
	mux.HandleFunc("/api/queues/delete", withAuth(srv.handleQueueDelete))
	mux.HandleFunc("/api/queues/webhooks", withAuth(srv.handleGetWebhooks))

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		stats, totalWebhooks := b.GetStats()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"stats":          stats,
			"total_webhooks": totalWebhooks,
		})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "7800"
	}

	addr := ":" + port
	httpServer := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Printf("TinyMQ listening on port %s\n", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down TinyMQ gracefully...")
	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()

	if err := httpServer.Shutdown(ctxShutdown); err != nil {
		log.Fatalf("Forced shutdown: %v", err)
	}

	store.CloseAll()
	log.Println("TinyMQ stopped.")
}
