package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"embed"
	"html/template"
	"runtime"
	"strconv"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/storage"
	"github.com/x-name15/tinymq/internal/message"
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
		Name string `json:"name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := s.broker.CreateTopic(payload.Name); err != nil {
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


func main() {
	log.Printf("Starting TinyMQ v%s...\n", Version)

	dataDir := "./data"
	store, err := storage.New(dataDir)
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

	srv := &Server{broker: b}

	mux := http.NewServeMux()
	mux.HandleFunc("/publish/", srv.handlePublish)
	mux.HandleFunc("/consume/", srv.handleConsume)
	mux.HandleFunc("/ack/", srv.handleAck)
	mux.HandleFunc("/requeue", srv.handleRequeue)
	mux.HandleFunc("/webhook/", srv.handleRegisterWebhook)
	mux.HandleFunc("/api/topics", srv.handleCreateTopic)
	mux.HandleFunc("/dashboard", srv.handleDashboard)

	mux.HandleFunc("/api/queues", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			srv.handleListQueues(w, r)
		case http.MethodPost:
			srv.handleCreateTopic(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/queues/publish", srv.handleQueuePublish)
	mux.HandleFunc("/api/queues/consume", srv.handleQueueConsume)
	mux.HandleFunc("/api/queues/peek", srv.handleQueuePeek)
	mux.HandleFunc("/api/queues/purge", srv.handleQueuePurge)
	mux.HandleFunc("/api/queues/delete", srv.handleQueueDelete)
	mux.HandleFunc("/api/queues/webhooks", srv.handleGetWebhooks)

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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("Forced shutdown: %v", err)
	}

	store.CloseAll()
	log.Println("TinyMQ stopped.")
}