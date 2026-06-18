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

	"tinymq/internal/broker"
	"tinymq/internal/storage"
	"tinymq/internal/message"
)

type Server struct {
	broker *broker.Broker
}

var dashboardFS embed.FS
var compiledDashboardTemplate = template.Must(template.ParseFS(dashboardFS, "dashboard.html"))

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

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	if len(body) == 0 {
		http.Error(w, "Payload is empty", http.StatusBadRequest)
		return
	}

	s.broker.Publish(topic, body)

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
	if timeoutStr == "" {
		timeout = 0 * time.Second 
	} else {
		var err error
		timeout, err = time.ParseDuration(timeoutStr)
		if err != nil {
			http.Error(w, "Invalid timeout format. Example: 5s, 500ms", http.StatusBadRequest)
			return
		}
	}

	notifyChan := make(chan message.Message, 1)

	msg, ok := s.broker.Consume(topic, notifyChan)
	
	if !ok && timeout > 0 {
		log.Printf("Topic '%s' empty. Consumer waiting for up to %v...\n", topic, timeout)
		
		select {
		case receivedMsg := <-notifyChan:
			msg = &receivedMsg
			ok = true
		case <-time.After(timeout):
			s.broker.RemoveWaitingConsumer(topic, notifyChan)
		case <-r.Context().Done():
			log.Printf("🔌 Consumer disconnected prematurely from topic '%s'. Cleaning up...\n", topic)
			s.broker.RemoveWaitingConsumer(topic, notifyChan)
			return
		}
	}

	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"status": "empty", "message": "No messages in topic"}`))
		return
	}

	autoAckParam := r.URL.Query().Get("auto_ack")
	if autoAckParam == "true" {
		s.broker.Ack(topic, msg.ID)
		log.Printf("Auto-Acknowledged message %s in topic: %s\n", msg.ID, topic)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msg)
	log.Printf("Consumed message from topic: %s\n", topic)
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
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := s.broker.GetStats()
	totalMessages := 0
	for _, t := range stats {
		totalMessages += t.MessageCount
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memoryKB := fmt.Sprintf("%.2f KB", float64(m.Alloc)/1024.0)

	data := struct {
		Version         string 
		TotalTopics     int
		TotalMessages   int
		MemoryAllocated string
		Topics          []broker.TopicStat
	}{
		Version:         Version,
		TotalTopics:     len(stats),
		TotalMessages:   totalMessages,
		MemoryAllocated: memoryKB,
		Topics:          stats,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	compiledDashboardTemplate.Execute(w, data)
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
	mux.HandleFunc("/dashboard", srv.handleDashboard)
	
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

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	<-stopChan

	log.Println("\nShutting down TinyMQ gracefully (Graceful Shutdown)...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("Error shutting down the HTTP server: %v\n", err)
	}

	if err := store.CloseAll(); err != nil {
		log.Printf("Error closing disk logs: %v\n", err)
	} else {
		log.Println("All data in memory was successfully persisted to disk.")
	}

	log.Println("Sayonara!")
}