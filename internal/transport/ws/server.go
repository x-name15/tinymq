package ws

import (
	"crypto/sha1"
	"encoding/base64"
	"log"
	"net/http"
	"sync"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/message"
)

const wsMagicString = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type Server struct {
	broker  *broker.Broker
	clients map[*Client]bool
	mu      sync.RWMutex
}

func NewServer(b *broker.Broker) *Server {
	return &Server{
		broker:  b,
		clients: make(map[*Client]bool),
	}
}

func (s *Server) AddClient(c *Client) {
	s.mu.Lock()
	s.clients[c] = true
	s.mu.Unlock()
	log.Printf("[WS] New client connected. Total active: %d", len(s.clients))
}

func (s *Server) RemoveClient(c *Client) {
	s.mu.Lock()
	if _, ok := s.clients[c]; ok {
		delete(s.clients, c)
		c.conn.Close()
		log.Printf("[WS] Client disconnected. Total active: %d", len(s.clients))
	}
	s.mu.Unlock()
}

func (s *Server) HandleWS(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Upgrade") != "websocket" {
		http.Error(w, "Requires WebSocket protocol", http.StatusBadRequest)
		return
	}

	clientKey := r.Header.Get("Sec-WebSocket-Key")
	if clientKey == "" {
		http.Error(w, "Missing Sec-WebSocket-Key", http.StatusBadRequest)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "Server does not support connection hijacking", http.StatusInternalServerError)
		return
	}

	conn, bufrw, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h := sha1.New()
	h.Write([]byte(clientKey + wsMagicString))
	acceptKey := base64.StdEncoding.EncodeToString(h.Sum(nil))

	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"

	if _, err := bufrw.WriteString(response); err != nil {
		conn.Close()
		return
	}
	bufrw.Flush()

	client := &Client{
		hub:   s,
		conn:  conn,
		rw:    bufrw,
		spies: make(map[string]chan message.Message),
		done:  make(chan struct{}),
	}

	s.AddClient(client)
	go client.readPump()
}

func (s *Server) ActiveClientCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clients)
}
