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

// Official RFC 6455 Magic String
const wsMagicString = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type Server struct {
	broker   *broker.Broker
	clients  map[*Client]bool
	register chan *Client
	remove   chan *Client
	mu       sync.RWMutex
}

func NewServer(b *broker.Broker) *Server {
	s := &Server{
		broker:   b,
		clients:  make(map[*Client]bool),
		register: make(chan *Client),
		remove:   make(chan *Client),
	}
	go s.runHub()
	return s
}

// runHub manages the connection lifecycle to prevent blocking
func (s *Server) runHub() {
	for {
		select {
		case client := <-s.register:
			s.mu.Lock()
			s.clients[client] = true
			s.mu.Unlock()
			log.Printf("[WS] New client connected. Total active: %d", len(s.clients))

		case client := <-s.remove:
			s.mu.Lock()
			if _, ok := s.clients[client]; ok {
				delete(s.clients, client)
				client.conn.Close()
				log.Printf("[WS] Client disconnected. Total active: %d", len(s.clients))
			}
			s.mu.Unlock()
		}
	}
}

// HandleWS is the endpoint that hijacks the HTTP connection
func (s *Server) HandleWS(w http.ResponseWriter, r *http.Request) {
	// 1. Validate WebSocket upgrade request
	if r.Header.Get("Upgrade") != "websocket" {
		http.Error(w, "Requires WebSocket protocol", http.StatusBadRequest)
		return
	}

	clientKey := r.Header.Get("Sec-WebSocket-Key")
	if clientKey == "" {
		http.Error(w, "Missing Sec-WebSocket-Key", http.StatusBadRequest)
		return
	}

	// 2. Hijack the underlying TCP connection
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

	// 3. Generate the acceptance hash (RFC 6455)
	h := sha1.New()
	h.Write([]byte(clientKey + wsMagicString))
	acceptKey := base64.StdEncoding.EncodeToString(h.Sum(nil))

	// 4. Write the HTTP 101 response directly to raw bytes
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"

	if _, err := bufrw.WriteString(response); err != nil {
		conn.Close()
		return
	}
	bufrw.Flush()

	// 5. Connection established! Register client in the Hub
	client := &Client{
		hub:   s,
		conn:  conn,
		rw:    bufrw,
		spies: make(map[string]chan message.Message),
	}

	s.register <- client

	// Start the goroutine to listen to this client
	go client.readPump()
}
