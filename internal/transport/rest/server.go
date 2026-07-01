package rest

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/cluster"
	"github.com/x-name15/tinymq/internal/message"
	"github.com/x-name15/tinymq/internal/transport/ws"
)

// ProxyTransport
var proxyTransport = &http.Transport{
	MaxIdleConns:          100,
	MaxIdleConnsPerHost:   100,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

//go:embed dashboard.html
var dashboardFS embed.FS
var compiledDashboardTemplate = template.Must(template.ParseFS(dashboardFS, "dashboard.html"))

//go:embed static
var staticFS embed.FS

type Server struct {
	broker      *broker.Broker
	httpServer  *http.Server
	version     string
	startTime   time.Time
	clusterNode *cluster.Node
	rateLimiter *ipRateLimiter
}

func NewServer(b *broker.Broker, port string, version string, c *cluster.Node) *Server {
	s := &Server{
		broker:      b,
		version:     version,
		startTime:   time.Now(),
		clusterNode: c,
	}

	mux := http.NewServeMux()

	// ── Health Check ──────────────────────────────────────────
	mux.HandleFunc("/healthz", s.handleHealthz)

	// ── Routes with Auth and Proxy ─────────────────────────────────────────
	mux.HandleFunc("/publish/", s.leaderProxy(s.withAuth(s.handlePublish)))
	mux.HandleFunc("/consume/", s.leaderProxy(s.withAuth(s.handleConsume)))
	mux.HandleFunc("/ack/", s.leaderProxy(s.withAuth(s.handleAck)))
	mux.HandleFunc("/requeue", s.leaderProxy(s.withAuth(s.handleRequeue)))
	mux.HandleFunc("/webhook/", s.leaderProxy(s.withAuth(s.handleRegisterWebhook)))
	mux.HandleFunc("/api/topics", s.leaderProxy(s.withAuth(s.handleCreateTopic)))
	mux.HandleFunc("/stream/", s.leaderProxy(s.withAuth(s.handleStream)))

	// ── Routes with Auth (read-only, no proxy needed) ─────────────────
	mux.HandleFunc("/dashboard", s.withAuth(s.handleDashboard))
	mux.Handle("/static/", s.withAuth(http.FileServerFS(staticFS).ServeHTTP))
	mux.HandleFunc("/metrics", s.withAuth(s.handleMetrics))

	mux.HandleFunc("/api/stats", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		stats, totalWebhooks := b.GetStats()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"stats":          stats,
			"total_webhooks": totalWebhooks,
		})
	}))

	mux.HandleFunc("/api/queues", s.withAuth(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			s.handleListQueues(w, r)
		case http.MethodPost:
			s.leaderProxy(s.handleCreateTopic)(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}))

	mux.HandleFunc("/api/queues/publish", s.leaderProxy(s.withAuth(s.handleQueuePublish)))
	mux.HandleFunc("/api/queues/consume", s.leaderProxy(s.withAuth(s.handleQueueConsume)))
	mux.HandleFunc("/api/queues/peek", s.leaderProxy(s.withAuth(s.handleQueuePeek)))
	mux.HandleFunc("/api/queues/purge", s.leaderProxy(s.withAuth(s.handleQueuePurge)))
	mux.HandleFunc("/api/queues/delete", s.leaderProxy(s.withAuth(s.handleQueueDelete)))
	mux.HandleFunc("/api/queues/webhooks", s.leaderProxy(s.withAuth(s.handleGetWebhooks)))

	// WebSocket
	wsServer := ws.NewServer(b)
	mux.HandleFunc("/ws", s.leaderProxy(s.withAuth(wsServer.HandleWS)))

	// ── Rate limiting ──────────────────────────────────────────────────────
	if rate := getRateLimitFromEnv(); rate > 0 {
		s.rateLimiter = newIPRateLimiter(rate)
		log.Printf("[RateLimit] Enabled: %.0f req/s per IP\n", rate)
	}

	s.httpServer = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	return s
}

// --- MIDDLEWARE REVERSE PROXY ---
func (s *Server) leaderProxy(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.clusterNode != nil && !s.clusterNode.IsLeader() {
			leaderAddr := s.clusterNode.GetLeaderHTTP()
			if leaderAddr != "" {
				log.Printf("[Proxy] Forwarding %s request to Leader (%s) [Conn: %s]\n", r.Method, leaderAddr, r.Header.Get("Upgrade"))

				target, _ := url.Parse("http://" + leaderAddr)
				proxy := httputil.NewSingleHostReverseProxy(target)
				proxy.Transport = proxyTransport

				proxy.FlushInterval = 50 * time.Millisecond

				proxy.Director = func(req *http.Request) {
					req.URL.Scheme = target.Scheme
					req.URL.Host = target.Host
					req.URL.Path = r.URL.Path
					req.URL.RawQuery = r.URL.RawQuery

					if strings.ToLower(r.Header.Get("Upgrade")) == "websocket" {
						req.Header.Set("Upgrade", "websocket")
						req.Header.Set("Connection", "Upgrade")
					}

					if auth := r.Header.Get("Authorization"); auth != "" {
						req.Header.Set("Authorization", auth)
					}

					req.Header.Set("X-Forwarded-For", r.RemoteAddr)
					req.Header.Set("X-Real-IP", r.RemoteAddr)
				}

				proxy.ServeHTTP(w, r)
				return
			}
			http.Error(w, "Cluster is electing a new leader, please retry", http.StatusServiceUnavailable)
			return
		}
		next(w, r)
	}
}

func (s *Server) Start() error {
	certFile := os.Getenv("TINYMQ_TLS_CERT")
	keyFile := os.Getenv("TINYMQ_TLS_KEY")

	if certFile != "" && keyFile != "" {
		log.Printf("[REST] TinyMQ HTTPS (TLS) listening on %s\n", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServeTLS(certFile, keyFile); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("HTTPS server error: %w", err)
		}
		return nil
	}

	log.Printf("[REST] TinyMQ HTTP listening on %s\n", s.httpServer.Addr)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server error: %w", err)
	}
	return nil
}

func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// --- Middleware ---

func (s *Server) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.rateLimiter != nil {
			ip := extractIP(r)
			if !s.rateLimiter.Allow(ip) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				http.Error(w, `{"error":"rate_limit_exceeded","retry_after_seconds":1}`, http.StatusTooManyRequests)
				return
			}
		}

		token := os.Getenv("TINYMQ_API_KEY")
		if token == "" {
			next(w, r)
			return
		}

		// 1. Bearer Header (REST)
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			got := strings.TrimPrefix(authHeader, "Bearer ")
			if subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1 {
				next(w, r)
				return
			}
		}

		// 2. Basic Auth (Dashboard)
		_, pwd, ok := r.BasicAuth()
		if ok && subtle.ConstantTimeCompare([]byte(pwd), []byte(token)) == 1 {
			next(w, r)
			return
		}

		// 3. Query Param (WebSocket)
		if got := r.URL.Query().Get("token"); got != "" {
			if subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1 {
				next(w, r)
				return
			}
		}

		w.Header().Set("WWW-Authenticate", `Basic realm="TinyMQ Secure Dashboard"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}
}

// --- Handlers ---

func extractTopicFromPath(r *http.Request, prefix string) string {
	path := r.URL.Path
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	topic := strings.TrimPrefix(path, prefix)

	topic = strings.TrimSuffix(topic, "/")
	return topic
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := extractTopicFromPath(r, "/publish/")
	if topic == "" {
		http.Error(w, "Topic is required. Usage: POST /publish/{topic}", http.StatusBadRequest)
		return
	}

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

	iKey := r.Header.Get("Idempotency-Key")
	if r.URL.Query().Get("idempotency") == "auto" {
		hash := sha256.Sum256(body)
		iKey = hex.EncodeToString(hash[:])
	}

	if iKey != "" && s.broker.IsIdempotent(iKey) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"status": "ignored", "reason": "idempotency_key_exists", "topic": "%s"}`, topic)))
		return
	}

	userHeaders := make(map[string]string)
	for k, v := range r.Header {
		if strings.HasPrefix(strings.ToUpper(k), "X-MQ-") {
			userHeaders[k] = v[0]
		}
	}

	priority := r.URL.Query().Get("priority")
	if priority == "" {
		priority = "normal"
	}

	var expiresAt *time.Time
	if ttlStr := r.URL.Query().Get("ttl"); ttlStr != "" {
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

	if err := s.broker.Publish(topic, body, userHeaders, priority, expiresAt, deliverAt, isBroadcast); err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "capacity") || strings.Contains(err.Error(), "limit") {
			status = http.StatusTooManyRequests
		} else if strings.Contains(err.Error(), "invalid topic") {
			status = http.StatusBadRequest
		}
		http.Error(w, err.Error(), status)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "{\"status\": \"accepted\", \"topic\": \"%s\"}\n", topic)
}

func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := extractTopicFromPath(r, "/consume/")
	if topic == "" {
		http.Error(w, "Topic is required", http.StatusBadRequest)
		return
	}

	limit := 1
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsedLimit, err := strconv.Atoi(limitStr); err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	if r.URL.Query().Get("peek") == "true" {
		msgs := s.broker.Peek(topic, limit)
		if len(msgs) == 0 {
			if !s.broker.TopicExists(topic) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"status": "not_found", "message": "Topic does not exist"}`))
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		s.respondWithQoL(w, msgs, limit)
		return
	}

	if group := r.URL.Query().Get("group"); group != "" {
		vt, err := s.broker.CreateGroup(topic, group)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		topic = vt
	}

	var timeout time.Duration
	if timeoutStr := r.URL.Query().Get("timeout"); timeoutStr != "" {
		if t, err := time.ParseDuration(timeoutStr); err == nil {
			timeout = t
		}
	}

	notifyChan := make(chan message.Message, 1)
	msgs, ok := s.broker.Consume(topic, limit, notifyChan)

	if !ok && timeout > 0 {
		select {
		case receivedMsg := <-notifyChan:
			msgs = []message.Message{receivedMsg}
			ok = true
		case <-time.After(timeout):
			s.broker.RemoveWaitingConsumer(topic, notifyChan)
		case <-r.Context().Done():
			s.broker.RemoveWaitingConsumer(topic, notifyChan)
			return
		}
	}

	if !ok || len(msgs) == 0 {
		if !s.broker.TopicExists(topic) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte(`{"status": "not_found", "message": "Topic does not exist"}`))
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.URL.Query().Get("auto_ack") == "true" {
		for _, m := range msgs {
			s.broker.Ack(topic, m.ID)
		}
	}

	s.respondWithQoL(w, msgs, limit)
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/ack/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		http.Error(w, "Topic and Message ID required", http.StatusBadRequest)
		return
	}
	topic := parts[0]
	msgID := parts[1]

	if !s.broker.Ack(topic, msgID) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"status": "error", "message": "Message not found"}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "success", "message": "Message acknowledged"}`))
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

	if msg.ID == "" || msg.Topic == "" || !s.broker.IsValidTopicName(msg.Topic) || !s.broker.TopicExists(msg.Topic) {
		http.Error(w, "Invalid or missing topic/id", http.StatusBadRequest)
		return
	}

	s.broker.Requeue(msg)
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status": "requeued"}`))
}

func (s *Server) handleRegisterWebhook(w http.ResponseWriter, r *http.Request) {
	topic := extractTopicFromPath(r, "/webhook/")
	if topic == "" {
		http.Error(w, "Topic required", http.StatusBadRequest)
		return
	}

	var payload struct {
		URL    string `json:"url"`
		Secret string `json:"secret,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.URL == "" {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := validateWebhookURL(payload.URL); err != nil {
		http.Error(w, fmt.Sprintf("Security rejection: %v", err), http.StatusForbidden)
		return
	}

	s.broker.RegisterWebhook(topic, payload.URL, payload.Secret)
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status": "webhook_registered"}`))
}

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Name   string `json:"name"`
		Policy string `json:"policy"`
		Retain string `json:"retain"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if payload.Policy == "" {
		payload.Policy = os.Getenv("TINYMQ_DEFAULT_POLICY")
	}

	var retention time.Duration
	if payload.Retain != "" {
		d, err := time.ParseDuration(payload.Retain)
		if err != nil {
			http.Error(w, "Invalid retain duration (use Go duration format: 1h, 30m, etc.)", http.StatusBadRequest)
			return
		}
		retention = d
	}

	if err := s.broker.CreateTopic(payload.Name, payload.Policy, retention); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(`{"status": "topic_created"}`))
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
		Version:         s.version,
		TotalTopics:     len(stats),
		TotalMessages:   totalMsgs,
		MemoryAllocated: fmt.Sprintf("%.2f MB", float64(m.Alloc)/1024/1024),
		Uptime:          time.Since(s.startTime).Round(time.Second).String(),
		TotalWebhooks:   totalWebhooks,
		Topics:          stats,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	compiledDashboardTemplate.Execute(w, data)
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	stats, totalWebhooks := s.broker.GetStats()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	fmt.Fprintf(w, "# HELP tinymq_topics_total Total Queue/Topics active on Memory\n# TYPE tinymq_topics_total gauge\ntinymq_topics_total %d\n\n", len(stats))
	fmt.Fprintf(w, "# HELP tinymq_webhooks_total Subscribed URLs\n# TYPE tinymq_webhooks_total gauge\ntinymq_webhooks_total %d\n\n", totalWebhooks)

	if len(stats) > 0 {
		fmt.Fprintf(w, "# HELP tinymq_topic_messages Messages held in RAM\n# TYPE tinymq_topic_messages gauge\n")
		for _, st := range stats {
			fmt.Fprintf(w, "tinymq_topic_messages{topic=\"%s\"} %d\n", st.Name, st.MessageCount)
		}
		fmt.Fprintf(w, "\n# HELP tinymq_topic_consumers Consumers waiting in Long-Polling\n# TYPE tinymq_topic_consumers gauge\n")
		for _, st := range stats {
			fmt.Fprintf(w, "tinymq_topic_consumers{topic=\"%s\"} %d\n", st.Name, st.WaitingConsumers)
		}
	}
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	topic := extractTopicFromPath(r, "/stream/")
	if topic == "" {
		http.Error(w, "Topic required", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	spyChan, err := s.broker.AddSpy(topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	defer s.broker.RemoveSpy(topic, spyChan)

	fmt.Fprintf(w, "data: {\"status\":\"connected\",\"topic\":\"%s\"}\n\n", topic)
	flusher.Flush()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-spyChan:
			bytes, _ := json.Marshal(msg)
			fmt.Fprintf(w, "data: %s\n\n", string(bytes))
			flusher.Flush()
		}
	}
}

// -- Queue UI API --
func (s *Server) handleListQueues(w http.ResponseWriter, _ *http.Request) {
	stats, _ := s.broker.GetStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleQueuePublish(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	var req struct {
		Queue     string `json:"queue"`
		Payload   string `json:"payload"`
		TTL       string `json:"ttl,omitempty"`
		Delay     string `json:"delay,omitempty"`
		Broadcast bool   `json:"broadcast,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Queue == "" {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var expiresAt, deliverAt *time.Time
	if req.TTL != "" {
		if d, err := time.ParseDuration(req.TTL); err == nil {
			exp := time.Now().Add(d)
			expiresAt = &exp
		}
	}
	if req.Delay != "" {
		if d, err := time.ParseDuration(req.Delay); err == nil {
			del := time.Now().Add(d)
			deliverAt = &del
		}
	}

	if err := s.broker.Publish(req.Queue, []byte(req.Payload), nil, "normal", expiresAt, deliverAt, req.Broadcast); err != nil {
		http.Error(w, err.Error(), http.StatusTooManyRequests)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(fmt.Sprintf(`{"status": "accepted", "queue": "%s"}`, req.Queue)))
}

func (s *Server) handleQueueConsume(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Queue string `json:"queue"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Queue == "" {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	notifyChan := make(chan message.Message, 1)
	msgs, ok := s.broker.Consume(req.Queue, 1, notifyChan)

	if !ok || len(msgs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.broker.Ack(req.Queue, msgs[0].ID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs[0])
}

func (s *Server) handleQueuePeek(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")
	msgs := s.broker.Peek(queue, 10)
	if msgs == nil {
		msgs = []message.Message{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}

func (s *Server) handleQueuePurge(w http.ResponseWriter, r *http.Request) {
	if err := s.broker.Purge(r.URL.Query().Get("queue")); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleQueueDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.broker.DeleteTopic(r.URL.Query().Get("queue")); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetWebhooks(w http.ResponseWriter, r *http.Request) {
	urls := s.broker.GetWebhooks(r.URL.Query().Get("queue"))
	if urls == nil {
		urls = []string{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(urls)
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

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	uptime := time.Since(s.startTime).Seconds()

	res := map[string]any{
		"status":         "ok",
		"version":        s.version,
		"uptime_seconds": int(uptime),
	}

	if s.clusterNode != nil {
		role := "follower"
		if s.clusterNode.IsLeader() {
			role = "leader"
		}
		res["cluster_role"] = role
		res["cluster_term"] = s.clusterNode.GetCurrentTerm()
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}

func (s *Server) respondWithQoL(w http.ResponseWriter, msgs []message.Message, limit int) {
	w.Header().Set("Content-Type", "application/json")

	type QoLMessageResponse struct {
		message.Message
		PayloadEncoding string `json:"payload_encoding"`
		PayloadText     string `json:"payload_text,omitempty"`
	}

	var batchRes []QoLMessageResponse
	for _, m := range msgs {
		qol := QoLMessageResponse{Message: m, PayloadEncoding: "base64"}
		if utf8.Valid(m.Payload) {
			qol.PayloadText = string(m.Payload)
		}
		batchRes = append(batchRes, qol)
	}

	if limit == 1 && len(batchRes) == 1 {
		json.NewEncoder(w).Encode(batchRes[0])
	} else {
		json.NewEncoder(w).Encode(batchRes)
	}
}

func (s *Server) Handler() http.Handler {
	return s.httpServer.Handler
}
