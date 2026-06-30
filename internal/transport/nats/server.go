package nats

import (
	"bufio"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/message"
)

type Server struct {
	broker   *broker.Broker
	listener net.Listener
	quit     chan struct{}
	wg       sync.WaitGroup
}

type natsConn struct {
	conn net.Conn
	mu   sync.Mutex
}

type subscription struct {
	sid     string
	subject string
	ch      chan message.Message
}

func NewServer(b *broker.Broker) *Server {
	return &Server{
		broker: b,
		quit:   make(chan struct{}),
	}
}

func (nc *natsConn) write(data []byte) error {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	_, err := nc.conn.Write(data)
	return err
}

func (nc *natsConn) writeString(s string) error {
	return nc.write([]byte(s))
}

func (s *Server) Start(port string) error {
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("[NATS] failed to bind on port %s: %w", port, err)
	}
	s.listener = l
	log.Printf("[NATS] Server listening natively on TCP port %s\n", port)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					continue
				}
			}
			go s.handleClient(conn)
		}
	}()
	return nil
}

func (s *Server) Stop() {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	log.Println("[NATS] Server stopped gracefully.")
}

func (s *Server) handleClient(raw net.Conn) {
	addr := raw.RemoteAddr().String()
	log.Printf("[NATS] (+) Connection from %s\n", addr)

	nc := &natsConn{conn: raw}
	subs := make(map[string]*subscription)

	defer func() {
		for _, sub := range subs {
			s.broker.RemoveSpy(sub.subject, sub.ch)
		}
		raw.Close()
		log.Printf("[NATS] (-) Connection closed for %s\n", addr)
	}()

	s.sendInfo(nc)

	authenticated := os.Getenv("TINYMQ_API_KEY") == ""
	reader := bufio.NewReaderSize(raw, 32*1024)

	for {
		raw.SetDeadline(time.Now().Add(60 * time.Second))

		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}

		fields := strings.SplitN(line, " ", 4)
		verb := strings.ToUpper(strings.TrimSpace(fields[0]))

		switch verb {

		case "CONNECT":
			ok, err := s.handleConnect(fields, &authenticated)
			if err != nil {
				nc.writeString(natsErr(err.Error()))
				return
			}
			if ok {
				nc.writeString("+OK\r\n")
			}

		case "PUB":
			if !authenticated {
				nc.writeString(natsErr("Authorization Violation"))
				return
			}
			if err := s.handlePub(nc, fields, reader); err != nil {
				nc.writeString(natsErr(err.Error()))
			}

		case "SUB":
			if !authenticated {
				nc.writeString(natsErr("Authorization Violation"))
				return
			}
			if err := s.handleSub(nc, fields, subs); err != nil {
				nc.writeString(natsErr(err.Error()))
			}

		case "UNSUB":
			if !authenticated {
				nc.writeString(natsErr("Authorization Violation"))
				return
			}
			s.handleUnsub(fields, subs)

		case "PING":
			nc.writeString("PONG\r\n")

		case "PONG":
			nc.writeString("PING\r\n")

		default:
			nc.writeString(natsErr("Unknown Protocol Operation"))
		}
	}
}

func (s *Server) handleConnect(fields []string, authenticated *bool) (bool, error) {
	if len(fields) < 2 {
		return false, fmt.Errorf("CONNECT requires a JSON argument")
	}

	jsonStr := strings.Join(fields[1:], " ")
	var opts struct {
		AuthToken string `json:"auth_token"`
		User      string `json:"user"`
		Pass      string `json:"pass"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &opts); err != nil {
		return false, fmt.Errorf("CONNECT payload is not valid JSON")
	}

	token := os.Getenv("TINYMQ_API_KEY")
	if token == "" {
		*authenticated = true
		return true, nil
	}

	candidates := []string{opts.AuthToken, opts.Pass, opts.User}
	for _, c := range candidates {
		if c != "" && subtle.ConstantTimeCompare([]byte(c), []byte(token)) == 1 {
			*authenticated = true
			return true, nil
		}
	}

	return false, fmt.Errorf("Authorization Violation")
}

func (s *Server) handlePub(nc *natsConn, fields []string, reader *bufio.Reader) error {
	if len(fields) < 3 {
		return fmt.Errorf("PUB requires subject and byte count")
	}

	subject := fields[1]
	sizeStr := strings.TrimSpace(fields[2])

	size, err := strconv.Atoi(sizeStr)
	if err != nil || size < 0 {
		return fmt.Errorf("PUB invalid byte count: %s", sizeStr)
	}

	const maxPayload = 2 << 20
	if size > maxPayload {
		return fmt.Errorf("PUB payload exceeds maximum size (%d bytes)", maxPayload)
	}

	topic := translateSubject(subject)
	if !s.broker.IsValidTopicName(topic) {
		return fmt.Errorf("PUB invalid subject: %s", subject)
	}

	payload := make([]byte, size)
	if size > 0 {
		if _, err := io.ReadFull(reader, payload); err != nil {
			return fmt.Errorf("PUB failed to read payload: %w", err)
		}
	}

	reader.ReadString('\n')
	log.Printf("[NATS] PUB %s (%d bytes)\n", subject, size)

	if err := s.broker.Publish(topic, payload, nil, "normal", nil, nil, false); err != nil {
		log.Printf("[NATS] Broker rejected PUB on '%s': %v\n", topic, err)
		return err
	}
	return nil
}

func (s *Server) handleSub(nc *natsConn, fields []string, subs map[string]*subscription) error {
	if len(fields) < 3 {
		return fmt.Errorf("SUB requires subject and sid")
	}

	subject := strings.TrimSpace(fields[1])
	sid := strings.TrimSpace(fields[2])

	if sid == "" {
		return fmt.Errorf("SUB sid cannot be empty")
	}
	if _, exists := subs[sid]; exists {
		return fmt.Errorf("SUB sid '%s' already in use", sid)
	}

	topic := translateSubject(subject)
	if !s.broker.IsValidTopicName(topic) {
		return fmt.Errorf("SUB invalid subject: %s", subject)
	}

	ch, err := s.broker.AddSpy(topic)
	if err != nil {
		return fmt.Errorf("SUB broker error: %w", err)
	}

	sub := &subscription{sid: sid, subject: topic, ch: ch}
	subs[sid] = sub

	log.Printf("[NATS] SUB %s → topic '%s' (sid=%s)\n", subject, topic, sid)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.pushLoop(nc, sub)
	}()

	return nil
}

func (s *Server) handleUnsub(fields []string, subs map[string]*subscription) {
	if len(fields) < 2 {
		return
	}
	sid := strings.TrimSpace(fields[1])
	sub, exists := subs[sid]
	if !exists {
		return
	}

	s.broker.RemoveSpy(sub.subject, sub.ch)
	delete(subs, sid)
	log.Printf("[NATS] UNSUB sid=%s topic='%s'\n", sid, sub.subject)
}

func (s *Server) pushLoop(nc *natsConn, sub *subscription) {
	for {
		select {
		case msg, open := <-sub.ch:
			if !open {
				return
			}
			if err := s.sendMSG(nc, sub.sid, msg); err != nil {
				return
			}
		case <-s.quit:
			return
		}
	}
}

func (s *Server) sendMSG(nc *natsConn, sid string, msg message.Message) error {
	payload := msg.Payload
	header := fmt.Sprintf("MSG %s %s %d\r\n", msg.Topic, sid, len(payload))
	frame := make([]byte, 0, len(header)+len(payload)+2)
	frame = append(frame, header...)
	frame = append(frame, payload...)
	frame = append(frame, '\r', '\n')
	return nc.write(frame)
}

func (s *Server) sendInfo(nc *natsConn) {
	type infoPayload struct {
		ServerID     string `json:"server_id"`
		Version      string `json:"version"`
		Proto        int    `json:"proto"`
		MaxPayload   int    `json:"max_payload"`
		AuthRequired bool   `json:"auth_required"`
	}

	token := os.Getenv("TINYMQ_API_KEY")
	info := infoPayload{
		ServerID:     "tinymq",
		Version:      "2.8.5",
		Proto:        1,
		MaxPayload:   2 << 20,
		AuthRequired: token != "",
	}

	raw, _ := json.Marshal(info)
	nc.writeString(fmt.Sprintf("INFO %s\r\n", string(raw)))
}

func translateSubject(subject string) string {
	if strings.HasSuffix(subject, ".>") {
		subject = subject[:len(subject)-2] + ".*"
	} else if subject == ">" {
		subject = "*"
	}
	return subject
}

func natsErr(reason string) string {
	return fmt.Sprintf("-ERR '%s'\r\n", reason)
}
