package tests

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/message"
	"github.com/x-name15/tinymq/internal/transport/mqtt"
	"github.com/x-name15/tinymq/internal/transport/rest"
)

// setupIntegrationStack boots a full REST + MQTT stack on random ports and
// returns both servers plus the ports, ready to accept connections.
// The caller gets t.Cleanup for free — nothing leaks.
func setupIntegrationStack(t *testing.T) (b *broker.Broker, restPort string, mqttPort string) {
	t.Helper()

	findFreePort := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("could not bind to a free port: %v", err)
		}
		port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
		l.Close()
		return port
	}

	restPort = findFreePort()
	mqttPort = findFreePort()

	b = broker.New(nil)

	restSrv := rest.NewServer(b, restPort, "integration-test", nil)
	go restSrv.Start()

	mqttSrv := mqtt.NewServer(b)
	go mqttSrv.Start(mqttPort)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		restSrv.Stop(ctx)
		mqttSrv.Stop()
	})

	time.Sleep(250 * time.Millisecond)

	return b, restPort, mqttPort
}

// httpPublish fires a POST /publish/{topic} and returns the HTTP status code.
func httpPublish(t *testing.T, port, topic string, payload []byte, queryParams string) int {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%s/publish/%s", port, topic)
	if queryParams != "" {
		url += "?" + queryParams
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("POST /publish/%s failed: %v", topic, err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// httpConsume fires GET /consume/{topic} and returns the parsed response body.
func httpConsume(t *testing.T, port, topic string) (int, map[string]any) {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%s/consume/%s", port, topic)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /consume/%s failed: %v", topic, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return resp.StatusCode, nil
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode consume response: %v", err)
	}
	return resp.StatusCode, result
}

// dialWSAndHandshake opens a raw TCP connection and performs the RFC 6455 upgrade.
func dialWSAndHandshake(t *testing.T, port string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		t.Fatalf("failed to dial WebSocket: %v", err)
	}

	clientKey := "dGhlIHNhbXBsZSBub25jZQ=="
	req := "GET /ws HTTP/1.1\r\n" +
		"Host: 127.0.0.1:" + port + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + clientKey + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	conn.Write([]byte(req))

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("failed to read WebSocket handshake: %v", err)
	}
	if resp.StatusCode != 101 {
		t.Fatalf("expected 101 Switching Protocols, got %d", resp.StatusCode)
	}

	expectedHash := sha1.Sum([]byte(clientKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	expectedAccept := base64.StdEncoding.EncodeToString(expectedHash[:])
	if resp.Header.Get("Sec-WebSocket-Accept") != expectedAccept {
		t.Fatalf("invalid Sec-WebSocket-Accept from server")
	}

	return conn
}

// dialMQTTAndConnect opens a raw TCP connection, sends a CONNECT packet, and
// reads the CONNACK. Returns the open connection on success.
func dialMQTTAndConnect(t *testing.T, mqttPort, clientID string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+mqttPort, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial MQTT: %v", err)
	}

	idBytes := []byte(clientID)
	idLen := len(idBytes)
	remainingLen := 10 + 2 + idLen
	packet := []byte{
		0x10, byte(remainingLen),
		0x00, 0x04, 'M', 'Q', 'T', 'T',
		0x04,
		0x02,
		0x00, 0x3C,
		byte(idLen >> 8), byte(idLen),
	}
	packet = append(packet, idBytes...)
	conn.Write(packet)

	connAck := make([]byte, 4)
	if _, err := io.ReadFull(conn, connAck); err != nil {
		t.Fatalf("failed to read CONNACK: %v", err)
	}
	if connAck[0] != 0x20 || connAck[3] != 0x00 {
		t.Fatalf("MQTT CONNACK rejected. Response: %v", connAck)
	}

	return conn
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 1 — MQTT publish → HTTP consume
// Validates that a message injected through the MQTT gateway is persisted in
// the broker and retrievable via the HTTP consume endpoint.
// ─────────────────────────────────────────────────────────────────────────────

func TestIntegration_MQTTPublish_HTTPConsume(t *testing.T) {
	_, restPort, mqttPort := setupIntegrationStack(t)

	topic := "integration/mqtt/http"
	expectedPayload := "sensor-reading-42"

	mqttConn := dialMQTTAndConnect(t, mqttPort, "test-mqtt-producer")
	defer mqttConn.Close()

	topicBytes := []byte(topic)
	topicLen := len(topicBytes)
	payloadBytes := []byte(expectedPayload)
	remainingLen := 2 + topicLen + len(payloadBytes)

	pubPacket := []byte{0x30, byte(remainingLen), byte(topicLen >> 8), byte(topicLen)}
	pubPacket = append(pubPacket, topicBytes...)
	pubPacket = append(pubPacket, payloadBytes...)
	mqttConn.Write(pubPacket)

	time.Sleep(100 * time.Millisecond)

	status, body := httpConsume(t, restPort, topic)
	if status != http.StatusOK {
		t.Fatalf("expected 200 from HTTP consume, got %d", status)
	}

	payloadText, ok := body["payload_text"].(string)
	if !ok || payloadText != expectedPayload {
		t.Errorf("payload_text mismatch: expected %q, got %q", expectedPayload, payloadText)
	}

	t.Logf("SUCCESS: MQTT → HTTP. payload_text=%q", payloadText)
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 2 — HTTP publish → WebSocket receive
// Validates that a message published via the HTTP API is pushed in real-time
// to a WebSocket subscriber that was already connected and listening.
// ─────────────────────────────────────────────────────────────────────────────

func TestIntegration_HTTPPublish_WebSocketReceive(t *testing.T) {
	_, restPort, _ := setupIntegrationStack(t)

	topic := "integration/http/ws"
	expectedPayload := "hello-from-http"

	wsConn := dialWSAndHandshake(t, restPort)
	defer wsConn.Close()

	subscribeCmd := fmt.Sprintf(`{"action":"subscribe","topic":"%s"}`, topic)
	writeWSFrame(wsConn, subscribeCmd)

	subAck := readWSFrame(wsConn)
	if !strings.Contains(subAck, "subscribed") {
		t.Fatalf("WebSocket subscription failed: %s", subAck)
	}

	status := httpPublish(t, restPort, topic, []byte(expectedPayload), "")
	if status != http.StatusAccepted {
		t.Fatalf("HTTP publish returned %d, expected 202", status)
	}

	wsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	pushed := readWSFrame(wsConn)

	if pushed == "" {
		t.Fatal("WebSocket subscriber never received the pushed message")
	}

	expectedBase64 := base64.StdEncoding.EncodeToString([]byte(expectedPayload))
	if !strings.Contains(pushed, expectedBase64) {
		t.Errorf("pushed frame does not contain expected Base64 payload.\nExpected to find: %s\nGot: %s", expectedBase64, pushed)
	}

	t.Logf("SUCCESS: HTTP → WebSocket. pushed frame contains correct Base64 payload.")
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 3 — Priority ordering (high / normal / low)
// Publishes one message at each priority level in reverse order (low first),
// then consumes three times and asserts the order: high → normal → low.
// ─────────────────────────────────────────────────────────────────────────────

func TestIntegration_PriorityOrdering(t *testing.T) {
	b, _, _ := setupIntegrationStack(t)

	topic := "integration/priority"

	b.Publish(topic, []byte("low-msg"), nil, "low", nil, nil, false)
	b.Publish(topic, []byte("normal-msg"), nil, "normal", nil, nil, false)
	b.Publish(topic, []byte("high-msg"), nil, "high", nil, nil, false)

	consume := func(expectedPayload string) {
		t.Helper()
		notifyChan := make(chan message.Message, 1)
		msgs, ok := b.Consume(topic, 1, notifyChan)
		if !ok || len(msgs) == 0 {
			t.Fatalf("expected a message from topic '%s', got none", topic)
		}
		got := string(msgs[0].Payload)
		if got != expectedPayload {
			t.Errorf("priority ordering broken: expected %q, got %q", expectedPayload, got)
		}
		t.Logf("  consumed: %q ✓", got)
	}

	t.Log("Consuming in expected priority order: high → normal → low")
	consume("high-msg")
	consume("normal-msg")
	consume("low-msg")

	t.Log("SUCCESS: Messages were delivered in correct priority order.")
}

// ─────────────────────────────────────────────────────────────────────────────
// TEST 4 — Topic-level retention (retain field)
// Creates a topic with a very short retention window, publishes a message,
// waits for it to expire, and asserts the broker drops it on consume.
// ─────────────────────────────────────────────────────────────────────────────

func TestIntegration_TopicRetentionExpiry(t *testing.T) {
	b, _, _ := setupIntegrationStack(t)

	topic := "integration/retention"

	if err := b.CreateTopic(topic, "reject", 100*time.Millisecond); err != nil {
		t.Fatalf("failed to create topic with retention: %v", err)
	}

	if err := b.Publish(topic, []byte("will-expire"), nil, "normal", nil, nil, false); err != nil {
		t.Fatalf("publish failed: %v", err)
	}

	notifyChan := make(chan message.Message, 1)
	msgs, ok := b.Consume(topic, 1, notifyChan)
	if !ok || len(msgs) == 0 {
		t.Fatal("message should be accessible before the retention window expires")
	}
	t.Log("  message visible before expiry ✓")

	if err := b.Publish(topic, []byte("will-expire-2"), nil, "normal", nil, nil, false); err != nil {
		t.Fatalf("second publish failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	notifyChan2 := make(chan message.Message, 1)
	msgsAfter, okAfter := b.Consume(topic, 1, notifyChan2)

	if okAfter || len(msgsAfter) > 0 {
		t.Errorf("broker delivered an expired message that should have been dropped by the retention policy")
	}

	t.Log("SUCCESS: Expired message was correctly dropped by topic-level retention.")
}
