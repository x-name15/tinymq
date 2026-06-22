package tests

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/transport/rest"
)

// setupTestServer runs broker server and port for testing
func setupTestServer() (*broker.Broker, *rest.Server, string) {
	b := broker.New(nil)
	port := "17800"
	s := rest.NewServer(b, port, "test-v2.5.1")
	go s.Start()
	time.Sleep(100 * time.Millisecond)
	return b, s, port
}

// writeWSFrame masks (XOR) and send a payload pretending to be a web browser (RFC 6455)
func writeWSFrame(conn net.Conn, payload string) {
	data := []byte(payload)
	length := len(data)

	// OpCodeText (1) + FIN (0x80) = 0x81
	// Mask bit (0x80) + length (assuming len < 126 for the test)
	header := []byte{0x81, byte(length) | 0x80}

	// Static Mask key for testing purposes (in real scenarios, this should be random)
	maskKey := []byte{0x12, 0x34, 0x56, 0x78}
	header = append(header, maskKey...)

	maskedPayload := make([]byte, length)
	for i := 0; i < length; i++ {
		maskedPayload[i] = data[i] ^ maskKey[i%4]
	}

	conn.Write(header)
	conn.Write(maskedPayload)
}

// readWSFrame reads a unmasked frame coming from the server
func readWSFrame(conn net.Conn) string {
	header := make([]byte, 2)
	conn.Read(header)

	payloadLen := int(header[1] & 0x7F)
	if payloadLen == 0 {
		return ""
	}

	payload := make([]byte, payloadLen)
	conn.Read(payload)

	return string(payload)
}

// HandShake HTTP -> WS
func dialAndHandshake(t *testing.T, port string) net.Conn {
	conn, err := net.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
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
		t.Fatalf("Failed to read handshake response: %v", err)
	}

	if resp.StatusCode != 101 {
		t.Fatalf("Expected status 101 Switching Protocols, got %d", resp.StatusCode)
	}

	expectedHash := sha1.Sum([]byte(clientKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	expectedAccept := base64.StdEncoding.EncodeToString(expectedHash[:])
	if resp.Header.Get("Sec-WebSocket-Accept") != expectedAccept {
		t.Fatalf("Invalid Sec-WebSocket-Accept hash from server")
	}

	return conn
}

// Validate WebSocket Handshake and Ping/Pong interaction.
func TestWebSocketPingPong(t *testing.T) {
	_, s, port := setupTestServer()
	defer s.Stop(context.Background())

	conn := dialAndHandshake(t, port)
	defer conn.Close()

	writeWSFrame(conn, `{"action":"ping"}`)
	response := readWSFrame(conn)

	if !strings.Contains(response, "pong") {
		t.Errorf("Expected pong, got: %s", response)
	}
}

// Validate Full-Duplex Publish and Subscribe via WebSocket
func TestWebSocketPubSub(t *testing.T) {
	_, s, port := setupTestServer()
	defer s.Stop(context.Background())

	conn := dialAndHandshake(t, port)
	defer conn.Close()

	writeWSFrame(conn, `{"action":"subscribe", "topic":"*"}`)
	respSub := readWSFrame(conn)
	if !strings.Contains(respSub, "subscribed") {
		t.Fatalf("Failed to subscribe: %s", respSub)
	}

	writeWSFrame(conn, `{"action":"publish", "topic":"test.ws", "payload":"hello-ws"}`)

	resp1 := readWSFrame(conn)
	resp2 := readWSFrame(conn)

	combinedResponses := resp1 + resp2

	if !strings.Contains(combinedResponses, `"status":"published"`) {
		t.Errorf("Did not receive publish confirmation")
	}

	expectedBase64Payload := "aGVsbG8td3M="
	if !strings.Contains(combinedResponses, expectedBase64Payload) {
		t.Errorf("Did not receive the published message via subscription")
	}
}

// Validate that malformed JSON is rejected
func TestWebSocketInvalidFormat(t *testing.T) {
	_, s, port := setupTestServer()
	defer s.Stop(context.Background())

	conn := dialAndHandshake(t, port)
	defer conn.Close()

	writeWSFrame(conn, `not-json`)
	response := readWSFrame(conn)

	var errResp map[string]string
	json.Unmarshal([]byte(response), &errResp)

	if errResp["status"] != "error" {
		t.Errorf("Expected error status for invalid JSON, got: %s", response)
	}
}
