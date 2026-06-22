package tests

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/transport/rest"
)

func setupTestServer(t *testing.T) (*broker.Broker, *rest.Server, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	l.Close()

	b := broker.New(nil)
	s := rest.NewServer(b, port, "test-v2.6.0")
	go s.Start()
	time.Sleep(50 * time.Millisecond)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s.Stop(ctx)
	})

	return b, s, port
}

func writeWSFrame(conn net.Conn, payload string) {
	data := []byte(payload)
	length := len(data)

	header := []byte{0x81, byte(length) | 0x80}
	maskKey := []byte{0x12, 0x34, 0x56, 0x78}
	header = append(header, maskKey...)

	maskedPayload := make([]byte, length)
	for i := 0; i < length; i++ {
		maskedPayload[i] = data[i] ^ maskKey[i%4]
	}

	conn.Write(header)
	conn.Write(maskedPayload)
}

func readWSFrame(conn net.Conn) string {
	header := make([]byte, 2)
	io.ReadFull(conn, header)

	payloadLen := int(header[1] & 0x7F)
	switch payloadLen {
	case 126:
		ext := make([]byte, 2)
		io.ReadFull(conn, ext)
		payloadLen = int(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		io.ReadFull(conn, ext)
		payloadLen = int(binary.BigEndian.Uint64(ext))
	}

	if payloadLen == 0 {
		return ""
	}

	payload := make([]byte, payloadLen)
	io.ReadFull(conn, payload)

	return string(payload)
}

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

func TestWebSocketPingPong(t *testing.T) {
	_, _, port := setupTestServer(t)
	conn := dialAndHandshake(t, port)
	defer conn.Close()

	writeWSFrame(conn, `{"action":"ping"}`)
	response := readWSFrame(conn)

	if !strings.Contains(response, "pong") {
		t.Errorf("Expected pong, got: %s", response)
	}
}

func TestWebSocketPubSub(t *testing.T) {
	_, _, port := setupTestServer(t)
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

func TestWebSocketInvalidFormat(t *testing.T) {
	_, _, port := setupTestServer(t)
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
