package tests

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/transport/nats"
)

func setupNATSTestServer(t *testing.T) (*broker.Broker, *nats.Server, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	l.Close()

	b := broker.New(nil)
	s := nats.NewServer(b)
	if err := s.Start(port); err != nil {
		t.Fatalf("failed to start NATS server: %v", err)
	}
	t.Cleanup(func() {
		s.Stop()
	})
	return b, s, port
}

func dialNATSAndReadInfo(t *testing.T, port string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial NATS server: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read INFO greeting: %v", err)
	}
	if !strings.HasPrefix(strings.TrimSpace(line), "INFO ") {
		t.Fatalf("expected INFO greeting, got: %q", line)
	}
	return conn, reader
}

func natsConnect(t *testing.T, conn net.Conn, reader *bufio.Reader, jsonArgs string) {
	t.Helper()
	fmt.Fprintf(conn, "CONNECT %s\r\n", jsonArgs)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read CONNECT response: %v", err)
	}
	resp := strings.TrimRight(line, "\r\n")
	if resp != "+OK" {
		t.Fatalf("expected +OK after CONNECT, got: %q", resp)
	}
}

func natsSendLine(t *testing.T, conn net.Conn, line string) {
	t.Helper()
	if _, err := fmt.Fprintf(conn, "%s\r\n", line); err != nil {
		t.Fatalf("failed to send line %q: %v", line, err)
	}
}

func natsReadLine(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read line: %v", err)
	}
	return strings.TrimRight(line, "\r\n")
}

func natsPub(t *testing.T, conn net.Conn, subject string, payload []byte) {
	t.Helper()
	fmt.Fprintf(conn, "PUB %s %d\r\n%s\r\n", subject, len(payload), string(payload))
}

func TestNATSInfoGreeting(t *testing.T) {
	_, _, port := setupNATSTestServer(t)

	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(3 * time.Second))

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read INFO: %v", err)
	}
	line = strings.TrimRight(strings.TrimSpace(line), "\r\n")

	if !strings.HasPrefix(line, "INFO ") {
		t.Fatalf("expected INFO frame, got: %q", line)
	}

	jsonPart := strings.TrimPrefix(line, "INFO ")
	var info map[string]any
	if err := json.Unmarshal([]byte(jsonPart), &info); err != nil {
		t.Fatalf("INFO payload is not valid JSON: %v — raw: %s", err, jsonPart)
	}
	for _, field := range []string{"server_id", "version", "max_payload"} {
		if _, ok := info[field]; !ok {
			t.Errorf("INFO JSON missing required field %q", field)
		}
	}
	t.Logf("INFO payload: %s", jsonPart)
}

func TestNATSConnectNoAuth(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	_, _, port := setupNATSTestServer(t)
	conn, reader := dialNATSAndReadInfo(t, port)

	fmt.Fprintf(conn, "CONNECT {\"verbose\":false}\r\n")
	resp := natsReadLine(t, reader)
	if resp != "+OK" {
		t.Errorf("expected +OK for unauthenticated CONNECT, got: %q", resp)
	}
}

func TestNATSConnectWithAuthToken(t *testing.T) {
	os.Setenv("TINYMQ_API_KEY", "test-nats-secret")
	defer os.Unsetenv("TINYMQ_API_KEY")

	_, _, port := setupNATSTestServer(t)
	conn, reader := dialNATSAndReadInfo(t, port)

	fmt.Fprintf(conn, "CONNECT {\"auth_token\":\"test-nats-secret\"}\r\n")
	resp := natsReadLine(t, reader)
	if resp != "+OK" {
		t.Errorf("expected +OK for valid auth_token, got: %q", resp)
	}
}

func TestNATSConnectWithPassField(t *testing.T) {
	os.Setenv("TINYMQ_API_KEY", "pass-field-secret")
	defer os.Unsetenv("TINYMQ_API_KEY")

	_, _, port := setupNATSTestServer(t)
	conn, reader := dialNATSAndReadInfo(t, port)

	fmt.Fprintf(conn, "CONNECT {\"user\":\"anything\",\"pass\":\"pass-field-secret\"}\r\n")
	resp := natsReadLine(t, reader)
	if resp != "+OK" {
		t.Errorf("expected +OK for valid pass field, got: %q", resp)
	}
}

func TestNATSConnectInvalidToken(t *testing.T) {
	os.Setenv("TINYMQ_API_KEY", "real-secret")
	defer os.Unsetenv("TINYMQ_API_KEY")

	_, _, port := setupNATSTestServer(t)
	conn, reader := dialNATSAndReadInfo(t, port)

	fmt.Fprintf(conn, "CONNECT {\"auth_token\":\"wrong-token\"}\r\n")
	resp := natsReadLine(t, reader)
	if !strings.HasPrefix(resp, "-ERR") {
		t.Errorf("expected -ERR for invalid token, got: %q", resp)
	}
}

func TestNATSPingPong(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	_, _, port := setupNATSTestServer(t)
	conn, reader := dialNATSAndReadInfo(t, port)
	natsConnect(t, conn, reader, `{"verbose":false}`)

	natsSendLine(t, conn, "PING")
	resp := natsReadLine(t, reader)
	if resp != "PONG" {
		t.Errorf("expected PONG, got: %q", resp)
	}
}

func TestNATSPubBasic(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	b, _, port := setupNATSTestServer(t)
	conn, reader := dialNATSAndReadInfo(t, port)
	natsConnect(t, conn, reader, `{"verbose":false}`)
	natsPub(t, conn, "orders.eu", []byte(`{"user":"felix"}`))
	natsSendLine(t, conn, "PING")
	resp := natsReadLine(t, reader)
	if resp != "PONG" {
		t.Errorf("expected PONG after PUB, got: %q (server may have dropped connection)", resp)
	}

	time.Sleep(50 * time.Millisecond)
	if !b.TopicExists("orders.eu") {
		t.Error("broker should have created topic 'orders.eu' after NATS PUB")
	}
	t.Log("SUCCESS: PUB created topic in broker.")
}

func TestNATSPubInvalidSubject(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	_, _, port := setupNATSTestServer(t)
	conn, reader := dialNATSAndReadInfo(t, port)
	natsConnect(t, conn, reader, `{"verbose":false}`)

	fmt.Fprintf(conn, "PUB ../../../etc 5\r\nhello\r\n")
	resp := natsReadLine(t, reader)
	if !strings.HasPrefix(resp, "-ERR") {
		t.Errorf("expected -ERR for invalid subject, got: %q", resp)
	}
}

func TestNATSPubOversizedPayload(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	_, _, port := setupNATSTestServer(t)
	conn, reader := dialNATSAndReadInfo(t, port)
	natsConnect(t, conn, reader, `{"verbose":false}`)
	fmt.Fprintf(conn, "PUB test.topic %d\r\n", 3<<20)
	resp := natsReadLine(t, reader)
	if !strings.HasPrefix(resp, "-ERR") {
		t.Errorf("expected -ERR for oversized payload claim, got: %q", resp)
	}
}

func TestNATSPubUnauthenticated(t *testing.T) {
	os.Setenv("TINYMQ_API_KEY", "secret")
	defer os.Unsetenv("TINYMQ_API_KEY")

	_, _, port := setupNATSTestServer(t)
	conn, reader := dialNATSAndReadInfo(t, port)

	fmt.Fprintf(conn, "PUB orders 5\r\nhello\r\n")
	resp := natsReadLine(t, reader)
	if !strings.HasPrefix(resp, "-ERR") {
		t.Errorf("expected -ERR for unauthenticated PUB, got: %q", resp)
	}
}

func TestNATSSubAndUnsub(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	_, _, port := setupNATSTestServer(t)
	conn, reader := dialNATSAndReadInfo(t, port)
	natsConnect(t, conn, reader, `{"verbose":false}`)

	natsSendLine(t, conn, "SUB notifications 1")
	time.Sleep(30 * time.Millisecond)

	natsSendLine(t, conn, "UNSUB 1")
	time.Sleep(30 * time.Millisecond)

	natsSendLine(t, conn, "PING")
	resp := natsReadLine(t, reader)
	if resp != "PONG" {
		t.Errorf("expected PONG after SUB/UNSUB cycle, got: %q", resp)
	}
}

func TestNATSSubDuplicateSID(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	_, _, port := setupNATSTestServer(t)
	conn, reader := dialNATSAndReadInfo(t, port)
	natsConnect(t, conn, reader, `{"verbose":false}`)
	natsSendLine(t, conn, "SUB topic.a 42")
	natsSendLine(t, conn, "PING")
	natsReadLine(t, reader)
	natsSendLine(t, conn, "SUB topic.b 42")
	resp := natsReadLine(t, reader)
	if !strings.HasPrefix(resp, "-ERR") {
		t.Errorf("expected -ERR for duplicate SID, got: %q", resp)
	}
}

func TestNATSPubSubDelivery(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	_, _, port := setupNATSTestServer(t)

	subConn, subReader := dialNATSAndReadInfo(t, port)
	natsConnect(t, subConn, subReader, `{"verbose":false}`)
	natsSendLine(t, subConn, "SUB sensors.temp 1")

	time.Sleep(50 * time.Millisecond)

	pubConn, pubReader := dialNATSAndReadInfo(t, port)
	natsConnect(t, pubConn, pubReader, `{"verbose":false}`)
	payload := []byte(`{"celsius":24.5}`)
	natsPub(t, pubConn, "sensors.temp", payload)

	subConn.SetDeadline(time.Now().Add(2 * time.Second))
	msgLine := natsReadLine(t, subReader)

	if !strings.HasPrefix(msgLine, "MSG sensors.temp 1 ") {
		t.Fatalf("expected MSG frame for sensors.temp, got: %q", msgLine)
	}
	payloadLine := natsReadLine(t, subReader)
	if payloadLine != string(payload) {
		t.Errorf("payload mismatch: got %q, want %q", payloadLine, string(payload))
	}
	t.Logf("SUCCESS: NATS PUB → SUB delivered payload: %s", payloadLine)
}

func TestNATSUnsubStopsDelivery(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	_, _, port := setupNATSTestServer(t)

	subConn, subReader := dialNATSAndReadInfo(t, port)
	natsConnect(t, subConn, subReader, `{"verbose":false}`)
	natsSendLine(t, subConn, "SUB events.click 99")
	time.Sleep(40 * time.Millisecond)

	natsSendLine(t, subConn, "UNSUB 99")
	time.Sleep(40 * time.Millisecond)

	pubConn, pubReader := dialNATSAndReadInfo(t, port)
	natsConnect(t, pubConn, pubReader, `{"verbose":false}`)
	natsPub(t, pubConn, "events.click", []byte("should-not-arrive"))

	subConn.SetDeadline(time.Now().Add(300 * time.Millisecond))
	line, err := subReader.ReadString('\n')
	if err == nil {
		t.Errorf("expected no MSG after UNSUB, but received: %q", line)
	}
	t.Log("SUCCESS: No messages delivered after UNSUB.")
}

func TestNATSWildcardDelivery(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	_, _, port := setupNATSTestServer(t)

	subConn, subReader := dialNATSAndReadInfo(t, port)
	natsConnect(t, subConn, subReader, `{"verbose":false}`)
	natsSendLine(t, subConn, "SUB iot.> 55")
	time.Sleep(40 * time.Millisecond)

	pubConn, pubReader := dialNATSAndReadInfo(t, port)
	natsConnect(t, pubConn, pubReader, `{"verbose":false}`)
	natsPub(t, pubConn, "iot.sensor.temp", []byte("hot"))

	subConn.SetDeadline(time.Now().Add(2 * time.Second))
	msgLine := natsReadLine(t, subReader)
	if !strings.HasPrefix(msgLine, "MSG ") {
		t.Errorf("expected wildcard MSG frame, got: %q", msgLine)
	}
	t.Logf("SUCCESS: Wildcard SUB iot.> received frame: %s", msgLine)
}

func TestNATSMultipleSubscribers(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	_, _, port := setupNATSTestServer(t)

	connect := func() (net.Conn, *bufio.Reader) {
		c, r := dialNATSAndReadInfo(t, port)
		natsConnect(t, c, r, `{"verbose":false}`)
		return c, r
	}

	c1, r1 := connect()
	c2, r2 := connect()

	natsSendLine(t, c1, "SUB global.news 1")
	natsSendLine(t, c2, "SUB global.news 1")
	time.Sleep(50 * time.Millisecond)

	pubConn, pubReader := dialNATSAndReadInfo(t, port)
	natsConnect(t, pubConn, pubReader, `{"verbose":false}`)
	natsPub(t, pubConn, "global.news", []byte("breaking"))

	deadline := time.Now().Add(2 * time.Second)
	c1.SetDeadline(deadline)
	c2.SetDeadline(deadline)

	msg1 := natsReadLine(t, r1)
	if !strings.HasPrefix(msg1, "MSG global.news") {
		t.Errorf("subscriber 1 did not receive MSG, got: %q", msg1)
	}
	msg2 := natsReadLine(t, r2)
	if !strings.HasPrefix(msg2, "MSG global.news") {
		t.Errorf("subscriber 2 did not receive MSG, got: %q", msg2)
	}
	t.Log("SUCCESS: Both subscribers received the fan-out message.")
}

func TestNATSUnknownVerbKeepsConnectionOpen(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	_, _, port := setupNATSTestServer(t)
	conn, reader := dialNATSAndReadInfo(t, port)
	natsConnect(t, conn, reader, `{"verbose":false}`)

	natsSendLine(t, conn, "FOOBAR something")
	resp := natsReadLine(t, reader)
	if !strings.HasPrefix(resp, "-ERR") {
		t.Errorf("expected -ERR for unknown verb, got: %q", resp)
	}

	natsSendLine(t, conn, "PING")
	pong := natsReadLine(t, reader)
	if pong != "PONG" {
		t.Errorf("connection should stay open after unknown verb; PING got: %q", pong)
	}
	t.Log("SUCCESS: Unknown verb returned -ERR without closing connection.")
}

func TestNATSIntegration_PubViaHTTP_ReceiveViaNATS(t *testing.T) {
	os.Unsetenv("TINYMQ_API_KEY")
	b, _, natsPort := setupNATSTestServer(t)
	subConn, subReader := dialNATSAndReadInfo(t, natsPort)
	natsConnect(t, subConn, subReader, `{"verbose":false}`)
	natsSendLine(t, subConn, "SUB cross.transport 1")
	time.Sleep(50 * time.Millisecond)
	payload := []byte(`{"source":"http","event":"order_created"}`)
	if err := b.Publish("cross.transport", payload, nil, "normal", nil, nil, false); err != nil {
		t.Fatalf("broker Publish failed: %v", err)
	}

	subConn.SetDeadline(time.Now().Add(2 * time.Second))
	msgLine := natsReadLine(t, subReader)
	if !strings.HasPrefix(msgLine, "MSG cross.transport 1 ") {
		t.Fatalf("expected MSG frame for cross.transport, got: %q", msgLine)
	}
	payloadLine := natsReadLine(t, subReader)
	if payloadLine != string(payload) {
		t.Errorf("cross-transport payload mismatch: got %q, want %q", payloadLine, string(payload))
	}
	t.Logf("SUCCESS: HTTP broker publish → NATS subscriber received: %s", payloadLine)
}
