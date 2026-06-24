package benchmarks

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/transport/rest"
)

func wsHandshake(conn net.Conn, port string) error {
	clientKey := "dGhlIHNhbXBsZSBub25jZQ=="
	req := "GET /ws HTTP/1.1\r\n" +
		"Host: 127.0.0.1:" + port + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + clientKey + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		return err
	}

	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != 101 {
		return err
	}
	return nil
}

func wsWriteFrame(conn net.Conn, payload string) error {
	data := []byte(payload)
	length := len(data)

	header := []byte{0x81, byte(length) | 0x80}
	maskKey := []byte{0x12, 0x34, 0x56, 0x78}
	header = append(header, maskKey...)

	maskedPayload := make([]byte, length)
	for i := 0; i < length; i++ {
		maskedPayload[i] = data[i] ^ maskKey[i%4]
	}

	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(maskedPayload)
	return err
}

func wsReadFrame(conn net.Conn) (string, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", err
	}
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
		return "", nil
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return "", err
	}
	return string(payload), nil
}

func BenchmarkWSConnect(b *testing.B) {
	brk := setupBrokerNoStorage(b)
	port := findFreePort(b)
	srv := rest.NewServer(brk, port, "bench", nil)
	go srv.Start()
	time.Sleep(50 * time.Millisecond)
	defer srv.Stop(context.Background())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := net.Dial("tcp", "127.0.0.1:"+port)
		if err != nil {
			b.Fatalf("dial failed: %v", err)
		}
		if err := wsHandshake(conn, port); err != nil {
			b.Fatalf("handshake failed: %v", err)
		}
		conn.Close()
	}
}

func BenchmarkWSPublish(b *testing.B) {
	brk := setupBrokerNoStorage(b)
	port := findFreePort(b)
	srv := rest.NewServer(brk, port, "bench", nil)
	go srv.Start()
	time.Sleep(50 * time.Millisecond)
	defer srv.Stop(context.Background())

	conn, err := net.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		b.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err := wsHandshake(conn, port); err != nil {
		b.Fatalf("handshake failed: %v", err)
	}

	topic := "bench/ws"
	cmd := `{"action":"publish","topic":"` + topic + `","payload":"hello"}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := wsWriteFrame(conn, cmd); err != nil {
			b.Fatalf("write failed: %v", err)
		}
		_, _ = wsReadFrame(conn)
	}
}

func BenchmarkWSSubscribe(b *testing.B) {
	brk := setupBrokerNoStorage(b)
	port := findFreePort(b)
	srv := rest.NewServer(brk, port, "bench", nil)
	go srv.Start()
	time.Sleep(50 * time.Millisecond)
	defer srv.Stop(context.Background())

	conn, err := net.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		b.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err := wsHandshake(conn, port); err != nil {
		b.Fatalf("handshake failed: %v", err)
	}

	topic := "bench/ws"
	cmd := `{"action":"subscribe","topic":"` + topic + `"}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := wsWriteFrame(conn, cmd); err != nil {
			b.Fatalf("write failed: %v", err)
		}
		_, _ = wsReadFrame(conn)
	}
}

func BenchmarkWSPubSub(b *testing.B) {
	brk := setupBrokerNoStorage(b)
	port := findFreePort(b)
	srv := rest.NewServer(brk, port, "bench", nil)
	go srv.Start()
	time.Sleep(50 * time.Millisecond)
	defer srv.Stop(context.Background())

	conn, err := net.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		b.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err := wsHandshake(conn, port); err != nil {
		b.Fatalf("handshake failed: %v", err)
	}

	topic := "bench/ws"
	subscribeCmd := `{"action":"subscribe","topic":"` + topic + `"}`
	publishCmd := `{"action":"publish","topic":"` + topic + `","payload":"hello"}`

	if err := wsWriteFrame(conn, subscribeCmd); err != nil {
		b.Fatalf("subscribe write failed: %v", err)
	}
	wsReadFrame(conn)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := wsWriteFrame(conn, publishCmd); err != nil {
			b.Fatalf("publish write failed: %v", err)
		}
		wsReadFrame(conn)
		wsReadFrame(conn)
	}
}

func BenchmarkWSConcurrent(b *testing.B) {
	brk := setupBrokerNoStorage(b)
	port := findFreePort(b)
	srv := rest.NewServer(brk, port, "bench", nil)
	go srv.Start()
	time.Sleep(50 * time.Millisecond)
	defer srv.Stop(context.Background())

	topic := "bench/ws"
	subscribeCmd := `{"action":"subscribe","topic":"` + topic + `"}`
	publishCmd := `{"action":"publish","topic":"` + topic + `","payload":"hello"}`

	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.Dial("tcp", "127.0.0.1:"+port)
		if err != nil {
			b.Fatalf("dial failed: %v", err)
		}
		defer conn.Close()
		if err := wsHandshake(conn, port); err != nil {
			b.Fatalf("handshake failed: %v", err)
		}
		if err := wsWriteFrame(conn, subscribeCmd); err != nil {
			b.Fatalf("subscribe write failed: %v", err)
		}
		wsReadFrame(conn)

		for pb.Next() {
			if err := wsWriteFrame(conn, publishCmd); err != nil {
				b.Fatalf("publish write failed: %v", err)
			}
			wsReadFrame(conn)
			wsReadFrame(conn)
		}
	})
}
