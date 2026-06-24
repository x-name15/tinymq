package benchmarks

import (
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/transport/mqtt"
)

func mqttConnect(conn net.Conn, clientID string) error {
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
	if _, err := conn.Write(packet); err != nil {
		return err
	}

	connAck := make([]byte, 4)
	if _, err := io.ReadFull(conn, connAck); err != nil {
		return err
	}
	if connAck[0] != 0x20 || connAck[3] != 0x00 {
		return errors.New("invalid CONNACK received")
	}
	return nil
}

func mqttPublish(conn net.Conn, topic string, payload []byte) error {
	topicBytes := []byte(topic)
	topicLen := len(topicBytes)
	remainingLen := 2 + topicLen + len(payload)
	pubPacket := []byte{0x30, byte(remainingLen), byte(topicLen >> 8), byte(topicLen)}
	pubPacket = append(pubPacket, topicBytes...)
	pubPacket = append(pubPacket, payload...)
	_, err := conn.Write(pubPacket)
	return err
}

func mqttSubscribe(conn net.Conn, topic string) error {
	topicBytes := []byte(topic)
	topicLen := len(topicBytes)
	packetID := uint16(1)
	subPacket := []byte{
		0x82,
		byte(5 + topicLen),
		byte(packetID >> 8), byte(packetID),
		byte(topicLen >> 8), byte(topicLen),
	}
	subPacket = append(subPacket, topicBytes...)
	subPacket = append(subPacket, 0x00)

	if _, err := conn.Write(subPacket); err != nil {
		return err
	}

	subAck := make([]byte, 5)
	if _, err := io.ReadFull(conn, subAck); err != nil {
		return err
	}
	return nil
}

func BenchmarkMQTTConnect(b *testing.B) {
	brk := setupBrokerNoStorage(b)
	port := findFreePort(b)
	srv := mqtt.NewServer(brk)
	go srv.Start(port)
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 2*time.Second)
		if err != nil {
			b.Fatalf("dial failed: %v", err)
		}
		clientID := "client_" + strconv.Itoa(i)
		if err := mqttConnect(conn, clientID); err != nil {
			b.Fatalf("connect failed: %v", err)
		}
		conn.Close()
	}
}

func BenchmarkMQTTPublish(b *testing.B) {
	brk := setupBrokerNoStorage(b)
	port := findFreePort(b)
	srv := mqtt.NewServer(brk)
	go srv.Start(port)
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 2*time.Second)
	if err != nil {
		b.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err := mqttConnect(conn, "bench_client"); err != nil {
		b.Fatalf("connect failed: %v", err)
	}

	topic := "bench/topic"
	payload := []byte("hello world")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := mqttPublish(conn, topic, payload); err != nil {
			b.Fatalf("publish failed: %v", err)
		}
	}
}

func BenchmarkMQTTSubscribe(b *testing.B) {
	brk := setupBrokerNoStorage(b)
	port := findFreePort(b)
	srv := mqtt.NewServer(brk)
	go srv.Start(port)
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 2*time.Second)
	if err != nil {
		b.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err := mqttConnect(conn, "bench_client"); err != nil {
		b.Fatalf("connect failed: %v", err)
	}

	topic := "bench/topic"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := mqttSubscribe(conn, topic); err != nil {
			b.Fatalf("subscribe failed: %v", err)
		}
	}
}

func BenchmarkMQTTPubSub(b *testing.B) {
	brk := setupBrokerNoStorage(b)
	port := findFreePort(b)
	srv := mqtt.NewServer(brk)
	go srv.Start(port)
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 2*time.Second)
	if err != nil {
		b.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()
	if err := mqttConnect(conn, "bench_client"); err != nil {
		b.Fatalf("connect failed: %v", err)
	}

	topic := "bench/topic"
	payload := []byte("hello world")

	if err := mqttSubscribe(conn, topic); err != nil {
		b.Fatalf("subscribe failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := mqttPublish(conn, topic, payload); err != nil {
			b.Fatalf("publish failed: %v", err)
		}
		header := make([]byte, 1)
		if _, err := io.ReadFull(conn, header); err != nil {
			b.Fatalf("read header failed: %v", err)
		}
		remLenByte := make([]byte, 1)
		if _, err := io.ReadFull(conn, remLenByte); err != nil {
			b.Fatalf("read remaining len failed: %v", err)
		}
		remLen := int(remLenByte[0])
		data := make([]byte, remLen)
		if _, err := io.ReadFull(conn, data); err != nil {
			b.Fatalf("read payload failed: %v", err)
		}
	}
}

func BenchmarkMQTTConcurrent(b *testing.B) {
	brk := setupBrokerNoStorage(b)
	port := findFreePort(b)
	srv := mqtt.NewServer(brk)
	go srv.Start(port)
	defer srv.Stop()
	time.Sleep(50 * time.Millisecond)

	topic := "bench/topic"
	payload := []byte("hello world")

	b.RunParallel(func(pb *testing.PB) {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 2*time.Second)
		if err != nil {
			b.Fatalf("dial failed: %v", err)
		}
		defer conn.Close()
		clientID := "client_" + strconv.Itoa(int(time.Now().UnixNano()))
		if err := mqttConnect(conn, clientID); err != nil {
			b.Fatalf("connect failed: %v", err)
		}
		if err := mqttSubscribe(conn, topic); err != nil {
			b.Fatalf("subscribe failed: %v", err)
		}

		for pb.Next() {
			if err := mqttPublish(conn, topic, payload); err != nil {
				b.Fatalf("publish failed: %v", err)
			}
			header := make([]byte, 1)
			if _, err := io.ReadFull(conn, header); err != nil {
				continue
			}
			remLenByte := make([]byte, 1)
			if _, err := io.ReadFull(conn, remLenByte); err != nil {
				continue
			}
			remLen := int(remLenByte[0])
			data := make([]byte, remLen)
			io.ReadFull(conn, data)
		}
	})
}
