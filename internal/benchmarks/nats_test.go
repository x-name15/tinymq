package benchmarks

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/transport/nats"
)

func setupNATSBench(b *testing.B) (*broker.Broker, *nats.Server, string) {
	originalOutput := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() {
		log.SetOutput(originalOutput)
	})

	brk := broker.New(nil)
	natsSrv := nats.NewServer(brk)

	l, _ := net.Listen("tcp", "127.0.0.1:0")
	port := strings.Split(l.Addr().String(), ":")[1]
	l.Close()

	go natsSrv.Start(port)
	time.Sleep(50 * time.Millisecond)

	b.Cleanup(func() {
		natsSrv.Stop()
	})

	return brk, natsSrv, port
}

func BenchmarkNATSPublishSequential(b *testing.B) {
	_, _, port := setupNATSBench(b)

	conn, err := net.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		b.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	reader.ReadString('\n') // INFO
	fmt.Fprintf(conn, "CONNECT {\"verbose\":false}\r\n")
	reader.ReadString('\n') // +OK

	topic := "bench.seq"
	payload := "tiny-payload-123"
	pubLine := fmt.Sprintf("PUB %s %d\r\n%s\r\n", topic, len(payload), payload)
	pubBytes := []byte(pubLine)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := conn.Write(pubBytes)
		if err != nil {
			b.Fatalf("write failed: %v", err)
		}
	}
}

func BenchmarkNATSPublishParallel(b *testing.B) {
	_, _, port := setupNATSBench(b)

	topic := "bench.parallel"
	payload := "tiny-payload-123"
	pubLine := fmt.Sprintf("PUB %s %d\r\n%s\r\n", topic, len(payload), payload)
	pubBytes := []byte(pubLine)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		conn, _ := net.Dial("tcp", "127.0.0.1:"+port)
		defer conn.Close()

		reader := bufio.NewReader(conn)
		reader.ReadString('\n') // INFO
		fmt.Fprintf(conn, "CONNECT {\"verbose\":false}\r\n")
		reader.ReadString('\n') // +OK

		for pb.Next() {
			conn.Write(pubBytes)
		}
	})
}
