package benchmarks

import (
	"net"
	"strconv"
	"testing"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/storage"
)

func findFreePort(b *testing.B) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to find free port: %v", err)
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

func setupBrokerWithStorage(b *testing.B) (*broker.Broker, *storage.DiskStorage) {
	store, err := storage.New(b.TempDir(), false)
	if err != nil {
		b.Fatalf("failed to create storage: %v", err)
	}
	brk := broker.New(store)
	return brk, store
}

func setupBrokerNoStorage(b *testing.B) *broker.Broker {
	return broker.New(nil)
}
