package tests

import (
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/cluster"
)

// TestIntegrationClusterHMACMismatched verifies from outside the package (black-box)
func TestIntegrationClusterHMACMismatched(t *testing.T) {
	// 1. Force a secret in the test environment
	secret := "integration-secret-key-2026"
	os.Setenv("TINYMQ_CLUSTER_SECRET", secret)
	defer os.Unsetenv("TINYMQ_CLUSTER_SECRET")

	b := broker.New(nil)

	n := cluster.NewNode("127.0.0.1:9101", "7811", b)
	if err := n.Start(); err != nil {
		t.Fatalf("Failed to start test node: %v", err)
	}
	defer n.Stop()

	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", "127.0.0.1:9101")
	if err != nil {
		t.Fatalf("Failed to connect to cluster socket: %v", err)
	}
	defer conn.Close()

	payloadMalicious := "REPLICATE 1 topic.hacked cGF5bG9hZA=="
	invalidSignature := hex.EncodeToString([]byte("totally-fake-fake-signature"))

	fmt.Fprintf(conn, "%s %s\n", payloadMalicious, invalidSignature)

	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	buf := make([]byte, 1024)
	nBytes, err := conn.Read(buf)

	if err == nil {
		resp := string(buf[:nBytes])
		t.Logf("Response received from node for corrupted signature: %s", resp)
	}

	if b.Topics != nil && b.Topics["topic.hacked"] != nil {
		t.Fatal("CRITICAL: Node processed an injected TCP replication with an invalid signature")
	}
}

// TestIntegrationConsumerGroupHookReplication verifies that when the broker registers
func TestIntegrationConsumerGroupHookReplication(t *testing.T) {
	os.Setenv("TINYMQ_CLUSTER_SECRET", "test-secret")
	defer os.Unsetenv("TINYMQ_CLUSTER_SECRET")

	b := broker.New(nil)
	n := cluster.NewNode("127.0.0.1:9102", "7812", b)

	hookTriggered := false
	b.OnGroupCreate = func(topic, group string) error {
		hookTriggered = true
		return n.ReplicateBinding(topic, group)
	}

	_, err := b.CreateGroup("orders", "shipments")
	if err != nil {
		t.Fatalf("Error creating broker group: %v", err)
	}

	if !hookTriggered {
		t.Fatal("Broker OnGroupCreate hook did not communicate with the Node subsystem")
	}
}
