package tests

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/cluster"
	"github.com/x-name15/tinymq/internal/transport/mqtt"
)

// TestAllInOne_EndToEnd_Flow test total integration of TinyMQ: Broker + Storage + Cluster + MQTT
func TestAllInOne_EndToEnd_Flow(t *testing.T) {
	os.Setenv("TINYMQ_CLUSTER_SECRET", "e2e-super-secret-key")
	os.Setenv("TINYMQ_CLUSTER_LEADER", "false")
	os.Setenv("TINYMQ_CLUSTER_NODES", "127.0.0.1:40101,127.0.0.1:40102")

	defer os.Unsetenv("TINYMQ_CLUSTER_SECRET")
	defer os.Unsetenv("TINYMQ_CLUSTER_LEADER")
	defer os.Unsetenv("TINYMQ_CLUSTER_NODES")

	b1 := broker.New(nil)
	b2 := broker.New(nil)

	n1 := cluster.NewNode("127.0.0.1:40101", "7801", b1)
	n1.Role = cluster.Leader
	n1.CurrentTerm = 1
	n1.Peers["127.0.0.1:40102"] = &cluster.Peer{
		Address: "127.0.0.1:40102",
		IsAlive: true,
	}

	n2 := cluster.NewNode("127.0.0.1:40102", "7802", b2)
	n2.Role = cluster.Follower
	n2.CurrentTerm = 1
	n2.Peers["127.0.0.1:40101"] = &cluster.Peer{
		Address: "127.0.0.1:40101",
		IsAlive: true,
	}

	if err := n1.Start(); err != nil {
		t.Fatalf("Failed to start node N1: %v", err)
	}
	defer n1.Stop()

	if err := n2.Start(); err != nil {
		t.Fatalf("Failed to start node N2: %v", err)
	}
	defer n2.Stop()

	mqttPort := "40103"
	mqttSrv := mqtt.NewServer(b1)

	go mqttSrv.Start(mqttPort)
	defer mqttSrv.Stop()

	time.Sleep(200 * time.Millisecond)

	targetTopic := "e2e/sensors/temperature"

	mqttConn, err := net.DialTimeout("tcp", "127.0.0.1:"+mqttPort, 2*time.Second)
	if err != nil {
		t.Fatalf("MQTT client failed to connect: %v", err)
	}
	defer mqttConn.Close()

	connectPacket := []byte{
		0x10, 0x12,
		0x00, 0x04, 'M', 'Q', 'T', 'T',
		0x04, 0x02,
		0x00, 0x3C,
		0x00, 0x06, 'i', 'o', 't', '1', '2', '3',
	}

	mqttConn.Write(connectPacket)

	connAck := make([]byte, 4)
	io.ReadFull(mqttConn, connAck)

	if connAck[3] != 0x00 {
		t.Fatalf("MQTT connection rejected. CONNACK: %v", connAck)
	}

	topicLen := len(targetTopic)

	subPacket := []byte{
		0x82,
		byte(topicLen + 5),
		0x00,
		0x01,
		byte(topicLen >> 8),
		byte(topicLen),
	}

	subPacket = append(subPacket, []byte(targetTopic)...)
	subPacket = append(subPacket, 0x00)

	mqttConn.Write(subPacket)

	subAck := make([]byte, 5)
	io.ReadFull(mqttConn, subAck)

	wsChan, err := b2.AddSpy(targetTopic)
	if err != nil {
		t.Fatalf("Failed to add internal consumer to follower node: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	payload := []byte("FIRE_ALERT_99C")

	err = b1.Publish(targetTopic, payload, nil, nil, false)
	if err != nil {
		t.Fatalf("Base publish operation failed: %v", err)
	}

	select {
	case msg := <-wsChan:
		if !bytes.Equal(msg.Payload, payload) {
			t.Errorf(
				"Follower received corrupted data. Expected %s, received %s",
				payload,
				msg.Payload,
			)
		} else {
			t.Log("SUCCESS: Follower replicated cluster data and delivered it to the WebSocket client.")
		}

	case <-time.After(2 * time.Second):
		t.Fatal("ERROR: Message never reached the follower. Cluster TCP replication failed.")
	}

	mqttConn.SetReadDeadline(time.Now().Add(2 * time.Second))

	mqttHeader := make([]byte, 1)
	_, err = io.ReadFull(mqttConn, mqttHeader)
	if err != nil {
		t.Fatalf("ERROR: MQTT client did not receive the PUBLISH header: %v", err)
	}

	remLenBuf := make([]byte, 1)
	io.ReadFull(mqttConn, remLenBuf)

	remLen := int(remLenBuf[0])

	mqttData := make([]byte, remLen)
	io.ReadFull(mqttConn, mqttData)

	topicSize := binary.BigEndian.Uint16(mqttData[0:2])

	recTopic := string(mqttData[2 : 2+topicSize])
	recPayload := mqttData[2+topicSize:]

	if recTopic != targetTopic {
		t.Errorf(
			"MQTT routing failure. Expected topic: %s, received: %s",
			targetTopic,
			recTopic,
		)
	}

	if !bytes.Equal(recPayload, payload) {
		t.Errorf(
			"MQTT payload mismatch. Expected: %s, received: %s",
			payload,
			recPayload,
		)
	} else {
		t.Log("SUCCESS: MQTT gateway packaged the broker message and delivered it to the IoT device.")
	}

	t.Log("ALL-IN-ONE E2E TEST PASSED: Cluster, Broker, WebSockets, and MQTT operating correctly.")
}
