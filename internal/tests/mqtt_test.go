package tests

import (
	"io"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/transport/mqtt"
)

func setupMqttTestServer(t *testing.T) (*broker.Broker, *mqtt.Server, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	l.Close()

	b := broker.New(nil)
	s := mqtt.NewServer(b)
	if err := s.Start(port); err != nil {
		t.Fatalf("failed to start mqtt server: %v", err)
	}

	t.Cleanup(func() {
		s.Stop()
	})

	return b, s, port
}

func TestMqttConnectAndPing(t *testing.T) {
	_, _, port := setupMqttTestServer(t)

	conn, err := net.Dial("tcp", "127.0.0.1:"+port)
	if err != nil {
		t.Fatalf("failed to dial MQTT: %v", err)
	}
	defer conn.Close()

	connectPacket := []byte{
		0x10, 
		12,   
		0x00, 4, 'M', 'Q', 'T', 'T',
		4,        
		0x02,     
		0x00, 60, 
		0x00, 1, 'T', 
	}

	conn.Write(connectPacket)

	resp := make([]byte, 4)
	if _, err := io.ReadFull(conn, resp); err != nil {
		t.Fatalf("failed to read CONNACK: %v", err)
	}

	if resp[0] != 0x20 || resp[3] != 0x00 {
		t.Errorf("expected successful CONNACK, got: %v", resp)
	}

	conn.Write([]byte{0xC0, 0x00})
	pingResp := make([]byte, 2)
	io.ReadFull(conn, pingResp)

	if pingResp[0] != 0xD0 {
		t.Errorf("expected PINGRESP (0xD0), got: %X", pingResp[0])
	}
}

func TestMqttPubSub(t *testing.T) {
	_, _, port := setupMqttTestServer(t)

	conn := dialMqttAndConnect(t, port)
	defer conn.Close()

	subPacket := []byte{
		0x82,       
		5,          
		0x00, 0x01, 
		0x00, 1, 'a',
		0x00, 
	}
	conn.Write(subPacket)

	subAck := make([]byte, 5)
	io.ReadFull(conn, subAck) // Leer SUBACK

	pubPacket := []byte{
		0x30,         
		5,            
		0x00, 1, 'a', 
		'h', 'i', 
	}
	conn.Write(pubPacket)

	incomingPub := make([]byte, 7)
	io.ReadFull(conn, incomingPub)

	if !strings.Contains(string(incomingPub), "hi") {
		t.Errorf("expected payload 'hi' over MQTT stream, got: %s", string(incomingPub))
	}
}

func dialMqttAndConnect(t *testing.T, port string) net.Conn {
	conn, _ := net.Dial("tcp", "127.0.0.1:"+port)
	connectPacket := []byte{
		0x10, 12,
		0x00, 4, 'M', 'Q', 'T', 'T',
		4, 0x02, 0x00, 60,
		0x00, 1, 'T',
	}
	conn.Write(connectPacket)
	resp := make([]byte, 4)
	io.ReadFull(conn, resp)
	return conn
}
