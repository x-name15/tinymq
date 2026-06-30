package mqtt

import (
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/message"
)

type Server struct {
	broker   *broker.Broker
	listener net.Listener
	quit     chan struct{}
	wg       sync.WaitGroup
}

type mqttConn struct {
	conn net.Conn
	mu   sync.Mutex
}

func (mc *mqttConn) write(data []byte) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	_, err := mc.conn.Write(data)
	return err
}

func NewServer(b *broker.Broker) *Server {
	return &Server{
		broker: b,
		quit:   make(chan struct{}),
	}
}

func (s *Server) Start(port string) error {
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	s.listener = l
	log.Printf("[MQTT] Server listening natively on TCP port %s\n", port)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					continue
				}
			}
			go s.handleClient(conn)
		}
	}()
	return nil
}

func (s *Server) Stop() {
	close(s.quit)
	if s.listener != nil {
		s.listener.Close()
	}
	s.wg.Wait()
	log.Println("[MQTT] Server stopped gracefully.")
}

func (s *Server) handleClient(conn net.Conn) {
	addr := conn.RemoteAddr().String()
	log.Printf("[MQTT] (+) TCP Connection opened from %s", addr)

	mc := &mqttConn{conn: conn}
	var spies = make(map[string]chan message.Message)

	defer func() {
		for topic, ch := range spies {
			s.broker.RemoveSpy(topic, ch)
		}
		conn.Close()
		log.Printf("[MQTT] (-) TCP Connection closed for %s", addr)
	}()

	header := make([]byte, 1)
	for {
		// Reiniciar el deadline en cada iteración para evitar cierre prematuro
		conn.SetDeadline(time.Now().Add(30 * time.Second))

		if _, err := io.ReadFull(conn, header); err != nil {
			if err != io.EOF {
				log.Printf("[MQTT] Read error from %s: %v", addr, err)
			}
			return
		}

		packetType := header[0] >> 4
		flags := header[0] & 0x0F

		remLength, err := readRemainingLength(conn)
		if err != nil {
			log.Printf("[MQTT] Error reading remaining length: %v", err)
			return
		}

		const maxPacketSize = 2 << 20
		if remLength > maxPacketSize {
			log.Printf("[MQTT] Oversized packet from %s (%d bytes), dropping connection", addr, remLength)
			return
		}

		payload := make([]byte, remLength)
		if remLength > 0 {
			if _, err := io.ReadFull(conn, payload); err != nil {
				log.Printf("[MQTT] Error reading payload: %v", err)
				return
			}
		}

		if err := s.processPacket(mc, packetType, flags, payload, spies); err != nil {
			log.Printf("[MQTT] Protocol error (%s): %v", addr, err)
			return
		}
	}
}

func (s *Server) processPacket(mc *mqttConn, pType, flags byte, payload []byte, spies map[string]chan message.Message) error {
	switch pType {
	case PacketConnect:
		return s.handleConnect(mc, payload)
	case PacketPublish:
		return s.handlePublish(mc, flags, payload)
	case PacketSubscribe:
		return s.handleSubscribe(mc, payload, spies)
	case PacketPingReq:
		return mc.write([]byte{PacketPingResp << 4, 0x00})
	case PacketDisconnect:
		return errors.New("client disconnected gracefully")
	case PacketUnsubscribe:
		return s.handleUnsubscribe(mc, payload, spies)
	default:
		return fmt.Errorf("unsupported packet type: %d", pType)
	}
}

func (s *Server) handleConnect(mc *mqttConn, payload []byte) error {
	if len(payload) < 10 {
		log.Printf("[MQTT] SEC-ALERT: Malformed CONNECT packet received (length %d). Dropping connection.", len(payload))
		return errors.New("malformed CONNECT packet")
	}

	offset := 0
	protoName, err := readString(payload, &offset)
	if err != nil {
		return fmt.Errorf("failed to read protocol name: %v", err)
	}

	if protoName != "MQTT" && protoName != "MQIsdp" {
		return fmt.Errorf("invalid protocol name: %s", protoName)
	}

	if offset+4 > len(payload) {
		return io.ErrUnexpectedEOF
	}
	protoLevel := payload[offset] // Protocol version

	if protoLevel != 4 {
		mc.write([]byte{PacketConnAck << 4, 2, 0x00, 0x01})
		return fmt.Errorf("unsupported MQTT version: %d", protoLevel)
	}

	connFlags := payload[offset+1]
	offset += 4

	clientID, err := readString(payload, &offset)
	if err != nil {
		return err
	}

	var username, password string
	hasUsername := (connFlags & 0x80) != 0
	hasPassword := (connFlags & 0x40) != 0

	if hasPassword && !hasUsername {
		mc.write([]byte{PacketConnAck << 4, 2, 0x00, 0x04})
		return fmt.Errorf("password flag set without username flag (MQTT 3.1.1 §3.1.2.9)")
	}

	if hasUsername {
		username, err = readString(payload, &offset)
		if err != nil {
			return err
		}
	}
	if hasPassword {
		password, err = readString(payload, &offset)
		if err != nil {
			return err
		}
	}

	token := os.Getenv("TINYMQ_API_KEY")
	if token != "" {
		valid := false
		if hasPassword && subtle.ConstantTimeCompare([]byte(password), []byte(token)) == 1 {
			valid = true
		} else if hasUsername && subtle.ConstantTimeCompare([]byte(username), []byte(token)) == 1 {
			valid = true
		}
		if !valid {
			mc.write([]byte{PacketConnAck << 4, 2, 0x00, 0x05}) // 0x05 = Not Authorized
			return errors.New("authentication failed")
		}
	}

	log.Printf("[MQTT] Client '%s' successfully connected!", clientID)
	return mc.write([]byte{PacketConnAck << 4, 2, 0x00, 0x00}) // 0x00 = Accepted
}

func (s *Server) handlePublish(mc *mqttConn, flags byte, payload []byte) error {
	offset := 0
	topic, err := readString(payload, &offset)
	if err != nil {
		return err
	}

	qos := (flags >> 1) & 0x03
	var packetID uint16

	if qos > 0 {
		if offset+2 > len(payload) {
			return io.ErrUnexpectedEOF
		}
		packetID = binary.BigEndian.Uint16(payload[offset : offset+2])
		offset += 2
	}

	if qos == 2 {
		log.Printf("[MQTT] QoS 2 not supported, dropping message on topic '%s'", topic)
		return fmt.Errorf("QoS 2 not supported")
	}

	msgPayload := payload[offset:]
	log.Printf("[MQTT] Received PUBLISH on topic '%s' with %d bytes", topic, len(msgPayload))

	if err := s.broker.Publish(topic, msgPayload, nil, "normal", nil, nil, false); err != nil {
		log.Printf("[MQTT] Broker rejected publish: %v", err)
		return err
	}
	log.Printf("[MQTT] Publish accepted by broker for topic '%s'", topic)

	if qos == 1 {
		response := make([]byte, 4)
		response[0] = PacketPubAck << 4
		response[1] = 2
		binary.BigEndian.PutUint16(response[2:4], packetID)
		return mc.write(response)
	}
	return nil
}

func translateMQTTWildcard(topic string) string {
	if topic == "#" {
		return "*"
	}
	if strings.HasSuffix(topic, "/#") {
		return strings.ReplaceAll(topic, "/#", "/*")
	}
	if strings.Contains(topic, "+") {
		return strings.ReplaceAll(topic, "+", "*")
	}
	return topic
}

func (s *Server) handleSubscribe(mc *mqttConn, payload []byte, spies map[string]chan message.Message) error {
	offset := 0
	if offset+2 > len(payload) {
		return io.ErrUnexpectedEOF
	}
	packetID := binary.BigEndian.Uint16(payload[offset : offset+2])
	offset += 2

	var grantedQoS []byte

	for offset < len(payload) {
		topic, err := readString(payload, &offset)
		if err != nil {
			return err
		}

		if offset >= len(payload) {
			return io.ErrUnexpectedEOF
		}
		requestedQoS := payload[offset]
		offset++

		cleanTopic := translateMQTTWildcard(topic)
		log.Printf("[MQTT] Client subscribing to '%s' (QoS: %d, translated to '%s')", topic, requestedQoS, cleanTopic)
		if oldCh, exists := spies[cleanTopic]; exists {
			s.broker.RemoveSpy(cleanTopic, oldCh)
			delete(spies, cleanTopic)
		}

		spyChan, err := s.broker.AddSpy(cleanTopic)
		if err != nil {
			log.Printf("[MQTT] Broker rejected subscription to '%s': %v", cleanTopic, err)
			grantedQoS = append(grantedQoS, 0x80)
			continue
		}

		spies[cleanTopic] = spyChan
		grantedQoS = append(grantedQoS, 0x00)

		go func(ch chan message.Message, t string) {
			defer s.broker.RemoveSpy(t, ch)
			for {
				select {
				case msg, open := <-ch:
					if !open {
						return
					}
					topicBytes := []byte(msg.Topic)
					var varHeader []byte
					varHeader = append(varHeader, byte(len(topicBytes)>>8), byte(len(topicBytes)))
					varHeader = append(varHeader, topicBytes...)

					totalPayload := append(varHeader, msg.Payload...)
					remLenBytes := writeRemainingLength(len(totalPayload))

					packet := append([]byte{PacketPublish << 4}, remLenBytes...)
					packet = append(packet, totalPayload...)

					if err := mc.write(packet); err != nil {
						return
					}
				case <-s.quit:
					return
				}
			}
		}(spyChan, cleanTopic)
	}

	subAckHeader := []byte{PacketSubAck << 4}
	remLen := 2 + len(grantedQoS)
	packet := append(subAckHeader, writeRemainingLength(remLen)...)
	packet = append(packet, byte(packetID>>8), byte(packetID))
	packet = append(packet, grantedQoS...)

	return mc.write(packet)
}

func (s *Server) handleUnsubscribe(mc *mqttConn, payload []byte, spies map[string]chan message.Message) error {
	if len(payload) < 2 {
		return io.ErrUnexpectedEOF
	}

	packetID := payload[0:2]
	offset := 2

	for offset < len(payload) {
		topic, err := readString(payload, &offset)
		if err != nil {
			break
		}

		cleanTopic := translateMQTTWildcard(topic)

		if ch, exists := spies[cleanTopic]; exists {
			s.broker.RemoveSpy(cleanTopic, ch)
			delete(spies, cleanTopic)
			log.Printf("[MQTT] Client unsubscribed from '%s'", cleanTopic)
		}
	}

	ack := []byte{PacketUnsubAck << 4, 2, packetID[0], packetID[1]}
	return mc.write(ack)
}
