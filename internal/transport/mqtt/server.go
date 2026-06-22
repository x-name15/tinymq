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

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/message"
)

type Server struct {
	broker   *broker.Broker
	listener net.Listener
	quit     chan struct{}
	wg       sync.WaitGroup
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

		payload := make([]byte, remLength)
		if remLength > 0 {
			if _, err := io.ReadFull(conn, payload); err != nil {
				log.Printf("[MQTT] Error reading payload: %v", err)
				return
			}
		}

		log.Printf("[MQTT] RX Packet: Type=%d, Flags=%d, Len=%d from %s", packetType, flags, remLength, addr)

		if err := s.processPacket(conn, packetType, flags, payload, spies); err != nil {
			log.Printf("[MQTT] Protocol error (%s): %v", addr, err)
			return
		}
	}
}

func (s *Server) processPacket(conn net.Conn, pType, flags byte, payload []byte, spies map[string]chan message.Message) error {
	switch pType {
	case PacketConnect:
		return s.handleConnect(conn, payload)
	case PacketPublish:
		return s.handlePublish(conn, flags, payload)
	case PacketSubscribe:
		return s.handleSubscribe(conn, payload, spies)
	case PacketPingReq:
		_, err := conn.Write([]byte{PacketPingResp << 4, 0x00})
		return err
	case PacketDisconnect:
		return errors.New("client disconnected gracefully")
	case PacketUnsubscribe:
		return s.handleUnsubscribe(conn, payload, spies)
	default:
		return fmt.Errorf("unsupported packet type: %d", pType)
	}
}

func (s *Server) handleConnect(conn net.Conn, payload []byte) error {
	offset := 0
	protoName, err := readString(payload, &offset)
	if err != nil {
		return fmt.Errorf("failed to read protocol name: %v", err)
	}
	
	if protoName != "MQTT" && protoName != "MQIsdp" {
		return fmt.Errorf("invalid protocol name: %s", protoName)
	}

	if offset+2 > len(payload) { return io.ErrUnexpectedEOF }
	protoLevel := payload[offset] // Protocol version
	
	if protoLevel != 4 {
		conn.Write([]byte{PacketConnAck << 4, 2, 0x00, 0x01})
		return fmt.Errorf("unsupported MQTT version: %d (Only v3.1.1 supported. Check Postman settings!)", protoLevel)
	}

	connFlags := payload[offset+1]
	offset += 2

	// Skip KeepAlive (2 bytes)
	offset += 2

	clientID, err := readString(payload, &offset)
	if err != nil { return err }
	log.Printf("[MQTT] Client ID '%s' attempting handshake...", clientID)

	var username, password string
	hasUsername := (connFlags & 0x80) != 0
	hasPassword := (connFlags & 0x40) != 0

	if hasUsername {
		username, err = readString(payload, &offset)
		if err != nil { return err }
	}
	if hasPassword {
		password, err = readString(payload, &offset)
		if err != nil { return err }
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
			conn.Write([]byte{PacketConnAck << 4, 2, 0x00, 0x05}) // 0x05 = Not Authorized
			return errors.New("authentication failed")
		}
	}

	log.Printf("[MQTT] Client '%s' successfully connected!", clientID)
	_, err = conn.Write([]byte{PacketConnAck << 4, 2, 0x00, 0x00}) // 0x00 = Accepted
	return err
}

func (s *Server) handlePublish(conn net.Conn, flags byte, payload []byte) error {
	offset := 0
	topic, err := readString(payload, &offset)
	if err != nil { return err }

	qos := (flags >> 1) & 0x03
	var packetID uint16

	if qos > 0 {
		if offset+2 > len(payload) { return io.ErrUnexpectedEOF }
		packetID = binary.BigEndian.Uint16(payload[offset : offset+2])
		offset += 2
	}

	msgPayload := payload[offset:]
	log.Printf("[MQTT] Publishing to '%s', Payload Size: %d", topic, len(msgPayload))
	
	if err := s.broker.Publish(topic, msgPayload, nil, nil, false); err != nil {
		log.Printf("[MQTT] Failed to publish to broker: %v", err)
		return err
	}

	if qos == 1 {
		response := make([]byte, 4)
		response[0] = PacketPubAck << 4
		response[1] = 2
		binary.BigEndian.PutUint16(response[2:4], packetID)
		_, err = conn.Write(response)
		return err
	}
	return nil
}

func (s *Server) handleSubscribe(conn net.Conn, payload []byte, spies map[string]chan message.Message) error {
	offset := 0
	if offset+2 > len(payload) { return io.ErrUnexpectedEOF }
	packetID := binary.BigEndian.Uint16(payload[offset : offset+2])
	offset += 2

	var grantedQoS []byte

	for offset < len(payload) {
		topic, err := readString(payload, &offset)
		if err != nil { return err }

		if offset >= len(payload) { return io.ErrUnexpectedEOF }
		requestedQoS := payload[offset] 
		offset++

		cleanTopic := strings.ReplaceAll(topic, "#", "*")
		cleanTopic = strings.ReplaceAll(cleanTopic, "+", "*")

		log.Printf("[MQTT] Client subscribing to '%s' (QoS: %d, translated to '%s')", topic, requestedQoS, cleanTopic)

		spyChan, err := s.broker.AddSpy(cleanTopic)
		if err != nil {
			log.Printf("[MQTT] Broker rejected subscription to '%s': %v", cleanTopic, err)
			grantedQoS = append(grantedQoS, 0x80) 
			continue
		}

		spies[cleanTopic] = spyChan
		grantedQoS = append(grantedQoS, 0x00) 

		go func(ch chan message.Message, tName string) {
			for msg := range ch {
				topicBytes := []byte(tName)
				var varHeader []byte
				varHeader = append(varHeader, byte(len(topicBytes)>>8), byte(len(topicBytes)))
				varHeader = append(varHeader, topicBytes...)

				totalPayload := append(varHeader, msg.Payload...)
				remLenBytes := writeRemainingLength(len(totalPayload))

				packet := append([]byte{PacketPublish << 4}, remLenBytes...)
				packet = append(packet, totalPayload...)

				if _, err := conn.Write(packet); err != nil {
					return
				}
			}
		}(spyChan, topic)
	}

	subAckHeader := []byte{PacketSubAck << 4}
	remLen := 2 + len(grantedQoS)
	packet := append(subAckHeader, writeRemainingLength(remLen)...)
	packet = append(packet, byte(packetID>>8), byte(packetID))
	packet = append(packet, grantedQoS...)

	_, err := conn.Write(packet)
	return err
}

func (s *Server) handleUnsubscribe(conn net.Conn, payload []byte, spies map[string]chan message.Message) error {
	if len(payload) < 2 {
		return io.ErrUnexpectedEOF
	}
	
	packetID := payload[0:2]

	ack := []byte{PacketUnsubAck << 4, 2, packetID[0], packetID[1]}
	_, err := conn.Write(ack)
	
	log.Printf("[MQTT] Client unsubscribed (PacketID: %d)", binary.BigEndian.Uint16(packetID))
	return err
}