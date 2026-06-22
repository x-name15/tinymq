package ws

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/x-name15/tinymq/internal/message"
)

// Official WebSocket OpCodes (RFC 6455)
const (
	opCodeText  = 1
	opCodeClose = 8
	opCodePing  = 9
	opCodePong  = 10
)

type Client struct {
	hub   *Server
	conn  net.Conn
	rw    *bufio.ReadWriter
	mu    sync.Mutex                      // Protects concurrent socket writes
	spies map[string]chan message.Message // Tracks topics this client is subscribed to
}

type WSCommand struct {
	Action  string `json:"action"` // "publish", "subscribe", "ping"
	Topic   string `json:"topic,omitempty"`
	Payload string `json:"payload,omitempty"`
}

func (c *Client) readPump() {
	// Absolute cleanup if the client disconnects
	defer func() {
		c.mu.Lock()
		// Unsubscribe from all channels to prevent Memory Leaks in the Broker
		for topic, ch := range c.spies {
			c.hub.broker.RemoveSpy(topic, ch)
		}
		c.mu.Unlock()
		c.hub.remove <- c
	}()

	for {
		header := make([]byte, 2)
		if _, err := c.rw.Read(header); err != nil {
			break
		}

		opCode := header[0] & 0x0F
		isMasked := (header[1] & 0x80) != 0
		payloadLen := uint64(header[1] & 0x7F)

		if opCode == opCodeClose {
			break
		}
		if opCode == opCodePing {
			c.writePong()
			continue
		}
		if opCode == opCodePong {
			continue
		}

		if payloadLen == 126 {
			extLen := make([]byte, 2)
			if _, err := c.rw.Read(extLen); err != nil {
				break
			}
			payloadLen = uint64(binary.BigEndian.Uint16(extLen))
		} else if payloadLen == 127 {
			extLen := make([]byte, 8)
			if _, err := c.rw.Read(extLen); err != nil {
				break
			}
			payloadLen = binary.BigEndian.Uint64(extLen)
		}

		if payloadLen > 2<<20 {
			log.Println("[WS] Error: Payload exceeds the 2MB limit")
			break
		}

		var maskKey []byte
		if isMasked {
			maskKey = make([]byte, 4)
			if _, err := c.rw.Read(maskKey); err != nil {
				break
			}
		}

		payload := make([]byte, payloadLen)
		if payloadLen > 0 {
			if _, err := c.rw.Read(payload); err != nil {
				break
			}
		}

		if isMasked {
			for i := uint64(0); i < payloadLen; i++ {
				payload[i] ^= maskKey[i%4]
			}
		}

		if opCode == opCodeText {
			// TERMINAL LOG: Show exactly what they send us
			log.Printf("[WS] RX (%s) -> %s", c.conn.RemoteAddr().String(), string(payload))
			c.handleCommand(payload)
		}
	}
}

func (c *Client) handleCommand(raw []byte) {
	var cmd WSCommand
	if err := json.Unmarshal(raw, &cmd); err != nil {
		c.sendError("invalid json format")
		return
	}

	switch cmd.Action {
	case "ping":
		c.sendMessage(`{"status":"pong"}`)

	case "publish":
		if cmd.Topic == "" {
			c.sendError("topic required")
			return
		}
		err := c.hub.broker.Publish(cmd.Topic, []byte(cmd.Payload), nil, nil, false)
		if err != nil {
			c.sendError(err.Error())
		} else {
			c.sendMessage(fmt.Sprintf(`{"status":"published", "topic":"%s"}`, cmd.Topic))
		}

	case "subscribe":
		if cmd.Topic == "" {
			c.sendError("topic required")
			return
		}

		c.mu.Lock()
		if _, exists := c.spies[cmd.Topic]; exists {
			c.mu.Unlock()
			c.sendError("already subscribed to this topic")
			return
		}

		// Hook the broker's "Spy" already programmed for SSE
		spyChan := c.hub.broker.AddSpy(cmd.Topic)
		c.spies[cmd.Topic] = spyChan
		c.mu.Unlock()

		c.sendMessage(fmt.Sprintf(`{"status":"subscribed", "topic":"%s"}`, cmd.Topic))
		log.Printf("[WS] (%s) subscribed to '%s'", c.conn.RemoteAddr().String(), cmd.Topic)

		// Goroutine dedicated to push messages from this subscription to the client
		go func(topic string, ch chan message.Message) {
			for msg := range ch {
				bytes, _ := json.Marshal(msg)
				c.sendMessage(string(bytes))
			}
		}(cmd.Topic, spyChan)

	default:
		c.sendError("unknown or unimplemented action")
	}
}

func (c *Client) sendError(msg string) {
	c.sendMessage(fmt.Sprintf(`{"status":"error", "message":"%s"}`, msg))
}

func (c *Client) sendMessage(data string) {
	payload := []byte(data)
	length := len(payload)

	var header []byte
	header = append(header, 0x80|opCodeText)

	if length <= 125 {
		header = append(header, byte(length))
	} else if length <= 65535 {
		header = append(header, 126)
		lenBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(lenBytes, uint16(length))
		header = append(header, lenBytes...)
	} else {
		header = append(header, 127)
		lenBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(lenBytes, uint64(length))
		header = append(header, lenBytes...)
	}

	// Lock the socket only for this client to avoid mixing frames
	c.mu.Lock()
	defer c.mu.Unlock()

	c.rw.Write(header)
	c.rw.Write(payload)
	c.rw.Flush()

	// Optional: Show what we send (TX) in console
	log.Printf("[WS] TX (%s) <- %s", c.conn.RemoteAddr().String(), data)
}

func (c *Client) writePong() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rw.Write([]byte{0x80 | opCodePong, 0})
	c.rw.Flush()
}
