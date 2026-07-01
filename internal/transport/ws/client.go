package ws

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/x-name15/tinymq/internal/message"
)

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
	mu    sync.Mutex
	spies map[string]chan message.Message
	done  chan struct{}
}

type WSCommand struct {
	Action  string `json:"action"`
	Topic   string `json:"topic,omitempty"`
	Payload string `json:"payload,omitempty"`
}

type wsResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Topic   string `json:"topic,omitempty"`
}

func (c *Client) readPump() {
	defer func() {
		close(c.done)
		c.mu.Lock()
		for topic, ch := range c.spies {
			c.hub.broker.RemoveSpy(topic, ch)
		}
		c.mu.Unlock()
		c.hub.RemoveClient(c)
	}()
	for {
		c.conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		header := make([]byte, 2)
		if _, err := io.ReadFull(c.rw, header); err != nil {
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
			if _, err := io.ReadFull(c.rw, extLen); err != nil {
				break
			}
			payloadLen = uint64(binary.BigEndian.Uint16(extLen))
		} else if payloadLen == 127 {
			extLen := make([]byte, 8)
			if _, err := io.ReadFull(c.rw, extLen); err != nil {
				break
			}
			payloadLen = binary.BigEndian.Uint64(extLen)
		}
		if payloadLen > 2<<20 {
			log.Println("[WS] Error: Payload exceeds 2MB limit")
			break
		}
		var maskKey []byte
		if isMasked {
			maskKey = make([]byte, 4)
			if _, err := io.ReadFull(c.rw, maskKey); err != nil {
				break
			}
		}
		payload := make([]byte, payloadLen)
		if payloadLen > 0 {
			if _, err := io.ReadFull(c.rw, payload); err != nil {
				break
			}
		}
		if isMasked {
			for i := uint64(0); i < payloadLen; i++ {
				payload[i] ^= maskKey[i%4]
			}
		}
		if opCode == opCodeText {
			log.Printf("[WS] RX (%s) opcode=%d len=%d", c.conn.RemoteAddr().String(), opCode, payloadLen)
			c.handleCommand(payload)
		}
	}
}

func (c *Client) sendJSON(resp wsResponse) {
	bytes, _ := json.Marshal(resp)
	c.sendMessage(string(bytes))
}

func (c *Client) sendError(msg string) {
	c.sendJSON(wsResponse{Status: "error", Message: msg})
}

func (c *Client) handleCommand(raw []byte) {
	var cmd WSCommand
	if err := json.Unmarshal(raw, &cmd); err != nil {
		c.sendError("invalid json format")
		return
	}

	switch cmd.Action {
	case "ping":
		c.sendJSON(wsResponse{Status: "pong"})

	case "publish":
		if cmd.Topic == "" {
			c.sendError("topic required")
			return
		}
		err := c.hub.broker.Publish(cmd.Topic, []byte(cmd.Payload), nil, "normal", nil, nil, false)
		if err != nil {
			c.sendError(err.Error())
		} else {
			c.sendJSON(wsResponse{Status: "published", Topic: cmd.Topic})
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
		c.mu.Unlock()

		spyChan, err := c.hub.broker.AddSpy(cmd.Topic)
		if err != nil {
			c.sendError(err.Error())
			return
		}

		c.mu.Lock()
		c.spies[cmd.Topic] = spyChan
		c.mu.Unlock()

		c.sendJSON(wsResponse{Status: "subscribed", Topic: cmd.Topic})

		go func(topic string, ch chan message.Message) {
			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						return
					}
					bytes, _ := json.Marshal(msg)
					if err := c.sendMessage(string(bytes)); err != nil {
						log.Printf("[WS] Error sending message to client: %v", err)
						return
					}
					log.Printf("[WS] TX (%s) topic=%s len=%d", c.conn.RemoteAddr().String(), topic, len(msg.Payload))
					log.Printf("[WS] Sent message %s to client on topic %s\n", msg.ID, topic)
				case <-c.done:
					return
				}
			}
		}(cmd.Topic, spyChan)

	default:
		c.sendError("unknown action")
	}
}

func (c *Client) sendMessage(data string) error {
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

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, err := c.rw.Write(header); err != nil {
		return err
	}
	if _, err := c.rw.Write(payload); err != nil {
		return err
	}
	return c.rw.Flush()
}

func (c *Client) writePong() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rw.Write([]byte{0x80 | opCodePong, 0})
	c.rw.Flush()
}
