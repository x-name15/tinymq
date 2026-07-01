package client

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/x-name15/tinymq/internal/message"
)

const MaxFrameSize = 16 * 1024 * 1024 

type wsCommand struct {
	Action string `json:"action"`
	Topic  string `json:"topic,omitempty"`
}

type WSClient struct {
	addr       string
	conn       net.Conn
	rw         *bufio.ReadWriter
	writeMu    sync.Mutex
	handlersMu sync.RWMutex
	handlers   map[string]func(message.Message)
	stop       chan struct{}
	wg         sync.WaitGroup
	once       sync.Once
}

func NewWSClient(addr string) (*WSClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	c := &WSClient{
		addr:     addr,
		conn:     conn,
		rw:       bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn)),
		handlers: make(map[string]func(message.Message)),
		stop:     make(chan struct{}),
	}

	c.wg.Add(2)
	go c.readLoop()
	go c.startKeepalive()
	return c, nil
}

func (c *WSClient) startKeepalive() {
	defer c.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := c.writePingFrame(); err != nil {
				return
			}
		case <-c.stop:
			return
		}
	}
}

func (c *WSClient) writePingFrame() error {
	header := []byte{0x89, 0x80} 
	maskKey := make([]byte, 4)
	if _, err := rand.Read(maskKey); err != nil {
		return err
	}
	
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if _, err := c.rw.Write(header); err != nil {
		return err
	}
	if _, err := c.rw.Write(maskKey); err != nil {
		return err
	}
	return c.rw.Flush()
}

func (c *WSClient) writeFrame(payload string) error {
	data := []byte(payload)
	length := len(data)
	var header []byte

	header = append(header, 0x81)

	if length <= 125 {
		header = append(header, byte(length)|0x80)
	} else if length <= 65535 {
		header = append(header, 126|0x80)
		header = append(header, byte(length>>8), byte(length))
	} else {
		header = append(header, 127|0x80)
		for i := 7; i >= 0; i-- {
			header = append(header, byte(length>>(i*8)))
		}
	}

	maskKey := make([]byte, 4)
	if _, err := rand.Read(maskKey); err != nil {
		return err
	}
	header = append(header, maskKey...)

	maskedPayload := make([]byte, length)
	for i := 0; i < length; i++ {
		maskedPayload[i] = data[i] ^ maskKey[i%4]
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if _, err := c.rw.Write(header); err != nil {
		return err
	}
	if _, err := c.rw.Write(maskedPayload); err != nil {
		return err
	}
	return c.rw.Flush()
}

func (c *WSClient) Subscribe(topic string, handler func(message.Message)) error {
	c.handlersMu.Lock()
	c.handlers[topic] = handler
	c.handlersMu.Unlock()

	subMsg, err := json.Marshal(wsCommand{Action: "subscribe", Topic: topic})
	if err != nil {
		return err
	}
	return c.writeFrame(string(subMsg))
}

func (c *WSClient) Unsubscribe(topic string) error {
	c.handlersMu.Lock()
	_, exists := c.handlers[topic]
	delete(c.handlers, topic)
	c.handlersMu.Unlock()

	if !exists {
		return nil
	}

	unsubMsg, err := json.Marshal(wsCommand{Action: "unsubscribe", Topic: topic})
	if err != nil {
		return err
	}
	return c.writeFrame(string(unsubMsg))
}

func (c *WSClient) readLoop() {
	defer c.wg.Done()
	for {
		payload, err := c.readFrame()
		if err != nil {
			select {
			case <-c.stop:
				return
			default:
			}

			if recErr := c.handleReconnection(); recErr != nil {
				return
			}
			continue
		}

		var raw map[string]interface{}
		if err := json.Unmarshal(payload, &raw); err != nil {
			continue
		}
		if _, hasStatus := raw["status"]; hasStatus {
			continue
		}

		var msg message.Message
		if err := json.Unmarshal(payload, &msg); err != nil {
			continue
		}

		if decoded, decErr := base64.StdEncoding.DecodeString(string(msg.Payload)); decErr == nil {
			msg.Payload = decoded
		}

		c.handlersMu.RLock()
		handler, exists := c.handlers[msg.Topic]
		c.handlersMu.RUnlock()

		if exists && handler != nil {
			handler(msg)
		}
	}
}

func (c *WSClient) handleReconnection() error {
	backoff := 1 * time.Second
	maxBackoff := 32 * time.Second

	for {
		select {
		case <-c.stop:
			return errors.New("client stopped intentionally")
		case <-time.After(backoff):
		}

		c.writeMu.Lock()
		if c.conn != nil {
			c.conn.Close()
		}
		conn, err := net.Dial("tcp", c.addr)
		if err != nil {
			c.writeMu.Unlock()
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		c.conn = conn
		c.rw = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		c.writeMu.Unlock()

		if err := c.resubscribeAll(); err != nil {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		return nil
	}
}

func (c *WSClient) resubscribeAll() error {
	c.handlersMu.RLock()
	topics := make([]string, 0, len(c.handlers))
	for topic := range c.handlers {
		topics = append(topics, topic)
	}
	c.handlersMu.RUnlock()

	for _, topic := range topics {
		subMsg, err := json.Marshal(wsCommand{Action: "subscribe", Topic: topic})
		if err != nil {
			return err
		}
		if err := c.writeFrame(string(subMsg)); err != nil {
			return err
		}
	}
	return nil
}

func (c *WSClient) readFrame() ([]byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(c.rw, header); err != nil {
		return nil, err
	}

	b1 := header[1]
	isMasked := (b1 & 0x80) != 0
	length := uint64(b1 & 0x7F)

	switch length {
	case 126:
		extended := make([]byte, 2)
		if _, err := io.ReadFull(c.rw, extended); err != nil {
			return nil, err
		}
		length = uint64(extended[0])<<8 | uint64(extended[1])
	case 127:
		extended := make([]byte, 8)
		if _, err := io.ReadFull(c.rw, extended); err != nil {
			return nil, err
		}
		length = 0
		for i := 0; i < 8; i++ {
			length = (length << 8) | uint64(extended[i])
		}
	}

	if length > MaxFrameSize {
		return nil, fmt.Errorf("frame size %d exceeds safety limit of %d bytes", length, MaxFrameSize)
	}

	var maskKey []byte
	if isMasked {
		maskKey = make([]byte, 4)
		if _, err := io.ReadFull(c.rw, maskKey); err != nil {
			return nil, err
		}
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(c.rw, payload); err != nil {
		return nil, err
	}

	if isMasked {
		for i := 0; i < len(payload); i++ {
			payload[i] ^= maskKey[i%4]
		}
	}

	return payload, nil
}

func (c *WSClient) Close() error {
	c.once.Do(func() {
		close(c.stop)
		c.writeMu.Lock()
		if c.conn != nil {
			c.conn.Close()
		}
		c.writeMu.Unlock()
	})
	c.wg.Wait()
	return nil
}