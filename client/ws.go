package client

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/x-name15/tinymq/internal/message"
)

type WSClient struct {
	baseURL string
	apiKey  string
	conn    net.Conn
	rw      *bufio.ReadWriter
	mu      sync.Mutex
}

type wsCommand struct {
	Action  string `json:"action"`
	Topic   string `json:"topic,omitempty"`
	Payload string `json:"payload,omitempty"`
}

func NewWSClient(baseURL string, apiKey ...string) *WSClient {
	wsURL := strings.Replace(baseURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)

	c := &WSClient{baseURL: wsURL}
	if len(apiKey) > 0 {
		c.apiKey = apiKey[0]
	}
	return c
}

func (c *WSClient) Connect() error {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return err
	}

	host := u.Host
	if !strings.Contains(host, ":") {
		if u.Scheme == "wss" {
			host += ":443"
		} else {
			host += ":80"
		}
	}

	conn, err := net.Dial("tcp", host)
	if err != nil {
		return err
	}

	keyBytes := make([]byte, 16)
	rand.Read(keyBytes)
	wsKey := base64.StdEncoding.EncodeToString(keyBytes)

	path := "/ws"
	if c.apiKey != "" {
		path += "?token=" + url.QueryEscape(c.apiKey)
	}

	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", path, u.Host, wsKey)

	if _, err := conn.Write([]byte(req)); err != nil {
		return err
	}

	c.conn = conn
	c.rw = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	resp, err := http.ReadResponse(c.rw.Reader, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != 101 {
		return fmt.Errorf("websocket handshake failed with status %d", resp.StatusCode)
	}

	return nil
}

func (c *WSClient) writeFrame(payload string) error {
	data := []byte(payload)
	length := len(data)

	header := []byte{0x81}
	var lenByte byte

	if length <= 125 {
		lenByte = byte(length) | 0x80
	} else if length <= 65535 {
		lenByte = 126 | 0x80
	} else {
		lenByte = 127 | 0x80
	}
	header = append(header, lenByte)

	if length > 125 && length <= 65535 {
		lenBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(lenBytes, uint16(length))
		header = append(header, lenBytes...)
	} else if length > 65535 {
		lenBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(lenBytes, uint64(length))
		header = append(header, lenBytes...)
	}

	maskKey := make([]byte, 4)
	rand.Read(maskKey)
	header = append(header, maskKey...)

	maskedPayload := make([]byte, length)
	for i := 0; i < length; i++ {
		maskedPayload[i] = data[i] ^ maskKey[i%4]
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, err := c.rw.Write(header); err != nil {
		return err
	}
	if _, err := c.rw.Write(maskedPayload); err != nil {
		return err
	}
	return c.rw.Flush()
}

func (c *WSClient) readFrame() ([]byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(c.rw, header); err != nil {
		return nil, err
	}

	opCode := header[0] & 0x0F
	payloadLen := uint64(header[1] & 0x7F)

	switch payloadLen {
	case 126:
		extLen := make([]byte, 2)
		io.ReadFull(c.rw, extLen)
		payloadLen = uint64(binary.BigEndian.Uint16(extLen))
	case 127:
		extLen := make([]byte, 8)
		io.ReadFull(c.rw, extLen)
		payloadLen = binary.BigEndian.Uint64(extLen)
	}

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		io.ReadFull(c.rw, payload)
	}

	if opCode == 8 {
		return nil, errors.New("websocket connection closed by server")
	}

	return payload, nil
}

func (c *WSClient) Subscribe(topic string, handler func(message.Message)) error {
	subMsg, err := json.Marshal(wsCommand{Action: "subscribe", Topic: topic})
	if err != nil {
		return err
	}
	if err := c.writeFrame(string(subMsg)); err != nil {
		return err
	}

	for {
		payload, err := c.readFrame()
		if err != nil {
			return err
		}

		var raw map[string]interface{}
		if err := json.Unmarshal(payload, &raw); err != nil {
			continue
		}

		if _, hasStatus := raw["status"]; hasStatus {
			continue
		}

		var msg message.Message
		if err := json.Unmarshal(payload, &msg); err == nil {
			if decoded, decErr := base64.StdEncoding.DecodeString(string(msg.Payload)); decErr == nil {
				msg.Payload = decoded
			}

			go handler(msg)
		}
	}
}

func (c *WSClient) Publish(topic string, payload []byte) error {
	pubMsg, err := json.Marshal(wsCommand{Action: "publish", Topic: topic, Payload: string(payload)})
	if err != nil {
		return err
	}
	
	return c.writeFrame(string(pubMsg))
}
