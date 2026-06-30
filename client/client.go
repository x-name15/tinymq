package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/x-name15/tinymq/internal/message"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
	apiKey     string
}

type PublishOptions struct {
	TTL       time.Duration
	Delay     time.Duration
	Broadcast bool
}

type SubscriptionOptions struct {
	Timeout string
}

type MessageHandler func(msg message.Message) error

func NewClient(baseURL string, apiKey ...string) *Client {
	c := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 35 * time.Second,
		},
	}
	if len(apiKey) > 0 {
		c.apiKey = apiKey[0]
	}
	return c
}

func (c *Client) authorize(req *http.Request) {
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
}

func (c *Client) Publish(topic string, payload []byte, opts ...PublishOptions) error {
	params := url.Values{}

	if len(opts) > 0 {
		opt := opts[0]
		if opt.TTL > 0 {
			params.Add("ttl", opt.TTL.String())
		}
		if opt.Delay > 0 {
			params.Add("delay", opt.Delay.String())
		}
		if opt.Broadcast {
			params.Add("broadcast", "true")
		}
	}

	safeTopic := url.PathEscape(topic)
	var u string
	if len(params) > 0 {
		u = fmt.Sprintf("%s/publish/%s?%s", c.baseURL, safeTopic, params.Encode())
	} else {
		u = fmt.Sprintf("%s/publish/%s", c.baseURL, safeTopic)
	}

	req, _ := http.NewRequest(http.MethodPost, u, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	c.authorize(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("broker rejected message with status: %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) PublishBroadcast(topic string, payload []byte) error {
	return c.Publish(topic, payload, PublishOptions{Broadcast: true})
}

func (c *Client) Subscribe(ctx context.Context, topic string, options SubscriptionOptions, handler MessageHandler) {
	params := url.Values{}
	params.Add("auto_ack", "false")
	params.Add("limit", "1")

	if options.Timeout != "" {
		params.Add("timeout", options.Timeout)
	} else {
		params.Add("timeout", "5s")
	}

	safeTopic := url.PathEscape(topic)
	u := fmt.Sprintf("%s/consume/%s?%s", c.baseURL, safeTopic, params.Encode())

	baseBackoff := 1 * time.Second
	maxBackoff := 32 * time.Second
	currentBackoff := baseBackoff

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		c.authorize(req)
		resp, err := c.httpClient.Do(req)

		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			currentBackoff = baseBackoff
			continue
		}

		if resp.StatusCode == http.StatusOK {
			var msg message.Message
			var msgList []message.Message

			var buf bytes.Buffer
			_, err := buf.ReadFrom(resp.Body)
			bodyBytes := buf.Bytes()

			resp.Body.Close()
			if err != nil {
				continue
			}

			if json.Unmarshal(bodyBytes, &msgList) == nil && len(msgList) > 0 {
				msg = msgList[0]
			} else if json.Unmarshal(bodyBytes, &msg) != nil {
				continue
			}

			err = handler(msg)

			safeMsgID := url.PathEscape(msg.ID)

			if err == nil {
				ackURL := fmt.Sprintf("%s/ack/%s/%s", c.baseURL, safeTopic, safeMsgID)
				ackReq, _ := http.NewRequest(http.MethodPost, ackURL, nil)
				ackReq.Header.Set("Content-Type", "application/json")
				c.authorize(ackReq)

				if ackResp, ackErr := c.httpClient.Do(ackReq); ackErr == nil {
					ackResp.Body.Close()
				}
				currentBackoff = baseBackoff
			} else {
				fmt.Printf("[SDK Resilience] Handler Failed: %v.\n", err)
				fmt.Printf("[SDK Resilience] Re-queuing message %s to preserve...\n", msg.ID)

				msgData, _ := json.Marshal(msg)
				requeueURL := fmt.Sprintf("%s/requeue", c.baseURL)
				reqReq, _ := http.NewRequest(http.MethodPost, requeueURL, bytes.NewBuffer(msgData))
				reqReq.Header.Set("Content-Type", "application/json")
				c.authorize(reqReq)
				requeueOK := false
				if reqResp, reqErr := c.httpClient.Do(reqReq); reqErr == nil {
					requeueOK = reqResp.StatusCode == http.StatusAccepted
					reqResp.Body.Close()
				}
				if requeueOK {
					ackURL := fmt.Sprintf("%s/ack/%s/%s", c.baseURL, safeTopic, safeMsgID)
					ackReq, _ := http.NewRequest(http.MethodPost, ackURL, nil)
					ackReq.Header.Set("Content-Type", "application/json")
					c.authorize(ackReq)
					if ackResp, ackErr := c.httpClient.Do(ackReq); ackErr == nil {
						ackResp.Body.Close()
					}
				} else {
					fmt.Printf("[SDK Resilience] Requeue failed for message %s, leaving original unacknowledged.\n", msg.ID)
				}
				fmt.Printf("[SDK Resilience] Sleeping worker for %v before next attempt...\n", currentBackoff)
				time.Sleep(currentBackoff)

				currentBackoff *= 2
				if currentBackoff > maxBackoff {
					currentBackoff = maxBackoff
				}
			}
			continue
		}
		resp.Body.Close()
	}
}
