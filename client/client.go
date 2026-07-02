package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	TTL         time.Duration
	Delay       time.Duration
	Broadcast   bool
	Priority    string
	Idempotency string
	Headers     map[string]string
}

type SubscriptionOptions struct {
	Timeout string
	Group   string
}

func NewClient(baseURL string, apiKey ...string) *Client {
	key := ""
	if len(apiKey) > 0 {
		key = apiKey[0]
	}
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		apiKey: key,
	}
}

func (c *Client) req(ctx context.Context, method, endpoint string, body []byte) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	r, err := http.NewRequestWithContext(ctx, method, c.baseURL+endpoint, reader)
	if err != nil {
		return nil, err
	}
	if c.apiKey != "" {
		r.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	return r, nil
}

func (c *Client) Publish(ctx context.Context, topic string, payload []byte, opts *PublishOptions) error {
	u, err := url.Parse(c.baseURL + "/publish/" + topic)
	if err != nil {
		return err
	}

	q := u.Query()
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")

	if opts != nil {
		if opts.TTL > 0 {
			q.Set("ttl", opts.TTL.String())
		}
		if opts.Delay > 0 {
			q.Set("delay", opts.Delay.String())
		}
		if opts.Broadcast {
			q.Set("broadcast", "true")
		}
		if opts.Priority != "" {
			q.Set("priority", opts.Priority)
		}
		if opts.Idempotency != "" {
			headers.Set("Idempotency-Key", opts.Idempotency)
		}
		for k, v := range opts.Headers {
			headers.Set(k, v)
		}
	}
	u.RawQuery = q.Encode()

	req, err := c.req(ctx, http.MethodPost, u.String()[len(c.baseURL):], payload)
	if err != nil {
		return err
	}
	for k, v := range headers {
		req.Header[k] = v
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("publish failed (status %d): %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func (c *Client) Subscribe(ctx context.Context, topic string, opts *SubscriptionOptions) ([]message.Message, error) {
	endpoint := fmt.Sprintf("/consume/%s?auto_ack=true", topic)
	if opts != nil {
		if opts.Timeout != "" {
			endpoint += "&timeout=" + opts.Timeout
		}
		if opts.Group != "" {
			endpoint += "&group=" + opts.Group
		}
	}

	req, err := c.req(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("subscribe failed (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	var msgs []message.Message
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	return msgs, nil
}

func (c *Client) CreateTopic(ctx context.Context, topic, policy string, retain time.Duration) error {
	payload := struct {
		Name   string `json:"name"`
		Policy string `json:"policy"`
		Retain string `json:"retain,omitempty"`
	}{
		Name:   topic,
		Policy: policy,
	}
	if retain > 0 {
		payload.Retain = retain.String()
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := c.req(ctx, http.MethodPost, "/api/topics", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create topic (status %d): %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func (c *Client) CreateGroup(ctx context.Context, topic, group string) error {
	payload := struct {
		Topic string `json:"topic"`
		Group string `json:"group"`
	}{Topic: topic, Group: group}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := c.req(ctx, http.MethodPost, "/api/groups", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create group (status %d): %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func (c *Client) Peek(ctx context.Context, topic string, limit int) ([]message.Message, error) {
	endpoint := fmt.Sprintf("/api/queues/peek?queue=%s", url.QueryEscape(topic))
	req, err := c.req(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("peek failed (status %d): %s", resp.StatusCode, string(bodyBytes))
	}
	var messages []message.Message
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		return nil, err
	}
	if limit > 0 && limit < len(messages) {
		messages = messages[:limit]
	}
	return messages, nil
}

func (c *Client) ClusterStatus(ctx context.Context) (map[string]interface{}, error) {
	req, err := c.req(ctx, http.MethodGet, "/api/cluster/status", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("cluster status failed (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}
	return status, nil
}
