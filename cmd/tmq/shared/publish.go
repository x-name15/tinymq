package shared

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func HandlePublish(baseURL string, args []string) {
	pubCmd := flag.NewFlagSet("pub", flag.ExitOnError)
	ttl := pubCmd.String("ttl", "", "Message TTL (e.g., 5m, 1h)")
	delay := pubCmd.String("delay", "", "Delivery delay (e.g., 10s, 1m)")
	broadcast := pubCmd.Bool("broadcast", false, "Enable Fan-out / Broadcast mode")
	pubCmd.Parse(args)
	leftover := pubCmd.Args()
	if len(leftover) < 2 {
		fmt.Println("Use: tmq pub <topic> <payload> [--ttl=duration] [--delay=duration] [--broadcast]")
		return
	}
	topic := leftover[0]
	payload := leftover[1]
	params := url.Values{}
	if *ttl != "" {
		params.Add("ttl", *ttl)
	}
	if *delay != "" {
		params.Add("delay", *delay)
	}
	if *broadcast {
		params.Add("broadcast", "true")
	}
	u := fmt.Sprintf("%s/publish/%s?%s", baseURL, url.PathEscape(topic), params.Encode())
	resp, err := DoAuthRequest(http.MethodPost, u, bytes.NewBuffer([]byte(payload)))
	if err != nil {
		fmt.Printf("%s[Error] Network error: %v%s\n", ColorRed, err, ColorReset)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		fmt.Printf("%s[Error] Unauthorized. Ensure TINYMQ_API_KEY is correctly set.%s\n", ColorRed, ColorReset)
		return
	}
	if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
		fmt.Printf("%s✔ Message published successfully in '%s'%s\n", ColorGreen, topic, ColorReset)
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("%sThe broker rejected the message (%d): %s%s\n", ColorRed, resp.StatusCode, string(body), ColorReset)
	}
}
func HandleConsume(baseURL string, args []string) {
	subCmd := flag.NewFlagSet("sub", flag.ExitOnError)
	timeout := subCmd.String("timeout", "0s", "Timeout for Long Polling (e.g., 5s)")
	limit := subCmd.Int("limit", 1, "Maximum number of messages to retrieve")
	autoAck := subCmd.Bool("auto-ack", true, "Automatic acknowledgment when consuming")
	subCmd.Parse(args)
	leftover := subCmd.Args()
	if len(leftover) < 1 {
		fmt.Println("Use: tmq sub <topic> [--timeout=duration] [--limit=count] [--auto-ack=true/false]")
		return
	}
	topic := leftover[0]
	params := url.Values{}
	params.Add("timeout", *timeout)
	params.Add("limit", fmt.Sprintf("%d", *limit))
	if *autoAck {
		params.Add("auto_ack", "true")
	} else {
		params.Add("auto_ack", "false")
	}
	u := fmt.Sprintf("%s/consume/%s?%s", baseURL, url.PathEscape(topic), params.Encode())
	resp, err := DoAuthRequest(http.MethodGet, u, nil)
	if err != nil {
		fmt.Printf("%s[Error] Network error: %v%s\n", ColorRed, err, ColorReset)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		fmt.Printf("%s[Error] Unauthorized. Ensure TINYMQ_API_KEY is correctly set.%s\n", ColorRed, ColorReset)
		return
	}
	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("%s[Empty] No messages available in '%s'%s\n", ColorYellow, topic, ColorReset)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	messages, err := parseMessagesPayload(body)
	if err != nil {
		fmt.Printf("%s[Error] Error interpreting response: %v%s\n", ColorRed, err, ColorReset)
		return
	}
	for _, msg := range messages {
		fmt.Printf("%s------------------------------------------%s\n", ColorBlue, ColorReset)
		fmt.Printf("%sID:%s %s  |  %sRetries:%s %d\n", ColorBold+ColorCyan, ColorReset, msg.ID, ColorBold+ColorYellow, ColorReset, msg.RetryCount)
		fmt.Printf("%sDate:%s %s\n", ColorBold, ColorReset, msg.Timestamp.Format("2006-01-02 15:04:05"))
		fmt.Printf("%sPayload:%s %s\n", ColorBold, ColorReset, string(msg.Payload))
	}
	fmt.Printf("%s------------------------------------------%s\n", ColorBlue, ColorReset)
}
func HandlePeek(baseURL string, args []string) {
	peekCmd := flag.NewFlagSet("peek", flag.ExitOnError)
	limit := peekCmd.Int("limit", 10, "Maximum limit of messages to inspect")
	peekCmd.Parse(args)
	leftover := peekCmd.Args()
	if len(leftover) < 1 {
		fmt.Println("Use: tmq peek <topic/queue> [--limit=count]")
		return
	}
	topic := leftover[0]
	u := fmt.Sprintf("%s/api/queues/peek?queue=%s", baseURL, url.QueryEscape(topic))
	resp, err := DoAuthRequest(http.MethodGet, u, nil)
	if err != nil {
		fmt.Printf("%s[Error] Network error: %v%s\n", ColorRed, err, ColorReset)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		fmt.Printf("%s[Error] Unauthorized. Ensure TINYMQ_API_KEY is correctly set.%s\n", ColorRed, ColorReset)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	var messages []CLIMessage
	if err := json.Unmarshal(body, &messages); err != nil {
		fmt.Printf("%s[Error] Error decoding broker RAM: %v%s\n", ColorRed, err, ColorReset)
		return
	}
	if len(messages) == 0 {
		fmt.Printf("%s[RAM] The queue '%s' is empty (0 messages in wait).%s\n", ColorYellow, topic, ColorReset)
		return
	}
	fmt.Printf("\n%sPEEKING RAM OF '%s' (Showing first %d)%s\n", ColorBold+ColorYellow, topic, *limit, ColorReset)
	for i, msg := range messages {
		if i >= *limit {
			break
		}
		fmt.Printf("\n%s[%d] ID: %s | Attempts: %d | %s%s\n", ColorBold+ColorCyan, i+1, msg.ID, msg.RetryCount, msg.Timestamp.Format("15:04:05"), ColorReset)
		fmt.Printf("   %s\n", string(msg.Payload))
	}
	fmt.Println()
}
func HandleTail(baseURL string, args []string) {
	if len(args) < 1 {
		fmt.Println("Use: tmq tail <topic>")
		return
	}
	topic := args[0]
	fmt.Printf("%sSpy Mode: Listening to '%s' in real-time via SSE... (Ctrl+C to exit)%s\n", ColorBold+ColorGreen, topic, ColorReset)
	safeTopic := url.PathEscape(topic)
	streamURL := fmt.Sprintf("%s/stream/%s", baseURL, safeTopic)
	for {
		req, err := http.NewRequest(http.MethodGet, streamURL, nil)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		if apiKey := os.Getenv("TINYMQ_API_KEY"); apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("%s[Connection Lost] Retrying in 2s...%s\n", ColorRed, ColorReset)
			time.Sleep(2 * time.Second)
			continue
		}
		if resp.StatusCode == http.StatusUnauthorized {
			resp.Body.Close()
			fmt.Printf("%s[Error] Unauthorized. Ensure TINYMQ_API_KEY is correctly set.%s\n", ColorRed, ColorReset)
			return
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			fmt.Printf("%s[Error] Broker returned status %d. Retrying in 2s...%s\n", ColorRed, resp.StatusCode, ColorReset)
			time.Sleep(2 * time.Second)
			continue
		}
		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				break
			}
			lineStr := string(line)
			if strings.HasPrefix(lineStr, "data: ") {
				dataStr := strings.TrimPrefix(lineStr, "data: ")
				dataStr = strings.TrimSpace(dataStr)
				if dataStr == "" {
					continue
				}
				var rawMap map[string]interface{}
				if err := json.Unmarshal([]byte(dataStr), &rawMap); err == nil {
					if status, ok := rawMap["status"].(string); ok && status == "connected" {
						fmt.Printf("%s[SSE] Connected successfully! Waiting for messages...%s\n", ColorYellow, ColorReset)
						continue
					}
				}
				var msg CLIMessage
				if err := json.Unmarshal([]byte(dataStr), &msg); err == nil && msg.ID != "" {
					var payloadStr string
					payloadStr = string(msg.Payload)
					fmt.Printf("%s[%s]%s %s %s->%s %s\n",
						ColorBlue, msg.Timestamp.Format("15:04:05"), ColorReset,
						ColorBold+ColorCyan, msg.ID[:8], ColorReset,
						payloadStr,
					)
				}
			}
		}
		resp.Body.Close()
		time.Sleep(2 * time.Second)
	}
}
func parseMessagesPayload(body []byte) ([]CLIMessage, error) {
	var list []CLIMessage
	if err := json.Unmarshal(body, &list); err == nil {
		return list, nil
	}
	var single CLIMessage
	if err := json.Unmarshal(body, &single); err == nil {
		if single.ID != "" {
			return []CLIMessage{single}, nil
		}
	}
	return nil, fmt.Errorf("incompatible JSON format")
}
