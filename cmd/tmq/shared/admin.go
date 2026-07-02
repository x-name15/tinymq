package shared

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

func HandleList(baseURL string) {
	resp, err := DoAuthRequest(http.MethodGet, baseURL+"/api/stats", nil)
	if err != nil {
		fmt.Printf("%s[Error] Error connecting to the broker at %s: %v%s\n", ColorRed, baseURL, err, ColorReset)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		fmt.Printf("%s[Error] Unauthorized. Ensure TINYMQ_API_KEY is correctly set.%s\n", ColorRed, ColorReset)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	var stats []TopicStat
	var wrapped StatsResponse
	if err := json.Unmarshal(body, &wrapped); err == nil && len(wrapped.Stats) > 0 {
		stats = wrapped.Stats
	} else {
		_ = json.Unmarshal(body, &stats)
	}
	if len(stats) == 0 {
		fmt.Println("No topics or queues active at the moment.")
		return
	}
	fmt.Printf("\n%sSTATE OF 🍃 TINYMQ (%s)%s\n\n", ColorBold+ColorCyan, baseURL, ColorReset)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 4, ' ', 0)
	fmt.Fprintf(w, "%sQUEUE / TOPIC NAME\tMESSAGES (RAM)\tWAITING WORKERS\tTYPE\tWEBHOOKS%s\n", ColorBold, ColorReset)
	for _, s := range stats {
		qType := "Standard"
		if s.IsDLQ {
			qType = ColorRed + "DLQ" + ColorReset
		} else if strings.Contains(s.Name, "*") {
			qType = ColorBlue + "Wildcard" + ColorReset
		}
		hasWh := "No"
		if s.HasWebhooks {
			hasWh = ColorGreen + "Yes" + ColorReset
		}
		msgCountStr := fmt.Sprintf("%d", s.MessageCount)
		if s.MessageCount > 0 {
			msgCountStr = ColorYellow + msgCountStr + ColorReset
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\n", s.Name, msgCountStr, s.WaitingConsumers, qType, hasWh)
	}
	w.Flush()
	fmt.Println()
}
func HandleCreate(baseURL string, args []string) {
	createCmd := flag.NewFlagSet("create", flag.ExitOnError)
	policy := createCmd.String("policy", "", "Queue policy: reject|drop-oldest (default: server's TINYMQ_DEFAULT_POLICY)")
	retention := createCmd.String("retention", "", "Message retention (e.g., 24h, 30m)")
	createCmd.Parse(args)
	rest := createCmd.Args()
	if len(rest) < 1 {
		fmt.Println("Use: tmq create <topic> [--policy=reject|drop-oldest] [--retention=duration]")
		return
	}
	topic := rest[0]
	payload := map[string]string{"name": topic}
	if *policy != "" {
		if *policy != "reject" && *policy != "drop-oldest" {
			fmt.Printf("%s[Error] --policy must be 'reject' or 'drop-oldest'%s\n", ColorRed, ColorReset)
			return
		}
		payload["policy"] = *policy
	}
	if *retention != "" {
		if _, err := time.ParseDuration(*retention); err != nil {
			fmt.Printf("%s[Error] --retention must be a valid Go duration (e.g. 24h, 30m): %v%s\n", ColorRed, err, ColorReset)
			return
		}
		payload["retain"] = *retention
	}
	body, _ := json.Marshal(payload)
	resp, err := DoAuthRequest(http.MethodPost, baseURL+"/api/topics", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("%s[Error] %v%s\n", ColorRed, err, ColorReset)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusCreated {
		fmt.Printf("%s✔ Queue '%s' created!%s\n", ColorGreen, topic, ColorReset)
		if *policy != "" {
			fmt.Printf("  Policy: %s\n", *policy)
		}
		if *retention != "" {
			fmt.Printf("  Retention: %s\n", *retention)
		}
	} else {
		b, _ := io.ReadAll(resp.Body)
		fmt.Printf("%s[Error] %s%s\n", ColorRed, string(b), ColorReset)
	}
}
func HandleRm(baseURL string, args []string, isPurge bool) {
	if len(args) < 1 {
		if isPurge {
			fmt.Println("Use: tmq purge <queue>")
		} else {
			fmt.Println("Use: tmq rm <queue>")
		}
		return
	}
	topic := args[0]
	endpoint := "/api/queues/delete"
	actionStr := "deleted"
	if isPurge {
		endpoint = "/api/queues/purge"
		actionStr = "purged"
	}
	u := fmt.Sprintf("%s%s?queue=%s", baseURL, endpoint, url.QueryEscape(topic))
	resp, err := DoAuthRequest(http.MethodDelete, u, nil)
	if err != nil {
		fmt.Printf("%s[Error] Network error: %v%s\n", ColorRed, err, ColorReset)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		fmt.Printf("%s✔ Queue '%s' successfully %s!%s\n", ColorGreen, topic, actionStr, ColorReset)
	} else {
		fmt.Printf("%s[Error] Broker returned status %d%s\n", ColorRed, resp.StatusCode, ColorReset)
	}
}
func HandleWebhook(baseURL string, args []string) {
	if len(args) < 2 {
		fmt.Println("Use: tmq webhook list <topic>\n    tmq webhook add <topic> <url>")
		return
	}
	action, topic := args[0], args[1]
	if action == "list" {
		u := fmt.Sprintf("%s/api/queues/webhooks?queue=%s", baseURL, url.QueryEscape(topic))
		resp, err := DoAuthRequest(http.MethodGet, u, nil)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		defer resp.Body.Close()
		var urls []string
		json.NewDecoder(resp.Body).Decode(&urls)
		fmt.Printf("\n%sWebhooks for '%s':%s\n", ColorBold+ColorCyan, topic, ColorReset)
		if len(urls) == 0 {
			fmt.Println("  (No webhooks registered)")
		} else {
			for _, u := range urls {
				fmt.Printf("  - %s\n", u)
			}
		}
		fmt.Println()
	} else if action == "add" && len(args) == 3 {
		targetURL := args[2]
		u := fmt.Sprintf("%s/webhook/%s", baseURL, url.PathEscape(topic))
		body := fmt.Sprintf(`{"url":"%s"}`, targetURL)
		resp, err := DoAuthRequest(http.MethodPost, u, bytes.NewBuffer([]byte(body)))
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusCreated {
			fmt.Printf("%s✔ Webhook registered successfully!%s\n", ColorGreen, ColorReset)
		} else {
			b, _ := io.ReadAll(resp.Body)
			fmt.Printf("%s[Error] %s%s\n", ColorRed, string(b), ColorReset)
		}
	} else {
		fmt.Println("Invalid webhook command.")
	}
}
func HandleGroup(baseURL string, args []string) {
	if len(args) < 2 {
		fmt.Println("Use: tmq group list <topic>\n    tmq group create <topic> <group>")
		return
	}
	action, topic := args[0], args[1]
	switch action {
	case "list":
		u := fmt.Sprintf("%s/api/groups?topic=%s", baseURL, url.QueryEscape(topic))
		resp, err := DoAuthRequest(http.MethodGet, u, nil)
		if err != nil {
			fmt.Printf("%s[Error] %v%s\n", ColorRed, err, ColorReset)
			return
		}
		defer resp.Body.Close()
		var result struct {
			Topic  string   `json:"topic"`
			Groups []string `json:"groups"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			fmt.Printf("%s[Error] Could not parse response: %v%s\n", ColorRed, err, ColorReset)
			return
		}
		fmt.Printf("\n%sConsumer groups for '%s':%s\n", ColorBold+ColorCyan, topic, ColorReset)
		if len(result.Groups) == 0 {
			fmt.Println("  (No groups registered)")
		} else {
			for _, g := range result.Groups {
				fmt.Printf("  - %s\n", g)
			}
		}
		fmt.Println()
	case "create":
		if len(args) < 3 {
			fmt.Println("Use: tmq group create <topic> <group>")
			return
		}
		group := args[2]
		body, _ := json.Marshal(map[string]string{"topic": topic, "group": group})
		resp, err := DoAuthRequest(http.MethodPost, baseURL+"/api/groups", bytes.NewBuffer(body))
		if err != nil {
			fmt.Printf("%s[Error] %v%s\n", ColorRed, err, ColorReset)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusCreated {
			fmt.Printf("%s✔ Consumer group '%s' created for topic '%s'!%s\n", ColorGreen, group, topic, ColorReset)
		} else {
			b, _ := io.ReadAll(resp.Body)
			fmt.Printf("%s[Error] %s%s\n", ColorRed, string(b), ColorReset)
		}
	default:
		fmt.Println("Invalid group command.")
	}
}
func HandleTop(baseURL string) {
	for {
		fmt.Print("\033[H\033[2J")
		HandleList(baseURL)
		fmt.Printf("\n%s(Refreshing every 2s. Press Ctrl+C to exit)%s\n", ColorYellow, ColorReset)
		time.Sleep(2 * time.Second)
	}
}
