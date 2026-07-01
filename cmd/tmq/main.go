package main

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
)

type TopicStat struct {
	Name             string
	MessageCount     int
	WaitingConsumers int
	IsDLQ            bool
	HasWebhooks      bool
}

type StatsResponse struct {
	Stats         []TopicStat `json:"stats"`
	TotalWebhooks int         `json:"total_webhooks"`
}

type CLIMessage struct {
	ID         string    `json:"id"`
	Topic      string    `json:"topic"`
	Payload    []byte    `json:"payload"`
	Timestamp  time.Time `json:"timestamp"`
	RetryCount int       `json:"retry_count"`
}

type ClusterPeer struct {
	Address  string    `json:"address"`
	Alive    bool      `json:"alive"`
	LastSeen time.Time `json:"last_seen"`
}
type ClusterStatusResponse struct {
	ClusteringEnabled bool          `json:"clustering_enabled"`
	Role              string        `json:"role"`
	Term              int           `json:"term"`
	LeaderHTTP        string        `json:"leader_http"`
	Peers             []ClusterPeer `json:"peers"`
}

func doAuthRequest(method, urlStr string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if apiKey := os.Getenv("TINYMQ_API_KEY"); apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}

	client := &http.Client{}
	return client.Do(req)
}

func main() {
	baseURL := os.Getenv("TINYMQ_URL")
	if baseURL == "" {
		baseURL = "http://localhost:7800"
	}

	if len(os.Args) < 2 {
		printHelp()
		os.Exit(1)
	}

	cmd := os.Args[1]
	switch cmd {
	case "status", "list":
		handleList(baseURL)
	case "pub", "publish":
		handlePublish(baseURL, os.Args[2:])
	case "sub", "consume":
		handleConsume(baseURL, os.Args[2:])
	case "peek":
		handlePeek(baseURL, os.Args[2:])
	case "tail":
		handleTail(baseURL, os.Args[2:])
	case "bench":
		handleBench(baseURL, os.Args[2:])
	case "backup":
		handleBackup(os.Args[2:])
	case "restore":
		handleRestore(os.Args[2:])
	case "rm", "delete":
		handleRm(baseURL, os.Args[2:], false)
	case "purge":
		handleRm(baseURL, os.Args[2:], true)
	case "webhook":
		handleWebhook(baseURL, os.Args[2:])
	case "top":
		handleTop(baseURL)
	case "shell":
		handleShell(baseURL)
	case "cluster":
		handleCluster(baseURL, os.Args[2:])
	case "create":
		handleCreate(baseURL, os.Args[2:])
	case "help", "-h", "--help":
		printHelp()
	default:
		fmt.Printf("%s[Error] Unknown command: %s%s\n\n", colorRed, cmd, colorReset)
		printHelp()
		os.Exit(1)
	}
}

func handleList(baseURL string) {
	resp, err := doAuthRequest(http.MethodGet, baseURL+"/api/stats", nil)
	if err != nil {
		fmt.Printf("%s[Error] Error connecting to the broker at %s: %v%s\n", colorRed, baseURL, err, colorReset)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		fmt.Printf("%s[Error] Unauthorized. Ensure TINYMQ_API_KEY is correctly set.%s\n", colorRed, colorReset)
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

	fmt.Printf("\n%sSTATE OF 🍃 TINYMQ (%s)%s\n\n", colorBold+colorCyan, baseURL, colorReset)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 4, ' ', 0)
	fmt.Fprintf(w, "%sQUEUE / TOPIC NAME\tMESSAGES (RAM)\tWAITING WORKERS\tTYPE\tWEBHOOKS%s\n", colorBold, colorReset)

	for _, s := range stats {
		qType := "Standard"
		if s.IsDLQ {
			qType = colorRed + "DLQ" + colorReset
		} else if strings.Contains(s.Name, "*") {
			qType = colorBlue + "Wildcard" + colorReset
		}

		hasWh := "No"
		if s.HasWebhooks {
			hasWh = colorGreen + "Yes" + colorReset
		}

		msgCountStr := fmt.Sprintf("%d", s.MessageCount)
		if s.MessageCount > 0 {
			msgCountStr = colorYellow + msgCountStr + colorReset
		}

		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\n", s.Name, msgCountStr, s.WaitingConsumers, qType, hasWh)
	}
	w.Flush()
	fmt.Println()
}

func handlePublish(baseURL string, args []string) {
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
	resp, err := doAuthRequest(http.MethodPost, u, bytes.NewBuffer([]byte(payload)))
	if err != nil {
		fmt.Printf("%s[Error] Network error: %v%s\n", colorRed, err, colorReset)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		fmt.Printf("%s[Error] Unauthorized. Ensure TINYMQ_API_KEY is correctly set.%s\n", colorRed, colorReset)
		return
	}

	if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
		fmt.Printf("%s✔ Message published successfully in '%s'%s\n", colorGreen, topic, colorReset)
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("%sThe broker rejected the message (%d): %s%s\n", colorRed, resp.StatusCode, string(body), colorReset)
	}
}

func handleConsume(baseURL string, args []string) {
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
	resp, err := doAuthRequest(http.MethodGet, u, nil)
	if err != nil {
		fmt.Printf("%s[Error] Network error: %v%s\n", colorRed, err, colorReset)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		fmt.Printf("%s[Error] Unauthorized. Ensure TINYMQ_API_KEY is correctly set.%s\n", colorRed, colorReset)
		return
	}

	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("%s[Empty] No messages available in '%s'%s\n", colorYellow, topic, colorReset)
		return
	}

	body, _ := io.ReadAll(resp.Body)
	messages, err := parseMessagesPayload(body)
	if err != nil {
		fmt.Printf("%s[Error] Error interpreting response: %v%s\n", colorRed, err, colorReset)
		return
	}

	for _, msg := range messages {
		fmt.Printf("%s------------------------------------------%s\n", colorBlue, colorReset)
		fmt.Printf("%sID:%s %s  |  %sRetries:%s %d\n", colorBold+colorCyan, colorReset, msg.ID, colorBold+colorYellow, colorReset, msg.RetryCount)
		fmt.Printf("%sDate:%s %s\n", colorBold, colorReset, msg.Timestamp.Format("2006-01-02 15:04:05"))
		fmt.Printf("%sPayload:%s %s\n", colorBold, colorReset, string(msg.Payload))
	}
	fmt.Printf("%s------------------------------------------%s\n", colorBlue, colorReset)
}

func handlePeek(baseURL string, args []string) {
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
	resp, err := doAuthRequest(http.MethodGet, u, nil)
	if err != nil {
		fmt.Printf("%s[Error] Network error: %v%s\n", colorRed, err, colorReset)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		fmt.Printf("%s[Error] Unauthorized. Ensure TINYMQ_API_KEY is correctly set.%s\n", colorRed, colorReset)
		return
	}

	body, _ := io.ReadAll(resp.Body)
	var messages []CLIMessage
	if err := json.Unmarshal(body, &messages); err != nil {
		fmt.Printf("%s[Error] Error decoding broker RAM: %v%s\n", colorRed, err, colorReset)
		return
	}

	if len(messages) == 0 {
		fmt.Printf("%s[RAM] The queue '%s' is empty (0 messages in wait).%s\n", colorYellow, topic, colorReset)
		return
	}

	fmt.Printf("\n%sPEEKING RAM OF '%s' (Showing first %d)%s\n", colorBold+colorYellow, topic, *limit, colorReset)
	for i, msg := range messages {
		if i >= *limit {
			break
		}
		fmt.Printf("\n%s[%d] ID: %s | Attempts: %d | %s%s\n", colorBold+colorCyan, i+1, msg.ID, msg.RetryCount, msg.Timestamp.Format("15:04:05"), colorReset)
		fmt.Printf("   %s\n", string(msg.Payload))
	}
	fmt.Println()
}

func handleTail(baseURL string, args []string) {
	if len(args) < 1 {
		fmt.Println("Use: tmq tail <topic>")
		return
	}
	topic := args[0]

	fmt.Printf("%sSpy Mode: Listening to '%s' in real-time via SSE... (Ctrl+C to exit)%s\n", colorBold+colorGreen, topic, colorReset)

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
			fmt.Printf("%s[Connection Lost] Retrying in 2s...%s\n", colorRed, colorReset)
			time.Sleep(2 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusUnauthorized {
			resp.Body.Close()
			fmt.Printf("%s[Error] Unauthorized. Ensure TINYMQ_API_KEY is correctly set.%s\n", colorRed, colorReset)
			return
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			fmt.Printf("%s[Error] Broker returned status %d. Retrying in 2s...%s\n", colorRed, resp.StatusCode, colorReset)
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
						fmt.Printf("%s[SSE] Connected successfully! Waiting for messages...%s\n", colorYellow, colorReset)
						continue
					}
				}

				var msg CLIMessage
				if err := json.Unmarshal([]byte(dataStr), &msg); err == nil && msg.ID != "" {
					var payloadStr string
					payloadStr = string(msg.Payload)

					fmt.Printf("%s[%s]%s %s %s->%s %s\n",
						colorBlue, msg.Timestamp.Format("15:04:05"), colorReset,
						colorBold+colorCyan, msg.ID[:8], colorReset,
						payloadStr,
					)
				}
			}
		}

		resp.Body.Close()
		time.Sleep(2 * time.Second)
	}
}

func handleBench(baseURL string, args []string) {
	benchCmd := flag.NewFlagSet("bench", flag.ExitOnError)
	total := benchCmd.Int("total", 20000, "Total messages to publish")
	concurrency := benchCmd.Int("concurrency", 50, "Number of concurrent workers")
	protocol := benchCmd.String("protocol", "http", "Protocol to benchmark: 'http' or 'nats'")
	target := benchCmd.String("target", "127.0.0.1:40104", "Target TCP address for NATS (e.g., 127.0.0.1:40104)")

	benchCmd.Parse(args)
	leftover := benchCmd.Args()

	if len(leftover) < 1 {
		fmt.Println("Usage: tmq bench <topic> [--protocol=http|nats] [--total=20000] [--concurrency=50] [--target=ip:port]")
		return
	}
	topic := leftover[0]

	if *protocol == "nats" {
		runNatsBench(*target, topic, *total, *concurrency)
		return
	}

	// ─── HTTP Benchmark Logic ──────────────────────────────────────────
	safeTopic := url.PathEscape(topic)
	fmt.Printf("Starting TinyMQ Benchmark...\n")
	fmt.Printf("Protocol: HTTP\nTarget: %s/publish/%s\n", baseURL, topic)
	fmt.Printf("Messages: %d | Concurrency: %d\n\n", *total, *concurrency)
	originalOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(originalOutput)

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 5 * time.Second,
	}

	payload := []byte(`{"event":"bench","data":"stress_test_payload_123456789"}`)
	var successCount int32
	var failCount int32
	var wg sync.WaitGroup

	jobs := make(chan int, *total)
	start := time.Now()

	apiKey := os.Getenv("TINYMQ_API_KEY")

	for w := 1; w <= *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				url := fmt.Sprintf("%s/publish/%s", baseURL, safeTopic)
				req, _ := http.NewRequest("POST", url, bytes.NewBuffer(payload))
				req.Header.Set("Content-Type", "application/json")
				if apiKey != "" {
					req.Header.Set("Authorization", "Bearer "+apiKey)
				}

				resp, err := client.Do(req)
				if err != nil {
					atomic.AddInt32(&failCount, 1)
					continue
				}

				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
					atomic.AddInt32(&successCount, 1)
				} else {
					atomic.AddInt32(&failCount, 1)
				}
			}
		}()
	}

	for j := 1; j <= *total; j++ {
		jobs <- j
	}
	close(jobs)
	wg.Wait()

	duration := time.Since(start)
	msgsPerSec := float64(successCount) / duration.Seconds()

	fmt.Printf("📊 --- BENCHMARK RESULTS ---\n")
	fmt.Printf("Time Taken:       %.2f seconds\n", duration.Seconds())
	fmt.Printf("Successful:       %d\n", successCount)
	fmt.Printf("Failed:           %d\n", failCount)
	fmt.Printf("Throughput:       \033[32m\033[1m%.2f msgs/sec\033[0m\n", msgsPerSec)
}

func runNatsBench(target, topic string, total, concurrency int) {
	fmt.Printf("Starting TinyMQ Benchmark...\n")
	fmt.Printf("Protocol: NATS TCP\nTarget: %s | Topic: %s\n", target, topic)
	fmt.Printf("Messages: %d | Concurrency: %d\n\n", total, concurrency)

	originalOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(originalOutput)

	payload := "stress_test_payload_123456789"
	pubLine := fmt.Sprintf("PUB %s %d\r\n%s\r\n", topic, len(payload), payload)
	pubBytes := []byte(pubLine)

	var successCount int32
	var failCount int32
	var wg sync.WaitGroup

	msgsPerWorker := total / concurrency
	start := time.Now()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)

		workerMsgs := msgsPerWorker
		if w == concurrency-1 {
			workerMsgs += total % concurrency
		}

		go func(msgs int) {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", target, 5*time.Second)
			if err != nil {
				atomic.AddInt32(&failCount, int32(msgs))
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)
			reader.ReadString('\n') // Consume INFO
			fmt.Fprintf(conn, "CONNECT {\"verbose\":false}\r\n")
			reader.ReadString('\n') // Consume +OK

			for i := 0; i < msgs; i++ {
				_, err := conn.Write(pubBytes)
				if err != nil {
					atomic.AddInt32(&failCount, int32(msgs-i))
					break
				}
				atomic.AddInt32(&successCount, 1)
			}
		}(workerMsgs)
	}

	wg.Wait()

	duration := time.Since(start)
	msgsPerSec := float64(successCount) / duration.Seconds()

	fmt.Printf("📊 --- BENCHMARK RESULTS ---\n")
	fmt.Printf("Time Taken:       %.2f seconds\n", duration.Seconds())
	fmt.Printf("Successful:       %d\n", successCount)
	fmt.Printf("Failed:           %d\n", failCount)
	fmt.Printf("Throughput:       \033[32m\033[1m%.2f msgs/sec\033[0m\n", msgsPerSec)
}

func handleBackup(args []string) {
	backupCmd := flag.NewFlagSet("backup", flag.ExitOnError)
	format := backupCmd.String("format", "zip", "Backup format: 'zip' or 'tar'")

	backupCmd.Parse(args)
	dataDir := "./data"

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		fmt.Printf("%s[Error] Cannot find './data' directory.%s\n", colorRed, colorReset)
		return
	}

	if *format == "tar" {
		outName := fmt.Sprintf("tinymq_backup_%d.tar.gz", time.Now().Unix())
		createTarGzBackup(dataDir, outName)
	} else {
		outName := fmt.Sprintf("tinymq_backup_%d.zip", time.Now().Unix())
		createZipBackup(dataDir, outName)
	}
}

func createZipBackup(dataDir string, outZip string) {
	fmt.Printf("Backing up TinyMQ data to '%s' (Format: ZIP)...\n", outZip)

	outFile, err := os.Create(outZip)
	if err != nil {
		fmt.Printf("❌ Error creating zip file: %v\n", err)
		return
	}
	defer outFile.Close()

	w := zip.NewWriter(outFile)
	defer w.Close()

	filesAdded := 0
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".log" {
			return nil
		}

		relPath, _ := filepath.Rel(dataDir, path)
		f, err := w.Create(relPath)
		if err != nil {
			return err
		}

		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()

		if _, err = io.Copy(f, src); err == nil {
			filesAdded++
		}
		return nil
	})

	fmt.Printf("✅ Backup complete! %d topic logs compressed securely.\n", filesAdded)
}

func createTarGzBackup(dataDir string, outTar string) {
	fmt.Printf("Backing up TinyMQ data to '%s' (Format: TAR.GZ)...\n", outTar)

	outFile, err := os.Create(outTar)
	if err != nil {
		fmt.Printf("❌ Error creating tar file: %v\n", err)
		return
	}
	defer outFile.Close()

	gw := gzip.NewWriter(outFile)
	defer gw.Close()

	tw := tar.NewWriter(gw)
	defer tw.Close()

	filesAdded := 0
	filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".log" {
			return nil
		}

		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()

		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}

		relPath, _ := filepath.Rel(dataDir, path)
		header.Name = relPath

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		if _, err = io.Copy(tw, src); err == nil {
			filesAdded++
		}
		return nil
	})

	fmt.Printf("✅ Backup complete! %d topic logs compressed securely.\n", filesAdded)
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

func handleRm(baseURL string, args []string, isPurge bool) {
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
	resp, err := doAuthRequest(http.MethodDelete, u, nil)
	if err != nil {
		fmt.Printf("%s[Error] Network error: %v%s\n", colorRed, err, colorReset)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("%s✔ Queue '%s' successfully %s!%s\n", colorGreen, topic, actionStr, colorReset)
	} else {
		fmt.Printf("%s[Error] Broker returned status %d%s\n", colorRed, resp.StatusCode, colorReset)
	}
}

func handleCreate(baseURL string, args []string) {
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
			fmt.Printf("%s[Error] --policy must be 'reject' or 'drop-oldest'%s\n", colorRed, colorReset)
			return
		}
		payload["policy"] = *policy
	}
	if *retention != "" {
		if _, err := time.ParseDuration(*retention); err != nil {
			fmt.Printf("%s[Error] --retention must be a valid Go duration (e.g. 24h, 30m): %v%s\n", colorRed, err, colorReset)
			return
		}
		payload["retain"] = *retention
	}
	body, _ := json.Marshal(payload)
	resp, err := doAuthRequest(http.MethodPost, baseURL+"/api/topics", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("%s[Error] %v%s\n", colorRed, err, colorReset)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusCreated {
		fmt.Printf("%s✔ Queue '%s' created!%s\n", colorGreen, topic, colorReset)
		if *policy != "" {
			fmt.Printf("  Policy: %s\n", *policy)
		}
		if *retention != "" {
			fmt.Printf("  Retention: %s\n", *retention)
		}
	} else {
		b, _ := io.ReadAll(resp.Body)
		fmt.Printf("%s[Error] %s%s\n", colorRed, string(b), colorReset)
	}
}

func handleWebhook(baseURL string, args []string) {
	if len(args) < 2 {
		fmt.Println("Use: tmq webhook list <topic>\n    tmq webhook add <topic> <url>")
		return
	}
	action, topic := args[0], args[1]

	if action == "list" {
		u := fmt.Sprintf("%s/api/queues/webhooks?queue=%s", baseURL, url.QueryEscape(topic))
		resp, err := doAuthRequest(http.MethodGet, u, nil)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		defer resp.Body.Close()
		var urls []string
		json.NewDecoder(resp.Body).Decode(&urls)
		fmt.Printf("\n%sWebhooks for '%s':%s\n", colorBold+colorCyan, topic, colorReset)
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
		resp, err := doAuthRequest(http.MethodPost, u, bytes.NewBuffer([]byte(body)))
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusCreated {
			fmt.Printf("%s✔ Webhook registered successfully!%s\n", colorGreen, colorReset)
		} else {
			b, _ := io.ReadAll(resp.Body)
			fmt.Printf("%s[Error] %s%s\n", colorRed, string(b), colorReset)
		}
	} else {
		fmt.Println("Invalid webhook command.")
	}
}

func handleGroup(baseURL string, args []string) {
	if len(args) < 2 {
		fmt.Println("Use: tmq group list <topic>\n    tmq group create <topic> <group>")
		return
	}
	action, topic := args[0], args[1]
	switch action {
	case "list":
		u := fmt.Sprintf("%s/api/groups?topic=%s", baseURL, url.QueryEscape(topic))
		resp, err := doAuthRequest(http.MethodGet, u, nil)
		if err != nil {
			fmt.Printf("%s[Error] %v%s\n", colorRed, err, colorReset)
			return
		}
		defer resp.Body.Close()
		var result struct {
			Topic  string   `json:"topic"`
			Groups []string `json:"groups"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			fmt.Printf("%s[Error] Could not parse response: %v%s\n", colorRed, err, colorReset)
			return
		}
		fmt.Printf("\n%sConsumer groups for '%s':%s\n", colorBold+colorCyan, topic, colorReset)
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
		resp, err := doAuthRequest(http.MethodPost, baseURL+"/api/groups", bytes.NewBuffer(body))
		if err != nil {
			fmt.Printf("%s[Error] %v%s\n", colorRed, err, colorReset)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusCreated {
			fmt.Printf("%s✔ Consumer group '%s' created for topic '%s'!%s\n", colorGreen, group, topic, colorReset)
		} else {
			b, _ := io.ReadAll(resp.Body)
			fmt.Printf("%s[Error] %s%s\n", colorRed, string(b), colorReset)
		}
	default:
		fmt.Println("Invalid group command.")
	}
}

func handleTop(baseURL string) {
	for {
		fmt.Print("\033[H\033[2J")
		handleList(baseURL)
		fmt.Printf("\n%s(Refreshing every 2s. Press Ctrl+C to exit)%s\n", colorYellow, colorReset)
		time.Sleep(2 * time.Second)
	}
}

func handleRestore(args []string) {
	restoreCmd := flag.NewFlagSet("restore", flag.ExitOnError)
	fileFlag := restoreCmd.String("file", "", "Backup file to restore (.zip or .tar.gz)")
	dataDirFlag := restoreCmd.String("data-dir", "./data", "Target data directory (default: ./data)")
	restoreCmd.Parse(args)

	if *fileFlag == "" {
		fmt.Printf("%s[Error] --file is required. Example: tmq restore --file tinymq_backup_1234.tar.gz%s\n", colorRed, colorReset)
		return
	}

	filename := *fileFlag
	dataDir := *dataDirFlag

	if entries, err := os.ReadDir(dataDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".log") {
				fmt.Printf("%s[Warn] '%s' already contains .log files. Restoring will overwrite them.%s\n", colorYellow, dataDir, colorReset)
				fmt.Printf("Continue? [y/N]: ")
				var answer string
				fmt.Scanln(&answer)
				if strings.ToLower(strings.TrimSpace(answer)) != "y" {
					fmt.Println("Restore cancelled.")
					return
				}
				break
			}
		}
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Printf("%s[Error] Cannot create data directory '%s': %v%s\n", colorRed, dataDir, err, colorReset)
		return
	}

	var err error
	switch {
	case strings.HasSuffix(filename, ".tar.gz") || strings.HasSuffix(filename, ".tgz"):
		err = restoreFromTarGz(filename, dataDir)
	case strings.HasSuffix(filename, ".zip"):
		err = restoreFromZip(filename, dataDir)
	default:
		fmt.Printf("%s[Error] Unrecognised file format. Expected .zip or .tar.gz%s\n", colorRed, colorReset)
		return
	}

	if err != nil {
		fmt.Printf("%s[Error] Restore failed: %v%s\n", colorRed, err, colorReset)
		return
	}

	fmt.Printf("%s✔ Restore complete! Data written to '%s'%s\n", colorGreen, dataDir, colorReset)
	fmt.Printf("%s  Restart TinyMQ to load the recovered messages.%s\n", colorYellow, colorReset)
}

func restoreFromTarGz(filename, dataDir string) error {
	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("cannot open file: %w", err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("cannot read gzip stream: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	restored := 0

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading tar entry: %w", err)
		}

		if header.Typeflag != tar.TypeReg || !strings.HasSuffix(header.Name, ".log") {
			continue
		}

		entryName := filepath.Base(header.Name)
		if entryName == "." || entryName == ".." {
			continue
		}

		destPath := filepath.Join(dataDir, entryName)

		out, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("cannot create '%s': %w", destPath, err)
		}

		if _, err := io.Copy(out, io.LimitReader(tr, 512<<20)); err != nil {
			out.Close()
			return fmt.Errorf("error writing '%s': %w", entryName, err)
		}
		out.Close()
		restored++
		fmt.Printf("  ↳ restored: %s\n", entryName)
	}

	if restored == 0 {
		return fmt.Errorf("archive contained no .log files")
	}

	fmt.Printf("  %d topic log(s) restored.\n", restored)
	return nil
}

func restoreFromZip(filename, dataDir string) error {
	r, err := zip.OpenReader(filename)
	if err != nil {
		return fmt.Errorf("cannot open zip file: %w", err)
	}
	defer r.Close()

	restored := 0

	for _, f := range r.File {
		if f.FileInfo().IsDir() || !strings.HasSuffix(f.Name, ".log") {
			continue
		}

		entryName := filepath.Base(f.Name)
		if entryName == "." || entryName == ".." {
			continue
		}

		destPath := filepath.Join(dataDir, entryName)

		rc, err := f.Open()
		if err != nil {
			return fmt.Errorf("cannot open zip entry '%s': %w", f.Name, err)
		}

		out, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			rc.Close()
			return fmt.Errorf("cannot create '%s': %w", destPath, err)
		}

		if _, err := io.Copy(out, io.LimitReader(rc, 512<<20)); err != nil {
			rc.Close()
			out.Close()
			return fmt.Errorf("error writing '%s': %w", entryName, err)
		}
		rc.Close()
		out.Close()
		restored++
		fmt.Printf("  ↳ restored: %s\n", entryName)
	}

	if restored == 0 {
		return fmt.Errorf("archive contained no .log files")
	}

	fmt.Printf("  %d topic log(s) restored.\n", restored)
	return nil
}

func handleCluster(baseURL string, args []string) {
	if len(args) == 0 {
		fmt.Println("Use: tmq cluster status\n    tmq cluster peers [--watch]")
		return
	}
	switch args[0] {
	case "status":
		printClusterStatus(baseURL)
	case "peers":
		peersCmd := flag.NewFlagSet("peers", flag.ExitOnError)
		watch := peersCmd.Bool("watch", false, "Continuously refresh the peer table every 2s")
		peersCmd.Parse(args[1:])
		if *watch {
			for {
				fmt.Print("\033[H\033[2J")
				printClusterStatus(baseURL)
				fmt.Printf("\n%s(Refreshing every 2s. Press Ctrl+C to exit)%s\n", colorYellow, colorReset)
				time.Sleep(2 * time.Second)
			}
		}
		printClusterStatus(baseURL)
	default:
		fmt.Println("Use: tmq cluster status\n    tmq cluster peers [--watch]")
	}
}

func printClusterStatus(baseURL string) {
	resp, err := doAuthRequest(http.MethodGet, baseURL+"/api/cluster/status", nil)
	if err != nil {
		fmt.Printf("%s[Error] Error connecting to the broker at %s: %v%s\n", colorRed, baseURL, err, colorReset)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		fmt.Printf("%s[Error] Unauthorized. Ensure TINYMQ_API_KEY is correctly set.%s\n", colorRed, colorReset)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	var status ClusterStatusResponse
	if err := json.Unmarshal(body, &status); err != nil {
		fmt.Printf("%s[Error] Could not parse cluster status response: %v%s\n", colorRed, err, colorReset)
		return
	}
	if !status.ClusteringEnabled {
		fmt.Println("Clustering is not enabled on this node (standalone mode).")
		return
	}
	roleColor := colorGreen
	if status.Role == "candidate" {
		roleColor = colorYellow
	} else if status.Role == "follower" {
		roleColor = colorBlue
	}
	fmt.Printf("\n%sTINYMQ CLUSTER STATUS (%s)%s\n\n", colorBold+colorCyan, baseURL, colorReset)
	fmt.Printf("Role:  %s%s%s\n", roleColor, status.Role, colorReset)
	fmt.Printf("Term:  %d\n", status.Term)
	if status.Role != "leader" {
		leaderStr := status.LeaderHTTP
		if leaderStr == "" {
			leaderStr = colorYellow + "unknown (electing)" + colorReset
		}
		fmt.Printf("Leader: %s\n", leaderStr)
	}
	if len(status.Peers) == 0 {
		fmt.Println("\nNo peers configured.")
		return
	}
	fmt.Println()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 4, ' ', 0)
	fmt.Fprintf(w, "%sADDRESS\tSTATUS\tLAST SEEN%s\n", colorBold, colorReset)
	for _, p := range status.Peers {
		statusStr := colorRed + "dead" + colorReset
		if p.Alive {
			statusStr = colorGreen + "alive" + colorReset
		}
		lastSeen := "never"
		if !p.LastSeen.IsZero() {
			lastSeen = time.Since(p.LastSeen).Round(time.Second).String() + " ago"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", p.Address, statusStr, lastSeen)
	}
	w.Flush()
	fmt.Println()
}

func handleShell(baseURL string) {
	fmt.Printf("%sEntering TinyMQ Interactive Shell. Type 'exit' to quit.%s\n", colorBold+colorGreen, colorReset)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("%stinymq>%s ", colorCyan, colorReset)
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "exit" || input == "quit" {
			break
		}
		if input == "" {
			continue
		}
		parts := strings.SplitN(input, " ", 3)
		cmd := parts[0]
		var shellArgs []string
		if len(parts) > 1 {
			if (cmd == "pub" || cmd == "publish") && len(parts) == 3 {
				shellArgs = []string{parts[1], parts[2]}
			} else {
				shellArgs = strings.Split(input[len(cmd)+1:], " ")
			}
		}
		switch cmd {
		case "list", "status":
			handleList(baseURL)
		case "create":
			handleCreate(baseURL, shellArgs)
		case "pub", "publish":
			handlePublish(baseURL, shellArgs)
		case "sub", "consume":
			handleConsume(baseURL, shellArgs)
		case "peek":
			handlePeek(baseURL, shellArgs)
		case "rm", "delete":
			handleRm(baseURL, shellArgs, false)
		case "purge":
			handleRm(baseURL, shellArgs, true)
		case "webhook":
			handleWebhook(baseURL, shellArgs)
		case "cluster":
			handleCluster(baseURL, shellArgs)
		case "group":
			handleGroup(baseURL, os.Args[2:])
		case "restore":
			handleRestore(shellArgs)
		case "bench":
			handleBench(baseURL, shellArgs)
		case "help":
			printHelp()
		default:
			fmt.Println("Unknown command. Type 'help' to see available commands.")
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("%s[Error] %v%s\n", colorRed, err, colorReset)
	}
}

func printHelp() {
	fmt.Printf("%s🍃 TinyMQ CLI (tmq) - Terminal Control Panel%s\n\n", colorBold+colorGreen, colorReset)
	fmt.Println("Use:")
	fmt.Println("  tmq <command> [arguments] [flags]")
	fmt.Println("\nAvailable commands:")
	fmt.Println("  status, list          Shows the table of active queues, RAM and consumers.")
	fmt.Println("  create <queue>        Explicitly creates a queue (flags: --policy=reject|drop-oldest, --retention).")
	fmt.Println("  pub <queue> <data>    Publishes a message (flags: --ttl, --delay, --broadcast).")
	fmt.Println("  sub <queue>           Consumes messages from the queue (flags: --timeout, --limit).")
	fmt.Println("  peek <queue>          Inspects messages in RAM without consuming them.")
	fmt.Println("  tail <queue>          Live streaming mode (prints messages in real-time via SSE).")
	fmt.Println("  bench <queue>         Runs a stress test (flags: --protocol=http|nats, --total, --concurrency, --target).")
	fmt.Println("  backup                Compresses ./data into an archive (flags: --format=zip|tar).")
	fmt.Println("  restore               Restores a backup archive into ./data (flags: --file, --data-dir).")
	fmt.Println("  rm <queue>            Deletes a queue and its log file entirely.")
	fmt.Println("  purge <queue>         Empties a queue without deleting it.")
	fmt.Println("  webhook <add|list>    Manages webhooks for a topic.")
	fmt.Println("  cluster status        Shows this node's role, term, leader, and peer health.")
	fmt.Println("  cluster peers         Same view, focused on peers (flag: --watch, refresh every 2s).")
	fmt.Println("  group <create|list>   Manages consumer groups for a topic (create <topic> <group> / list <topic>).")
	fmt.Println("  top                   Live dashboard in your terminal (refreshes every 2s).")
	fmt.Println("  shell                 Opens an interactive REPL session.")
	fmt.Println("\nEnvironment variables:")
	fmt.Println("  TINYMQ_URL            Broker URL (default: http://localhost:7800)")
	fmt.Println("  TINYMQ_API_KEY        API token for authenticated endpoints (optional)")
	fmt.Println()
}
