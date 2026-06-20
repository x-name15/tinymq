package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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
	case "help", "-h", "--help":
		printHelp()
	default:
		fmt.Printf("%s[Error] Unknown command: %s%s\n\n", colorRed, cmd, colorReset)
		printHelp()
		os.Exit(1)
	}
}

func handleList(baseURL string) {
	resp, err := http.Get(baseURL + "/api/stats")
	if err != nil {
		fmt.Printf("%s[Error] Error connecting to the broker at %s: %v%s\n", colorRed, baseURL, err, colorReset)
		return
	}
	defer resp.Body.Close()

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

	u := fmt.Sprintf("%s/publish/%s?%s", baseURL, topic, params.Encode())
	resp, err := http.Post(u, "application/json", bytes.NewBuffer([]byte(payload)))
	if err != nil {
		fmt.Printf("%s[Error] Network error: %v%s\n", colorRed, err, colorReset)
		return
	}
	defer resp.Body.Close()

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

	u := fmt.Sprintf("%s/consume/%s?%s", baseURL, topic, params.Encode())
	resp, err := http.Get(u)
	if err != nil {
		fmt.Printf("%s[Error] Network error: %v%s\n", colorRed, err, colorReset)
		return
	}
	defer resp.Body.Close()

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
	resp, err := http.Get(u)
	if err != nil {
		fmt.Printf("%s[Error] Network error: %v%s\n", colorRed, err, colorReset)
		return
	}
	defer resp.Body.Close()

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

	fmt.Printf("%sSpy Mode: Listening to '%s' in real-time... (Ctrl+C to exit)%s\n", colorBold+colorGreen, topic, colorReset)

	for {
		u := fmt.Sprintf("%s/consume/%s?timeout=5s&limit=1&auto_ack=true", baseURL, topic)
		resp, err := http.Get(u)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			messages, err := parseMessagesPayload(body)
			if err == nil && len(messages) > 0 {
				msg := messages[0]
				fmt.Printf("%s[%s]%s %s %s->%s %s\n", 
					colorBlue, msg.Timestamp.Format("15:04:05"), colorReset, 
					colorBold+colorCyan, msg.ID[:8], colorReset, 
					string(msg.Payload),
				)
			}
		}
		resp.Body.Close()
	}
}

func handleBench(baseURL string, args []string) {
	benchCmd := flag.NewFlagSet("bench", flag.ExitOnError)
	total := benchCmd.Int("total", 20000, "Total messages to publish")
	concurrency := benchCmd.Int("concurrency", 50, "Number of concurrent workers")

	benchCmd.Parse(args)
	leftover := benchCmd.Args()

	if len(leftover) < 1 {
		fmt.Println("Usage: tmq bench <topic> [--total=20000] [--concurrency=50]")
		return
	}
	topic := leftover[0]

	fmt.Printf("Starting TinyMQ Benchmark...\n")
	fmt.Printf("Target: %s/publish/%s\n", baseURL, topic)
	fmt.Printf("Messages: %d | Concurrency: %d\n\n", *total, *concurrency)

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

	for w := 1; w <= *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				url := fmt.Sprintf("%s/publish/%s", baseURL, topic)
				req, _ := http.NewRequest("POST", url, bytes.NewBuffer(payload))
				req.Header.Set("Content-Type", "application/json")

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

func printHelp() {
	fmt.Printf("%s🍃 TinyMQ CLI (tmq) - Terminal Control Panel%s\n\n", colorBold+colorGreen, colorReset)
	fmt.Println("Use:")
	fmt.Println("  tmq <command> [arguments] [flags]")
	fmt.Println("\nAvailable commands:")
	fmt.Println("  status, list          Shows the table of active queues, RAM and consumers.")
	fmt.Println("  pub <queue> <data>    Publishes a message (supports flags --ttl, --delay, --broadcast).")
	fmt.Println("  sub <queue>           Consumes/extracts messages from the queue (supports --timeout, --limit).")
	fmt.Println("  peek <queue>          Inspects messages in RAM without deleting them.")
	fmt.Println("  tail <queue>          Live streaming mode (prints messages in real-time).")
	fmt.Println("  bench <queue>         Runs a stress test (flags: --total, --concurrency).")
	fmt.Println("  backup                Compresses the ./data folder (flags: --format=zip|tar).")
	fmt.Println("\nEnvironment variables:")
	fmt.Println("  TINYMQ_URL            Broker URL (Default: http://localhost:7800)")
	fmt.Println()
}