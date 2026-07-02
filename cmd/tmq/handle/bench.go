package handle

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func HandleBench(baseURL string, args []string) {
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
			reader.ReadString('\n')
			fmt.Fprintf(conn, "CONNECT {\"verbose\":false}\r\n")
			reader.ReadString('\n')
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
