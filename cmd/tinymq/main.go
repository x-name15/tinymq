package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/helper"
	"github.com/x-name15/tinymq/internal/storage"
	"github.com/x-name15/tinymq/internal/transport/rest"
)

func main() {
	helper.LoadEnv()

	log.Printf("Starting TinyMQ v%s...\n", Version)

	dataDir := "./data"
	syncWrites := os.Getenv("TINYMQ_FSYNC") == "true"
	if syncWrites {
		log.Println("Strict disk durability (FSync) is ENABLED.")
	}

	store, err := storage.New(dataDir, syncWrites)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	b := broker.New(store)

	// Recover existing logs
	files, err := os.ReadDir(dataDir)
	if err == nil {
		var topicsToRecover []string
		for _, file := range files {
			if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
				topicName := strings.TrimSuffix(file.Name(), ".log")
				topicsToRecover = append(topicsToRecover, topicName)
			}
		}

		if len(topicsToRecover) > 0 {
			b.LoadExistingTopics(topicsToRecover)
			for _, name := range topicsToRecover {
				if err := store.CompactLog(name); err != nil {
					log.Printf("Failed to compact log for topic %s: %v\n", name, err)
				} else {
					log.Printf("Log for topic '%s' successfully compacted!\n", name)
				}
			}
		}
	}

	ctx, cancelGC := context.WithCancel(context.Background())
	defer cancelGC()

	// Garbage Collector Background Routine
	go func() {
		compactionInterval := 10 * time.Minute
		if envInt := os.Getenv("TINYMQ_COMPACT_INTERVAL"); envInt != "" {
			if d, err := time.ParseDuration(envInt); err == nil {
				compactionInterval = d
			}
		}

		ticker := time.NewTicker(compactionInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				stats, _ := b.GetStats()
				compacted := 0
				for _, st := range stats {
					if err := store.CompactLog(st.Name); err == nil {
						compacted++
					}
				}
				if compacted > 0 {
					log.Printf("[GC] Auto-compacted %d WAL files to free disk space.\n", compacted)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Initializing Transports (Ports & Adapters)
	port := os.Getenv("PORT")
	if port == "" {
		port = "7800"
	}

	restServer := rest.NewServer(b, port, Version)
	
	go func() {
		if err := restServer.Start(); err != nil {
			log.Fatalf("Failed to start REST server: %v", err)
		}
	}()

	// Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down TinyMQ gracefully...")
	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()

	if err := restServer.Stop(ctxShutdown); err != nil {
		log.Fatalf("Forced shutdown: %v", err)
	}

	store.CloseAll()
	log.Println("TinyMQ stopped.")
}