package main

import (
	"fmt"
	"os"

	"github.com/x-name15/tinymq/cmd/tmq/handle"
	"github.com/x-name15/tinymq/cmd/tmq/shared"
)

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
		shared.HandleList(baseURL)
	case "pub", "publish":
		shared.HandlePublish(baseURL, os.Args[2:])
	case "sub", "consume":
		shared.HandleConsume(baseURL, os.Args[2:])
	case "peek":
		shared.HandlePeek(baseURL, os.Args[2:])
	case "tail":
		shared.HandleTail(baseURL, os.Args[2:])
	case "bench":
		handle.HandleBench(baseURL, os.Args[2:])
	case "backup":
		handle.HandleBackup(os.Args[2:])
	case "restore":
		handle.HandleRestore(os.Args[2:])
	case "rm", "delete":
		shared.HandleRm(baseURL, os.Args[2:], false)
	case "purge":
		shared.HandleRm(baseURL, os.Args[2:], true)
	case "webhook":
		shared.HandleWebhook(baseURL, os.Args[2:])
	case "top":
		shared.HandleTop(baseURL)
	case "shell":
		handleShell(baseURL)
	case "cluster":
		handle.HandleCluster(baseURL, os.Args[2:])
	case "create":
		shared.HandleCreate(baseURL, os.Args[2:])
	case "group":
		shared.HandleGroup(baseURL, os.Args[2:])
	case "doctor":
		handle.HandleDoctor(baseURL, os.Args[2:])
	case "help", "-h", "--help":
		printHelp()
	default:
		fmt.Printf("%s[Error] Unknown command: %s%s\n\n", shared.ColorRed, cmd, shared.ColorReset)
		printHelp()
		os.Exit(1)
	}
}
