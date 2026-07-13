package main

import (
	"bufio"
	"os"
	"strings"

	"fmt"

	"github.com/x-name15/tinymq/cmd/tmq/handle"
	"github.com/x-name15/tinymq/cmd/tmq/shared"
)

func handleShell(baseURL string) {
	fmt.Printf("%sEntering TinyMQ Interactive Shell. Type 'exit' to quit.%s\n", shared.ColorBold+shared.ColorGreen, shared.ColorReset)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("%stinymq>%s ", shared.ColorCyan, shared.ColorReset)
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
			shared.HandleList(baseURL)
		case "create":
			shared.HandleCreate(baseURL, shellArgs)
		case "pub", "publish":
			shared.HandlePublish(baseURL, shellArgs)
		case "sub", "consume":
			shared.HandleConsume(baseURL, shellArgs)
		case "peek":
			shared.HandlePeek(baseURL, shellArgs)
		case "rm", "delete":
			shared.HandleRm(baseURL, shellArgs, false)
		case "purge":
			shared.HandleRm(baseURL, shellArgs, true)
		case "webhook":
			shared.HandleWebhook(baseURL, shellArgs)
		case "cluster":
			handle.HandleCluster(baseURL, shellArgs)
		case "group":
			shared.HandleGroup(baseURL, shellArgs)
		case "dlq":
			if len(shellArgs) < 2 || shellArgs[0] != "redrive" {
				fmt.Println("Use: dlq redrive <topic>")
			} else {
				shared.HandleRedrive(baseURL, shellArgs[1])
			}
		case "restore":
			handle.HandleRestore(shellArgs)
		case "bench":
			handle.HandleBench(baseURL, shellArgs)
		case "help":
			printHelp()
		default:
			fmt.Println("Unknown command. Type 'help' to see available commands.")
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("%s[Error] %v%s\n", shared.ColorRed, err, shared.ColorReset)
	}
}
