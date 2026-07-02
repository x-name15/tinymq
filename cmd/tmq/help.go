package main

import (
	"fmt"

	"github.com/x-name15/tinymq/cmd/tmq/shared"
)

func printHelp() {
	fmt.Printf("%s🍃 TinyMQ CLI (tmq) - Terminal Control Panel%s\n\n", shared.ColorBold+shared.ColorGreen, shared.ColorReset)
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
	fmt.Println("  cluster drain <url>   Marks a specific node as draining (rejects new requests before a controlled restart).")
	fmt.Println("  group <create|list>   Manages consumer groups for a topic (create <topic> <group> / list <topic>).")
	fmt.Println("  top                   Live dashboard in your terminal (refreshes every 2s).")
	fmt.Println("  shell                 Opens an interactive REPL session.")
	fmt.Println("  doctor                Runs local sanity checks (data dir, ports, env vars, broker reachability).")
	fmt.Println("\nEnvironment variables:")
	fmt.Println("  TINYMQ_URL            Broker URL (default: http://localhost:7800)")
	fmt.Println("  TINYMQ_API_KEY        API token for authenticated endpoints (optional)")
	fmt.Println()
}
