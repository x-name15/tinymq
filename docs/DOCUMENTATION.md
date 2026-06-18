# TinyMQ - Official Documentation

Welcome to the TinyMQ documentation. This file covers the HTTP API, internal architecture, SDK usage, and deployment.

## Internal architecture & guarantees

TinyMQ is designed around simplified distributed-systems principles to be lightweight, resilient, and exceptionally fast.

### Commit log cycle (PUT/ACK strategy)

TinyMQ uses an append-only `.log` file per topic (Write-Ahead Log):

- Publishing appends a `PUT` event.
- Acknowledging appends an `ACK` event.

On startup, the broker replays logs to rebuild the in-memory state of unacknowledged messages. An auto-compaction routine runs on boot to purge confirmed records and free disk space. *Lazy Initialization* ensures that `.log` files are only created when the first message is published, avoiding empty files for manually created topics.

### Lock-free routing & wildcards

The broker minimizes global `Mutex` contention: the global lock is used only to locate a topic; dispatching and disk I/O occur under per-topic locks. Wildcards (e.g., `events.*`) are supported via cached compiled regular expressions.

### Dead Letter Queues (DLQ) & Resiliency

To prevent "poison pill" messages from permanently blocking a work queue, TinyMQ natively supports Dead Letter Queues. If a consumer using the SDK fails to process a message 3 times, the broker automatically isolates it into a `{topic_name}.dlq` queue to keep the main pipeline flowing.

### Time-Based Routing (TTL & Delays)

TinyMQ handles time efficiently using *Lazy Expiration*. Expired messages (TTL) are silently dropped and acknowledged on the fly when a consumer attempts to read them. Delayed messages are kept in memory but hidden from consumers until their scheduled delivery time is reached, preventing thread blocking.

### Graceful shutdown & memory safety

On shutdown (Ctrl+C or `docker stop`) TinyMQ runs `CloseAll()` to flush buffers and close files cleanly. The code also performs explicit nil assignments when discarding messages to avoid GC retention.

---

## HTTP API reference (language-agnostic)

You can interact with TinyMQ via `curl`, Go, Python, Node.js, Rust, etc. Payloads may be JSON, plain text, binary, or other formats.

### Publish a message

**Endpoint:** `POST /publish/{topic}`

**Query Parameters (Optional):**
- `ttl` (e.g., `30s`, `1h`): Time-To-Live. The message will be destroyed if not consumed within this window.
- `delay` (e.g., `5m`, `10s`): Delays the delivery. The message will be hidden from consumers until this time passes.
- `broadcast` (`true`): Ephemeral Fan-out. Dispatches the message to all currently waiting consumers simultaneously without persisting it to disk.

```bash
curl -X POST "http://127.0.0.1:7800/publish/orders.eu?delay=5s" \
  -H "Content-Type: application/json" \
  -d '{"user_id": 42, "item": "laptop"}'
```

**Response:** `202 Accepted`

```json
{
  "status": "accepted",
  "topic": "orders.eu"
}
```

### Consume a message (Pull / Long-Polling)

**Endpoint:** `GET /consume/{topic}`

**Query Parameters (Optional):**
- `timeout` (e.g., `5s`, `500ms`): How long to hold the connection when the queue is empty (Long-polling).
- `auto_ack` (`true`/`false`): If `true`, the message is marked processed and removed immediately.
- `limit` (e.g., `10`): Batching/Prefetch. Extracts up to `X` messages in a single network call.

```bash
curl -X GET "http://127.0.0.1:7800/consume/orders.*?limit=5&auto_ack=true&timeout=10s"
```

**Response (200):**
If `limit=1` (Default), returns a single JSON object. If `limit > 1`, returns a JSON Array.

```json
[
  {
    "id": "e4b3a1d2-7c89-4b1a-9f5e-123456789abc",
    "topic": "orders.eu",
    "payload": "eyJ1c2VyX2lkIjogNDIsICJpdGVtIjogImxhcHRvcCJ9",
    "timestamp": "2026-06-18T10:00:00Z"
  }
]
```

*(Binary payloads may be base64-encoded depending on your client.)*

**Response (404):** When timeout expires and no message arrived.

```json
{
  "status": "empty",
  "message": "No messages in topic"
}
```

### Register a Webhook (Push Consumers)

For passive integration, TinyMQ can take the initiative and push messages directly to your external services (Fire-and-Forget).

**Endpoint:** `POST /webhook/{topic}`

```bash
curl -X POST http://127.0.0.1:7800/webhook/orders.eu \
  -H "Content-Type: application/json" \
  -d '{"url": "https://api.my-service.com/incoming"}'
```

### Manual acknowledgment (ACK)

**Endpoint:** `POST /ack/{topic}/{message_id}`

If `auto_ack=false`, you must call this endpoint after processing to remove the message from RAM and disk:

```bash
curl -X POST http://127.0.0.1:7800/ack/orders.eu/e4b3a1d2-7c89-4b1a-9f5e-123456789abc
```

### Create Topic Manually

**Endpoint:** `POST /api/topics`

Pre-initialize a topic safely (Validates alphanumeric characters, max length, and idempotency).

```bash
curl -X POST http://127.0.0.1:7800/api/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "analytics.events"}'
```

### Dashboard

Visit `http://127.0.0.1:7800/dashboard` to access the interactive web interface. Features include:
- Auto-Refresh mode.
- Uptime and memory footprint monitoring.
- Visual indicators for Active Webhooks and Dead Letter Queues (☠️ DLQ).
- Manual topic creation UI.
- Real-time waiting consumers tracking.

---

## Go SDK integration (advanced)

The native SDK (`client/client.go`) abstracts HTTP calls, handles ACKs, and offers resilient worker patterns.

### Installation

Install into your Go project:

```bash
go get github.com/x-name15/tinymq/client
```

### Connecting & publishing

```go
package main

import "github.com/x-name15/tinymq/client"

func main() {
    mq := client.NewClient("http://127.0.0.1:7800")
    payload := []byte(`{"event": "user_signup", "id": 99}`)
    
    // Standard Publish
    if err := mq.Publish("users.new", payload); err != nil {
        panic("broker unreachable")
    }
    
    // Broadcast (Fan-out)
    mq.PublishBroadcast("users.new", payload)
}
```

### High-resilience subscription (workers)

`Subscribe` uses long-polling and exponential backoff (1s up to 32s). If a handler returns an error, the SDK automatically calls `/requeue` to increment the retry count. After 3 failures, the broker safely moves the payload to a `.dlq` topic.

```go
package main

import (
    "fmt"
    "errors"
    "github.com/x-name15/tinymq/client"
    "github.com/x-name15/tinymq/internal/message"
)

func main() {
    mq := client.NewClient("http://127.0.0.1:7800")

    go mq.Subscribe("orders", client.SubscriptionOptions{Timeout: "10s"}, func(msg message.Message) error {
        fmt.Printf("Processing order: %s\n", string(msg.Payload))
        if string(msg.Payload) == "bad_data" {
            return errors.New("database connection lost") // Will trigger backoff & DLQ logic
        }
        return nil
    })

    select {}
}
```

---

## Configuration & deployment

TinyMQ requires no configuration files by default; it uses environment variables and Docker volumes.

### Using the pre-built Docker image (GHCR)

```bash
docker pull ghcr.io/x-name15/tinymq:latest

docker run -d \
  --name tinymq \
  -p 7800:7800 \
  -v $(pwd)/data:/root/data \
  ghcr.io/x-name15/tinymq:latest
```

### Environment variables

- `PORT`: HTTP listening port (default `7800`).

### Persistent data (Docker Compose)

TinyMQ writes WAL `.log` files into `./data`. In Docker Compose, mount this path to a persistent volume.

```yaml
# docker-compose.yml example
services:
  tinymq:
    image: ghcr.io/x-name15/tinymq:latest
    environment:
      - PORT=7800
    ports:
      - "7800:7800"
    volumes:
      - ./data:/root/data
    restart: unless-stopped
```