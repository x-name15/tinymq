# TinyMQ - Official Documentation

Welcome to the TinyMQ documentation. This file covers the HTTP API, internal architecture, SDK usage, and deployment.

## Internal architecture & guarantees

TinyMQ is designed around simplified distributed-systems principles to be lightweight, resilient, and exceptionally fast.

### Commit log cycle (PUT/ACK strategy)

TinyMQ uses an append-only `.log` file per topic (Write-Ahead Log):

- Publishing appends a `PUT` event.
- Acknowledging appends an `ACK` event.

On startup, the broker replays logs to rebuild the in-memory state of unacknowledged messages. An **Auto-Compaction** background routine (Garbage Collector) runs periodically to purge confirmed records and prevent infinite disk growth. *Lazy Initialization* ensures that `.log` files are only created when the first message is published.

### Lock-free routing & wildcards

The broker minimizes global `Mutex` contention: the global lock is used only to locate a topic; dispatching and disk I/O occur under per-topic locks. Wildcards (e.g., `events.*`) are supported via cached compiled regular expressions.

### Dead Letter Queues (DLQ) & Resiliency

To prevent "poison pill" messages from permanently blocking a work queue, TinyMQ natively supports Dead Letter Queues. If a consumer using the SDK fails to process a message 3 times, the broker automatically isolates it into a `{topic_name}.dlq` queue to keep the main pipeline flowing.

### Strict Disk Durability (FSync)
For bank-grade reliability, TinyMQ can be configured to force the physical host disk to sync and flush buffers after every single `PUT` or `ACK` operation, protecting data even against sudden power loss.

### Time-Based Routing (TTL & Delays)

TinyMQ handles time efficiently using *Lazy Expiration*. Expired messages (TTL) are silently dropped and acknowledged on the fly when a consumer attempts to read them. Delayed messages are kept in memory but hidden from consumers until their scheduled delivery time is reached, preventing thread blocking.

### Graceful shutdown & memory safety

On shutdown (Ctrl+C or `docker stop`) TinyMQ runs `CloseAll()` to flush buffers and close files cleanly. The code also performs explicit nil assignments when discarding messages to avoid GC retention.

### Consumer Groups (Virtual Topic Binding)
TinyMQ supports Pub/Sub patterns through lightweight Consumer Groups. When a consumer requests a topic with a specific group name (e.g., `?group=emails`), the broker creates a "Virtual Topic" (`topic::emails`) bound to the original. Messages published to the main topic are instantly cloned to all bound virtual topics. This allows multiple independent microservices to consume the same event stream without stealing messages from each other, each maintaining its own independent `.log` file and Dead Letter Queue (DLQ).

---

## HTTP API reference (language-agnostic)

You can interact with TinyMQ via `curl`, Go, Python, Node.js, Rust, etc. Payloads may be JSON, plain text, binary, or other formats.

### Publish a message

**Endpoint:** `POST /publish/{topic}`

**Headers (Optional):**
- `Authorization`: Required if `TINYMQ_API_KEY` is set. Format: `Bearer <your_token>`.
- `Idempotency-Key`: A unique string. If a network retry occurs within 5 minutes with the same key, the broker will safely ignore the duplicate and return `200 OK` (status: `ignored`) without duplicating the payload.

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
- `group` (e.g., `emails`, `invoices`): Enables **Consumer Groups** (Pub/Sub). Binds a virtual sub-queue so multiple independent services can read the same message stream without competing for the same payload.

```bash
# Worker 1 (Email Service)
curl -X GET "http://127.0.0.1:7800/consume/orders.eu?group=emails&timeout=10s"
# Worker 2 (Invoice Service)
curl -X GET "http://127.0.0.1:7800/consume/orders.eu?group=invoices&timeout=10s"
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
### Live Streaming (Server-Sent Events)

**Endpoint:** `GET /stream/{topic}`

Opens a persistent HTTP/1.1 chunked connection. Messages published to the topic will be streamed to the client in real-time. This is a **"Spy Mode"** (non-destructive) and does not dequeue the message from actual workers.

```bash
curl -N "[http://127.0.0.1:7800/stream/orders.eu](http://127.0.0.1:7800/stream/orders.eu)"
```

### Observability & Metrics (Prometheus)

**Endpoint:** `GET /metrics`
Returns broker statistics (RAM messages, waiting consumers, total webhooks) formatted natively for Prometheus scraping, requiring 0 external agents.

```bash
curl "[http://127.0.0.1:7800/metrics](http://127.0.0.1:7800/metrics)"
```


### Register a Webhook (Push Consumers)

For passive integration, TinyMQ can take the initiative and push messages directly to your external services (Fire-and-Forget).
> **Security Note:** To prevent Server-Side Request Forgery (SSRF) attacks, the broker strictly validates the provided URL. It will actively reject any webhook destinations that resolve to loopback (`localhost`), private (e.g., `192.168.x.x`, `10.x.x.x`), or link-local internal network addresses.

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
- Visual indicators for Active Webhooks and Dead Letter Queues (DLQ).
- Manual topic creation UI.
- Real-time waiting consumers tracking.

---

## tmq CLI

`tmq` is a command-line tool to interact with a running TinyMQ broker from your terminal. It runs on your local machine and connects to the broker over HTTP — it does not need to run inside Docker.

### Installation

**Option A — Download a pre-built binary (recommended)**

Go to the [GitHub Releases page](https://github.com/x-name15/tinymq/releases) and download the binary for your platform:

| Platform       | File                      |
|----------------|---------------------------|
| Linux (amd64)  | `tmq-linux-amd64`         |
| macOS (Intel)  | `tmq-darwin-amd64`        |
| macOS (Apple Silicon) | `tmq-darwin-arm64` |
| Windows        | `tmq-windows-amd64.exe`   |

On Linux/macOS, make it executable after downloading:

```bash
chmod +x tmq-linux-amd64
sudo mv tmq-linux-amd64 /usr/local/bin/tmq
```

**Option B — Install with Go**

If you have Go 1.23+ installed:

```bash
go install github.com/x-name15/tinymq/cmd/tmq@latest
```

> **Note:** This requires the module path in `go.mod` to be `github.com/x-name15/tinymq`. If `go install` fails, use Option A instead.

### Configuration

By default, `tmq` connects to `http://localhost:7800`. To point it at a remote broker, set the `TINYMQ_URL` environment variable:

```bash
export TINYMQ_URL=http://your-server-ip:7800
```

### Commands

```bash
# List all active topics and their status
tmq status

# Publish a message to a topic
tmq pub <topic> <payload>
tmq pub orders.eu '{"user_id": 42}' --ttl=5m
tmq pub notifications '{"msg": "hello"}' --delay=10s --broadcast

# Consume a message from a topic
tmq sub <topic>
tmq sub orders.eu --timeout=10s --limit=5 --auto-ack=false

# Peek at messages without consuming them
tmq peek <topic>

# Watch a topic in real time
tmq tail <topic>

# Run a high-concurrency stress test against the broker
tmq bench <topic> --total=50000 --concurrency=100

# Safely compress active WAL (.log) files for easy state migrations (TAR)
tmq backup --format=tar

# Safely compress active WAL (.log) files for easy state migrations (ZIP)
tmq backup --format=zip

```

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

### System Limits & Security
To protect the host environment from Out-Of-Memory (OOM) crashes and DoS attacks, TinyMQ enforces the following hard limits natively:
- **Max Payload Size:** `2 MB` per HTTP request. Exceeding this limit will safely abort the connection and return an `HTTP 413 Request Entity Too Large` error.
- **Max Queue Capacity:** Configurable via `TINYMQ_MAX_MESSAGES` (Default: `100,000`). Controls the memory footprint per topic. When exceeded, the broker follows the `TINYMQ_DEFAULT_POLICY` (reject or drop-oldest).
- **Topic & Group Validation:** To prevent Path Traversal injections, all topic and consumer group names are strictly validated against the `^[a-zA-Z0-9._:-]+$` regex. The underlying disk engine also actively blocks any paths containing `..`, `/`, or `\`.
- **Max Active Topics:** Configurable via `TINYMQ_MAX_TOPICS` (Default: `10,000`). Prevents Denial of Service (DoS) attacks that attempt to exhaust server RAM by dynamically generating millions of unique topic names. If the limit is reached, topic creation requests are safely rejected.

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
- `TINYMQ_FSYNC`: Set to `true` to force physical disk flushes (Bank-grade durability).
- `TINYMQ_COMPACT_INTERVAL`: Background WAL garbage collector interval (default `10m`).
- `TINYMQ_DEFAULT_POLICY`: Defines memory behavior when a queue hits its limit. Set to `reject` (returns HTTP 429) or `drop-oldest` (acts as a Ring Buffer).
- `TINYMQ_MAX_MESSAGES`: Maximum number of messages held in `RAM` per topic (default `100000`).
- `TINYMQ_API_KEY`: Secures the broker. If set, all endpoints (including the Dashboard) will require an `Authorization: Bearer  HTTP header`.
- `TINYMQ_MAX_TOPICS`: Limits the maximum number of unique topics/queues allowed in memory (default `10000`) to protect against DoS attacks.

### Persistent data (Docker Compose)

TinyMQ writes WAL `.log` files into `./data`. In Docker Compose, mount this path to a persistent volume.

```yaml
services:
  tinymq:
    image: ghcr.io/x-name15/tinymq:latest
    env_file:
      - .env
    ports:
      - "${PORT:-7800}:7800"
    volumes:
      - ./data:/root/data
    restart: unless-stopped
```