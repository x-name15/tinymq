# TinyMQ - Official Documentation

Welcome to the TinyMQ documentation. This file covers the HTTP API, internal architecture, SDK usage, and deployment.

## Internal architecture & guarantees

TinyMQ is designed around simplified distributed-systems principles to be lightweight, resilient, and fast.

### Commit log cycle (PUT/ACK strategy)

TinyMQ uses an append-only `.log` file per topic (Write-Ahead Log):

- Publishing appends a `PUT` event.
- Acknowledging appends an `ACK` event.

On startup the broker replays logs to rebuild the in-memory state of unacknowledged messages. An auto-compaction routine runs on boot to purge confirmed records and free disk space.

### Lock-free routing & wildcards

The broker minimizes global `Mutex` contention: the global lock is used only to locate a topic; dispatching and disk I/O occur under per-topic locks. Wildcards (e.g., `events.*`) are supported via cached compiled regular expressions.

### Graceful shutdown & memory safety

On shutdown (Ctrl+C or `docker stop`) TinyMQ runs `CloseAll()` to flush buffers and close files cleanly. The code also performs explicit nil assignments when discarding messages to avoid GC retention.

## HTTP API reference (language-agnostic)

You can interact with TinyMQ via `curl`, Go, Python, Node.js, Rust, etc. Payloads may be JSON, plain text, binary, or other formats.

### Publish a message

**Endpoint:** `POST /publish/{topic}`

```bash
curl -X POST http://127.0.0.1:7800/publish/orders.eu \
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

### Consume a message (long polling)

**Endpoint:** `GET /consume/{topic}?timeout={duration}&auto_ack={bool}`

```bash
curl -X GET "http://127.0.0.1:7800/consume/orders.*?auto_ack=true&timeout=10s"
```

Query parameters:

- `timeout` (e.g., `5s`, `500ms`): how long to hold the connection when the queue is empty.
- `auto_ack` (`true`/`false`): if `true`, the message is marked processed and removed immediately.

**Response (200):** when a message is available

```json
{
  "id": "e4b3a1d2-7c89-4b1a-9f5e-123456789abc",
  "topic": "orders.eu",
  "payload": "eyJ1c2VyX2lkIjogNDIsICJpdGVtIjogImxhcHRvcCJ9",
  "timestamp": "2026-06-18T10:00:00Z"
}
```

*(Binary payloads may be base64-encoded depending on your client.)*

**Response (404):** when timeout expires and no message arrived

```json
{
  "status": "empty",
  "message": "No messages in topic"
}
```

### Manual acknowledgment (ACK)

**Endpoint:** `POST /ack/{topic}/{message_id}`

If `auto_ack=false` you must call this endpoint after processing to remove the message from RAM:

```bash
curl -X POST http://127.0.0.1:7800/ack/orders.eu/e4b3a1d2-7c89-4b1a-9f5e-123456789abc
```

**Response:** `200 OK`

```json
{
  "status": "success",
  "message": "Message acknowledged"
}
```

### Dashboard

Visit `http://127.0.0.1:7800/dashboard` to monitor topics, pending messages, and memory usage.

## Go SDK integration (advanced)

The native SDK (`client/client.go`) abstracts HTTP calls, handles ACKs, and offers resilient worker patterns.

### Installation

Install into your Go project (replace `yourusername`):

```bash
go get github.com/yourusername/tinymq/client
```

### Connecting & publishing

```go
package main

import "github.com/yourusername/tinymq/client"

func main() {
    mq := client.NewClient("http://127.0.0.1:7800")
    payload := []byte(`{"event": "user_signup", "id": 99}`)
    if err := mq.Publish("users.new", payload); err != nil {
        panic("broker unreachable")
    }
}
```

### High-resilience subscription (workers)

`Subscribe` uses long polling and exponential backoff (1s up to 32s). If a handler returns an error, the SDK re-queues the message, ACKs the original ID, and backs off the worker.

```go
package main

import (
    "fmt"
    "errors"
    "github.com/yourusername/tinymq/client"
    "github.com/yourusername/tinymq/internal/message"
)

func main() {
    mq := client.NewClient("http://127.0.0.1:7800")

    go mq.Subscribe("orders", client.SubscriptionOptions{Timeout: "10s"}, func(msg message.Message) error {
        fmt.Printf("Processing order: %s\n", string(msg.Payload))
        if string(msg.Payload) == "bad_data" {
            return errors.New("database connection lost")
        }
        return nil
    })

    select {}
}
```

## Configuration & deployment

TinyMQ requires no configuration files by default; it uses environment variables and Docker volumes.

### Using the pre-built Docker image (GHCR)

```bash
docker pull ghcr.io/x-name15/tinymq:latest

docker run -d \
  --name tinymq \
  -p 7800:7800 \
  -v $(pwd)/data:/root/data \
  ghcr.io/yourusername/tinymq:latest
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

<!-- EOF -->

