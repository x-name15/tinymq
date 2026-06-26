# TinyMQ - Official Documentation

Welcome to the TinyMQ documentation. This file covers the HTTP API, internal architecture, SDK usage, and deployment.

## Internal architecture & guarantees

TinyMQ is designed around simplified distributed-systems principles to be lightweight, resilient, and exceptionally fast.

### Commit log cycle (PUT/ACK strategy)

TinyMQ uses an append-only `.log` file per topic (Write-Ahead Log):

- Publishing appends a `PUT` event.
- Acknowledging appends an `ACK` event.

Each WAL record now includes a CRC32 checksum. On recovery and compaction, TinyMQ verifies the checksum and skips corrupted records instead of replaying them silently.

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

> **⚠️ Important Note on Laziness (Non-Retroactive):** Consumer Groups in TinyMQ are created lazily. The virtual sub-queue is born exactly when the first consumer requests it via `?group=name`. Messages published to the main topic **before** the group was first requested will **not** be retroactively copied into the new group. If your architecture requires a group to catch absolutely all messages from the beginning of time, ensure you initialize the group (by making a dummy `/consume` request or doing it via the Dashboard) before turning on your publishers.

---

## HTTP API reference (language-agnostic)

You can interact with TinyMQ via `curl`, Go, Python, Node.js, Rust, etc. Payloads may be JSON, plain text, binary, or other formats.

### Publish a message

**Endpoint:** `POST /publish/{topic}`

**Query Parameters (Optional):**
- `ttl` (e.g., `30s`, `1h`): Time-To-Live. The message will be destroyed if not consumed within this window.
- `delay` (e.g., `5m`, `10s`): Delays the delivery. The message will be hidden from consumers until this time passes.
- `broadcast` (`true`): Ephemeral Fan-out. Dispatches the message to all currently waiting consumers simultaneously without persisting it to disk.
- `priority` (`high` | `normal` | `low`): Message priority. Default is `normal`. Within a topic, `high` messages are always consumed before `normal`, and `normal` before `low`. Fully retrocompatible — existing consumers require no changes.
- `idempotency` (`auto`): Enables automatic deduplication by hashing the payload with SHA256. If the same payload is published again within 5 minutes, the broker silently ignores the duplicate and returns `{"status": "ignored", "reason": "idempotency_key_exists"}`. Useful when the client retries on network errors without managing keys manually.

**Headers (Optional):**
- `Authorization`: Required if `TINYMQ_API_KEY` is set. Format: `Bearer <your_token>`.
- `Idempotency-Key`: A unique string. If a network retry occurs within 5 minutes with the same key, the broker safely ignores the duplicate.
- `X-MQ-*`: Custom user-defined metadata headers. Any header starting with `X-MQ-` (e.g., `X-MQ-Correlation-Id`, `X-MQ-Source`) is stored with the message and returned on consume. Use these to pass routing metadata without modifying your payload schema.

> Note: The `{topic}` parameter can include forward slashes (/) to create hierarchical topics. The broker will preserve the full path (e.g., /publish/orders/eu will publish to topic orders/eu).

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

### Example with priority and custom headers:
```bash
curl -X POST "http://127.0.0.1:7800/publish/emails?priority=high" \
  -H "X-MQ-Correlation-Id: req-abc-123" \
  -H "X-MQ-Source: checkout-service" \
  -d '{"type": "password_reset", "user_id": 42}'
```

**Alternative Endpoint (JSON Body):**
If your HTTP client or framework prefers passing arguments via a strict JSON body rather than URL path parameters and query strings (this is the method used internally by the TinyMQ UI Dashboard), you can use the alternate API:

**Endpoint:** `POST /api/queues/publish`
```bash
curl -X POST "[http://127.0.0.1:7800/api/queues/publish](http://127.0.0.1:7800/api/queues/publish)" \
  -H "Content-Type: application/json" \
  -d '{
    "queue": "orders.eu", 
    "payload": "{\"user_id\": 42, \"item\": \"laptop\"}", 
    "delay": "5s",
    "broadcast": false
  }'
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
{
  "id": "e4b3a1d2-7c89-4b1a-9f5e-123456789abc",
  "topic": "orders.eu",
  "payload": "eyJ1c2VyX2lkIjogNDIsICJpdGVtIjogImxhcHRvcCJ9",
  "payload_encoding": "base64",
  "payload_text": "{\"user_id\": 42, \"item\": \"laptop\"}",
  "headers": {
    "X-MQ-Correlation-Id": "req-abc-123",
    "X-MQ-Source": "checkout-service"
  },
  "timestamp": "2026-06-18T10:00:00Z"
}
```

- `payload` is always Base64-encoded for binary safety.
- `payload_encoding` is always `"base64"`.
- `payload_text` is present only when the payload is valid UTF-8. It contains the decoded string directly — no Base64 decoding needed for simple integrations.
- `headers` contains any `X-MQ-*` headers that were set at publish time.

```json
{
  "status": "empty",
  "message": "No messages in topic"
}
```

### Peek without consuming (`?peek=true`)

**Endpoint:** `GET /consume/{topic}?peek=true&limit={N}`

Inspects up to `N` messages in the queue without removing them. This is the REST-native alternative to the Dashboard's `/api/queues/peek` endpoint.

```bash
# Inspect the next 5 messages in orders.eu without consuming them
curl "http://127.0.0.1:7800/consume/orders.eu?peek=true&limit=5"
```

- Returns `200` with the message array if messages are present.
- Returns `204 No Content` if the topic exists but the queue is empty.
- Returns `404 Not Found` if the topic has never been created.

### Real-Time Full-Duplex (WebSockets)

**Endpoint:** `GET /ws`

TinyMQ natively implements the RFC 6455 WebSocket protocol to offer a single, bidirectional TCP connection for sub-millisecond publishing and subscribing. 

**Authentication:** If `TINYMQ_API_KEY` is enabled, you must authenticate the connection. Browsers can pass the token via URL parameter: `ws://127.0.0.1:7800/ws?token=<your_token>`. Programmatic clients can use standard HTTP `Authorization: Bearer <token>` headers during the initial handshake.

Once connected, communication uses a simple JSON Command structure (`TMP-WS`). 

**Commands:**
* **Subscribe:** Listen to a topic or wildcard (`*`).
    * *Send:* `{"action": "subscribe", "topic": "events.*"}`
    * *Receive:* `{"status": "subscribed", "topic": "events.*"}`
* **Publish:** Dispatch a message into the broker.
    * *Send:* `{"action": "publish", "topic": "sensor.data", "payload": "temperature-high"}`
    * *Receive:* `{"status": "published", "topic": "sensor.data"}`
* **Ping (Heartbeat):** Keep the connection alive.
    * *Send:* `{"action": "ping"}`
    * *Receive:* `{"status": "pong"}`

When messages arrive on a subscribed topic, the broker pushes them instantly. Note: The `payload` field is natively encoded in **Base64** to remain binary-safe across ecosystems. You must decode it (e.g., using `atob()` in JS) upon receipt.

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

For passive integration, TinyMQ can push messages directly to your external services (Fire-and-Forget).

> **Security Note:** To prevent SSRF attacks, the broker rejects webhook destinations that resolve to loopback (`localhost`), private (e.g., `192.168.x.x`, `10.x.x.x`), or link-local internal addresses.

**Endpoint:** `POST /webhook/{topic}`

```bash
# Basic webhook (no signature)
curl -X POST http://127.0.0.1:7800/webhook/orders.eu \
  -H "Content-Type: application/json" \
  -d '{"url": "https://api.my-service.com/incoming"}'

# Webhook with HMAC-SHA256 signing secret
curl -X POST http://127.0.0.1:7800/webhook/orders.eu \
  -H "Content-Type: application/json" \
  -d '{"url": "https://api.my-service.com/incoming", "secret": "my-webhook-secret"}'
```

When a `secret` is set, TinyMQ adds an `X-TinyMQ-Signature: sha256=<hmac>` header to every delivery, calculated over the raw message payload. The receiver can verify the signature the same way GitHub, Stripe, and others do — ensuring the POST originated from TinyMQ and was not tampered with.

### Manual acknowledgment (ACK)

**Endpoint:** `POST /ack/{topic}/{message_id}`

If `auto_ack=false`, you must call this endpoint after processing to remove the message from RAM and disk:

```bash
curl -X POST http://127.0.0.1:7800/ack/orders.eu/e4b3a1d2-7c89-4b1a-9f5e-123456789abc
```

### Create Topic Manually

**Endpoint:** `POST /api/topics`

Pre-initialize a topic safely (validates name format, max length, and idempotency).

```bash
# Basic topic with default policy
curl -X POST http://127.0.0.1:7800/api/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "analytics.events"}'

# Topic with sliding retention window — all messages auto-expire after 2h
# unless they carry an explicit ?ttl= at publish time
curl -X POST http://127.0.0.1:7800/api/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "sensor.temperature", "retain": "2h", "policy": "drop-oldest"}'
```

**Body fields:**
- `name` (required): Topic name. Validated against `^[a-zA-Z0-9._:\-/]+$`, max 255 characters.
- `policy` (`reject` | `drop-oldest`): Overflow behavior. Defaults to `TINYMQ_DEFAULT_POLICY`.
- `retain` (e.g. `2h`, `30m`): Automatic TTL applied to every incoming message on this topic. Messages published with an explicit `?ttl=` override this value.

### Inspect Messages (Peek)
**Endpoint:** `GET /api/queues/peek?queue={topic}&limit={count}`
Safely inspects up to `limit` messages in RAM without consuming or deleting them.

### Purge Queue
**Endpoint:** `DELETE /api/queues/purge?queue={topic}`
Empties a queue of all messages but keeps the queue and its metadata active.

### Delete Queue
**Endpoint:** `DELETE /api/queues/delete?queue={topic}`
Completely destroys the queue, its consumers, and permanently deletes its underlying `.log` file.

### Health Check

**Endpoint:** `GET /healthz`

Returns the broker status. Always `200 OK` when the process is running and accepting requests. Designed for Kubernetes liveness/readiness probes, Docker Compose healthchecks, and load balancers.

```bash
curl http://127.0.0.1:7800/healthz
```

**Response (standalone mode):**
```json
{
  "status": "ok",
  "version": "2.8.0",
  "uptime_seconds": 3600
}
```

**Response (cluster mode):**
```json
{
  "status": "ok",
  "version": "2.8.0",
  "uptime_seconds": 3600,
  "cluster_role": "leader",
  "cluster_term": 3
}
```

**Docker Compose healthcheck:**
```yaml
healthcheck:
  test: ["CMD", "wget", "-qO-", "http://localhost:7800/healthz"]
  interval: 10s
  timeout: 5s
  retries: 3
  start_period: 5s
```

### Dashboard

Visit `http://127.0.0.1:7800/dashboard` to access the interactive web interface. Features include:
- Auto-Refresh mode.
- Uptime and memory footprint monitoring.
- Visual indicators for Active Webhooks and Dead Letter Queues (DLQ).
- Manual topic creation UI.
- Real-time waiting consumers tracking.

---
## MQTT Gateway (IoT)

TinyMQ features a native MQTT v3.1.1 gateway on TCP port `1883` (configurable via `TINYMQ_MQTT_PORT`). This allows embedded microcontrollers, Arduinos, and IoT sensors to stream data directly into the broker with absolute minimum overhead.

### Authentication
When `TINYMQ_API_KEY` is active, IoT clients must present the token inside the **Password** field of the MQTT connection frame. Connections with missing or incorrect tokens are immediately rejected with error code `0x05` (Not Authorized).

### Topic Mapping
MQTT topic layers are fully compatible with TinyMQ's core wildcard architecture. The multi-level MQTT wildcard `#` is automatically translated to TinyMQ's internal global wildcard `*`.

---

## tmq CLI

`tmq` is a command-line tool to interact with a running TinyMQ broker from your terminal. It runs on your local machine and connects to the broker over HTTP — it does not need to run inside Docker.

### Installation

**Option A — Download a pre-built binary (recommended)**

Go to the [GitHub Releases page](https://github.com/x-name15/tinymq/releases) and download the binary for your platform:
> **Keep in MInd:** tmq CLI is bundled with the Broker Server on releases page.

| Platform              | File                            |
|-----------------------|---------------------------------|
| Linux (amd64)         | `tinymq-linux-amd64.tar.gz`    |
| macOS (Intel)         | `tinymq-darwin-amd64.tar.gz`   |
| macOS (Apple Silicon) | `tinymq-darwin-arm64.tar.gz`   |
| Windows               | `tinymq-windows-amd64.zip`     |

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
# General info
tmq status              # Shows active queues, RAM, and consumers
tmq top                 # Opens a live, auto-refreshing dashboard in your terminal
tmq shell               # Opens an interactive REPL session (tinymq> prompt)

# Queue operations
tmq pub <topic> <data>  # Publishes a message (--ttl, --delay, --broadcast)
tmq sub <topic>         # Consumes messages (--timeout, --limit, --auto-ack)
tmq peek <topic> [--limit=N]  # Inspects messages in RAM without consuming
tmq tail <topic>        # Zero-latency live stream monitoring (SSE)

# Administration
tmq rm <topic>          # Completely deletes a topic and its .log file
tmq purge <topic>       # Empties a topic without deleting it
tmq webhook list <top>  # Lists registered webhooks for a topic
tmq webhook add <top> <url> # Registers a new webhook destination
tmq restore             # Restores a backup archive into ./data (--file, --data-dir)

# Utilities
tmq bench <topic>       # Runs a high-concurrency stress test
tmq backup              # Compresses the ./data folder (--format=zip|tar)
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
    "context"
    "fmt"
    "errors"
    "github.com/x-name15/tinymq/client"
    "github.com/x-name15/tinymq/internal/message"
)

func main() {
    mq := client.NewClient("http://127.0.0.1:7800")

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    go mq.Subscribe(ctx, "orders", client.SubscriptionOptions{Timeout: "10s"}, func(msg message.Message) error {
        fmt.Printf("Processing order: %s\n", string(msg.Payload))
        if string(msg.Payload) == "bad_data" {
            return errors.New("database connection lost") // Will trigger backoff & DLQ logic
        }
        return nil
    })

    select {}
}
```

### Real-Time WebSocket Client

For sub-millisecond latency without HTTP overhead, you can use the native WebSocket client. This is ideal for high-throughput, long-lived connections.

`WSClient` handles the RFC 6455 handshake, frame masking, and base64 payload decoding natively.

```go
package main

import (
    "fmt"
    "[github.com/x-name15/tinymq/client](https://github.com/x-name15/tinymq/client)"
    "[github.com/x-name15/tinymq/internal/message](https://github.com/x-name15/tinymq/internal/message)"
)

func main() {
    // URL uses http/https, the client automatically upgrades it to ws/wss
    ws := client.NewWSClient("[http://127.0.0.1:7800](http://127.0.0.1:7800)", "optional_api_key")
    
    if err := ws.Connect(); err != nil {
        panic(err)
    }

    // Subscribe asynchronously
    go ws.Subscribe("iot.sensors.*", func(msg message.Message) {
        fmt.Printf("Instant WS Push -> ID: %s | Payload: %s\n", msg.ID, string(msg.Payload))
    })

    // Publish instantly without HTTP connection overhead
    ws.Publish("iot.sensors.temperature", []byte(`{"celsius": 24.5}`))

    select {} // Block forever
}
```
## Appendix: High Availability & Ephemeral Clustering

TinyMQ includes a custom-built, ultra-lightweight, zero-dependency P2P clustering engine designed for high availability and strict data consistency without external consensus tools (like Raft or ZooKeeper).

### Architectural Design

The clustering system works on two fundamental pillars:
1. **Transparent Leader Proxying:** Followers run in a read-only state for data-modifying mutations. Any HTTP mutation (`/publish/`, `/consume/`, `/ack/`, etc.) hitting a Follower node is automatically intercepted by a high-performance Reverse Proxy and forwarded to the active Leader.
2. **Quorum-Based Ephemeral Replication:** When the Leader accepts a publish action, it broadcasts the message to all known peers via short-lived TCP sockets using a specialized `REPLICATE` protocol. The operation is only acknowledged to the client (`202 Accepted`) if a strict majority (Quorum) of cluster nodes acknowledge the storage write.
````
                  [ Client HTTP Request ]
                            │
                            ▼
                  ┌───────────────────┐
                  │  Follower Node    │
                  │  (REST Server)    │
                  └─────────┬─────────┘
                            │ (Transparent Proxy)
                            ▼
                  ┌───────────────────┐
                  │    Leader Node    │
                  │  (REST Server)    │
                  └─────────┬─────────┘
                            │
           ┌────────────────┴────────────────┐
           ▼ (TCP REPLICATE)                 ▼ (Local Storage)
┌───────────────────┐               ┌───────────────────┐
│   Follower Node   │               │   Leader WAL      │
│   (TCP Socket)    │               │   (Disk Write)    │
└───────────────────┘               └───────────────────┘
````
### Cluster Environment Variables

To activate clustering, configure the following keys in your `.env` file or environment:

* `TINYMQ_CLUSTER_ADDR`: The TCP binding address for intra-cluster communication (e.g., `127.0.0.1:7901`).
* `TINYMQ_CLUSTER_NODES`: Comma-separated addresses of other cluster participants (e.g., `127.0.0.1:7902,127.0.0.1:7903`).
* `TINYMQ_CLUSTER_SECRET`: **[SECURITY CRITICAL]** The HMAC-SHA256 secret key. **Warning:** If left empty, the cluster TCP port accepts connections from any peer without authentication, exposing your broker to arbitrary data injection!
* `TINYMQ_CLUSTER_HTTP_ADVERTISE`: **[Routing]** The HTTP address advertised to followers for Reverse Proxy redirection (e.g., `192.168.1.10:7800`). Crucial for Docker NAT environments.
* `TINYMQ_CLUSTER_REPLICATE_TIMEOUT`: Custom timeout for Quorum acknowledgement (Default: `500ms`).
* `TINYMQ_CLUSTER_LEADER`: Set to `true` to declare a static, designated Leader node and disable automated election timeouts.

### Operational Verification

To monitor cluster consensus health in real-time, inspect the application logging streams. Active peer discovery, reverse proxy redirection, and atomic synchronization states will output under the `[Cluster]` and `[Proxy]` log scopes:

```bash
[Cluster] Node 127.0.0.1:7902 is now ONLINE
[Proxy] Forwarding POST request to Leader (127.0.0.1:7801)
[Cluster] Message replicated to 2 nodes (Quorum OK)
```

### System Limits & Security
To protect the host environment from Out-Of-Memory (OOM) crashes and DoS attacks, TinyMQ enforces the following hard limits natively:
- **Max Payload Size:** `2 MB` per HTTP request. Exceeding this limit will safely abort the connection and return an `HTTP 413 Request Entity Too Large` error.
- **Max Queue Capacity:** Configurable via `TINYMQ_MAX_MESSAGES` (Default: `100,000`). Controls the memory footprint per topic. When exceeded, the broker follows the `TINYMQ_DEFAULT_POLICY` (reject or drop-oldest).
- **"Topic & Group Validation:** To prevent Path Traversal injections, all topic and consumer group names are strictly validated against the `^[a-zA-Z0-9._:\-/]+$` regex. The forward slash (`/`) is allowed to enable hierarchical topic structures (`e.g., orders/eu, sensors/temperature`)."
- **Max Active Topics:** Configurable via `TINYMQ_MAX_TOPICS` (Default: `10,000`). Prevents Denial of Service (DoS) attacks that attempt to exhaust server RAM by dynamically generating millions of unique topic names. If the limit is reached, topic creation requests are safely rejected.

> **Note on 503 Service Unavailable:** If the cluster is experiencing a split-brain, leader election, or the Leader node is unreachable, write-operations (`POST`, `DELETE`) on follower nodes will safely reject the request with a `503` status code to prevent data divergence.

---

## Configuration & deployment

TinyMQ requires no configuration files by default; it uses environment variables and Docker volumes.

### Using the pre-built Docker image
> **Architecture note:** The pre-built images published to GHCR and Docker Hub are `linux/amd64` only. ARM hosts (Raspberry Pi, Apple Silicon running Linux VMs, AWS Graviton, etc.) must build from source.

### Using the pre-built Docker image (GHCR)

```bash
docker pull ghcr.io/x-name15/tinymq:latest

docker run -d \
  --name tinymq \
  -p 7800:7800 \
  -p 1883:1883 \
  -p 7901:7901 \
  --env-file .env \
  -v $(pwd)/data:/home/tinymq/data \
  ghcr.io/x-name15/tinymq:latest
```

#### From Docker Hub

```bash
docker pull flez71/tinymq:latest

docker run -d \
  --name tinymq \
  -p 7800:7800 \
  -p 1883:1883 \
  -p 7901:7901 \
  --env-file .env \
  -v $(pwd)/data:/home/tinymq/data \
  flez71/tinymq:latest
```
> If you are running a **cluster node**, add `-p 7901:7901` (or whichever port you set in `TINYMQ_CLUSTER_ADDR`) to expose the intra-cluster TCP channel.
### Environment variables

- `PORT`: HTTP listening port (default `7800`).
- `TINYMQ_FSYNC`: Set to `true` to force physical disk flushes (Bank-grade durability).
- `TINYMQ_COMPACT_INTERVAL`: Background WAL garbage collector interval (default `10m`).
- `TINYMQ_DEFAULT_POLICY`: Defines memory behavior when a queue hits its limit. Set to `reject` (returns HTTP 429) or `drop-oldest` (acts as a Ring Buffer).
- `TINYMQ_MAX_MESSAGES`: Maximum number of messages held in RAM per topic (default `100000`).
- `TINYMQ_API_KEY`: Secures the broker. If set, all endpoints (including the Dashboard) will require an `Authorization: Bearer <token>` HTTP header.
- `TINYMQ_RATE_LIMIT`: Per-IP request rate limit for authenticated REST routes, in requests per second. Set to a positive value to enable throttling.
- `TINYMQ_TLS_CERT`: Path to the TLS certificate file for the REST server. If unset, TinyMQ stays on plain HTTP.
- `TINYMQ_TLS_KEY`: Path to the TLS private key file for the REST server. TLS is enabled only when this and `TINYMQ_TLS_CERT` are both set.
- `TINYMQ_MAX_TOPICS`: Limits the maximum number of unique topics/queues allowed in memory (default `10000`) to protect against DoS attacks.

### MQTT settings

- `TINYMQ_MQTT_PORT`: TCP port for the MQTT gateway (default `1883`).
- `TINYMQ_MQTT_DISABLE`: Set to `true` on secondary cluster nodes to shut down the MQTT server and free network file descriptors.

### Clustering settings

>  **Security:** Without `TINYMQ_CLUSTER_SECRET`, the intra-cluster TCP channel accepts connections from **any peer without authentication**. Always set this variable when the cluster port is reachable from outside a trusted private network.

- `TINYMQ_CLUSTER_ADDR`: The TCP address where this node listens for cluster connections (e.g., `127.0.0.1:7901`).
- `TINYMQ_CLUSTER_NODES`: Comma-separated addresses of other cluster participants (e.g., `127.0.0.1:7902,127.0.0.1:7903`).
- `TINYMQ_CLUSTER_LEADER`: Set to `true` to declare a static Leader node and disable automatic election timeouts.
- `TINYMQ_CLUSTER_SECRET`: Shared secret used to sign and verify all intra-cluster TCP messages via HMAC-SHA256. If unset, communication is unauthenticated and peers are accepted without verification.
- `TINYMQ_CLUSTER_HTTP_ADVERTISE`: The HTTP address this node advertises to followers for reverse proxying (e.g., `192.168.1.10:7800`). Required when the node's bind address is not reachable by peers directly (Docker bridge networks, NAT, etc.).
- `TINYMQ_CLUSTER_REPLICATE_TIMEOUT`: Timeout for each peer acknowledgment during quorum replication (default `500ms`). Accepts Go duration strings (e.g., `1s`, `200ms`).

### Persistent data (Docker Compose)

TinyMQ writes WAL `.log` files into `/home/tinymq/data` inside the container. Mount a local directory to persist data across restarts:

```yaml
services:
  tinymq:
    image: ghcr.io/x-name15/tinymq:latest   # use pre-built image; replace with build: . to build from source
    container_name: tinymq
    env_file:
      - .env
    ports:
      - "${PORT:-7800}:7800"
      - "${TINYMQ_MQTT_PORT:-1883}:${TINYMQ_MQTT_PORT:-1883}"
      # Uncomment the line below if running as a cluster node:
      # - "${TINYMQ_CLUSTER_ADDR_PORT:-7901}:7901"
    volumes:
      - ./data:/home/tinymq/data
    restart: unless-stopped
    user: "10001:10001"
```

> **Permissions note:** TinyMQ runs as an unprivileged user (`UID 10001`). Before starting, ensure the mounted directory is writable:
> ```bash
> mkdir -p ./data && sudo chown -R 10001:10001 ./data
> ```