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

### Create/List Consumer Groups Explicitly

**Endpoint:** `POST /api/groups` | `GET /api/groups?topic={topic}`

Consumer Groups can be created implicitly via `?group=` on `/consume/{topic}` (see above), or explicitly pre-registered/inspected through this endpoint — useful for provisioning groups ahead of time or auditing which groups exist on a topic.

```bash
# Register a consumer group binding for a topic
curl -X POST http://127.0.0.1:7800/api/groups \
  -H "Content-Type: application/json" \
  -d '{"topic": "orders.eu", "group": "emails"}'
```

**Response:** `201 Created`
```json
{
  "status": "created",
  "virtual_topic": "orders.eu::emails"
}
```

```bash
# List all groups currently bound to a topic
curl "http://127.0.0.1:7800/api/groups?topic=orders.eu"
```

**Response:** `200 OK`
```json
{
  "topic": "orders.eu",
  "groups": ["emails", "invoices"]
}
```

> **Note:** In cluster mode, `POST /api/groups` is proxied to the leader (group creation triggers replication), while `GET /api/groups` resolves locally on whichever node receives the request.

### Cluster Status

**Endpoint:** `GET /api/cluster/status`

Returns this node's cluster role, current Raft-style term, recognized leader address, and the health of its known peers. Unlike most write endpoints, this one is **not** proxied to the leader — it intentionally reports the querying node's own local view, which is what you need when diagnosing a split-brain or a stuck election.

```bash
curl http://127.0.0.1:7800/api/cluster/status
```

**Response (clustering enabled):**
```json
{
  "clustering_enabled": true,
  "role": "leader",
  "term": 3,
  "leader_http": "127.0.0.1:7800",
  "peers": [
    { "address": "10.0.1.5:7946", "alive": true, "last_seen": "2026-07-01T10:00:00Z" },
    { "address": "10.0.1.6:7946", "alive": false, "last_seen": "2026-07-01T09:58:12Z" }
  ]
}
```

**Response (standalone mode):**
```json
{
  "clustering_enabled": false
}
```

> `role` can be `"leader"`, `"follower"`, or `"candidate"` (mid-election). This is the same role classification now used internally by `/healthz`'s `cluster_role` field.

### Drain a Node

**Endpoint:** `POST /api/drain`

Marks this node as draining: it immediately stops accepting new requests (returning `503` on every route) while letting in-flight requests finish naturally. Intended for controlled maintenance restarts — call this before terminating a node so existing clients get a clean `503` (and can retry against another node) instead of a hard connection drop.

```bash
curl -X POST http://127.0.0.1:7800/api/drain
```

**Response:** `200 OK`
```json
{
  "status": "draining",
  "in_flight_requests": 3
}
```

> Draining is permanent for the lifetime of the process — there is no "undrain" endpoint. Restart the node to resume accepting traffic. This is intentional: drain is meant as the last step before a planned shutdown, not a toggle.

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

Returns the broker status. In standalone mode, always `200 OK` when the process is running and accepting requests. In cluster mode, returns `503 Service Unavailable` (with `"status": "electing"`) if this node is not the leader and has not yet recognized one — this lets Kubernetes readiness probes correctly stop routing traffic to a node stuck mid-election instead of treating it as ready.

```bash
curl http://127.0.0.1:7800/healthz
```

**Response (standalone mode):**
```json
{
  "status": "ok",
  "version": "3.1.0",
  "uptime_seconds": 3600
}
```

**Response (cluster mode, healthy leader/follower):**
```json
{
  "status": "ok",
  "version": "3.1.0",
  "uptime_seconds": 3600,
  "cluster_role": "leader",
  "cluster_term": 3
}
```

**Response (cluster mode, mid-election — `503`):**
```json
{
  "status": "electing",
  "version": "3.1.0",
  "uptime_seconds": 12,
  "cluster_role": "follower",
  "cluster_term": 4
}
```

> `cluster_role` can be `"leader"`, `"follower"`, or `"candidate"` — the same classification used by `role` in `/api/cluster/status`.

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
## NATS Gateway (Protocol Compatibility)

TinyMQ includes a native **NATS text-protocol** gateway, enabling any existing NATS client library to connect directly without a custom SDK or OpenAPI generator. This means Python (`nats-py`), Go (`nats.go`), Node.js (`nats.ws`), Rust (`async-nats`), and any other NATS-compatible client can publish and subscribe to TinyMQ topics out of the box.

Enable it by setting `TINYMQ_NATS_PORT=4222` (the standard NATS port) in your environment.

### How it works

The NATS gateway connects to the same broker core as the HTTP, WebSocket, and MQTT transports. Messages flow freely between transports:

- A Python client publishes via NATS → a Go microservice consumes via HTTP.
- An HTTP `POST /publish/alerts` → a browser WebSocket receives the push → an IoT sensor subscribed via MQTT also receives it → a Node.js service subscribed via NATS receives it too.

Subscriptions use TinyMQ's **Spy mode** (non-destructive fan-out), identical to the `/stream/` SSE endpoint and WebSocket subscriptions. A message published to a topic is pushed to all active NATS subscribers without being dequeued from the main queue.

### Authentication

When `TINYMQ_API_KEY` is set, clients must include the token in the `CONNECT` JSON payload. The gateway accepts it in any of the three standard NATS auth fields:

```json
// Via auth_token (most common)
{"verbose": false, "auth_token": "<your_token>"}

// Via user/pass fields
{"verbose": false, "user": "tinymq", "pass": "<your_token>"}
```

Connections that provide no token or a wrong token receive `-ERR 'Authorization Violation'` and are immediately closed.

### Subject mapping

NATS subjects use dot-separated hierarchies. They map directly to TinyMQ topics:

| NATS Subject | TinyMQ Topic | Notes |
|---|---|---|
| `orders.eu.new` | `orders.eu.new` | Direct 1:1 mapping |
| `iot.>` | `iot.*` | NATS multi-level wildcard `>` → TinyMQ `*` |
| `>` | `*` | Receive all messages |
| `sensors.*.temp` | `sensors.*.temp` | Single-level `*` passes through unchanged |

### Supported NATS commands

| Command | Direction | Description |
|---|---|---|
| `INFO {...}` | S→C | Sent immediately on connect. Contains `server_id`, `version`, `max_payload`, `auth_required`. |
| `CONNECT {...}` | C→S | JSON handshake. Validates auth token if `TINYMQ_API_KEY` is set. |
| `PUB <subject> <bytes>\r\n[payload]\r\n` | C→S | Publishes `payload` to the broker on the given subject. |
| `SUB <subject> <sid>\r\n` | C→S | Registers a non-destructive subscription. `sid` is a client-chosen identifier used in MSG frames. |
| `UNSUB <sid>\r\n` | C→S | Removes the subscription identified by `sid`. Message delivery stops immediately. |
| `PING\r\n` | C↔S | Heartbeat. Server replies with `PONG\r\n`. |
| `MSG <subject> <sid> <bytes>\r\n[payload]\r\n` | S→C | Server push frame for active subscribers. |
| `+OK\r\n` | S→C | Acknowledgement for a successful `CONNECT`. |
| `-ERR '<reason>'\r\n` | S→C | Protocol or auth error. Non-fatal for unknown verbs; fatal for auth violations. |

> **Note:** NATS `CONNECT` verbose mode is ignored — TinyMQ only sends `+OK` once, not on every command. `PONG` responses to server-initiated `PING` frames are accepted and silently discarded.

### System limits

The NATS gateway inherits TinyMQ's global limits:

- **Max payload per PUB:** `2 MB`. Payloads exceeding this are rejected with `-ERR 'max payload exceeded'` before the body is read.
- **Max active topics:** Governed by `TINYMQ_MAX_TOPICS` (default 10,000).
- **Idle connection timeout:** 60 seconds. The deadline resets on every received command, so long-lived subscribers are never kicked out while active.

---
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
* `TINYMQ_CLUSTER_REPLICATE_TIMEOUT`: Custom timeout for Quorum acknowledgement (Default: `500ms`). Increase to `2s` or more in Kubernetes and other orchestrated environments where DNS resolution adds latency at pod startup.
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
tmq create <topic> [--policy=reject|drop-oldest] [--retention=duration] # Explicitly provisions a queue
tmq pub <topic> <data>  # Publishes a message (--ttl, --delay, --broadcast)
tmq sub <topic>         # Consumes messages (--timeout, --limit, --auto-ack)
tmq peek <topic> [--limit=N]  # Inspects messages in RAM without consuming
tmq tail <topic>        # Zero-latency live stream monitoring (SSE)

# Consumer groups
tmq group create <topic> <group> # Registers a named consumer group binding for a topic
tmq group list <topic>  # Lists consumer groups bound to a topic

# Cluster
tmq cluster status       # Shows this node's role, term, leader, and peer health
tmq cluster peers [--watch] # Same view focused on peers; --watch refreshes every 2s (e.g. for watching a failover live)
tmq cluster drain <node-url> # Marks a specific node as draining ahead of a controlled restart (calls POST /api/drain)

# Administration
tmq rm <topic>          # Completely deletes a topic and its .log file
tmq purge <topic>       # Empties a topic without deleting it
tmq webhook list <top>  # Lists registered webhooks for a topic
tmq webhook add <top> <url> # Registers a new webhook destination
tmq restore             # Restores a backup archive into ./data (--file, --data-dir)

# Utilities
tmq bench <topic>       # Runs a high-concurrency stress test
tmq bench <topic> --protocol=nats --target=127.0.0.1:40104 # Runs an ultra-fast TCP benchmark
tmq bench <topic> --format=json # Outputs benchmark results as JSON instead of text (also supports --format=csv)
tmq backup              # Compresses the ./data folder (--format=zip|tar)
```

> **Note:** `tmq cluster status`/`tmq cluster peers` only return useful data when the connected broker is running with clustering enabled (`TINYMQ_CLUSTER_SECRET` / `TINYMQ_CLUSTER_NODES` set). Against a standalone node, they report `Clustering is not enabled on this node (standalone mode).`
> **Note 2:** `tmq cluster drain` takes an explicit `<node-url>` rather than reusing `TINYMQ_URL`/`baseURL` — a drain is a destructive, per-node operation, and requiring the target URL explicitly avoids draining the wrong node by accident.

## Go SDK integration (advanced)

The native Go SDK (`client/client.go`) abstracts HTTP calls, handles advanced routing (Priorities, Consumer Groups), and provides a highly resilient WebSocket client with automatic exponential backoff.

### Installation

Install into your Go project:

```bash
go get [github.com/x-name15/tinymq/client](https://github.com/x-name15/tinymq/client)
```

### HTTP Client: Publishing & Administration

The standard HTTP client uses modern Go idioms, requiring a `context.Context` for safe cancellation and timeouts. You can now use `PublishOptions` for advanced message routing.

```go
package main

import (
    "context"
    "log"
    "time"

    "[github.com/x-name15/tinymq/client](https://github.com/x-name15/tinymq/client)"
)

func main() {
    // Initialize client with a default 60s safe timeout
    mq := client.NewClient("[http://127.0.0.1:7800](http://127.0.0.1:7800)", "optional_api_key")
    ctx := context.Background()

    // 1. Cluster Administration (Optional)
    mq.CreateTopic(ctx, "orders", "durable", 10000)
    mq.CreateGroup(ctx, "orders", "billing-service")

    // 2. Standard Publish
    payload := []byte(`{"event": "user_signup", "id": 99}`)
    if err := mq.Publish(ctx, "users.new", payload, nil); err != nil {
        log.Fatalf("publish failed: %v", err)
    }

    // 3. Advanced Publish (Priority, Idempotency, Broadcast & Headers)
    opts := &client.PublishOptions{
        Priority:    "high",
        Broadcast:   true, // Fan-out to all queues
        Idempotency: "txn_987654321",
        Headers: map[string]string{
            "X-Source": "api-gateway",
        },
    }
    
    if err := mq.Publish(ctx, "users.premium", payload, opts); err != nil {
        log.Fatalf("advanced publish failed: %v", err)
    }
}
```

### High-resilience HTTP Polling (Consumer Groups)

For standard HTTP consumers, `Subscribe` acts as a synchronous long-polling fetcher. It now natively supports **Consumer Groups** for safe, distributed load balancing among multiple workers.

```go
package main

import (
    "context"
    "fmt"
    "log"

    "[github.com/x-name15/tinymq/client](https://github.com/x-name15/tinymq/client)"
)

func main() {
    mq := client.NewClient("[http://127.0.0.1:7800](http://127.0.0.1:7800)")
    ctx := context.Background()

    // Configure Long-Polling with Consumer Groups
    opts := &client.SubscriptionOptions{
        Timeout: "30s", 
        Group:   "billing-service", // Ensures messages are load-balanced across workers
    }

    log.Println("Worker started, polling for orders...")

    // Standard consumer loop
    for {
        msgs, err := mq.Subscribe(ctx, "orders", opts)
        if err != nil {
            log.Printf("Network or broker error: %v (retrying...)", err)
            continue
        }

        for _, msg := range msgs {
            fmt.Printf("Processing order ID: %s | Payload: %s\n", msg.ID, string(msg.Payload))
            // Implement your business logic / DLQ handling here
        }
    }
}
```

### Real-Time WebSocket Client (Auto-Reconnecting)

For sub-millisecond latency without HTTP overhead, use the native WebSocket client. This is ideal for high-throughput, long-lived connections.

The `WSClient` features a **Thread-Safe Single Read-Loop**, a full **WebSocket upgrade handshake** (`Sec-WebSocket-Key` generation + `101` validation), and an **Automated Reconnection Pipeline**. If the server drops or the network partitions, the SDK automatically re-dials, re-authenticates, and re-subscribes to all active topics using an exponential backoff strategy (1s up to 32s). Close frames from the server trigger immediate reconnection; Pong frames are handled transparently as part of the keepalive.

```go
package main

import (
    "fmt"
    "log"

    "github.com/x-name15/tinymq/client"
    "github.com/x-name15/tinymq/internal/message"
)

func main() {
    // NewWSClient dials, performs the WS upgrade handshake, and boots the
    // auto-reconnect & keepalive loops. The API key is optional and is
    // passed as a query param (?token=...) during the handshake, matching
    // dashboard auth; it's also stored for re-authentication on reconnect.
    ws, err := client.NewWSClient("127.0.0.1:7800", "optional_api_key")
    if err != nil {
        log.Fatalf("Initial connection failed: %v", err)
    }
    defer ws.Close() // Safely tears down resources and routines

    // 1. Subscribe asynchronously (Thread-Safe)
    err = ws.Subscribe("iot.sensors.*", func(msg message.Message) {
        // Enforced backpressure: Handlers run synchronously per connection by default.
        // For massive concurrency, dispatch to your own worker pool here.
        fmt.Printf("Instant Push -> Topic: %s | Payload: %s\n", msg.Topic, string(msg.Payload))
    })
    if err != nil {
        log.Fatalf("Subscription failed: %v", err)
    }

    // 2. Publish over the same socket (fire-and-forget, no ack)
    if err := ws.Publish("iot.sensors.control", []byte(`{"cmd":"calibrate"}`)); err != nil {
        log.Fatalf("Publish failed: %v", err)
    }

    // 3. Dynamic Unsubscribe (Optional)
    // ws.Unsubscribe("iot.sensors.*")

    select {} // Block forever while WS handles traffic in the background
}
```

> **Note:** `WSClient.Publish` is fire-and-forget over the WS frame — it does not return a broker ack the way the HTTP client's `Publish` does. If you need delivery confirmation, use the HTTP client for that call instead.

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
- `TINYMQ_TRUST_PROXY_HEADERS`: Set to `true` to make the rate limiter trust the `X-Real-IP` header for per-IP identification. Only enable this when TinyMQ runs behind a trusted reverse proxy that sets this header itself — otherwise clients can spoof it to bypass rate limiting entirely. Defaults to `false`, which uses the real TCP connection address.
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
- `TINYMQ_CLUSTER_SELF`: The address this node advertises to peers as its own cluster identity (e.g., `192.168.1.10:7901`). Required when the TCP bind address (`TINYMQ_CLUSTER_ADDR`) is not reachable by other nodes—most commonly `0.0.0.0:7901` in Docker or Kubernetes. In Kubernetes, set this to `$(POD_NAME).tinymq-headless:7901` using the Downward API. When unset, the bind address is used as-is, which is correct for local deployments where all nodes share the same network.
- `TINYMQ_CLUSTER_ALLOW_INSECURE`: Only for local testing. Set to `true` to bypass the fail-closed check and allow the cluster node to start without HMAC authentication even if `TINYMQ_CLUSTER_SECRET` is empty. **Never enable this in production.** Default: `false`.

### NATS gateway settings

- `TINYMQ_NATS_PORT`: TCP port for the NATS-compatible gateway. Set to `4222` (the standard NATS port) to enable. Leave empty to disable (default). When enabled, any NATS client library can connect without a custom SDK.
---

### Deploying to Kubernetes (Highly Available Cluster)

TinyMQ ships with a production-ready Kubernetes manifest (`k8s/tinymq-cluster.yaml`) that deploys a 3-node highly available cluster. Because TinyMQ uses a Write-Ahead Log (`.log`) and requires stable network identities for peer discovery, the manifest uses a `StatefulSet` combined with a `Headless Service`.

#### Prerequisites

- A running Kubernetes cluster (tested with [kind](https://kind.sigs.k8s.io/) locally and standard managed clusters)
- `kubectl` configured and pointing at your target cluster
- The TinyMQ image available to your cluster (see image note below)

#### Step 1 — Create the cluster secret

The HMAC secret used to authenticate intra-cluster TCP messages must be injected as a Kubernetes Secret. Never hardcode it in the manifest:

```bash
kubectl create secret generic tinymq-secrets \
  --from-literal=cluster-secret=<your-strong-secret>
```

#### Step 2 — Apply the manifest

```bash
kubectl apply -f k8s/tinymq-cluster.yaml
```

#### Step 3 — Wait for leader election

Pods start sequentially (`tinymq-0` → `tinymq-1` → `tinymq-2`). Watch until all three are `1/1 Running`:

```bash
kubectl get pods -l app=tinymq -w
```

To observe the election process in real-time:

```bash
kubectl logs -l app=tinymq -f --prefix --max-log-requests 3
```

A healthy startup looks like:

```
[Cluster] Node tinymq-1.tinymq-headless:7901 is now ONLINE
[Cluster] Vote GRANTED to candidate tinymq-0.tinymq-headless:7901 for Term 1
[Cluster] Yipiie! We received 2 votes. We are the new LEADER for Term 1!
[Cluster] Stepping down to Follower. Recognized Leader: tinymq-0.tinymq-headless:7901
```

#### Step 4 — Access the cluster

Port-forward to any node (followers automatically proxy writes to the leader):

```bash
kubectl port-forward pod/tinymq-0 7800:7800
```

```bash
# Publish a message
curl -X POST http://localhost:7800/publish/orders \
  -H "Content-Type: application/json" \
  -d '{"payload": "hello from k8s"}'

# Consume from a different node to verify replication
kubectl port-forward pod/tinymq-1 7801:7800
curl "http://localhost:7801/consume/orders?peek=true"
```

#### Manifest reference (`k8s/tinymq-cluster.yaml`)

```yaml
---
apiVersion: v1
kind: Service
metadata:
  name: tinymq-headless
  labels:
    app: tinymq
spec:
  clusterIP: None
  publishNotReadyAddresses: true  # allows peer discovery during pod startup
  ports:
    - port: 7800
      name: http
    - port: 1883
      name: mqtt
    - port: 7901
      name: cluster
  selector:
    app: tinymq
---
apiVersion: v1
kind: Service
metadata:
  name: tinymq-service
spec:
  selector:
    app: tinymq
  ports:
    - port: 7800
      targetPort: 7800
      name: http
    - port: 1883
      targetPort: 1883
      name: mqtt
    # port 7901 intentionally omitted: internal P2P traffic only
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: tinymq
spec:
  serviceName: "tinymq-headless"
  replicas: 3
  selector:
    matchLabels:
      app: tinymq
  template:
    metadata:
      labels:
        app: tinymq
    spec:
      containers:
      - name: tinymq
        image: ghcr.io/x-name15/tinymq:latest
        ports:
        - containerPort: 7800
          name: http
        - containerPort: 1883
          name: mqtt
        - containerPort: 7901
          name: cluster
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: TINYMQ_CLUSTER_ADDR
          value: "0.0.0.0:7901"
        - name: TINYMQ_CLUSTER_SELF
          value: "$(POD_NAME).tinymq-headless:7901"
        - name: TINYMQ_CLUSTER_HTTP_ADVERTISE
          value: "$(POD_NAME).tinymq-headless:7800"
        - name: TINYMQ_CLUSTER_NODES
          value: "tinymq-0.tinymq-headless:7901,tinymq-1.tinymq-headless:7901,tinymq-2.tinymq-headless:7901"
        - name: TINYMQ_CLUSTER_REPLICATE_TIMEOUT
          value: "2s"
        - name: TINYMQ_CLUSTER_SECRET
          valueFrom:
            secretKeyRef:
              name: tinymq-secrets
              key: cluster-secret
        - name: TINYMQ_MQTT_PORT
          value: "1883"
        - name: TINYMQ_RATE_LIMIT
          value: "100"
        readinessProbe:
          httpGet:
            path: /healthz
            port: 7800
          initialDelaySeconds: 5
          periodSeconds: 5
          failureThreshold: 6
        livenessProbe:
          httpGet:
            path: /healthz
            port: 7800
          initialDelaySeconds: 15
          periodSeconds: 10
        volumeMounts:
        - name: data-volume
          mountPath: /home/tinymq/data
  volumeClaimTemplates:
  - metadata:
      name: data-volume
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 1Gi
```

#### Key design decisions

- **`publishNotReadyAddresses: true`** on the Headless Service: ensures DNS records for all pods are registered immediately, even before their readiness probe passes. Without this, `tinymq-1` cannot resolve `tinymq-0.tinymq-headless` during the startup window and the first election fails.
- **`TINYMQ_CLUSTER_SELF`**: set to the pod's own FQDN via the Downward API (`$(POD_NAME).tinymq-headless:7901`). TinyMQ binds on `0.0.0.0` but must announce a reachable identity to peers — without this, all protocol messages carry `0.0.0.0:7901` as the sender, causing peer discovery rejections and failed elections.
- **`TINYMQ_CLUSTER_REPLICATE_TIMEOUT: 2s`**: the default `500ms` is too short for DNS resolution latency at pod startup in most managed clusters. Set to `2s` in the manifest; tune up for high-latency cross-zone deployments.
- **Port 7901 omitted from `tinymq-service`**: the load-balanced Service only exposes HTTP and MQTT. Cluster TCP traffic flows exclusively through the Headless Service DNS entries and must never be exposed externally.
- **`readinessProbe.failureThreshold: 6`** (~30s of tolerance): since `/healthz` now returns `503` while a node is mid-election (not just when the process is unhealthy), a low threshold would flap pod readiness during normal leader elections. `6` gives enough headroom for a typical election to resolve without marking the pod `NotReady` prematurely.

#### Testing locally with kind

[kind](https://kind.sigs.k8s.io/) (Kubernetes-in-Docker) lets you validate the full manifest on your machine before deploying to a managed cluster:

```bash
# Create a local cluster
kind create cluster --name tinymq-test

# Build and load the image (kind has no access to your local Docker daemon)
docker build -t tinymq:dev .
kind load docker-image tinymq:dev --name tinymq-test

# In the manifest, set image: tinymq:dev and imagePullPolicy: Never for local testing

# Create the secret and apply
kubectl create secret generic tinymq-secrets --from-literal=cluster-secret=local-test-secret
kubectl apply -f k8s/tinymq-cluster.yaml

# Clean up when done
kind delete cluster --name tinymq-test
```

#### Troubleshooting

| Symptom | Command | Likely cause |
|---|---|---|
| Pods stuck in `Pending` | `kubectl describe pod tinymq-0` | No PVC provisioner available |
| `CrashLoopBackOff` | `kubectl logs tinymq-0 --previous` | Bad env var or secret missing |
| Election loop (`Leader timeout!` repeating) | `kubectl logs -l app=tinymq \| grep -E "Election\|VOTE\|ONLINE"` | DNS not resolving between pods; check `publishNotReadyAddresses` |
| `SEC-ALERT: Invalid HMAC` | `kubectl logs tinymq-0 \| grep SEC-ALERT` | Secret mismatch between pods |
| TCP connectivity | `kubectl exec -it tinymq-0 -- nc -zv tinymq-1.tinymq-headless 7901` | NetworkPolicy blocking port 7901 |


### Persistent data (Docker Compose)

TinyMQ writes WAL `.log` files into `/home/tinymq/data` inside the container. Mount a local directory to persist data across restarts:

```yaml
services:
  tinymq:
    build: .
    image: tinymq:latest
    container_name: tinymq
    env_file:
      - .env
    ports:
      - "${PORT:-7800}:7800"
      - "${TINYMQ_MQTT_PORT:-1883}:${TINYMQ_MQTT_PORT:-1883}"
      # Uncomment if running as a cluster node:
      # - "${TINYMQ_CLUSTER_ADDR_PORT:-7901}:7901"
      - "${TINYMQ_NATS_PORT:-4222}:${TINYMQ_NATS_PORT:-4222}"
    volumes:
      - ./data:/home/tinymq/data
    restart: unless-stopped
    user: "10001:10001"
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:7800/healthz"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 5s
```

> **Permissions note:** TinyMQ runs as an unprivileged user (`UID 10001`). Before starting, ensure the mounted directory is writable:
> ```bash
> mkdir -p ./data && sudo chown -R 10001:10001 ./data
> ```

> **Build security note:** `.dockerignore` excludes `.env` from the build context, so a local `.env` with secrets (`TINYMQ_API_KEY`, `TINYMQ_CLUSTER_SECRET`) never ends up in any Docker image layer, including the intermediate `builder` stage. Secrets should always be passed at runtime via `--env-file .env` or Kubernetes Secrets, never baked into the image.