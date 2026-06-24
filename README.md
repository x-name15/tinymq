# 🍃 TinyMQ
[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat-square&logo=go)](https://go.dev/)
[![Go Report Card](https://goreportcard.com/badge/github.com/x-name15/tinymq?style=flat-square)](https://goreportcard.com/report/github.com/x-name15/tinymq)
[![License](https://img.shields.io/badge/License-GPL%20v3-blue.svg?style=flat-square)](LICENSE)
[![Latest Release](https://img.shields.io/badge/Release-v2.8.2-green?style=flat-square)](https://github.com/x-name15/tinymq/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/x-name15/tinymq/release.yaml?style=flat-square&logo=githubactions&logoColor=white)](https://github.com/x-name15/tinymq/actions)
[![Docker Image Size](https://img.shields.io/badge/Image_Size-~15MB-2496ED?style=flat-square&logo=docker)](https://github.com/x-name15/tinymq/pkgs/container/tinymq)
[![Docker](https://img.shields.io/badge/Docker-Engine%20%2F%20Desktop-2496ED?style=flat-square&logo=docker)](https://www.docker.com/)

A tiny, ultra-lightweight message broker for side projects, prototypes, and internal tools.  
Built from scratch in Go with zero external heavy dependencies.

**TinyMQ** offers a true "plug & play" alternative to heavy brokers like RabbitMQ or Kafka. No complex clusters, no Erlang, no JVM — just a single Dockerized binary with built-in disk persistence, strict FIFO delivery, wildcard routing, a simple HTTP API, and now **native multi-node clustering**.

## Key Features

| Feature | Description |
|---------|-------------|
| **Zero Dependencies** | Pure Go implementation under ~15MB. Extremely low RAM and CPU footprint. |
| **Consumer Groups (Pub/Sub)** | Multiple microservices can read the same event stream independently via Virtual Topic Binding (`?group=name`) without competing for payloads. |
| **Native WebSockets** | Full-duplex TCP connections (`/ws`) for sub-millisecond, bi-directional publishing and subscribing. |
| **Native MQTT (IoT)** | Native v3.1.1 gateway on port `1883`. Seamlessly routes binary IoT data to HTTP/WS clients and back. |
| **Native Clustering (HA)** | Built-in zero-dependency P2P clustering with automatic leader election, quorum-based replication, and transparent follower proxying. |
| **Smart Disk Persistence & GC** | Append-only Write-Ahead Log (`.log`) architecture with a background Auto-Compactor (Garbage Collector) to prevent infinite disk growth. |
| **Strict Durability (FSync)** | Configurable bank-grade physical disk flushing after every operation to protect against sudden power loss. |
| **Live Streaming (SSE)** | Real-time, non-destructive topic monitoring (`GET /stream`) utilizing native Server-Sent Events. |
| **Dead Letter Queues (DLQ)** | Automatically isolates "poison pill" messages after 3 failed retries to keep your main pipelines flowing. |
| **Time-Based Routing** | Schedule delayed messages for the future or set expiration times natively (TTL & Delays). |
| **Network Idempotency** | Caches `Idempotency-Key` headers to safely ignore duplicate publish requests caused by network retries. |
| **Batching & Prefetch** | Pull multiple messages in a single HTTP call (`?limit=X`) to drastically reduce network overhead. |
| **Push Consumers (Webhooks)** | Passive integration. Let the broker `POST` directly to your external endpoints with built-in SSRF protection. |
| **Native Observability** | Built-in `/metrics` endpoint formatted natively for Prometheus scraping, requiring zero external agents. |
| **Interactive UI & CLI** | Built-in web Dashboard to monitor queues visually, and a native terminal CLI (`tmq`) for management, offline backups, and high-concurrency stress testing (`bench`). |
| **Graceful Shutdown** | Safely flushes OS-level buffers to disk on `SIGTERM`/`SIGINT` to prevent data loss. |
| **Plug & Play Configuration** | Auto-loads `.env` files dynamically. No complex XML/YAML configuration files required. |

## Why use TinyMQ?

Let's be honest: Kafka and RabbitMQ are incredible engineering marvels, but they are often **massive overkill** for small to medium projects.  
Setting up an Erlang runtime, managing JVMs, or configuring Zookeeper/Kraft clusters just to pass a few JSON messages between three microservices is exhausting.

TinyMQ solves the "over-engineering" problem. It is built for developers who need reliable, enterprise-grade asynchronous communication without the operational tax.

### ✅ You should use TinyMQ if:

- **Building a side project or MVP** — move fast without operational overhead
- **Limited server resources** — runs smoothly on a $4/month VPS
- **Want true Plug & Play** — no XML/YAML config files, no heavy runtimes, just run it
- **Need lightweight High Availability** — multi-node clustering with automatic leader election, no external consensus tools required

### ❌ You should NOT use TinyMQ if:

- You need to process millions of messages per second (consider Kafka)
- You need a battle-tested, enterprise-grade distributed consensus system with years of production hardening (consider RabbitMQ or Apache Pulsar)

## Dashboard Preview

![TinyMQ Dashboard](images/tinymq-dash.png)

*All in HTML & Vanilla JS*

## Documentation

📚 Full documentation: [docs folder](./docs/DOCUMENTATION.md) for API reference, SDK usage, clustering setup, and architecture details.

📖 Official website: [TinyMQ Docs](https://tinymq.mrjacket.dev/)

---

## Quick Start (Docker)

> **Permissions Note:** TinyMQ runs as a secure, unprivileged user (`UID 10001`). If you are bind-mounting a local directory like `./data`, ensure the container has write permissions to it before starting:
> ```bash
> mkdir -p ./data && sudo chown -R 10001:10001 ./data
> ```

### 1. Create your environment file (Optional but recommended)

Create a `.env` file in your current directory to configure security and limits. You can use the provided `.env.example` as a base.

### 2. Run using pre-built Docker images

You can pull the official, highly-secured (non-root) image from two different registries:

#### From GHCR (GitHub Container Registry)

```bash
docker pull ghcr.io/x-name15/tinymq:latest

docker run -d \
  --name tinymq \
  -p 7800:7800 \
  -p 1883:1883 \
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
  --env-file .env \
  -v $(pwd)/data:/home/tinymq/data \
  flez71/tinymq:latest
```

### Or using Docker Compose

```bash
git clone https://github.com/x-name15/tinymq.git
cd tinymq
docker compose up --build -d
```

### Verification

The broker will start on port `7800` (HTTP/WS) and `1883` (MQTT). Data persists locally in the `./data` directory.

Access the dashboard at: **http://localhost:7800/dashboard**

> **Security Note:** If you set `TINYMQ_API_KEY`, the dashboard will be protected. When prompted by your browser, you can leave the username blank (or type anything) and paste your API Key into the password field.

---

## LICENSE

TinyMQ is licensed under the GPL v3. See [`LICENSE`](./LICENSE) for details.

## Contributions

Found a bug or want to improve TinyMQ? We'd love your help!

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Open a Pull Request

### Credits

**Author:** Mr Jacket / Felix Manrique