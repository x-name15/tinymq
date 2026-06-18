# 🍃 TinyMQ
[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat-square&logo=go)](https://go.dev/)
[![Go Report Card](https://goreportcard.com/badge/github.com/x-name15/tinymq?style=flat-square)](https://goreportcard.com/report/github.com/x-name15/tinymq)
[![License](https://img.shields.io/badge/License-GPL%20v3-blue.svg?style=flat-square)](LICENSE)
[![Latest Release](https://img.shields.io/badge/Release-v1.0.0-green?style=flat-square)](https://github.com/x-name15/tinymq/releases)
[![Build Status](https://img.shields.io/github/actions/workflow/status/x-name15/tinymq/release.yaml?style=flat-square&logo=githubactions&logoColor=white)](https://github.com/x-name15/tinymq/actions)
[![Docker Image Size](https://img.shields.io/badge/Image_Size-~20MB-2496ED?style=flat-square&logo=docker)](https://github.com/x-name15/tinymq/pkgs/container/tinymq)
[![Docker](https://img.shields.io/badge/Docker-Engine%20%2F%20Desktop-2496ED?style=flat-square&logo=docker)](https://www.docker.com/)


A tiny, ultra-lightweight message broker for side projects, prototypes, and internal tools. 
Built from scratch in Go with zero external heavy dependencies.

TinyMQ offers a true "plug & play" alternative to heavy brokers like RabbitMQ or Kafka. No complex clusters, no Erlang, no JVM — just a single Dockerized binary with built-in disk persistence, strict FIFO delivery, wildcard routing, and a simple HTTP API.

## Key Features

- **Zero Dependencies:** Pure Go implementation.
- **Smart Disk Persistence (WAL):** Messages are persisted to an append-only `.log` file per topic and kept in RAM for fast reads.
- **Graceful Shutdown:** Syncs OS-level buffers to disk on `SIGTERM`/`SIGINT` to prevent data loss.
- **Reactive Long Polling:** Consumers sleep via Go channels when queues are empty and wake instantly on new messages.
- **Wildcard Routing:** Support for topic wildcards (e.g., `orders.*`) with regex compilation caching.
- **Resilient Go SDK:** Official client includes exponential backoff and automatic message re-queuing.

## Image of the Dashboard
![Texto alternativo](images/tinymq-dash.png)
- All in HTML btw

## Quick Start (Docker)

Run the broker using Docker Compose:

```bash
git clone https://github.com/x-name15/tinymq.git
cd tinymq
docker compose up --build
```

The broker will start on port `7800`. Data persists locally in the `./data` directory.

## Documentation

See the full documentation in the [`docs` folder](./docs/DOCUMENTATION.md) for API reference, SDK usage, and architecture details.

<!-- EOF -->
