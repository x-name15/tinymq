# Changelog

All notable changes of the proyect will be documented on this file.

---

---
## [2.3.0] - 2026-06-20 — Pub/Sub, Bounded Queues & Configurable Limits

## Added
- **Consumer Groups (Virtual Topic Binding):** Implemented a lock-free Pub/Sub architecture. 
  - Clients can now consume from a topic using the `?group={name}` query parameter. 
  - The broker dynamically creates a virtual sub-queue (e.g., `topic::group`) bound to the main topic.
  - Messages published to the root topic are instantly cloned and routed to all registered group queues. 
  - This allows multiple independent microservices to read the exact same event stream at their own pace without competing for messages, each maintaining its own isolated `.log` file and Dead Letter Queue (DLQ) state.
- **Ring Buffers (Memory Eviction Policies):** Introduced bounded queue controls.
  - New environment variable `TINYMQ_DEFAULT_POLICY` (options: `reject` or `drop-oldest`).
  - When a topic hits its message limit, `drop-oldest` discards the oldest message (performing an automatic ACK in the WAL) to make room for incoming traffic without disrupting publishers.
- **Configurable System Limits:** Added `TINYMQ_MAX_MESSAGES` to allow custom RAM footprint per broker instance (Default: 100,000).

## Refactored
- **Topic Naming Validation:** Updated regex validation to support `::` characters, enabling support for virtual consumer groups while maintaining strict security against injection.
- **Internal Storage API:** The `storage.New` function now accepts durability parameters directly, centralizing the configuration of the disk write-ahead log.

---
## [2.2.0] - 2026-06-20 — Observability, CLI - Backups, Streaming  & Resilience Update

## Added
- **Native Prometheus Metrics:** Introduced a `/metrics` endpoint that outputs broker statistics (active topics, RAM messages, webhooks, and waiting consumers) in plain text formatted specifically for Prometheus scraping. Achieved with 0 external dependencies.
- **Network Idempotency:** Added support for the `Idempotency-Key` HTTP header in the `/publish` endpoint. The broker now caches keys for 5 minutes, safely ignoring duplicate requests caused by network retries (returning HTTP 200 OK with an `ignored` status) without duplicating payloads.
- **CLI Expansion (Bench & Backup):** Added `tmq bench` to run high-concurrency stress tests directly against the broker respecting the `TINYMQ_URL` binding. Added `tmq backup` with `--format=zip|tar` flags to safely compress active WAL (`.log`) files for easy state migrations.
- **Background Garbage Collector (Auto-Compaction):** Introduced a silent background routine that automatically compacts Write-Ahead Log (`.log`) files to prevent infinite disk growth on long-running servers. Configurable via `TINYMQ_COMPACT_INTERVAL` (default: 10m).
- **Strict Disk Durability (FSync):** Added the `TINYMQ_FSYNC` environment variable. When set to `true`, the broker forces the host's physical disk to flush buffers and sync after every single message operation, providing bank-grade durability at the cost of raw throughput.
- **Server-Sent Events (SSE):** Introduced the `GET /stream/{topic}` endpoint. Clients can now open a persistent HTTP connection to receive a real-time stream of messages as they are published to a topic, utilizing standard HTTP/1.1 chunked transfer encoding.
- **Broker Spy Mode:** The internal engine now supports non-destructive listeners. Connecting to the SSE stream allows administrators to monitor live traffic without "stealing" payloads from actual consumer workers, as messages remain safely in the queue for processing.
- **Native `.env` Loader:** Added a zero-dependency helper (`internal/helper/env.go`) that automatically parses local `.env` files if present, streamlining local development and Docker Compose environments.

## Fixed
- **Docker Compose Volume Path:** Corrected the volume mapping in `docker-compose.yml` from `/app/data` to `/root/data` to properly match the `scratch` base image `WORKDIR`.

---
## [2.1.0] - 2026-06-19 — Expand Dashboard Functionality
## Added

- **Dashboard:** Upgraded the embedded HTML interface to support advanced broker controls without adding any external dependencies. The dashboard now features:
  - **Live Search:** Filter queues in real-time.
  - **Native Dark Mode:** With `localStorage` preference saving.
  - **Toast Notifications & Clipboard Integration:** Replaced native browser alerts with custom toasts and added 1-click "Copy Payload" buttons for easier debugging.
- **Advanced Publish Control:** The UI now exposes the full power of the Go SDK. Users can publish messages with `TTL`, `Delay`, and `Broadcast` flags directly from the browser.
- **Destructive Queue Management:** Added `Purge` (empty queue) and `Delete` (destroy queue and log file) capabilities directly to the UI.
- **Webhook Inspection:** Users can now click the Webhook badge in the dashboard to view the exact URLs registered to a specific topic.
- **New API Endpoints:** Introduced `DELETE /api/queues/purge`, `DELETE /api/queues/delete`, and `GET /api/queues/webhooks` to support the new dashboard features.

## Changed

- **Storage Engine Extensions:** Upgraded `internal/storage/storage.go` with `ClearLog` and `DeleteLog` methods to natively support truncating or removing `.log` files without blocking the global mutex.

## Fixed

- **Critical Persistence Bug (Empty Queues):** Fixed an issue in `LoadExistingTopics` where fully consumed queues (0 messages in RAM, but existing `.log` file) were ignored during broker startup. Empty queues are now properly loaded into RAM, ensuring they remain visible in the dashboard and are correctly auto-compacted after a container restart.

---
## [2.0.0] - 2026-06-18 — The "Featherweight Fortress" Update
## Added
...
- **Interactive Self-Service Dashboard:** The embedded UI now acts as a complete control center. Added interactive modals allowing users to directly **Publish** JSON payloads, **Consume (Pop)** messages to view them, and **Peek** into queues natively from the browser without writing any code.
- **Queue Inspection API (Peek):** Introduced the `GET /api/queues/peek` endpoint and internal `broker.Peek()` method. This allows administrators to safely inspect up to X messages currently held in RAM without acknowledging or removing them from the queue.
- **UI API Suite:** Added dedicated JSON endpoints (`/api/queues/publish` and `/api/queues/consume`) specifically designed to serve frontend interactions and external non-standard clients cleanly.
- **Independent CLI Binary (`cmd/tmq`):** Built a standalone administrative terminal tool (`tmq`). It brings rich terminal diagnostics featuring multi-environment binding (`TINYMQ_URL`), raw RAM inspections (`tmq peek`), real-time queue streaming (`tmq tail`), and tabwriter-aligned telemetry matrices (`tmq list/status`).

## Changed

- **Docker Image:** Migrated to a pure `scratch` base image for the final stage. The production image now weighs a mere **~13 MB**, stripping out all OS-level bloatware and achieving 0 system vulnerabilities (CVEs).
- **Go SDK (`client/client.go`):** Extensively overhauled to support a single, unified, variadic `Publish` API. Developers can now map advanced routing parameters (`TTL`, `Delay`, `Broadcast`) using fluent `PublishOptions` in a single invocation, drastically cleaning up implementation code.
- **Dashboard Visuals:** Redesigned the data tables to include a new "Actions" column with dedicated, color-coded buttons for queue management.
- **CI/CD Pipeline:** Updated the GitHub Actions workflow (`release.yaml`) to compile using Go 1.26, perfectly syncing the build matrix with the `go.mod` specification.

## Security

- **OOM Protection:** Implemented `http.MaxBytesReader` across all publish endpoints (`/publish` and `/api/queues/publish`). Payloads are now strictly capped at 2MB to prevent Out-Of-Memory denial-of-service attacks from unbounded requests.
- **RAM Backpressure:** Added a hard memory ceiling (`MaxMessagesPerTopic = 100000`). If a queue reaches this limit because workers are offline, the broker will now protect its host environment by returning `HTTP 429 Too Many Requests` instead of infinitely accumulating messages in RAM.


---
## [1.5.0] - 2026-06-18 — Enterprise Features, Resiliency, and Dashboard Revamp

## Added

- **Dead Letter Queues (DLQ):** Introduced automatic isolation for "poison pill" messages. Messages that fail to process 3 times via the SDK are now safely routed to a `{topic}.dlq` queue to prevent pipeline blocking.
- **Time-Based Routing (TTL & Delay):** Added native support for scheduled messages (`?delay=X`) and Lazy Expiration (`?ttl=X`) without introducing background polling threads.
- **Consumer Batching / Prefetch:** Implemented multi-message extraction (`?limit=X`). Consumers can now fetch arrays of messages in a single HTTP request, drastically reducing network overhead.
- **Broadcast Mode (Fan-out):** Added ephemeral pub/sub capabilities (`?broadcast=true`) to dispatch a single event to multiple independent consumers simultaneously.
- **Push Consumers (Webhooks):** Implemented native passive integration. The broker can now be configured (`/webhook/{topic}`) to automatically `POST` new messages to external URLs (Fire-and-Forget).
- **Manual Topic Creation:** Added a secure API endpoint (`/api/topics`) to pre-initialize topics safely, enforcing alphanumeric regex validation and idempotency.

## Changed

- **Interactive Dashboard:** Completely revamped the embedded UI (`/dashboard`). It now features Vanilla JS Auto-Refresh, Uptime tracking, Webhook indicators, DLQ badges, and a manual topic creation interface—all remaining under 1KB of JS/CSS.
- **Go SDK (`client/client.go`):** Upgraded the worker subscription model. It now handles exponential backoff (1s to 32s) and automatically calls the new `/requeue` endpoint when a handler returns an error to preserve the retry count.
- **Core Engine:** Refactored the internal `Publish` and `Consume` methods to support arrays, batching, and delayed message skipping without locking the global mutex.
- **Documentation:** Fully updated `README.md` and `DOCUMENTATION.md` to reflect the new enterprise-grade features while retaining the ~25MB image size and zero-dependency promises.

---
## [1.0.5] - 2026-06-18 — Embedded dashboard, Makefile and Readme Enhancements

## Added

- **Asset Embedding:** Moved `dashboard.html` to an external file and utilized `//go:embed` to include it within the binary, maintaining a single-file delivery while improving maintainability.
- **Improved README:** Added "Why use TinyMQ?" rationale and comprehensive "Configuration & depl

## Changed
- **Workflow:** Optimized `Makefile` to focus on essential tasks (fmt, build, clean).

---
## [1.0.1] - 2026-06-18 — TinyMQ is NOW completely zero-dependency

## Removed

- **External UUID package:** Completely removed the `github.com/google/uuid` dependency from the broker to strictly fulfill the project's promise of zero external dependencies.
- **`go.sum` file:** Removed from the repository, as third-party module integrity checks are no longer needed.
- **`go mod download` step:** Removed from the build phase in the `Dockerfile`, which also speeds up the image build process.

## Changed

- **Native UUID generator:** Implemented an internal Go helper using exclusively the standard library (`crypto/rand`) to securely generate unique message identifiers.
- **`go.mod` cleanup:** Cleared the file by removing all external requirement blocks, leaving the project in a pure standard-library state.

---
## [1.0.0] - 2026-06-18 — Official initial release of TinyMQ

# TinyMQ - Official Release Notes
This is the first official release of TinyMQ. It evolves the initial prototype into a robust, lightweight, and production-ready message broker.

## Added

- **Disk Persistence (WAL):** Implemented a write-ahead log using append-only `.log` files per topic to ensure zero message loss.
- **Auto-Compaction:** Automatic purge algorithm on server boot to clean up acknowledged (ACK) records and free disk space.
- **Reactive Long Polling:** Efficient message consumption by suspending goroutines via Go channels with timeout support.
- **Wildcard Routing:** Support for consuming topics using patterns (e.g., `events.*`) with a pre-compiled regex caching system.
- **Official Go SDK:** Native client at `client/client.go` with robust subscription mechanisms, exponential backoff (1s up to 32s), and re-queuing on failures.
- **Integrated Web Dashboard:** Lightweight UI at `/dashboard` for monitoring topics, pending messages, and RAM usage.
- **CI/CD Pipeline:** GitHub Actions workflow (`release.yaml`) for multi-platform binaries and Docker image publishing to GHCR.

## Architecture & performance

- **Lock-Free Routing:** Minimize global lock contention by releasing the broker mutex before disk I/O.
- **Graceful Shutdown:** Intercept `SIGTERM`/`SIGINT` to flush and close files cleanly.
- **Memory Leak Prevention:** Explicit nil assignments before reslicing and handling of `r.Context().Done()` to free blocked goroutines.
- **File Descriptor Caching:** Reuse open `*os.File` handles to reduce OS-level open/close overhead.

## Deployment

- Multi-stage Docker image based on Alpine, final image under ~20MB.
- Production orchestration via `docker-compose.yml`, mounting a persistent `./data` directory.

<!-- EOF -->