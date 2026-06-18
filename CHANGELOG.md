# Changelog

All notable changes of the proyect will be documented on this file.

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