## [1.0.0] - 2026-06-18 — Official initial release of TinyMQ

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