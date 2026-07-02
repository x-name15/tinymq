# Changelog

All notable changes of the proyect will be documented on this file.
---
## [3.1.1] - 2026-07-02 — Cluster Readiness, Graceful Drain & Bench Tooling

### Added
- **`GET /healthz` now reports real cluster readiness.** Previously always returned `200 OK` regardless of election state. Now returns `503 Service Unavailable` with `"status": "electing"` when the node is not leader and has not yet recognized a leader (`LeaderHTTP == ""`), so Kubernetes readiness probes correctly stop routing traffic to a node mid-election instead of treating it as ready.
- **`POST /api/drain`** — new REST endpoint that marks the node as draining (rejects new requests with `503`) and returns the current in-flight request count. Backs the new `tmq cluster drain <node-url>` CLI command.
- **`tmq cluster drain <node-url>`** — new CLI subcommand to manually trigger a controlled drain on a specific node ahead of a maintenance restart.
- **`Server.trackInFlight` middleware** — tracks in-flight HTTP request count via `atomic.Int64` and short-circuits new requests once `draining` is set, wrapping the entire mux.
- **Verbose graceful shutdown logging.** On `SIGINT`/`SIGTERM`, the broker now logs the number of in-flight HTTP requests and active WebSocket connections being drained before shutting down transports, instead of a single generic "shutting down" line.
- **`tmq bench --format=json|csv`** — benchmark results (`http` and `nats` protocols) can now be emitted as JSON or CSV in addition to the default text report, enabling CI-based performance regression tracking across releases.

### Fixed
- **`.dockerignore` did not exclude `.env`.** A local `.env` containing `TINYMQ_API_KEY`/`TINYMQ_CLUSTER_SECRET` would be copied into the Docker build context and persist in the `builder` stage's layer history (though not in the final `scratch`-based image). Added `.env` to `.dockerignore`.

### Changed
- **`k8s/tinymq-cluster.yaml`**: raised `readinessProbe.failureThreshold` from `3` to `6` (~30s of tolerance) to avoid flapping pod readiness during normal leader elections, now that `/healthz` reflects election state accurately.

### Documentation
- Updated the `/healthz` reference in `DOCUMENTATION.md` with the new `503`/`"electing"` response shape and a sample payload for mid-election state.
- Added a new "Drain a Node" section documenting `POST /api/drain`, including its request/response shape and a note clarifying that draining is permanent for the process lifetime (no "undrain" endpoint).
- Added `tmq create`, `tmq group create/list`, `tmq cluster status`/`tmq cluster peers [--watch]`, `tmq cluster drain <node-url>`, and `tmq bench --format=json|csv` to the `tmq CLI` command reference, with a note on why `cluster drain` requires an explicit `<node-url>` instead of reusing `TINYMQ_URL`.
- Updated the `k8s/tinymq-cluster.yaml` manifest reference to `readinessProbe.failureThreshold: 6`, with a new "Key design decisions" bullet explaining the increased tolerance now that `/healthz` reflects real election state.
- Added a build-security note under "Persistent data (Docker Compose)" clarifying that `.dockerignore` excludes `.env` from the Docker build context, including the intermediate `builder` stage.

---
## [3.1.0] - 2026-07-01 — SDK Architecture Overhaul & Production Hardening

### Added
- **Automated WebSocket Reconnection Pipeline (`client/ws.go`)**: Integrated a highly resilient auto-reconnect layer utilizing an Exponential Backoff strategy (starting at 1s, doubling up to a 32s threshold). If connection limits drop or network partitions occur, the client safely cycles network sockets and automatically transmits re-subscription frames to recover state without manual system intervention.
- **`context.Context` Propagation Across REST Methods (`client/client.go`)**: Standardized explicit context control across `Publish`, `CreateTopic`, `CreateGroup`, `Peek`, and `ClusterStatus` endpoints. Prevents un-cancellable resource starvation loops and ensures immediate routine teardown if root parent microservices drop.
- **`WSClient.Unsubscribe()` Implementation**: Introduced programmatic support to gracefully sever active stream listeners and propagate appropriate `"unsubscribe"` events over the socket connection to notify the broker.
- **HTTP SDK Parity (`client/client.go`)**: Brought the core Go SDK up to date with server version 3.0.2 REST capabilities. 
  - `PublishOptions` now supports `Priority` routing, `Idempotency` keys, and custom `Headers` (`X-MQ-*`).
  - `SubscriptionOptions` now explicitly supports Consumer Groups routing (`Group` field) in HTTP polling.
- **Admin Wrapper Methods (`client/client.go`)**: Introduced high-level administration bindings missing from previous versions:
  - `Client.CreateTopic(topic, policy, maxQueueSize)`: Allows programmatic topic provisioning.
  - `Client.CreateGroup(topic, group)`: Supports programmatic creation of Consumer Groups.
  - `Client.Peek(topic, limit)`: Enables safe examination of queue heads without consuming data.
  - `Client.ClusterStatus()`: Exposes node roles, health, and cluster topography to client applications.

### Fixed
- **Critical WebSocket Race Condition (`client/ws.go`)**: Resolved an issue where the background keepalive ping loop could perform concurrent write operations alongside user commands (`Subscribe`/`Unsubscribe`), causing data interleaving and protocol desynchronization on the TCP stream. Added strict serialization using `sync.Mutex` on all network writes.
- **WebSocket Unbounded Resource Exhaustion (`client/ws.go`)**: Fixed an implicit DoS vulnerability where each incoming message dynamically spawned an unmonitored goroutine. Processing is now handled synchronously within the execution line, delegate-loading any performance scaling to the consumer's implementation and enforcing connection backpressure.
- **Arbitrary Memory Allocation/OOM Panic (`client/ws.go`)**: Added a strict payload constraint (`MaxFrameSize = 16MB`) inside the WebSocket reader. This prevents remote panics and Out-Of-Memory (OOM) crashes triggered by parsing corrupted or intentionally malicious frame headers containing oversized length declarations.
- **Infinite Loop and CPU Spikes**: Solved a performance regression where unmarshal syntax failures on malformed frames induced an unbounded high-frequency loop utilizing `continue` instructions without yielding or severing the broken pipe.
- **Silent Subscription Inactivity Disconnects**: Implemented standard automated `PING` frames transmitted every 30 seconds by the client, neutralizing idle socket terminations enforced by the server's 90-second timeout policy (introduced in v3.0.2).

### Changed
- **Multiplexed Architecture Redesign (`client/ws.go`)**: Migrated the SDK's internal execution to a streamlined "Single Read-Loop" pattern. Executing `WSClient.Subscribe()` no longer locks the caller's main routing loop, safely unblocking multiple concurrent topic subscriptions over a single underlying connection.
- **Client-Side Message Dispatching (`client/ws.go`)**: Rectified data-routing logic to properly inspect incoming message topics against a local synchronized handler map, ensuring consumers no longer experience overlapping data ingestion from cross-subscribed paths.
- **HTTP Client Hardening (`client/client.go`)**: The default `http.Client` instantiated by `NewClient` now establishes a strict 60-second default timeout to prevent indefinite thread freezing during network outages or long-polling freezes.

---
## [3.0.2] - 2026-07-01 — CLI Expansion: Cluster Visibility

### Added
- **`tmq cluster status`** — new CLI command showing the connected node's cluster role (leader/follower/candidate), current Raft term, recognized leader address, and a peer health table (address, alive/dead, time since last seen). Available both as a top-level command and inside `tmq shell`.
- **`GET /api/cluster/status`** — new REST endpoint backing the CLI command above. Read-only, does not proxy to the leader (intentionally answerable by any node, since diagnosing a cluster split requires querying each node's own view). Returns `{"clustering_enabled": false}` when the node isn't running with clustering configured.
- **`Node.RoleString()` and `Node.GetPeersSnapshot()`** — new exported methods on `cluster.Node` consolidating role-to-string mapping (now correctly includes `"candidate"`, previously only `"leader"`/`"follower"` were distinguished in `/healthz`) and peer state snapshotting.
- **`tmq group create/list <topic>`** — new CLI command exposing `Broker.CreateGroup` (previously only reachable implicitly via `?group=` on `/consume/`). `create` registers a named consumer group binding for a topic; `list` shows all groups currently bound to it.
- **`GET /api/groups` and `POST /api/groups`** — new REST endpoints backing the CLI command. GET resolves locally on any node (read-only); POST is proxied to the cluster leader, since group creation triggers replication via `OnGroupCreate`.
- **`Broker.GetGroups(topic string) []string`** — new exported method returning the group names bound to a topic, derived from the existing internal `bindings` map.
- **`tmq cluster peers [--watch]`** — new CLI subcommand for observing cluster peer health, term, and leader in real time. `--watch` clears and refreshes the terminal every 2s (same pattern as `tmq top`), useful for watching a leader election happen live (e.g. during a `kubectl delete pod` failover test). Shares its underlying request/render logic with `tmq cluster status` via the new `printClusterStatus` helper.
- **`tmq create <topic> [--policy=reject|drop-oldest] [--retention=duration]`** — new CLI command exposing `Broker.CreateTopic` explicitly, so queues can be provisioned with a specific eviction policy and/or message retention up front instead of relying on implicit creation (with default policy, no retention) on first publish.

### Changed
- **`/healthz` now reports `cluster_role` via `Node.RoleString()`** instead of a local `IsLeader()` check, so a node mid-election now correctly reports `"candidate"` instead of being lumped in with `"follower"`.

### Documentation
- Added `tmq create`, `tmq group create/list`, and `tmq cluster status`/`tmq cluster peers [--watch]` to the `tmq CLI` command reference table, grouped under new "Consumer groups" and "Cluster" sections.
- Added a note clarifying that `tmq cluster status`/`tmq cluster peers` only return meaningful data against a broker running with clustering enabled, and report `Clustering is not enabled on this node (standalone mode).` otherwise.
- Documented `POST /api/groups` and `GET /api/groups` in the HTTP API reference, including the explicit-registration alternative to the existing implicit `?group=` binding on `/consume/{topic}`, and the request/response payload for both create and list.
- Documented `GET /api/cluster/status` in the HTTP API reference, including sample responses for both clustering-enabled and standalone modes, and a note that it deliberately does not proxy to the leader (unlike other cluster-aware endpoints) since it reports the querying node's own local view.
- Clarified that `role` in `/api/cluster/status` and `cluster_role` in `/healthz` now share the same three-value classification (`leader`/`follower`/`candidate`), correcting a prior gap where `/healthz` only distinguished `leader`/`follower`.

---
## [3.0.1] - 2026-07-01 — Core & Transport Reliability Fixes

### Fixed
- **Cluster HMAC now fails closed when unset.** Nodes previously started with cluster TCP authentication silently disabled if `TINYMQ_CLUSTER_SECRET` was empty, only logging a warning. A node now refuses to start under this condition unless `TINYMQ_CLUSTER_ALLOW_INSECURE=true` is explicitly set, since the open port allowed injecting `REPLICATE`/`BIND_GROUP`/`HEARTBEAT` messages from any reachable host.
- **Storage no longer serializes all topics behind one lock.** `DiskStorage` used a single global mutex for every topic's disk I/O, meaning compacting one topic's WAL blocked publish/ack for every other topic. Switched to per-topic locking so topics are now fully independent on disk.
- **WAL write order now matches in-memory enqueue order per topic.** `AppendPut` was writing to the WAL outside the topic lock, so concurrent publishes to the same topic could persist in a different order than they were queued/delivered, causing message reordering on crash recovery. The persist now happens inside the same topic-lock critical section as the delivery/enqueue decision.
- **WAL compaction now fsyncs the parent directory after rename.** Closes a durability gap where a crash immediately after compaction's atomic rename could lose the rename on some filesystems.
- **Consume/expiry no longer does disk I/O while holding the topic lock.** Expired-message ACKs during `Consume` were written to disk inside `Topic.mu`, stalling that topic's publishers/consumers under TTL-heavy load. ACKs for expired messages are now flushed after the lock is released.
- **Leader heartbeat step-down now requires a strictly greater term (or an already-recognized leader at the same term).** Previously accepted `term >= currentTerm` unconditionally, which could let two same-term leaders flip-flop recognizing each other under anomalous conditions.
- **`/api/queues/peek` and `/api/queues/webhooks` now proxy to the cluster leader like every other read/write route.** Followers were answering these two endpoints from local (possibly stale/empty) state instead of forwarding to the leader.
- **Idempotency key cap exhaustion is now logged.** `IsIdempotent` silently stopped tracking new keys once the 20k cap was hit, degrading duplicate detection with no visibility.
- **`Broker.Requeue` could silently drop a message if the waiting consumer disappeared mid-race.** Unlike every other delivery path in the broker (which uses a non-blocking `select`/`default` send and falls back to re-enqueuing), `Requeue` sent directly to the consumer's notification channel. If the consumer had already timed out and unsubscribed just as a message was being requeued, the message was written into a channel nobody would ever read again, and was lost instead of falling back to the topic queue.
- **Cluster `REPLICATE` and `BIND_GROUP` messages were accepted from any authenticated peer at `term >= CurrentTerm`, without verifying the sender was the recognized leader.** Same weak-consensus pattern already fixed for `HEARTBEAT` (which requires a strictly greater term, or the already-recognized leader at the same term), but not previously propagated to these two message types. Any cluster-authenticated node could inject replicated messages or consumer-group bindings by simply matching the current term. Both messages now carry the leader's address and are validated against `VotedFor` before being accepted.
- **MQTT transport did not require `CONNECT` before accepting `PUBLISH`/`SUBSCRIBE`.** A client could open a raw TCP connection and send `PUBLISH`/`SUBSCRIBE` packets directly, bypassing `TINYMQ_API_KEY` authentication entirely for that transport. Connections are now gated: any packet other than `CONNECT` sent before a successful `CONNECT` is rejected.
- **WebSocket transport had no read timeout.** A client that opened a WS connection and went silent (no data, no ping) held its goroutine and file descriptor open indefinitely. `readPump` now enforces a 90-second idle deadline, refreshed on every received frame.
- **WebSocket transport had no `unsubscribe` action.** Clients could only stop receiving a topic's messages by closing the entire connection, leaking a spy subscription (goroutine + channel on the broker) for the lifetime of long-lived connections that subscribed/unsubscribed repeatedly. Added `"unsubscribe"` as a supported WS command action.

### Enviroment
- `.env.example` documents `TINYMQ_CLUSTER_ALLOW_INSECURE` next to `TINYMQ_CLUSTER_SECRET`.

---
## [3.0.0] - 2026-06-30 — Dashboard Redesign, UX Improvements & i18n Support

### Added

#### Dashboard
- Complete visual redesign of the embedded web dashboard (`/dashboard`) with:
  - New topbar layout.
  - Refined metric cards.
  - Monospace data styling.
  - Softer light/dark color palette built entirely on CSS variables.
- Language switcher (🇪🇸 Spanish / 🇬🇧 English) with automatic browser-language detection on first load and `localStorage` persistence. Switching languages requires no page reload.
- New **DLQ Queues** metric card, computed client-side from the existing `/api/stats` response (no broker API changes required).
- Command palette (`⌘K`) for quickly searching queues and triggering actions (publish, consume, peek and tail) entirely from the keyboard.
- Real-time broker health monitoring via periodic `/healthz` checks, displaying **Online**, **Checking**, or **Offline** status.
- Sortable queue table (queue name, waiting consumers and messages in RAM), with sorting preferences persisted in `localStorage`.
- Custom confirmation dialogs for destructive actions (purge and delete), replacing native browser `confirm()` prompts.
- Skeleton loading states for Consume, Peek and Webhooks modals.
- Locale-aware number formatting across dashboard metrics and tables.
- JSON syntax highlighting for Peek and Consume views using pure CSS.
- Queue names now truncate with ellipsis while preserving the full name via tooltip on hover.
- Toast notifications now stack without overlapping.
- New keyboard shortcuts:
  - `⌘K` — Toggle command palette
  - `/` — Focus queue search
  - `Esc` — Close any modal or the command palette
- Improved animations, including:
  - Queue row fade-in during auto-refresh.
  - Modal entrance transitions.
  - Toast enter/exit animations.
- Refined hover and focus states across both light and dark themes.

#### Internationalization (i18n)
- Translation strings extracted into `internal/transport/rest/static/{es,en}.json`.
- Translation files are fetched lazily, cached in memory, and resolved through a unified `t(key)` helper.
- Adding a new language now only requires dropping a new `<lang>.json` file into `static/`; no Go code changes are necessary.
- Added 25+ new translation keys covering health status, command palette, confirmation dialogs, queue creation and error messages.

#### REST
- Added a new `/static/` endpoint serving embedded assets through `go:embed`, protected by the same `withAuth` middleware as `/dashboard`.

### Changed

#### Dashboard
- Auto-refresh now pauses while any modal or the command palette is open, resuming automatically when closed.
- Health badge updates dynamically without requiring a page refresh.
- Queue sorting and filtering are now performed entirely client-side using cached statistics, eliminating unnecessary API requests.
- Theme toggle redesigned from a checkbox slider to a single icon button (🌙 / ☀️).
- All UI text previously hardcoded in JavaScript now resolves through the centralized `t(key)` translation helper.

### Notes

- No changes were made to the broker core, REST API contracts, WAL format or cluster protocol. This release is entirely focused on the dashboard and user experience.
- Although the version was bumped to **3.0.0**, there are **no breaking API or protocol changes**. The major version reflects the complete dashboard rewrite and significant UX improvements.
- The project philosophy remains unchanged: **zero external dependencies**, **single binary**, and **pure Go standard library**. All static assets are embedded into the binary at build time via `go:embed`.

---
## [2.9.5] - 2026-06-29 — Cluster Consensus & NATS Transport Hardening

### Added
- **REST:** New `TINYMQ_TRUST_PROXY_HEADERS` environment variable to control whether the rate limiter trusts the `X-Real-IP` header. Defaults to `false` (uses the real TCP connection address). Documented in `.env.example` and `DOCUMENTATION.md`.

### Fixed
- **Cluster:** Fixed a data race in `Node.handlePeer()` (`REQUEST_VOTE` case) where `n.CurrentTerm` was read without synchronization after `evaluateVote()`, racing with writes protected by `n.mu.Lock()` in `startElection()` and `handleHeartbeat()`. The term is now snapshotted under `RLock` before building the `VOTE_GRANTED`/`VOTE_DENIED` response.
- **NATS:** Fixed a short-read bug in `Server.handlePub()` that used `reader.Read(payload)` instead of `io.ReadFull()`, which could truncate large or fragmented payloads and desync the NATS protocol parser on the next line read.
- **MQTT:** Fixed a goroutine leak and broker memory leak in `Server.handleSubscribe()`: re-subscribing to the same topic overwrote `spies[cleanTopic]` without calling `RemoveSpy()` on the previous channel, leaving the old dispatch goroutine and its `Topic.spies` registration orphaned indefinitely, even after client disconnect.
- **REST:** Fixed a rate-limit bypass vulnerability in `extractIP()` (`internal/transport/rest/ratelimit.go`), where the `X-Real-IP` header was trusted unconditionally. Any client could spoof a different `X-Real-IP` per request to evade per-IP rate limiting entirely and inflate the `ipRateLimiter` bucket map. The header is now only trusted when `TINYMQ_TRUST_PROXY_HEADERS=true` is explicitly set; otherwise the connection's real `RemoteAddr` is used.
- **Broker:** Fixed a DLQ-bypass bug in `Broker.Requeue()`: `msg.RetryCount` comes directly from client-supplied JSON in `POST /requeue` with no validation. A negative value (e.g. `-1000`) prevented the counter from ever reaching the dead-letter threshold, letting poison messages bounce indefinitely in the live queue instead of being routed to `.dlq`. The count is now clamped to a minimum of `0` before incrementing.
- **REST/Cluster:** Fixed a data race in `handleHealthz()` (`internal/transport/rest/server.go`), which read the exported field `Node.CurrentTerm` directly without synchronization, racing with writes under `n.mu.Lock()` in `startElection()`, `evaluateVote()`, and `handleHeartbeat()`. Added `Node.GetCurrentTerm()` as a thread-safe accessor and updated the `/healthz` handler to use it.
- **SDK (golang):** Fixed a message-loss bug in `Client.Subscribe()` (`client/client.go`): on handler failure, the SDK acknowledged the original message *before* confirming the `/requeue` call succeeded. If `/requeue` failed (network blip, broker restart), the message was permanently lost despite the "SDK Resilience" retry path being designed to prevent exactly that. The order is now reversed: ack only fires after a confirmed `202 Accepted` from `/requeue`.
- **SDK (golang):** Fixed unsafe manual JSON construction in `WSClient.Subscribe()` and `WSClient.Publish()` (`client/ws.go`). Topic names were never escaped and payload escaping only handled double quotes, allowing malformed or injected JSON when topics/payloads contained quotes, backslashes, or control characters. Both methods now use `json.Marshal` with a `wsCommand` struct.
---
## [2.9.5] - 2026-06-29 — Native K8s Support

### Fixed
- **[Cluster] Deadlock on leader election**: `calculateQuorum()` attempted to acquire the write lock while `requestVoteFromPeer` already held it, permanently blocking the newly-elected leader from being proclaimed. Quorum is now computed before the mutex is taken.
- **[Cluster] Node incorrectly added itself as a peer**: `loadPeersFromEnv()` compared the bind address (`0.0.0.0:7901`) against peer entries, which never matched DNS-based addresses (e.g. `tinymq-0.tinymq-headless:7901`), causing each node to treat itself as an external peer and skewing quorum counts. Self-filtering now uses `TINYMQ_CLUSTER_SELF` via `selfAddr()` (with `n.Address` as fallback).
- **[Cluster] Bind address leaked into inter-node protocol messages**: `pingPeer()`, `sendHeartbeat()`, `startElection()`, `requestVoteFromPeer()`, and `requestSync()` were all announcing `0.0.0.0:7901` as the sender identity instead of the node's reachable address. This caused peer discovery rejections and votes being granted to an unresolvable address. All protocol messages now use `selfAddr()`, which resolves to `TINYMQ_CLUSTER_SELF` when set.
- **[Cluster] TCP timeouts too short for orchestrated environments**: Gossip timeouts (500 ms) and vote request timeouts (1 s) were insufficient for DNS resolution latency at pod startup. Increased to 2 s and 3 s respectively. Election timeout range widened to 8–12 s to accommodate StatefulSet startup sequencing.

### Added
- **[Cluster] `selfAddr()` helper**: Internal method that returns the node's advertised address (`TINYMQ_CLUSTER_SELF`) with a fallback to the TCP bind address. Used consistently across all outbound protocol messages to decouple the bind address from the identity announced to peers. Required in Kubernetes where `0.0.0.0` is never a valid peer address; optional but harmless in local deployments.
- **[Cluster] Kubernetes support**: New `TINYMQ_CLUSTER_SELF` environment variable lets a node advertise a reachable address to peers independently of its TCP bind address. When unset, behaviour is unchanged for local deployments.
- **[K8s] Official Kubernetes manifest** (`k8s/tinymq-cluster.yaml`): StatefulSet with Headless Service, per-pod persistent volumes, and Secret-based cluster auth. `publishNotReadyAddresses: true` on the headless service ensures peer discovery works during rolling pod startup. Readiness and liveness probes added via `/healthz`.

### Documentation
- Expanded the Kubernetes deployment section in `DOCUMENTATION.md` with a full step-by-step guide, production-ready manifest reference, design decision notes, kind-based local testing instructions, and a troubleshooting table.
- Added `TINYMQ_CLUSTER_SELF` to the clustering environment variable reference.
- Clarified `TINYMQ_CLUSTER_REPLICATE_TIMEOUT` default (`500ms`) versus recommended value for orchestrated environments (`2s`).
- Updated `docker-compose.yml` with a `/healthz` healthcheck and cluster port commented out by default.

---
## [2.9.0] - 2026-06-26 — Native NATS Gateway, Cross-Transport Routing, some fixes and Repo Management

### Features & Transport
- **Native NATS Gateway:** Added a built-in NATS TCP server. TinyMQ now speaks the core NATS protocol natively (`INFO`, `CONNECT`, `PING`, `PONG`, `PUB`, `SUB`, `UNSUB`).
- **Cross-Transport Interoperability:** True multi-protocol routing. Messages published via HTTP or MQTT can now be seamlessly consumed by NATS subscribers, and vice versa.
- **Protocol Subject Translation:** Automatic on-the-fly mapping of NATS multi-level wildcards (e.g., `>` and `.>`) to TinyMQ's native wildcard syntax (`*`).

### Security & Configuration
- **NATS Authentication:** NATS clients are authenticated via the standard `CONNECT` JSON payload. TinyMQ automatically validates `auth_token`, `user`, or `pass` fields against the existing `TINYMQ_API_KEY`.
- **Opt-In Gateway:** The NATS server remains zero-overhead and is disabled by default. It can be explicitly enabled by setting the new `TINYMQ_NATS_PORT` environment variable.

### Performance
- **Zero-I/O Benchmarking:** The internal logger is now automatically muted when running performance tests (both internal `go test` benchmarks and the `tmq bench` CLI command). This prevents terminal standard output (stdout) from acting as an I/O bottleneck, revealing the true throughput limits of the broker.

### CLI (tmq)
- **Multi-Protocol Benchmarking:** The `tmq bench` command now supports evaluating both the HTTP API and the native NATS TCP gateway.
  - Added `--protocol` flag (defaults to `http`, accepts `nats`).
  - Added `--target` flag for direct TCP connection routing.
- **Example Usage:** `tmq bench events.click --protocol=nats --target=127.0.0.1:40104 --total=100000 --concurrency=100`

### Testing
- Added `BenchmarkNATSPublishSequential` and `BenchmarkNATSPublishParallel` to the internal suite to track real-time multi-core processing speeds against the TCP socket.

### Community & Repository Management
- **Contributing Guidelines:** Added `CONTRIBUTING.md` to establish clear workflows for local development, testing, and PR submissions.
- **Code of Conduct:** Added a pragmatically tailored `CODE_OF_CONDUCT.md` to protect maintainer bandwidth and ensure technical discussions remain respectful and productive.
- **Issue Templates:** Introduced structured GitHub Issue Templates for Bug Reports and Feature Requests to enforce minimum reproducible context and streamline triaging.

---
## [2.8.5] - 2026-06-25 — WAL Checksums, Rate Limit and new command to the CLI

### Security
- Added CRC32 checksums to WAL records and skip corrupted entries during recovery.
- Added per-IP rate limiting for authenticated REST routes, configurable via `TINYMQ_RATE_LIMIT`.

### Reliability
- Added checksum-aware log compaction and recovery logging for corrupted records.
- Added `tmq restore` support for `.zip` and `.tar.gz` backups generated by `tmq backup`.

### Transport
- Added optional HTTPS support for the REST server via `TINYMQ_TLS_CERT` and `TINYMQ_TLS_KEY`.
- Kept plain HTTP as the default behavior when TLS variables are not set.

### Documentation
- Updated `DOCUMENTATION.md` to describe WAL checksum verification, `tmq restore`, the new TLS environment variables, and REST rate limiting.
- Added the new runtime configuration entries to the environment variable reference.

---
## [2.8.4] - 2026-06-24 — Damn Dude, More Fixes

### Fixed

- **Memory Leak in `Ack()`:** Resolved a permanent memory leak where acknowledged messages were not always released. `Ack()` now correctly searches and removes messages from `HighMessages`, `Messages`, and `LowMessages`.
- **Leader HTTP Routing:** Fixed `GetLeaderHTTP()` to build leader URLs using `TINYMQ_CLUSTER_HTTP_ADVERTISE` instead of generating invalid IP-based addresses, restoring proxy functionality.
- **Quorum Cache Race Condition:** Eliminated a data race in `calculateQuorum()` by protecting quorum cache access with a mutex.
- **State Synchronization Security:** All `REPLICATE` commands sent during `SYNC_REQ` are now signed with HMAC-SHA256, preventing unauthorized state injection during cluster synchronization.
- **Consumer Group Replication Consistency:** `ReplicateBinding` now waits for follower acknowledgments (quorum) before confirming Consumer Group creation, preventing cluster desynchronization.
- **Priority Queue Visibility:** Fixed `Peek()` and `GetStateSnapshot()` so they correctly include high- and low-priority messages, ensuring dashboards and synchronized nodes receive a complete broker state view.
- **Topic Cleanup:** `DeleteTopic()` now properly removes orphaned Consumer Group bindings when topics are deleted.
- **REST Header Sanitization:** Added validation and sanitization for `X-MQ-*` headers to mitigate header injection and excessive memory allocation attacks.
- **Large WAL Recovery:** Increased the `bufio.Scanner` buffer in `LoadMessages()` to 4 MB, preventing large messages from being skipped during WAL recovery.
- **Cluster ACK Handling:** Added missing ACK responses for `BIND_GROUP` operations to improve replication reliability.
- **Election Timer Efficiency:** Optimized timer management inside `electionTimeoutLoop`, reducing unnecessary allocations and timer churn during leader elections.
- **Integration Test Reliability:** Reworked integration tests to use dynamic ports (`:0`) and adjusted timeouts, eliminating port collisions and significantly reducing flakiness under heavy CI load.

---
## [2.8.3] - 2026-06-24 — Benchmarks and Fixes

### Added
- **Performance Benchmarks Suite:** Introduced a comprehensive set of benchmarks under `internal/benchmarks/` to measure and track broker performance over time. Covers core operations: publish, consume, ack, priority queues, wildcard routing, broadcast, and concurrency. Includes benchmarks for MQTT gateway, REST API, WebSocket, and clustering replication.

### Fixed
- **Broker Deadlock under Concurrent Load:** `publishCore` was sending to `waitingConsumers`  channels while holding `t.mu` via `defer Unlock()`, causing all goroutines to deadlock when a consumer channel had no active receiver. Fixed by extracting the channel, releasing the mutex, then sending via non-blocking `select` with re-enqueue on consumer disappearance. Same fix applied to wildcard consumer delivery and broadcast goroutines.

---
## [2.8.2] - 2026-06-24 — Cigarettes After Fixes (bcs of the test files)

### Added
- **Multi‑architecture Docker Images:** The official Docker image now supports both `linux/amd64` and `linux/arm64` platforms, enabling native execution on Raspberry Pi, AWS Graviton, and other ARM64 environments.
- **Linux ARM64 Binary Bundle:** Added a pre‑compiled `tinymq-<version>-linux-arm64.tar.gz` asset to the GitHub Release for direct download.

### Fixed
- **REST Topic Path Handling:** Endpoints like `/publish/foo/bar` now correctly preserve the full topic path (`foo/bar`) instead of truncating at the first slash. This allows hierarchical topic names with `/` in all REST routes (`/publish/`, `/consume/`, `/ack/`, `/webhook/`, `/stream/`).
- **Priority Queue Consume:** `extractMessages` was only reading `t.Messages` (normal priority), silently ignoring `HighMessages` and `LowMessages`. Fixed to drain high → normal → low in order.
- **MQTT Deadline Reset:** The MQTT gateway now resets the connection deadline on each iteration, preventing premature closure when clients send packets with slight delays.

### Tests
- **Integration Tests:** Expanded test coverage for scenarios involving hierarchical topic names (`/`) and cross‑protocol flows (MQTT → HTTP, HTTP → WebSocket). All integration tests now pass reliably.

### CI/CD
- **ARM64 Cross‑compilation:** Added a CI step to verify that the broker compiles successfully for the `linux/arm64` platform, ensuring compatibility with ARM devices.
- **Release Pipeline:** Updated GitHub Actions workflow to build and publish Docker images for both `amd64` and `arm64`, and to include Linux ARM64 binary bundles in releases.

---
## [2.8.1] - 2026-06-24 — The "QoL for U" Update

### Added
- **Dev Container:** Added `.devcontainer/` configuration (Go 1.26-alpine3.24, golangci-lint, mosquitto-clients) for one-click development environments in VS Code and GitHub Codespaces.
- **Explicit Payload Telemetry:** Added an inline `payload_encoding: "base64"` field to consumer HTTP payloads to instantly guide developers during service integrations.
- **User-Defined Headers:** Support for `X-MQ-*` custom metadata headers in publish/consume requests.
- **Topic-Level Retention:** Added automatic message TTL configuration per topic via `?retain=...` policy.
- **Webhook HMAC Signing:** Added `X-TinyMQ-Signature` (SHA256) header for secure webhook delivery verification.
- **Healthcheck Endpoint:** Introduced `/healthz` for Kubernetes/Orchestrator monitoring.
- **Auto-Idempotency:** Native SHA256-based request deduplication triggered via `?idempotency=auto`.
- **Inline Payload Text:** Added `payload_text` field for UTF-8 payloads to avoid Base64 decoding for simple integration testing.

### Changed
- **Semantical REST Compliance:** Changed empty queue responses from an ambiguous `404 Not Found` status down to a correct, body-less `204 No Content` structure, aligning with RFC 7230 polling frameworks.
- **Consumer Group Lifecycle:** Added explicit architectural documentation clarifying the lazy, non-retroactive nature of virtual topic bindings to align expectations for engineers migrating from Kafka.
- **API Discoverability:** Officially documented the JSON-body alternative endpoints (`/api/queues/*`) previously hidden as internal dashboard routes, eliminating DX confusion regarding differing URL styles.

### Fixed
- **WAL Silent Data Corruption:** Migrated the storage layer from a flat character substitution (`@` for `/`) to an escaped token codec (`_b_` and `_a_`), eliminating name collisions and log corruption when handling topics with legitimate special characters.
- **Topic Length Boundary:** Enforced a strict maximum length ceiling of 255 characters on topic naming processing pipelines inside `publishCore`.
- **CLI Help Typo:** Cleaned duplicate output definitions for `bench` and `backup` commands in `tmq` help utilities.
- **Topic-Level Retention (completed):** `POST /api/topics` now accepts a `retain` field (Go duration string, e.g. `"2h"`, `"30m"`). Previously the `Retention` field existed in the `Topic` struct and was applied in `publishCore`, but `handleCreateTopic` and `broker.CreateTopic` did not expose it — making the feature unreachable via API. The circuit is now closed end-to-end.
- **WAL Codec Regression:** `writeRecord` was still using the legacy `@` substitution codec when opening new log files, while all read paths (`LoadMessages`, `CompactLog`, `ClearLog`, `DeleteLog`) had already been migrated to the `_b_`/`_a_` escaped token codec. New files are now created with the correct codec, consistent with the rest of the storage layer.

### Documentation
- **Fully documented 7 features shipped in v2.8.1 but missing from DOCUMENTATION.md:** `/healthz`, `?priority=high|normal|low`, `X-MQ-*` user headers, `payload_text` + `payload_encoding` in consume responses, `?peek=true` on `/consume/{topic}`, `?idempotency=auto`, and webhook HMAC signing (`secret` + `X-TinyMQ-Signature`).
- **Completed `retain` documentation** in `POST /api/topics` section now that the API implementation is complete.
- **Fixed CLI download table** — platform-to-filename mapping was swapped between Linux, macOS, and Windows entries.

---
## [2.8.0] - 2026-06-23 — Homemade Clustering Feature and More Stability and Security Fixes

### Added
- **High Availability Clustering (P2P):** Native, zero-dependency clustering engine for Leader/Follower topologies.
- **Transparent Reverse Proxy:** Follower nodes automatically proxy mutating HTTP requests (`POST`, `DELETE`, etc.) to the active Leader.
- **Quorum-Based TCP Replication:** Strict data consistency using an ephemeral TCP protocol (`REPLICATE`) that requires a majority of ACKs before confirming a publish action.
- **Dynamic Leader Election:** Raft-inspired gossip protocol with `PING` and `HEARTBEAT` timers for automatic failover.
- **Cluster Authentication:** Introduced `TINYMQ_CLUSTER_SECRET`. All intra-cluster TCP communication is now cryptographically signed and verified via HMAC-SHA256.
- **MQTT Opt-Out:** New `.env` variable `TINYMQ_MQTT_DISABLE=true` to turn off the MQTT gateway on worker nodes to save file descriptors and TCP sockets.

### Changed
- Refactored `Broker.Publish` into `publishCore` and `PublishReplicated` to safely decouple local network bindings from cluster-wide synchronization.
- Dashboard API routes strictly separated between read-only (local) and write operations (proxied to leader).

### Fixed
- Addressed a potential TCP buffer deadlock in cluster networking by implementing ephemeral, short-lived socket connections instead of persistent locking streams.
- Fixed a critical resource leak by enforcing a strict 30-second read deadline (`conn.SetDeadline`) on all active cluster TCP connections to prevent Slowloris attacks.
- Prevented potential panics caused by out-of-bounds array access when receiving malformed TCP commands in the cluster protocol.
- Fixed `cmd/tinymq/main.go` and `internal/tests/ws_test.go` instantiation errors due to missing dependency injection parameters.
- Resolved a proxy bug in `/api/queues` where a superfluous `http.Error` was written after a successful Leader proxy redirect, causing a double-write response error.
- Fixed an accessibility issue in Docker environments by explicitly mapping the Leader's advertised HTTP address for inter-node proxying via the `TINYMQ_CLUSTER_HTTP_ADVERTISE` logic.
- Corrected the Quorum mathematical calculation to strictly evaluate against the configured cluster size, preventing infinite split-votes or unreachable consensus when dynamically discovering peers.
- **Critical Persistence Ordering:** Refactored `Broker.publishCore` to persist payloads to the local disk (Write-Ahead Log) *before* triggering network replication, preventing permanent data divergence between the Leader and Followers in the event of an untimely crash.
- **DoS Protection:** Hardened peer discovery by silently rejecting unconfigured nodes, preventing memory exhaustion and Quorum inflation attacks.
- **Auditability:** Reverse proxy now correctly injects `X-Forwarded-For` and `X-Real-IP` headers, preserving the original client IP for security logging on the Leader.
- **Global Lock Starvation:** Eliminated global `RLock()` contention in `GetStateSnapshot()` by executing shallow pointer copies, preventing the broker from freezing during massive cluster synchronizations.
- **Goroutine Leak Protection:** Stabilized the background gossip routines via bounded semaphore channels, capping the concurrent network execution pool to a controlled maximum.
- **Stack Overflow Mitigation:** Implemented a maximum depth limiter (depth=10) inside `publishCore` recursion to prevent catastrophic broker crashes caused by circular Consumer Group bindings.
- **WebSocket Disconnect Contention:** Downgraded the global `Mutex.Lock()` to `Mutex.RLock()` in `RemoveSpy`, significantly reducing lock starvation during mass WebSocket client disconnections.
- **MQTT Edge-Case Crash:** Hardened the MQTT `CONNECT` packet parser with strict bounds checks, neutralizing denial-of-service crashes triggered by truncated 2-byte payloads.
- **MQTT Wildcard Isolation:** Corrected structural single-level (`+`) and multi-level (`#`) MQTT wildcard translation logic to preserve topic hierarchy separation.
- **MQTT Goroutine Leak Protection:** Implemented socket lifecycle monitors within subscriber dispatcher loops, guaranteeing immediate thread reclamation upon client abrupt disconnection.
- **Hot-Path Optimization:** Memoized the `TINYMQ_CLUSTER_NODES` environment variable parsing during Quorum calculation, eliminating expensive OS-level syscalls on every publish action.
- **Proxy Connection Pooling:** Injected a globally cached `http.Transport` into the Reverse Proxy middleware, allowing Followers to reuse persistent TCP sockets when routing HTTP requests to the Leader (massive throughput boost).

---
## [2.7.5] - 2026-06-22 — The Ecosystem Update

### Added
- **Native WS Client (SDK):** Introduced `client.WSClient`, a zero-dependency WebSocket client for Go. It handles the RFC 6455 handshake natively and decodes Base64 payloads automatically, offering developers sub-millisecond asynchronous message processing.
- **Real-Time CLI (SSE):** Upgraded the `tmq tail` command. It now utilizes Server-Sent Events (SSE) via the `/stream/` endpoint instead of HTTP long-polling, achieving zero-latency terminal monitoring with automatic reconnection logic.
- **Interactive CLI Shell:** Added `tmq shell` for an interactive REPL experience, allowing sequential command execution without re-authenticating or re-typing the broker URL.
- **Terminal Dashboard:** Added `tmq top` for a live, auto-refreshing terminal interface to monitor queue stats in real-time.
- **Queue Management Commands:** Added `tmq rm <queue>` (delete) and `tmq purge <queue>` (empty) for complete CLI-based lifecycle management.
- **Webhook Management:** Added `tmq webhook list <topic>` and `tmq webhook add <topic> <url>` to manage push-consumers directly from the terminal.

### Fixed
- **Staticcheck Compliance:** Optimized `readFrame` inside the WebSocket client to use a `switch` statement for `payloadLen`, fixing linter warnings and slightly improving execution speed.
- **Documentation:** Updated `DOCUMENTATION.md` to feature the new `WSClient` usage examples and the real-time capabilities of the CLI.
- **Scanner Safety:** Added mandatory `scanner.Err()` checks across the CLI to prevent silent failures during file I/O operations.
- **WebSocket Leak Mitigation:** Added `done` signal channel in WebSocket client to ensure background spy goroutines terminate immediately upon client disconnection.
- **CI/CD:** Fixed Docker volume paths and exposed MQTT port in manifest.
- **Storage Reliability:** Increased `bufio.Scanner` buffers to 4MB to support large payload recovery without truncation.

---
## [2.7.0] - 2026-06-22 — The Internet of Things (MQTT) Update

### Added
- **Native MQTT v3.1.1 Support:** Built a high-performance, zero-dependency MQTT gateway listening on TCP port `1883`. Seamlessly handles binary streams for `CONNECT`, `SUBSCRIBE`, `PUBLISH`, `PINGREQ`, and `DISCONNECT` packets.
- **Cross-Protocol Integration:** Messages published from MQTT are instantly routable to HTTP/WebSockets clients, and vice versa.
- **Embedded Security:** Fully protected by `TINYMQ_API_KEY`. The engine uses constant-time byte comparisons against the MQTT Password field to block illegitimate IoT devices before handshake acceptance.

### Security
- **MQTT OOM Protection:** Enforced a strict 2MB limit on incoming MQTT control packets to prevent malicious clients from exhausting server RAM via artificially inflated `RemainingLength` headers.
- **TCP Socket Thread-Safety:** Implemented a thread-safe connection wrapper (`mqttConn`) equipped with a dedicated Mutex to prevent data races and stream corruption during concurrent MQTT frame writes.
- **Protocol Downgrade/Bypass Prevention:** The broker now explicitly rejects MQTT `CONNECT` frames that set the Password flag without the Username flag, strictly enforcing MQTT 3.1.1 §3.1.2.9 and preventing potential authentication bypasses.
- **Strict Path Traversal Shield:** Hardened the `isSafePath` storage validator to reject absolute paths (`/`) and bypass attempts (`@`), ensuring logs are strictly confined to the `./data` directory.
- **Telemetry Race Condition:** Fixed a silent data race in the `/api/stats` endpoint where queue sizes were read outside of their respective mutexes.
- **Dynamic API Authentication:** The API token is now evaluated per-request rather than at startup, allowing for live credential rotation without requiring a broker restart.

### Fixed
- **Goroutine Leak in MQTT:** The MQTT `UNSUBSCRIBE` command now correctly removes the client from the broker's spy list and cleans up channels, completely eliminating memory and goroutine leaks.
- **Spec-Compliant MQTT Parser:** Rewrote the `readRemainingLength` decoder to strictly cap at 4 bytes and prevent silent integer overflows on 32-bit architectures, adhering perfectly to the OASIS specification.
- **REST API Sincerity:** Fixed the `/publish` HTTP endpoint to correctly propagate underlying broker errors (e.g., `429 Too Many Requests` on full capacity) instead of blindly returning `202 Accepted`. Additionally, `/consume` now returns standard `204 No Content` for empty queues instead of `404 Not Found`.
- **DLQ Memory Limits:** The `/requeue` endpoint and DLQ automatic routing now strictly respect the `TINYMQ_MAX_MESSAGES` RAM ceiling, protecting the host server from out-of-memory crashes during heavy retry loops.
- **MQTT QoS 2 Rejection:** The broker now explicitly aborts connections attempting QoS 2 publishes instead of silently downgrading them, avoiding infinite client retry loops.
- **Topic Delivery Accuracy:** Fixed a bug where MQTT clients subscribed via wildcards (e.g., `#`) would receive the wildcard string as the topic name instead of the actual origin topic of the message.
- **MQTT Port Toggle:** The MQTT server can now be fully disabled by leaving `TINYMQ_MQTT_PORT` empty, saving resources for HTTP-only deployments.
- **WebSocket Goroutine Leak:** Implemented a dedicated cancellation channel (c.done) to immediately terminate spy goroutines if a WebSocket client disconnects unexpectedly, preventing memory leaks.
- **Docker Configuration:** Exposed the MQTT TCP port (1883) natively in docker-compose.yml to allow external IoT devices to route traffic to the containerized broker.

### Performance & Reliability
- **Large Payload Persistence:** Increased the internal `bufio.Scanner` buffer from 64KB to 4MB for disk operations (`LoadMessages` and `CompactLog`). The broker can now safely recover and compact 2MB JSON payloads without truncating lines.
- **Lock Contention Reduction:** Optimized the `RemoveSpy` core method to use `RLock()` instead of `Lock()`, ensuring that WebSocket and MQTT client disconnects no longer block active publishers.
- **Slowloris Attack Mitigation:** Enforced a strict 30-second read deadline on the initial MQTT TCP connection handshake to prevent malicious clients from starving server resources by holding sockets open indefinitely.

---
## [2.6.0] - 2026-06-22 — The Native WebSocket & Performance Update

## Added
- **Native WebSockets (TMP-WS):** Introduced a zero-dependency WebSocket protocol implementation (`/ws`). Clients can now establish a persistent, full-duplex TCP connection to the broker for sub-millisecond latency. Supports `ping/pong` heartbeats, `publish`, and `subscribe` commands with native Base64 binary-safe JSON delivery.

## Security
- **WebSocket Authentication (CWE-306):** The `/ws` endpoint is now fully protected by the `TINYMQ_API_KEY`. Browsers can authenticate by appending the token to the URL (`ws://.../ws?token=...`).
- **DoS Protection on Spy Channels (CWE-770 & CWE-20):** Hardened the `AddSpy` method utilized by both WebSockets and SSE `/stream`. It now strictly enforces `validTopicRegex` against path traversal and blocks creation if the broker has reached the `TINYMQ_MAX_TOPICS` limit.
- **WebSocket Frame Corruption (CWE-116):** Migrated the WebSocket TCP reader from generic `Read` to `io.ReadFull` buffers, completely eliminating the risk of frame corruption under high latency or massive payload scenarios.
- **Strict JSON Marshal:** Refactored WebSocket responses to use struct-based `json.Marshal` instead of string interpolation (`fmt.Sprintf`), preventing silent JSON malformation if a queue name contained escape characters.
- **Silent Spy Drops (Observability):** The broker now explicitly logs a warning if a WebSocket or SSE client is consuming too slowly and its 50-message buffer gets full, dropping messages.
- **Strict Wildcard Validation (CWE-20):** Eliminated technical debt by introducing strict regex validation for wildcard subscriptions. The broker now natively accepts the global wildcard (`*`) or suffix wildcards (`events.*`) while explicitly rejecting malformed or embedded wildcards (e.g., `a*b` or `**`), hardening the routing engine against syntax-based logic errors.

## Performance
- **Lock-Free Telemetry (Stop-The-World Elimination):** Rewrote `GetStats()`. It no longer acquires topic-level Mutexes to read sizes. UI Auto-Refresh now fetches `/api/stats` natively using JS instead of triggering full HTML DOM reloads, drastically reducing `runtime.ReadMemStats` CPU pauses.
- **Zero-Latency WS Hub:** Eliminated blocking channels (`make(chan *Client)`) in the WebSocket connection Hub, replacing them with direct Mutex maps to remove artificial latencies during connection handshakes.
- **Global Lock Avoidance in `Consume`:** Further optimized wildcard routing. `Consume` now utilizes "Copy-under-lock" memory techniques, successfully releasing the broker's Global Mutex *before* performing disk I/O, allowing concurrent producers to operate uninterrupted even when massive wildcards are being queried.

## Quality
- **Test Suite Enhancements:** `ws_test` now dynamically allocates ports (`127.0.0.1:0`) and uses `t.Cleanup` context timeouts to fully support `t.Parallel()` without collision. Corrected logic in `TestConsumeAndAck` to validate the correct UUID.

---
## [2.5.1] - 2026-06-20 — More Fixes

## Fixed
- **Deadlock in Publish (Regression):** Fixed a critical race condition regression introduced in v2.5.0 where `Publish()` would permanently deadlock the topic mutex if no wildcard consumers were actively waiting. Removed the duplicate `t.mu.Lock()` acquisition.
- **CLI and SDK Authentication Gap:** Closed a functionality gap where the `client` SDK and the `tmq` CLI lacked support for passing the `Authorization` header. `client.NewClient` now optionally accepts an API key, and the CLI natively reads the `TINYMQ_API_KEY` environment variable to securely authenticate all commands against protected brokers.

## Security
- **Unprotected Metrics Endpoint:** Secured the `/api/stats` endpoint. It now correctly implements the `withAuth` middleware, preventing unauthorized telemetry scraping when `TINYMQ_API_KEY` is configured.
- **Residual XSS in Dashboard (CWE-79):** Fixed an incomplete XSS mitigation in the `peekQueue` modal. The "Copy" button previously used inline `onclick` string interpolation, which could be bypassed via payload manipulation. It now utilizes memory-based payload arrays and delegated event listeners, strictly preventing arbitrary code execution in the browser.
- **DNS Rebinding in Webhooks (CWE-298):** Resolved a Time-of-Check to Time-of-Use (TOCTOU) vulnerability where a webhook domain could be verified as external but instantly re-routed to an internal IP before the request was sent. The `Broker` now utilizes a custom `DialContext` to resolve and block local/private IPs milliseconds before establishing the TCP connection.

## Performance
- **Global Mutex Contention in Consume:** Radically reduced global lock blocking during wildcard topic consumption. `Consume()` now drops the global `b.mu` read lock before extracting messages and performing disk I/O, preventing high-volume wildcard listeners from paralyzing the rest of the broker.
- **Publish O(N) Wildcard Search Bottleneck:** Optimized `Publish()` routing logic. Instead of scanning every active topic to find matching wildcard listeners (an `O(N)` operation), the broker now maintains a specialized `wildcards` map (`O(1)` indexing), achieving predictable publish latency regardless of total queue count.
- **Dashboard Auto-Refresh Flicker:** Upgraded the embedded UI's Auto-Refresh system. It now utilizes a silent `DOMParser` fetch routine instead of `location.reload()`, enabling smooth, flicker-free telemetry updates without resetting user scroll or inputs.

## Quality & CI/CD
- **Synchronized Workflows:** Linked the `release.yaml` and `ci.yaml` GitHub Actions. Releases and Docker image builds will now only trigger if the CI pipeline passes successfully.
- **Unified Go Toolchain:** Resolved Go version mismatches across the repository. CI and Release workflows now dynamically read the required toolchain directly from `go.mod` via `go-version-file`.
- **Documentation Accuracy:** Updated `DOCUMENTATION.md` to reflect the new `context.Context` signature in the SDK's `Subscribe` method, and added security notes regarding volume permissions (`chown 10001:10001`) for Docker bind-mounts.
- **Binary Checksums (Integrity Verification):** Upgraded the GitHub Actions release pipeline. All release assets (Windows, Linux, macOS bundles and source archives) now automatically generate and publish a `SHA-256` checksum file to cryptographically verify binary integrity.
- **Automated Testing & Race Detection:** Addressed the root cause of regression bugs by establishing the project's first automated test suite (`broker_test.go`). The CI pipeline (`ci.yaml`) now strictly enforces `go test -race -timeout 30s ./...` on every push and PR, ensuring that deadlocks or concurrency regressions are caught before they can be merged or released.

## [2.5.0] - 2026-06-20 — Major Fixes and Stability Update

## Security
- **Global API Authentication (CWE-306):** Implemented a mandatory hybrid authentication middleware. Programmatic clients can use `Bearer <token>`, while web browsers gracefully fallback to native HTTP Basic Auth prompts for seamless, secure Dashboard access. Configured via the `TINYMQ_API_KEY` environment variable.
- **Path Traversal via Consumer Groups (CWE-22):** Fixed a critical vulnerability where an attacker could exploit the `?group=` query parameter to write `.log` files outside of the configured `./data` directory.
  - The `CreateGroup` method now strictly enforces alphanumeric regex validation.
  - The underlying `DiskStorage` engine now explicitly rejects any topic names containing `..`, `/`, or `\` as a defense-in-depth measure.
- **Server-Side Request Forgery (SSRF) in Webhooks (CWE-918):** Fixed a critical vulnerability where attackers could register internal or private IP addresses (e.g., `localhost` or AWS metadata endpoints) as webhook destinations. The webhook registration endpoint now strictly resolves hostnames and explicitly blocks loopback, private, and link-local IP ranges.
- **Unbounded Body in Requeue (CWE-770):** Applied `http.MaxBytesReader` (2MB limit) to the `/requeue` endpoint, matching the main `/publish` endpoints, preventing memory exhaustion from massive payload injections.
- **Docker Root Privilege Escalation Risk (CWE-269):** Hardened the Docker image. The container no longer runs as `root`. A dedicated unprivileged user (`UID 10001`) is now created during the builder phase and explicitly enforced in the final `scratch` image.

## Reliability & Stability
- **DoS Protection via Topic Limits (CWE-770):** Addressed a high-severity vulnerability where an attacker could exhaust broker memory by repeatedly consuming from dynamically generated, non-existent topic names.
  - Added the `TINYMQ_MAX_TOPICS` environment variable (Default: `10,000`).
  - `Publish`, `Consume`, `CreateGroup`, and `CreateTopic` now proactively enforce this global ceiling, returning safe errors/empty responses if the broker reaches maximum capacity.
- **Server Crash via Invalid Wildcard Regex (CWE-476):** Fixed a high-severity vulnerability where consuming a malformed wildcard pattern (e.g., `foo(*`) would cause a nil pointer dereference panic, crashing the entire broker. `regexp.Compile` errors are now properly handled, gracefully aborting invalid wildcard queries instead of crashing the process.
- **Idempotency Cache Unbounded Growth (CWE-770):** Added a hard limit (`20,000` keys) to the idempotency map. If under a massive flood of unique keys within the 5-minute window, the broker gracefully stops caching new keys instead of growing RAM indefinitely.
- **Concurrent Deletion Race Condition:** Fixed a critical concurrency bug where deleting a topic while simultaneously publishing to it could result in orphaned `.log` files or silent message loss. Implemented a strict `Deleted` atomic flag within the Topic mutex to safely abort pending publish routines if the topic is undergoing deletion.

## Performance
- **O(N²) CPU Bottleneck in Queue Dequeue:** Completely rewrote the `extractMessages` internal function. It previously used in-place array shifting, which caused exponential CPU degradation when reading from queues with tens of thousands of messages. It now uses a single-pass buffer allocation `O(N)`, drastically reducing CPU load and Garbage Collection pauses during high-throughput consumption.

## Fixed
- **False-Positive Acknowledgments:** Fixed a logical bug in the `/ack/{topic}/{id}` endpoint. Previously, acknowledging a non-existent or already processed message would incorrectly return a `200 OK` success response and write a spurious `ACK` record to the Write-Ahead Log. It now correctly returns a `404 Not Found` and prevents unnecessary disk I/O.
- **Environment Parser Precedence & Formatting:** Fixed `internal/helper/env.go` to properly strip quotation marks (`"`) from values. It now correctly respects OS-level environment variables (e.g., set via Docker or Kubernetes), applying `.env` values only as fallbacks.
- **URL Encoding in SDK and CLI:** Fixed multiple bugs in `client/client.go` and `cmd/tmq` where topic names or message IDs containing spaces, `#`, `%`, or `/` would corrupt HTTP requests. All path variables are now strictly sanitized using `url.PathEscape`.
- **SDK Graceful Shutdown:** The `client.Subscribe()` method now accepts a `context.Context` parameter, allowing long-polling workers to be gracefully cancelled and shut down without needing to forcefully kill the parent process.

## Quality & CI/CD
- **Strict CI Pipeline Enforcement:** Added comprehensive GitHub Actions workflows (`ci.yaml`). All pull requests and pushes to `main` now strictly require passing `go vet` static analysis, `gofmt` compliance, and successful compilation of both the server and CLI binaries.

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
