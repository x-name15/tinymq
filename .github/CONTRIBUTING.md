# Contributing to 🍃 TinyMQ

First off, thank you for considering contributing to TinyMQ! It's people like you that make open source such a great community.

TinyMQ is an open source project licensed under **GPL v3** (Note: the project uses GPL v3, not Apache 2.0). Even though it is currently maintained primarily by a single developer, contributions of any kind (bug reports, feature requests, documentation improvements, and pull requests) are warmly welcomed.

This document outlines the development setup, conventions, and tips to help you contribute efficiently.

## Contact

If you have any questions, want to discuss a new feature, or need help understanding the codebase, feel free to reach out via [GitHub Issues](https://github.com/x-name15/tinymq/issues) or Discussions.

## Development Workflow

Here is the suggested workflow for contributing code:

1. **Fork and Clone:** Fork the repository on GitHub and clone it locally to your machine.
2. **Environment Setup:** Set up your Go environment (see requirements below).
3. **Branching:** Create a topic branch from `main` for your contribution (e.g., `git checkout -b feature/nats-clustering`).
4. **Implementation:** Write your code.
   - Keep your commits logical and atomic.
   - Ensure you follow the commit message conventions below.
   - Add new tests if you are adding new features.
5. **Testing:** Run the test suite and ensure all tests pass (including the new ones you wrote).
6. **Push:** Push your topic branch to your GitHub fork.
7. **Pull Request:** Submit a Pull Request (PR) to the `main` branch of the original repository.
8. **Review:** Address any review comments. Once approved, your PR will be merged!

## Setting up the Development Environment

### Requirements

- Go 1.25+
- Docker & Docker Compose (for integration testing and cross-compilation)

### Build & Run Locally

To build the executable and run the broker locally without Docker:

```bash
# Get dependencies
go mod tidy

# Build the executable
go build -o bin/tinymq cmd/tinymq/main.go

# Run the broker (starts on port 7800 by default)
./bin/tinymq
```

### Testing

Testing is a critical part of TinyMQ's reliability guarantee. As a contributor, you are responsible for ensuring your code is tested.

- **Unit tests:** Verify individual functions (e.g., routing logic, parsers).
- **Integration tests:** Verify interactions across topics, the storage layer (`.log` files), and cluster nodes.
- **Benchmark tests:** Measure the performance of core broker features. *Please do not degrade existing benchmarks without a very good architectural reason.*

To run the full test suite locally:

```bash
# Run all standard tests
go test ./...

# Run performance benchmarks
go test -bench . -benchmem ./internal/benchmarks
```

To run end-to-end (E2E) integration tests using Docker (recommended before opening a PR):

```bash
docker compose -f docker-compose.yml up --build -d
# (Run your specific test scripts against localhost:7800)
```

## Commit Conventions

We prefer clear, descriptive commit messages. A good commit message helps reviewers understand why a change was made.

**Format:**

```text
Verb feature-name: Short description of what changed

Detailed explanation of why this change was necessary. 
Explain the problem it solves or the architectural decision made.
```

**Example:**

```text
Add native NATS gateway support

Introduced a native NATS TCP server listening on port 40104.
This allows cross-transport routing where messages published via 
HTTP or MQTT can now be seamlessly consumed by NATS subscribers.
```

- Subject line should be ≤ 70 characters.
- Use the imperative mood ("Add feature" not "Added feature").
- Leave a blank line after the subject.

## Code Review

All contributions are reviewed before merging. Even though the project is small, pull requests should include:

- A clear description of the change and the problem it solves.
- Relevant unit or integration tests.
- Code formatted with `gofmt` (`go fmt ./...`).

Happy coding, and thank you for helping make TinyMQ faster and lighter! 