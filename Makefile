BINARY_NAME=tinymq
CMD_PATH=./cmd/tinymq

.PHONY: all build run clean fmt test help

all: fmt test build

build:
	go build -ldflags="-s -w" -o bin/$(BINARY_NAME) $(CMD_PATH)

run:
	go run $(CMD_PATH)

fmt:
	go fmt ./...

test:
	go test -race -timeout 30s ./...

clean:
	rm -rf bin/ data/
	go clean

bench:
	go test -bench=. -benchmem ./internal/benchmarks/...

help:
	@echo "Available commands:"
	@echo "  make build  - Build the binary"
	@echo "  make run    - Run the project"
	@echo "  make fmt    - Format the code"
	@echo "  make bench  - Run performance benchmarks"
	@echo "  make test   - Run tests with race detector"
	@echo "  make clean  - Clean binaries and data"