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

help:
	@echo "Comandos disponibles:"
	@echo "  make build  - Compila el binario"
	@echo "  make run    - Ejecuta el proyecto"
	@echo "  make fmt    - Formatea el código"
	@echo "  make test   - Ejecuta los tests con race detector"
	@echo "  make clean  - Limpia binarios y data"