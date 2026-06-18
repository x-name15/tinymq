# === Phase 1 ===
FROM golang:1.23-alpine AS builder

RUN apk update && apk add --no-cache git ca-certificates && update-ca-certificates

WORKDIR /app

COPY go.mod ./

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o tinymq ./cmd/tinymq

# === Phase 2 ===
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/tinymq .

EXPOSE 7800

CMD ["./tinymq"]