# === Phase 1 ===
FROM golang:1.26-alpine3.24 AS builder

RUN apk update && apk add --no-cache ca-certificates && update-ca-certificates

WORKDIR /app

COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o tinymq ./cmd/tinymq

# === Phase 2 ===
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

WORKDIR /root/

COPY --from=builder /app/tinymq .
EXPOSE 7800

CMD ["./tinymq"]