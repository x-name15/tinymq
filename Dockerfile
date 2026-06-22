# === Phase 1 ===
FROM golang:1.26-alpine3.24 AS builder

RUN apk update && apk add --no-cache ca-certificates && update-ca-certificates
RUN adduser -D -g '' -u 10001 tinymq

WORKDIR /app

COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o tinymq ./cmd/tinymq

RUN mkdir -p /home/tinymq/data && chown 10001:10001 /home/tinymq/data

# === Phase 2 ===
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /etc/passwd /etc/passwd

WORKDIR /home/tinymq/

COPY --from=builder --chown=10001:10001 /app/tinymq .

COPY --from=builder --chown=10001:10001 /home/tinymq/data ./data

USER 10001

EXPOSE 7800 1883

CMD ["./tinymq"]
