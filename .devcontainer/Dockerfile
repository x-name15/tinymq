FROM golang:1.26-alpine3.24

# Install dev tooling
RUN apk add --no-cache \
    git \
    bash \
    curl \
    ca-certificates \
    build-base \
    mosquitto-clients

# Create a non-root user that VS Code expects
RUN addgroup -g 1000 vscode && \
    adduser -D -u 1000 -G vscode vscode

# Give the vscode user a proper home and shell
RUN mkdir -p /home/vscode && chown vscode:vscode /home/vscode
ENV HOME=/home/vscode
ENV GOPATH=/home/vscode/go
ENV PATH=$GOPATH/bin:$PATH

WORKDIR /workspaces/tinymq
RUN chown vscode:vscode /workspaces/tinymq

USER vscode 