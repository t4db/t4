# Builds the stratabench binary from the repo root.
# Run: docker build -f bench/Dockerfile -t stratabench .
FROM golang:1.25-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /stratabench ./bench/cmd/stratabench

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /stratabench /usr/local/bin/stratabench
ENTRYPOINT ["/usr/local/bin/stratabench"]
