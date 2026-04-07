# Builds the t4bench binary from the repo root.
# Run: docker build -f bench/Dockerfile -t t4bench .
FROM golang:1.25-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /t4bench ./bench/cmd/t4bench

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /t4bench /usr/local/bin/t4bench
ENTRYPOINT ["/usr/local/bin/t4bench"]
