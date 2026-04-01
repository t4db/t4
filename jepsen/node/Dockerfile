# ── Stage 1: build the strata binary ─────────────────────────────────────────
FROM golang:1.25-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /strata ./cmd/strata

# ── Stage 2: minimal runtime node ────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      openssh-server \
      iptables \
      iproute2 \
      ca-certificates \
      curl \
      sudo \
      awscli \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/run/sshd

# SSH: allow root login with the pre-shared test key (insecure by design).
RUN sed -i 's|#\?PermitRootLogin.*|PermitRootLogin yes|' /etc/ssh/sshd_config \
 && sed -i 's|#\?PubkeyAuthentication.*|PubkeyAuthentication yes|' /etc/ssh/sshd_config \
 && sed -i 's|#\?PasswordAuthentication.*|PasswordAuthentication no|' /etc/ssh/sshd_config

# Jepsen control node connects as root using this key pair.
# Generate with: make keys  (see jepsen/Makefile)
COPY jepsen/node/authorized_keys /root/.ssh/authorized_keys
RUN chmod 700 /root/.ssh && chmod 600 /root/.ssh/authorized_keys

COPY --from=builder /strata /usr/local/bin/strata

EXPOSE 22 2379 3380

CMD ["/usr/sbin/sshd", "-D", "-e"]
