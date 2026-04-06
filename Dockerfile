# Builds the strata server binary.
# Run: docker build -t strata .
FROM golang:1.25-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w" \
      -o /strata \
      ./cmd/strata

# ── Runtime image ─────────────────────────────────────────────────────────────
# Alpine is used instead of distroless because the Helm chart builds the
# command-line dynamically (--advertise-peer uses $MY_POD_NAME) via /bin/sh.
# The binary is statically linked (CGO_ENABLED=0) so it runs without libc.
FROM alpine:3.21

# ca-certificates is required for TLS connections to S3 and other HTTPS
# endpoints. addgroup/adduser mirror the distroless nonroot UID (65532).
RUN apk add --no-cache ca-certificates \
 && addgroup -g 65532 nonroot \
 && adduser  -u 65532 -G nonroot -s /sbin/nologin -D nonroot

USER nonroot

COPY --from=builder /strata /strata

EXPOSE 3379 3380 9090

ENTRYPOINT ["/strata"]
CMD ["run"]
