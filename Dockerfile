# Builds the t4 server binary.
# Run: docker build -t t4 .
FROM golang:1.25-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=
ARG DATE=

RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-s -w \
        -X github.com/t4db/t4/internal/version.Version=${VERSION} \
        -X github.com/t4db/t4/internal/version.Commit=${COMMIT} \
        -X github.com/t4db/t4/internal/version.Date=${DATE}" \
      -o /t4 \
      ./cmd/t4

# ── Runtime image ─────────────────────────────────────────────────────────────
# Alpine is used instead of distroless because the Helm chart builds the
# command-line dynamically (--advertise-peer uses $MY_POD_NAME) via /bin/sh.
# The binary is statically linked (CGO_ENABLED=0) so it runs without libc.
FROM alpine:3.24

# ca-certificates is required for TLS connections to S3 and other HTTPS
# endpoints. addgroup/adduser mirror the distroless nonroot UID (65532).
RUN apk add --no-cache ca-certificates \
 && addgroup -g 65532 nonroot \
 && adduser  -u 65532 -G nonroot -s /sbin/nologin -D nonroot

RUN mkdir -p /var/lib/t4 && chown 65532:65532 /var/lib/t4

USER nonroot

COPY --from=builder /t4 /t4

EXPOSE 3379 3380 9090

ENTRYPOINT ["/t4"]
CMD ["run"]
