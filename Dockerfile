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
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /strata /strata

EXPOSE 3379 3380 9090

ENTRYPOINT ["/strata"]
CMD ["run"]
