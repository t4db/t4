# Runs JuiceFS's own metadata-engine tests (pkg/meta) against T4 instead of
# etcd. See juicefs/README.md.
#
# Declared before the first FROM so both stages can see it: an ARG after a
# FROM belongs to that stage only.
ARG JUICEFS_VERSION=v1.4.2

# ── Stage 1: build the t4 binary ─────────────────────────────────────────────
FROM golang:1.27-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /t4 ./cmd/t4

# ── Stage 2: JuiceFS and its tests ───────────────────────────────────────────
# These are Go tests against the metadata engine, so this stage needs a Go
# toolchain rather than a JuiceFS image: nothing is mounted and no FUSE is
# involved.
FROM golang:1.27-bookworm

# Re-declared to bring the global value into this stage. The tag is pinned
# because the suite is pkg/meta's own test helpers, which are not a stable
# interface. Bump deliberately.
ARG JUICEFS_VERSION

# newKVMeta("etcd", $ETCD_ADDR) is the whole integration — JuiceFS reaches
# the engine through this variable, so there is no adapter to install and
# nothing in JuiceFS is patched. 3379 is the port its own CI uses for etcd.
ENV ETCD_ADDR=127.0.0.1:3379

RUN git clone --depth 1 --branch "$JUICEFS_VERSION" \
      https://github.com/juicedata/juicefs.git /juicefs

WORKDIR /juicefs
# Compile the test binary at build time so a run is only the run. -c writes a
# binary instead of executing anything, so neither a test nor a TestMain runs
# here, where there is no T4 to reach.
RUN go mod download && go test -c -o /dev/null ./pkg/meta/

COPY --from=builder /t4 /usr/local/bin/t4
COPY juicefs/entrypoint.sh /entrypoint.sh

# Default: the two etcd suites. Pass `go test` arguments to narrow or widen,
# e.g. -run TestFSOps -rapid.meta=etcd://127.0.0.1:3379/jfs ./pkg/meta/
ENTRYPOINT ["/entrypoint.sh"]
