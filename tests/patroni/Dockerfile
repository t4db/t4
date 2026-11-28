# Runs Patroni's own acceptance suite (features/*.feature) against T4 instead
# of etcd. See tests/patroni/README.md.
#
# Declared before the first FROM so both stages can see them: an ARG after a
# FROM belongs to that stage only.
ARG PG_MAJOR=17
ARG PATRONI_VERSION=v4.1.5

# ── Stage 1: build the t4 binary ─────────────────────────────────────────────
FROM golang:1.27-bookworm AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /t4 ./cmd/t4

# ── Stage 2: Postgres, Patroni and behave ────────────────────────────────────
# The postgres image supplies initdb, pg_ctl and the postgres user the suite
# needs: Patroni starts and stops real clusters, and Postgres refuses to run
# as root.
FROM postgres:${PG_MAJOR}

# Re-declared to bring the global values into this stage. The Patroni tag is
# pinned because the suite drives Patroni's internal harness, so an unpinned
# clone would change behaviour under us. Bump deliberately.
ARG PATRONI_VERSION
ARG PG_MAJOR

ENV PGHOME=/home/postgres \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PATH=/venv/bin:/usr/lib/postgresql/${PG_MAJOR}/bin:$PATH \
    DCS=t4

RUN set -ex \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
         ca-certificates \
         git \
         procps \
         python3-dev \
         python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    \
    && git clone --depth 1 --branch "$PATRONI_VERSION" \
         https://github.com/patroni/patroni.git /patroni \
    \
    && python3 -m venv /venv \
    # [etcd3] is python-etcd (and, through it, dnspython): patroni.dcs.etcd3
    # imports patroni.dcs.etcd, which imports both at module level, so the
    # extra is required even though T4 is reached over the v3 API alone.
    # psycopg2-binary: the suite connects to the clusters it starts.
    # coverage: PatroniController launches patroni under `coverage run`.
    && /venv/bin/pip install --no-cache-dir -e '/patroni[etcd3]' psycopg2-binary behave coverage \
    \
    # The harness derives its working directory from where environment.py
    # lives and writes data dirs, logs and features/output under it, so the
    # clone has to belong to the user the suite runs as.
    && mkdir -p "$PGHOME" \
    && sed -i "s|/var/lib/postgresql.*|$PGHOME:/bin/bash|" /etc/passwd \
    && chown -R postgres:postgres "$PGHOME" /patroni

COPY --from=builder /t4 /usr/local/bin/t4
COPY tests/patroni/t4_controller.py /t4_controller.py
RUN cat /t4_controller.py >> /patroni/features/environment.py \
    && chown postgres:postgres /patroni/features/environment.py

COPY tests/patroni/entrypoint.sh /entrypoint.sh

USER postgres
WORKDIR /patroni

# Default: the whole suite. Pass behave arguments to narrow it, e.g.
#   docker run ... features/dcs_failsafe_mode.feature
ENTRYPOINT ["/entrypoint.sh"]
