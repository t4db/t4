.PHONY: keys build up run clean

NODES      := n1,n2,n3,n4,n5
TIME_LIMIT := 120
WORKLOAD   := register
# 'kill' tests crash-recovery correctness (WAL + S3 checkpoint replay).
# 'partition-halves' and 'partition-s3' test leader election under network
# partitions: the S3 lock plus the leader lease must prevent split brain.
NEMESIS    := kill
# Object store under test: s3 (RustFS) or gcs (fake-gcs-server). Only the
# leader lock differs between them; see the store descriptor in db.clj.
STORE      := s3

# ── keys ─────────────────────────────────────────────────────────────────────
# Generate an ed25519 key pair for Jepsen SSH access.
# node/authorized_keys  → installed into each DB node image
# control/id_ed25519    → mounted into the control node
keys: node/authorized_keys control/id_ed25519

node/authorized_keys control/id_ed25519:
	@echo "Generating Jepsen SSH key pair..."
	ssh-keygen -t ed25519 -f control/id_ed25519 -N "" -C "jepsen@control"
	cp control/id_ed25519.pub node/authorized_keys
	@echo "Keys written to control/id_ed25519 and node/authorized_keys"

# ── build / up ────────────────────────────────────────────────────────────────
build: keys
	docker compose build

# Brings up only the object store under test, so a gcs run does not start
# RustFS and vice versa.
up: build
	docker compose up -d $(STORE) $(STORE)-init n1 n2 n3 n4 n5

# ── run a test ────────────────────────────────────────────────────────────────
run: up
	docker compose run --rm -e T4_JEPSEN_STORE=$(STORE) control \
	  lein run -- test \
	    --nodes  $(NODES) \
	    --username root \
	    --ssh-private-key /root/.ssh/id_ed25519 \
	    --workload $(WORKLOAD) \
	    --nemesis $(NEMESIS) \
	    --time-limit $(TIME_LIMIT)

# ── clean ─────────────────────────────────────────────────────────────────────
clean:
	docker compose down -v
	rm -f node/authorized_keys control/id_ed25519 control/id_ed25519.pub
