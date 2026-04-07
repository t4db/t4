.PHONY: keys build up run clean

NODES      := n1,n2,n3,n4,n5
TIME_LIMIT := 120
WORKLOAD   := register
# 'kill' tests crash-recovery correctness (WAL + S3 checkpoint replay).
# 'partition-halves' would require quorum consensus to pass — T4 uses a
# last-writer-wins S3 lock which is not safe under network partitions (AP, not CP).
NEMESIS    := kill

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

up: build
	docker compose up -d minio minio-init n1 n2 n3 n4 n5

# ── run a test ────────────────────────────────────────────────────────────────
run: up
	docker compose run --rm control \
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
