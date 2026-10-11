.PHONY: docs-generate test-apiserver test-etcd-diff test-robustness

docs-generate:
	go run ./hack/docgen

test-apiserver:
	tests/apiserver/run.sh

test-etcd-diff:
	cd tests/etcddiff && go test -count=1 -timeout 10m ./...

# Node data dirs and failure reports go under .tmp rather than /tmp, which
# may be RAM-backed. TMPDIR and RESULTS_DIR override.
ROBUSTNESS_TMP ?= $(CURDIR)/.tmp/robustness
test-robustness:
	mkdir -p $(ROBUSTNESS_TMP)
	cd tests/robustness && TMPDIR=$${TMPDIR:-$(ROBUSTNESS_TMP)} RESULTS_DIR=$${RESULTS_DIR:-$(ROBUSTNESS_TMP)/results} go test -count=1 -timeout 30m ./...
