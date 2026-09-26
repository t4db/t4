.PHONY: docs-generate test-apiserver test-etcd-diff

docs-generate:
	go run ./hack/docgen

test-apiserver:
	tests/apiserver/run.sh

test-etcd-diff:
	cd tests/etcddiff && go test -count=1 -timeout 10m ./...
