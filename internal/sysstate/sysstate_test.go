package sysstate

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/t4db/t4"
)

// recorder is a Node that records writes. Methods it does not implement
// panic through the nil embedded interface, so an unexpected call fails.
type recorder struct {
	Node
	meta  bool
	calls []string
}

func (r *recorder) MetaEnabled() (bool, error) { return r.meta, nil }

func (r *recorder) Put(_ context.Context, key string, _ []byte, _ int64) (int64, error) {
	r.calls = append(r.calls, "Put "+key)
	return 1, nil
}

func (r *recorder) Create(_ context.Context, key string, _ []byte, _ int64) (int64, error) {
	r.calls = append(r.calls, "Create "+key)
	return 1, nil
}

func (r *recorder) Delete(_ context.Context, key string) (int64, error) {
	r.calls = append(r.calls, "Delete "+key)
	return 1, nil
}

func (r *recorder) Txn(_ context.Context, req t4.TxnRequest) (t4.TxnResponse, error) {
	var parts []string
	for _, c := range req.Conditions {
		parts = append(parts, fmt.Sprintf("if(%d)", c.Target))
	}
	for _, op := range req.Success {
		parts = append(parts, fmt.Sprintf("op(%d)", op.Type))
	}
	for _, op := range req.Failure {
		parts = append(parts, fmt.Sprintf("else(%d)", op.Type))
	}
	r.calls = append(r.calls, "Txn "+strings.Join(parts, " "))
	return t4.TxnResponse{Succeeded: true}, nil
}

func exercise(t *testing.T, n Node) {
	t.Helper()
	ctx := context.Background()
	if err := Put(ctx, n, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := Delete(ctx, n, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := Create(ctx, n, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := Apply(ctx, n, []t4.TxnOp{{Type: t4.TxnDelete, Key: "attached"}}, Change{Key: "k", Delete: true}); err != nil {
		t.Fatal(err)
	}
}

// TestLegacyModeSendsOnlyV1Requests guards rolling upgrades: a follower on
// this release forwards these writes to a leader that may run an earlier
// release. That leader evaluates unknown txn conditions as false and
// silently drops unknown txn op types, so a legacy database must only see
// the requests earlier releases send.
func TestLegacyModeSendsOnlyV1Requests(t *testing.T) {
	r := &recorder{}
	exercise(t, r)
	want := []string{
		"Put k",
		"Delete k",
		"Create k",
		fmt.Sprintf("Txn op(%d) op(%d)", t4.TxnDelete, t4.TxnDelete),
	}
	if strings.Join(r.calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("calls:\n%s\nwant:\n%s", strings.Join(r.calls, "\n"), strings.Join(want, "\n"))
	}
}

func TestMetaModeUsesMetaOps(t *testing.T) {
	r := &recorder{meta: true}
	exercise(t, r)
	want := []string{
		fmt.Sprintf("Txn op(%d)", t4.TxnMetaPut),
		fmt.Sprintf("Txn op(%d)", t4.TxnMetaDelete),
		fmt.Sprintf("Txn if(%d) op(%d)", t4.TxnCondMetaExists, t4.TxnMetaPut),
		fmt.Sprintf("Txn op(%d) op(%d)", t4.TxnDelete, t4.TxnMetaDelete),
	}
	if strings.Join(r.calls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("calls:\n%s\nwant:\n%s", strings.Join(r.calls, "\n"), strings.Join(want, "\n"))
	}
}
