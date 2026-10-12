package etcd

import (
	"errors"
	"testing"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4"
)

func TestKVErrorMapsNoLeader(t *testing.T) {
	err := kvError(t4.ErrNoLeader)
	if !errors.Is(err, rpctypes.ErrGRPCNoLeader) {
		t.Fatalf("kvError(ErrNoLeader) = %v, want ErrGRPCNoLeader", err)
	}
}

// TestKVErrorMapsClosedToUnavailable: a node that stopped serving, on its own
// or by Close, answers Unavailable, so that clients retry reads on another
// node. It is not ErrGRPCNoLeader, which would also tell them a write never
// started.
func TestKVErrorMapsClosedToUnavailable(t *testing.T) {
	err := kvError(t4.ErrClosed)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("kvError(ErrClosed) = %v, want Unavailable", err)
	}
	if errors.Is(err, rpctypes.ErrGRPCNoLeader) {
		t.Fatalf("kvError(ErrClosed) = %v, want an error other than ErrGRPCNoLeader", err)
	}
}
