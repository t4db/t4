package etcd

import (
	"errors"
	"testing"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"

	"github.com/t4db/t4"
)

func TestKVErrorMapsNoLeader(t *testing.T) {
	err := kvError(t4.ErrNoLeader)
	if !errors.Is(err, rpctypes.ErrGRPCNoLeader) {
		t.Fatalf("kvError(ErrNoLeader) = %v, want ErrGRPCNoLeader", err)
	}
}
