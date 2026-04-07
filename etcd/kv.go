package etcd

import (
	"context"
	"errors"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4"
)

// Range implements KVServer.Range (Get / List).
func (s *Server) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	key := string(r.Key)
	rangeEnd := string(r.RangeEnd)

	// A read is linearizable when the client requests it AND the server is not
	// configured to force serializable reads.
	linearizable := !r.Serializable && s.node.ReadConsistency() != t4.ReadConsistencySerializable

	// Single-key lookup.
	if rangeEnd == "" {
		if isInternalKey(key) {
			return &etcdserverpb.RangeResponse{Header: s.header()}, nil
		}
		if r.CountOnly {
			var (
				kv  *t4.KeyValue
				err error
			)
			if linearizable {
				kv, err = s.node.LinearizableGet(ctx, key)
			} else {
				kv, err = s.node.Get(key)
			}
			if err != nil {
				return nil, err
			}
			count := int64(0)
			if kv != nil {
				count = 1
			}
			return &etcdserverpb.RangeResponse{Header: s.header(), Count: count}, nil
		}
		var (
			kv  *t4.KeyValue
			err error
		)
		if linearizable {
			kv, err = s.node.LinearizableGet(ctx, key)
		} else {
			kv, err = s.node.Get(key)
		}
		if err != nil {
			return nil, err
		}
		resp := &etcdserverpb.RangeResponse{Header: s.header()}
		if kv != nil {
			resp.Kvs = []*mvccpb.KeyValue{kvToProto(kv)}
			resp.Count = 1
		}
		return resp, nil
	}

	// Range / prefix scan.
	// Key is the prefix; rangeEnd = "\x00" means "all keys ≥ Key".
	prefix := key
	if key == "\x00" {
		prefix = ""
	}

	if r.CountOnly {
		var (
			all []*t4.KeyValue
			err error
		)
		if linearizable {
			all, err = s.node.LinearizableList(ctx, prefix)
		} else {
			all, err = s.node.List(prefix)
		}
		if err != nil {
			return nil, err
		}
		all = userKeyValues(all)
		var count int64
		for _, kv := range all {
			if rangeEnd != "\x00" && kv.Key >= rangeEnd {
				continue
			}
			count++
		}
		return &etcdserverpb.RangeResponse{Header: s.header(), Count: count}, nil
	}

	var (
		all []*t4.KeyValue
		err error
	)
	if linearizable {
		all, err = s.node.LinearizableList(ctx, prefix)
	} else {
		all, err = s.node.List(prefix)
	}
	if err != nil {
		return nil, err
	}
	all = userKeyValues(all)

	var kvs []*mvccpb.KeyValue
	for _, kv := range all {
		if rangeEnd != "\x00" && kv.Key >= rangeEnd {
			continue
		}
		kvs = append(kvs, kvToProto(kv))
	}

	if r.Limit > 0 && int64(len(kvs)) > r.Limit {
		kvs = kvs[:r.Limit]
	}

	return &etcdserverpb.RangeResponse{
		Header: s.header(),
		Kvs:    kvs,
		Count:  int64(len(kvs)),
	}, nil
}

// Put implements KVServer.Put.
func (s *Server) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	key := string(r.Key)
	if err := validateUserKey(key); err != nil {
		return nil, err
	}
	if r.Lease != 0 {
		if _, err := s.getLease(ctx, r.Lease, true); err != nil {
			return nil, err
		}
	}
	resp := &etcdserverpb.PutResponse{Header: s.header()}

	if r.PrevKv {
		prev, err := s.node.Get(key)
		if err != nil {
			return nil, err
		}
		if prev != nil {
			resp.PrevKv = kvToProto(prev)
		}
	}

	if _, err := s.node.Put(ctx, key, r.Value, r.Lease); err != nil {
		return nil, err
	}
	resp.Header = s.header()
	return resp, nil
}

// DeleteRange implements KVServer.DeleteRange.
// Single-key deletes are fully supported. Range deletes are not.
func (s *Server) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	if string(r.RangeEnd) != "" {
		return nil, status.Error(codes.Unimplemented, "range deletes not supported")
	}
	key := string(r.Key)
	if err := validateUserKey(key); err != nil {
		return nil, err
	}
	resp := &etcdserverpb.DeleteRangeResponse{Header: s.header()}

	if r.PrevKv {
		prev, err := s.node.Get(key)
		if err != nil {
			return nil, err
		}
		if prev != nil {
			resp.PrevKvs = []*mvccpb.KeyValue{kvToProto(prev)}
		}
	}

	newRev, err := s.node.Delete(ctx, key)
	if err != nil {
		return nil, err
	}
	resp.Header = s.header()
	if newRev > 0 {
		resp.Deleted = 1
	}
	return resp, nil
}

// Txn implements KVServer.Txn.
//
// Supported patterns:
//   - Single compare on MOD == 0 or VERSION == 0: create-if-not-exists.
//   - Single compare on MOD == X, Success=Put: compare-and-swap update.
//   - Single compare on MOD == X, Success=Delete: compare-and-swap delete.
func (s *Server) Txn(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if len(r.Compare) == 0 {
		return nil, status.Error(codes.Unimplemented, "transactions without compares are not supported")
	}

	if len(r.Compare) != 1 {
		return nil, status.Error(codes.Unimplemented, "multi-key transactions not supported")
	}

	cmp := r.Compare[0]
	key := string(cmp.Key)
	if err := validateUserKey(key); err != nil {
		return nil, err
	}
	putOp := successPut(r)
	delOp := successDelete(r)

	// Create-if-not-exists: compare MOD==0 or VERSION==0.
	if isZeroCompare(cmp) && putOp != nil {
		if err := validateUserKey(string(putOp.Key)); err != nil {
			return nil, err
		}
		if putOp.Lease != 0 {
			if _, err := s.getLease(ctx, putOp.Lease, true); err != nil {
				return nil, err
			}
		}
		rev, err := s.node.Create(ctx, string(putOp.Key), putOp.Value, putOp.Lease)
		if err != nil {
			if errors.Is(err, t4.ErrKeyExists) {
				ops, _ := s.execOps(ctx, r.Failure)
				return &etcdserverpb.TxnResponse{Header: s.header(), Succeeded: false, Responses: ops}, nil
			}
			return nil, err
		}
		_ = rev
		return &etcdserverpb.TxnResponse{Header: s.header(), Succeeded: true}, nil
	}

	// CAS update: compare MOD == X, success = put.
	if cmp.Target == etcdserverpb.Compare_MOD && cmp.Result == etcdserverpb.Compare_EQUAL && putOp != nil {
		if err := validateUserKey(string(putOp.Key)); err != nil {
			return nil, err
		}
		if putOp.Lease != 0 {
			if _, err := s.getLease(ctx, putOp.Lease, true); err != nil {
				return nil, err
			}
		}
		_, _, ok, err := s.node.Update(ctx, key, putOp.Value, cmp.GetModRevision(), putOp.Lease)
		if err != nil {
			return nil, err
		}
		if !ok {
			ops, _ := s.execOps(ctx, r.Failure)
			return &etcdserverpb.TxnResponse{Header: s.header(), Succeeded: false, Responses: ops}, nil
		}
		return &etcdserverpb.TxnResponse{Header: s.header(), Succeeded: true}, nil
	}

	// CAS delete: compare MOD == X, success = delete.
	if cmp.Target == etcdserverpb.Compare_MOD && cmp.Result == etcdserverpb.Compare_EQUAL && delOp != nil {
		if err := validateUserKey(string(delOp.Key)); err != nil {
			return nil, err
		}
		_, _, ok, err := s.node.DeleteIfRevision(ctx, key, cmp.GetModRevision())
		if err != nil {
			return nil, err
		}
		if !ok {
			ops, _ := s.execOps(ctx, r.Failure)
			return &etcdserverpb.TxnResponse{Header: s.header(), Succeeded: false, Responses: ops}, nil
		}
		return &etcdserverpb.TxnResponse{Header: s.header(), Succeeded: true}, nil
	}

	return nil, status.Errorf(codes.Unimplemented, "unsupported transaction pattern (target=%v result=%v)", cmp.Target, cmp.Result)
}

// Compact implements KVServer.Compact.
func (s *Server) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	if err := s.node.Compact(ctx, r.Revision); err != nil {
		return nil, err
	}
	return &etcdserverpb.CompactionResponse{Header: s.header()}, nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

// isZeroCompare returns true for "MOD == 0" or "VERSION == 0" compares,
// which are the create-if-not-exists patterns.
func isZeroCompare(c *etcdserverpb.Compare) bool {
	if c.Result != etcdserverpb.Compare_EQUAL {
		return false
	}
	switch c.Target {
	case etcdserverpb.Compare_MOD:
		return c.GetModRevision() == 0
	case etcdserverpb.Compare_VERSION:
		return c.GetVersion() == 0
	case etcdserverpb.Compare_CREATE:
		return c.GetCreateRevision() == 0
	}
	return false
}

// successPut returns the PutRequest from the first Success op, or nil.
func successPut(r *etcdserverpb.TxnRequest) *etcdserverpb.PutRequest {
	if len(r.Success) == 1 {
		if p, ok := r.Success[0].GetRequest().(*etcdserverpb.RequestOp_RequestPut); ok {
			return p.RequestPut
		}
	}
	return nil
}

// successDelete returns the DeleteRangeRequest from the first Success op, or nil.
func successDelete(r *etcdserverpb.TxnRequest) *etcdserverpb.DeleteRangeRequest {
	if len(r.Success) == 1 {
		if d, ok := r.Success[0].GetRequest().(*etcdserverpb.RequestOp_RequestDeleteRange); ok {
			return d.RequestDeleteRange
		}
	}
	return nil
}

// execOps executes a list of RequestOps and returns their ResponseOps.
func (s *Server) execOps(ctx context.Context, ops []*etcdserverpb.RequestOp) ([]*etcdserverpb.ResponseOp, error) {
	results := make([]*etcdserverpb.ResponseOp, len(ops))
	for i, op := range ops {
		switch v := op.GetRequest().(type) {
		case *etcdserverpb.RequestOp_RequestRange:
			resp, err := s.Range(ctx, v.RequestRange)
			if err != nil {
				return nil, err
			}
			results[i] = &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: resp}}
		case *etcdserverpb.RequestOp_RequestPut:
			resp, err := s.Put(ctx, v.RequestPut)
			if err != nil {
				return nil, err
			}
			results[i] = &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: resp}}
		case *etcdserverpb.RequestOp_RequestDeleteRange:
			resp, err := s.DeleteRange(ctx, v.RequestDeleteRange)
			if err != nil {
				return nil, err
			}
			results[i] = &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: resp}}
		}
	}
	return results, nil
}
