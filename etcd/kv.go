package etcd

import (
	"context"

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
func (s *Server) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	key := string(r.Key)
	rangeEnd := string(r.RangeEnd)

	// Single-key delete.
	if rangeEnd == "" {
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

	// Range / prefix delete: list all keys in range and delete them in atomic
	// Txn batches. This is O(1) WAL entries per batch instead of O(n), and each
	// batch commits at a single revision.
	prefix := key
	if key == "\x00" {
		prefix = ""
	}
	all, err := s.node.List(prefix)
	if err != nil {
		return nil, err
	}
	all = userKeyValues(all)

	matched := all[:0]
	for _, kv := range all {
		if rangeEnd != "\x00" && kv.Key >= rangeEnd {
			continue
		}
		matched = append(matched, kv)
	}

	resp := &etcdserverpb.DeleteRangeResponse{Header: s.header()}
	if len(matched) == 0 {
		return resp, nil
	}

	// Node.Txn caps at 65535 ops per branch; chunk to stay below.
	const maxTxnOps = 65535
	for i := 0; i < len(matched); i += maxTxnOps {
		end := min(i+maxTxnOps, len(matched))
		chunk := matched[i:end]
		ops := make([]t4.TxnOp, len(chunk))
		for j, kv := range chunk {
			ops[j] = t4.TxnOp{Type: t4.TxnDelete, Key: kv.Key}
		}
		txnResp, err := s.node.Txn(ctx, t4.TxnRequest{Success: ops})
		if err != nil {
			return nil, err
		}
		for _, kv := range chunk {
			if _, ok := txnResp.DeletedKeys[kv.Key]; !ok {
				continue
			}
			if r.PrevKv {
				resp.PrevKvs = append(resp.PrevKvs, kvToProto(kv))
			}
			resp.Deleted++
		}
	}
	resp.Header = s.header()
	return resp, nil
}

// Txn implements KVServer.Txn.
//
// All Compare conditions are evaluated atomically. Write ops (Put /
// DeleteRange) in the selected branch are applied as a single atomic revision.
// Range ops in the selected branch are executed non-atomically after the write
// commits (reads see the post-transaction state).
func (s *Server) Txn(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	// Convert conditions.
	conds := make([]t4.TxnCondition, 0, len(r.Compare))
	for _, cmp := range r.Compare {
		cond, err := convertCompare(cmp)
		if err != nil {
			return nil, err
		}
		conds = append(conds, cond)
	}

	// Convert both branches to t4 ops (write ops only).
	successOps, err := convertWriteOps(r.Success)
	if err != nil {
		return nil, err
	}
	failureOps, err := convertWriteOps(r.Failure)
	if err != nil {
		return nil, err
	}

	// Validate leases referenced by Put ops in both branches.  This mirrors
	// the check in standalone Put and prevents phantom lease IDs from being
	// committed even if the branch that contains them is never selected.
	if err := s.validateTxnOpLeases(ctx, r.Success); err != nil {
		return nil, err
	}
	if err := s.validateTxnOpLeases(ctx, r.Failure); err != nil {
		return nil, err
	}

	// Execute the atomic write portion.
	txnResp, err := s.node.Txn(ctx, t4.TxnRequest{
		Conditions: conds,
		Success:    successOps,
		Failure:    failureOps,
	})
	if err != nil {
		return nil, err
	}

	// Execute any Range ops in the selected branch (non-atomic read after write).
	selectedBranch := r.Failure
	if txnResp.Succeeded {
		selectedBranch = r.Success
	}
	responses, err := s.buildTxnResponses(ctx, selectedBranch, txnResp.DeletedKeys)
	if err != nil {
		return nil, err
	}

	return &etcdserverpb.TxnResponse{
		Header:    s.header(),
		Succeeded: txnResp.Succeeded,
		Responses: responses,
	}, nil
}

// convertCompare converts a single etcd Compare into a t4 TxnCondition.
func convertCompare(cmp *etcdserverpb.Compare) (t4.TxnCondition, error) {
	if err := validateUserKey(string(cmp.Key)); err != nil {
		return t4.TxnCondition{}, err
	}
	c := t4.TxnCondition{Key: string(cmp.Key)}

	switch cmp.Result {
	case etcdserverpb.Compare_EQUAL:
		c.Result = t4.TxnCondEqual
	case etcdserverpb.Compare_NOT_EQUAL:
		c.Result = t4.TxnCondNotEqual
	case etcdserverpb.Compare_GREATER:
		c.Result = t4.TxnCondGreater
	case etcdserverpb.Compare_LESS:
		c.Result = t4.TxnCondLess
	default:
		return t4.TxnCondition{}, status.Errorf(codes.Unimplemented, "unsupported compare result %v", cmp.Result)
	}

	switch cmp.Target {
	case etcdserverpb.Compare_MOD:
		c.Target = t4.TxnCondMod
		c.ModRevision = fromEtcdRevision(cmp.GetModRevision())
	case etcdserverpb.Compare_VERSION:
		// t4 does not track per-key write counts; VERSION is treated as a binary
		// presence flag (0 = absent, non-zero = present).  Only comparisons against
		// zero are correct: VERSION == 0 (key absent) and VERSION != 0 (key present).
		// Any non-zero RHS would produce silently wrong results for keys that have
		// been updated more than once, so reject them.
		if cmp.GetVersion() != 0 {
			return t4.TxnCondition{}, status.Error(codes.Unimplemented,
				"VERSION comparisons against non-zero values are not supported; use ModRevision instead")
		}
		c.Target = t4.TxnCondVersion
		c.Version = 0
	case etcdserverpb.Compare_CREATE:
		c.Target = t4.TxnCondCreate
		c.CreateRevision = fromEtcdRevision(cmp.GetCreateRevision())
	case etcdserverpb.Compare_VALUE:
		c.Target = t4.TxnCondValue
		c.Value = []byte(cmp.GetValue())
	case etcdserverpb.Compare_LEASE:
		c.Target = t4.TxnCondLease
		c.Lease = cmp.GetLease()
	default:
		return t4.TxnCondition{}, status.Errorf(codes.Unimplemented, "unsupported compare target %v", cmp.Target)
	}

	return c, nil
}

// convertWriteOps extracts the write ops (Put / DeleteRange) from a list of
// RequestOps and converts them to t4.TxnOps.  Range ops are skipped here and
// handled later by buildTxnResponses.  Nested Txn ops are rejected.
func convertWriteOps(ops []*etcdserverpb.RequestOp) ([]t4.TxnOp, error) {
	var result []t4.TxnOp
	for _, op := range ops {
		switch v := op.GetRequest().(type) {
		case *etcdserverpb.RequestOp_RequestPut:
			if err := validateUserKey(string(v.RequestPut.Key)); err != nil {
				return nil, err
			}
			result = append(result, t4.TxnOp{
				Type:  t4.TxnPut,
				Key:   string(v.RequestPut.Key),
				Value: v.RequestPut.Value,
				Lease: v.RequestPut.Lease,
			})
		case *etcdserverpb.RequestOp_RequestDeleteRange:
			key := string(v.RequestDeleteRange.Key)
			if err := validateUserKey(key); err != nil {
				return nil, err
			}
			// Only single-key deletes are supported in atomic txn branches.
			if len(v.RequestDeleteRange.RangeEnd) > 0 {
				return nil, status.Error(codes.Unimplemented, "range deletes are not supported in transaction branches")
			}
			result = append(result, t4.TxnOp{Type: t4.TxnDelete, Key: key})
		case *etcdserverpb.RequestOp_RequestRange:
			// Range ops are read-only; handled separately in buildTxnResponses.
		case *etcdserverpb.RequestOp_RequestTxn:
			return nil, status.Error(codes.Unimplemented, "nested transactions are not supported")
		}
	}
	return result, nil
}

// buildTxnResponses builds the ResponseOp list for the selected transaction
// branch. Write ops get responses based on the committed state; Range ops are
// executed and their results included.
func (s *Server) buildTxnResponses(ctx context.Context, ops []*etcdserverpb.RequestOp, deletedKeys map[string]struct{}) ([]*etcdserverpb.ResponseOp, error) {
	responses := make([]*etcdserverpb.ResponseOp, 0, len(ops))
	hdr := s.header()
	for _, op := range ops {
		switch v := op.GetRequest().(type) {
		case *etcdserverpb.RequestOp_RequestPut:
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: &etcdserverpb.PutResponse{Header: hdr},
				},
			})
		case *etcdserverpb.RequestOp_RequestDeleteRange:
			var deleted int64
			if _, ok := deletedKeys[string(v.RequestDeleteRange.Key)]; ok {
				deleted = 1
			}
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
					ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{Header: hdr, Deleted: deleted},
				},
			})
		case *etcdserverpb.RequestOp_RequestRange:
			resp, err := s.Range(ctx, v.RequestRange)
			if err != nil {
				return nil, err
			}
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: resp},
			})
		case *etcdserverpb.RequestOp_RequestTxn:
			return nil, status.Error(codes.Unimplemented, "nested transactions are not supported")
		}
	}
	return responses, nil
}

// Compact implements KVServer.Compact.
func (s *Server) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	if err := s.node.Compact(ctx, fromEtcdRevision(r.Revision)); err != nil {
		return nil, err
	}
	return &etcdserverpb.CompactionResponse{Header: s.header()}, nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

// validateTxnOpLeases checks that every Put op in ops that carries a non-zero
// Lease ID refers to an existing lease.  This mirrors the check in standalone
// Put and prevents phantom lease IDs from being written inside a transaction.
func (s *Server) validateTxnOpLeases(ctx context.Context, ops []*etcdserverpb.RequestOp) error {
	for _, op := range ops {
		if p, ok := op.GetRequest().(*etcdserverpb.RequestOp_RequestPut); ok {
			if p.RequestPut.Lease != 0 {
				if _, err := s.getLease(ctx, p.RequestPut.Lease, true); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
