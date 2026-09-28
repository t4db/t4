package etcd

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"slices"
	"sort"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/t4db/t4"
)

// Range implements KVServer.Range (Get / List).
func (s *Server) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	key := string(r.Key)
	rangeEnd := string(r.RangeEnd)
	readRev := fromEtcdRevision(r.Revision)
	// Like etcd, the header reports the current revision even for a read at
	// an older one; clients that asked for a revision already know it.
	header := s.header

	if r.Revision > 0 {
		if compactRev := s.node.CompactRevision(); compactRev > 0 && readRev < compactRev {
			return nil, rpctypes.ErrGRPCCompacted
		}
		if readRev == 0 {
			// Wire revision 1 is the empty store before any write.
			return &etcdserverpb.RangeResponse{Header: header()}, nil
		}
	}

	// A read is linearizable when the client requests it AND the server is not
	// configured to force serializable reads.
	linearizable := !r.Serializable && s.node.ReadConsistency() != t4.ReadConsistencySerializable

	if needsFullRead(r) && !r.CountOnly {
		return s.fullRange(ctx, r)
	}

	// Single-key lookup.
	if rangeEnd == "" {
		if isInternalKey(key) {
			return &etcdserverpb.RangeResponse{Header: header()}, nil
		}
		if r.CountOnly {
			exists, err := s.rangeExists(ctx, linearizable, key, readAt(readRev)...)
			if err != nil {
				return nil, kvError(err)
			}
			count := int64(0)
			if exists {
				count = 1
			}
			return &etcdserverpb.RangeResponse{Header: header(), Count: count}, nil
		}
		kv, err := s.rangeGet(ctx, linearizable, key, readAt(readRev)...)
		if err != nil {
			return nil, kvError(err)
		}
		resp := &etcdserverpb.RangeResponse{Header: header()}
		if kv != nil {
			resp.Kvs = []*mvccpb.KeyValue{kvToProtoForRange(kv, r)}
			resp.Count = 1
		}
		return resp, nil
	}

	// Range / prefix scan. When the range is prefix-shaped it is served by an
	// index seek scoped to [key, rangeEnd); otherwise fall back to listing the
	// whole keyspace and filtering with matchRange.
	if scanPrefix, ok := rangeScan(key, rangeEnd); ok {
		// FromKey pins the scan to the continuation point, so Count is the
		// number of keys remaining in the range — which is what Count and
		// More must report, and what apiserver surfaces as
		// RemainingItemCount.
		seek := []t4.ReadOption{t4.WithRevision(readRev), t4.WithFromKey(key)}

		if r.CountOnly {
			count, err := s.rangeCount(ctx, linearizable, scanPrefix, seek...)
			if err != nil {
				return nil, kvError(err)
			}
			return &etcdserverpb.RangeResponse{Header: header(), Count: count}, nil
		}

		// An unlimited read already materializes the whole range, so its
		// count comes from the result rather than a second scan.
		var total int64
		if r.Limit > 0 {
			count, err := s.rangeCount(ctx, linearizable, scanPrefix, seek...)
			if err != nil {
				return nil, kvError(err)
			}
			total = count
		}

		all, err := s.rangeList(ctx, linearizable, scanPrefix, append(seek, t4.WithLimit(r.Limit))...)
		if err != nil {
			return nil, kvError(err)
		}
		kvs := make([]*mvccpb.KeyValue, 0, len(all))
		for _, kv := range all {
			kvs = append(kvs, kvToProtoForRange(kv, r))
		}
		if r.Limit <= 0 {
			total = int64(len(kvs))
		}
		return &etcdserverpb.RangeResponse{
			Header: header(),
			Kvs:    kvs,
			Count:  total,
			More:   r.Limit > 0 && total > r.Limit,
		}, nil
	}

	all, err := s.rangeList(ctx, linearizable, "", readAt(readRev)...)
	if err != nil {
		return nil, kvError(err)
	}

	if r.CountOnly {
		var count int64
		for _, kv := range all {
			if matchRange(kv, key, rangeEnd) {
				count++
			}
		}
		return &etcdserverpb.RangeResponse{Header: header(), Count: count}, nil
	}

	total := int64(0)
	var kvs []*mvccpb.KeyValue
	for _, kv := range all {
		if !matchRange(kv, key, rangeEnd) {
			continue
		}
		total++
		if r.Limit > 0 && int64(len(kvs)) >= r.Limit {
			continue
		}
		kvs = append(kvs, kvToProtoForRange(kv, r))
	}

	return &etcdserverpb.RangeResponse{
		Header: header(),
		Kvs:    kvs,
		Count:  total,
		More:   r.Limit > 0 && total > r.Limit,
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
	resp := &etcdserverpb.PutResponse{}

	if r.PrevKv {
		prev, err := s.node.Get(key)
		if err != nil {
			return nil, err
		}
		if prev != nil {
			resp.PrevKv = kvToProto(prev)
		}
	}

	commitRev, err := s.node.Put(ctx, key, r.Value, r.Lease)
	if err != nil {
		return nil, kvError(err)
	}
	resp.Header = s.headerAt(commitRev)
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
		resp := &etcdserverpb.DeleteRangeResponse{}
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
			return nil, kvError(err)
		}
		if newRev > 0 {
			resp.Header = s.headerAt(newRev)
			resp.Deleted = 1
		} else {
			// Key didn't exist — no commit. Return current revision so the
			// client sees the cluster state at the moment of the no-op.
			resp.Header = s.header()
		}
		return resp, nil
	}

	// Range / prefix delete: list all keys in range and delete them in atomic
	// Txn batches. This is O(1) WAL entries per batch instead of O(n), and each
	// batch commits at a single revision.
	var all []*t4.KeyValue
	var err error
	if scanPrefix, ok := rangeScan(key, rangeEnd); ok {
		all, err = s.node.List(scanPrefix, t4.WithFromKey(key))
	} else {
		all, err = s.node.List("")
	}
	if err != nil {
		return nil, err
	}

	matched := all[:0]
	for _, kv := range all {
		if !matchRange(kv, key, rangeEnd) {
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
	var lastCommitRev int64
	for i := 0; i < len(matched); i += maxTxnOps {
		end := min(i+maxTxnOps, len(matched))
		chunk := matched[i:end]
		ops := make([]t4.TxnOp, len(chunk))
		for j, kv := range chunk {
			ops[j] = t4.TxnOp{Type: t4.TxnDelete, Key: kv.Key}
		}
		txnResp, err := s.node.Txn(ctx, t4.TxnRequest{Success: ops})
		if err != nil {
			return nil, kvError(err)
		}
		lastCommitRev = txnResp.Revision
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
	if lastCommitRev > 0 {
		resp.Header = s.headerAt(lastCommitRev)
	}
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
		return nil, kvError(err)
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

	selectedBranch := r.Failure
	if txnResp.Succeeded {
		selectedBranch = r.Success
	}
	responses, err := s.buildTxnResponses(ctx, selectedBranch, txnResp)
	if err != nil {
		return nil, err
	}

	return &etcdserverpb.TxnResponse{
		Header:    s.headerAt(txnResp.Revision),
		Succeeded: txnResp.Succeeded,
		Responses: responses,
	}, nil
}

// convertCompare converts a single etcd Compare into a t4 TxnCondition.
// compareRevision maps the etcd wire revision on the right-hand side of a
// ModRevision/CreateRevision compare onto t4's internal clock so the compare
// gives etcd's answer. An absent key compares as 0 in both clocks, and a
// present key's wire revision is its internal one plus 1 (see
// toEtcdRevision), so wire revisions >= 2 map exactly. The others have no
// internal equivalent and need care: wire 1 lies between "absent" and the
// first real revision, and negative values lie below every key.
func compareRevision(wire int64, result t4.TxnCondResult) int64 {
	switch {
	case wire >= 2:
		return wire - 1
	case wire == 0:
		return 0
	case wire < 0:
		return -1 // below every key, absent ones included
	}
	switch result {
	case t4.TxnCondLess:
		return 1 // "< 1" holds exactly for absent keys
	case t4.TxnCondGreater:
		return 0 // "> 1" holds exactly for present keys
	default:
		return -1 // no key equals 1
	}
}

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
		c.ModRevision = compareRevision(cmp.GetModRevision(), c.Result)
	case etcdserverpb.Compare_VERSION:
		c.Target = t4.TxnCondVersion
		c.Version = cmp.GetVersion()
	case etcdserverpb.Compare_CREATE:
		c.Target = t4.TxnCondCreate
		c.CreateRevision = compareRevision(cmp.GetCreateRevision(), c.Result)
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
//
// commitRev pins the inner Put / DeleteRange response headers to the actual
// txn commit revision so callers (kube-apiserver) compute the new resource
// version from a value that matches the key's mod_revision.
func (s *Server) buildTxnResponses(ctx context.Context, ops []*etcdserverpb.RequestOp, txnResp t4.TxnResponse) ([]*etcdserverpb.ResponseOp, error) {
	commitRev, deletedKeys := txnResp.Revision, txnResp.DeletedKeys
	// Like etcd, the ops run in order against one snapshot: the state the
	// conditions were evaluated on. A txn that wrote committed at commitRev,
	// directly on top of that state, so the snapshot is commitRev-1 plus the
	// writes of the ops executed so far; a txn that wrote nothing reports
	// the snapshot's revision itself.
	snapRev := commitRev
	if len(deletedKeys) > 0 || branchHasPut(ops) {
		snapRev = commitRev - 1
	}
	// written collects the keys written by the ops before the current one.
	// A branch writes each key at most once (t4 rejects duplicates), so a
	// written key's state as of the current op is its state at commitRev.
	var written []string
	responses := make([]*etcdserverpb.ResponseOp, 0, len(ops))
	hdr := s.headerAt(commitRev)
	for _, op := range ops {
		switch v := op.GetRequest().(type) {
		case *etcdserverpb.RequestOp_RequestPut:
			written = append(written, string(v.RequestPut.Key))
			responses = append(responses, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: &etcdserverpb.PutResponse{Header: hdr},
				},
			})
		case *etcdserverpb.RequestOp_RequestDeleteRange:
			written = append(written, string(v.RequestDeleteRange.Key))
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
			resp, err := s.txnRange(ctx, v.RequestRange, snapRev, commitRev, written)
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

func branchHasPut(ops []*etcdserverpb.RequestOp) bool {
	for _, op := range ops {
		if op.GetRequestPut() != nil {
			return true
		}
	}
	return false
}

// txnRange serves a Range op inside a transaction: at snapRev, except that
// the keys in written (those the preceding ops wrote) are read at commitRev.
// A Range that names its own revision is served as asked.
func (s *Server) txnRange(ctx context.Context, r *etcdserverpb.RangeRequest, snapRev, commitRev int64, written []string) (*etcdserverpb.RangeResponse, error) {
	if r.Revision > 0 {
		return s.Range(ctx, r)
	}
	// The txn may have been forwarded to the leader and committed there;
	// this node's reads must include commitRev before they can serve it.
	if err := s.node.WaitForRevision(ctx, commitRev); err != nil {
		return nil, kvError(err)
	}
	key, rangeEnd := string(r.Key), string(r.RangeEnd)
	var touched []string
	for _, k := range written {
		if k == key || (rangeEnd != "" && matchRange(&t4.KeyValue{Key: k}, key, rangeEnd)) {
			touched = append(touched, k)
		}
	}

	snap := proto.Clone(r).(*etcdserverpb.RangeRequest)
	snap.Revision = toEtcdRevision(snapRev)
	if len(touched) == 0 {
		return s.Range(ctx, snap)
	}

	// Merge the snapshot with the touched keys' committed state, then apply
	// the sort, filters, Limit, CountOnly and KeysOnly to the merged result.
	clearFullReadOptions(snap)
	snap.CountOnly = false
	resp, err := s.Range(ctx, snap)
	if err != nil {
		return nil, err
	}
	isTouched := make(map[string]bool, len(touched))
	for _, k := range touched {
		isTouched[k] = true
	}
	kvs := resp.Kvs[:0]
	for _, kv := range resp.Kvs {
		if !isTouched[string(kv.Key)] {
			kvs = append(kvs, kv)
		}
	}
	for _, k := range touched {
		cur, err := s.Range(ctx, &etcdserverpb.RangeRequest{Key: []byte(k), Revision: toEtcdRevision(commitRev), Serializable: r.Serializable})
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, cur.Kvs...)
	}
	sort.Slice(kvs, func(i, j int) bool { return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0 })
	sortKVs(kvs, r)

	resp.Count = int64(len(kvs))
	if r.CountOnly {
		resp.Kvs = nil
		return resp, nil
	}
	kvs = filterKVs(kvs, r)
	if r.Limit > 0 && int64(len(kvs)) > r.Limit {
		kvs, resp.More = kvs[:r.Limit], true
	}
	if r.KeysOnly {
		for _, kv := range kvs {
			applyKeysOnly(kv, r)
		}
	}
	resp.Kvs = kvs
	return resp, nil
}

// Compact implements KVServer.Compact.
func (s *Server) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	if err := s.node.Compact(ctx, fromEtcdRevision(r.Revision)); err != nil {
		return nil, kvError(err)
	}
	return &etcdserverpb.CompactionResponse{Header: s.header()}, nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

// rangeGet / rangeExists / rangeList / rangeCount dispatch to the linearizable
// or local variant of each read, removing the repeated if/else fork from Range.
// readAt returns the options for a read at internal revision rev: none for
// HEAD (rev 0), so the common current read doesn't allocate an option and
// can take the node's no-options path.
func readAt(rev int64) []t4.ReadOption {
	if rev == 0 {
		return nil
	}
	return []t4.ReadOption{t4.WithRevision(rev)}
}

func (s *Server) rangeGet(ctx context.Context, lin bool, key string, opts ...t4.ReadOption) (*t4.KeyValue, error) {
	if lin {
		return s.node.LinearizableGet(ctx, key, opts...)
	}
	return s.node.Get(key, opts...)
}

func (s *Server) rangeExists(ctx context.Context, lin bool, key string, opts ...t4.ReadOption) (bool, error) {
	if lin {
		return s.node.LinearizableExists(ctx, key, opts...)
	}
	return s.node.Exists(key, opts...)
}

func (s *Server) rangeList(ctx context.Context, lin bool, prefix string, opts ...t4.ReadOption) ([]*t4.KeyValue, error) {
	if lin {
		return s.node.LinearizableList(ctx, prefix, opts...)
	}
	return s.node.List(prefix, opts...)
}

func (s *Server) rangeCount(ctx context.Context, lin bool, prefix string, opts ...t4.ReadOption) (int64, error) {
	if lin {
		return s.node.LinearizableCount(ctx, prefix, opts...)
	}
	return s.node.Count(prefix, opts...)
}

func kvError(err error) error {
	switch {
	case errors.Is(err, t4.ErrNoLeader):
		return rpctypes.ErrGRPCNoLeader
	case errors.Is(err, t4.ErrCompacted):
		return rpctypes.ErrGRPCCompacted
	case errors.Is(err, t4.ErrFutureRevision):
		return rpctypes.ErrGRPCFutureRev
	default:
		return err
	}
}

func kvToProtoForRange(kv *t4.KeyValue, r *etcdserverpb.RangeRequest) *mvccpb.KeyValue {
	pb := kvToProto(kv)
	if r.KeysOnly {
		applyKeysOnly(pb, r)
	}
	return pb
}

// rangeSortOrder returns the order r's results must be re-sorted in, or NONE
// when the key-ascending order they are read in already is the answer. As in
// etcd, a target other than KEY with no order sorts ascending.
func rangeSortOrder(r *etcdserverpb.RangeRequest) etcdserverpb.RangeRequest_SortOrder {
	switch {
	case r.SortTarget != etcdserverpb.RangeRequest_KEY && r.SortOrder == etcdserverpb.RangeRequest_NONE:
		return etcdserverpb.RangeRequest_ASCEND
	case r.SortTarget == etcdserverpb.RangeRequest_KEY && r.SortOrder == etcdserverpb.RangeRequest_ASCEND:
		return etcdserverpb.RangeRequest_NONE
	}
	return r.SortOrder
}

// needsFullRead reports whether r must read its whole range before Limit
// applies: its results are re-sorted, or filtered by revision.
func needsFullRead(r *etcdserverpb.RangeRequest) bool {
	return rangeSortOrder(r) != etcdserverpb.RangeRequest_NONE ||
		r.MinModRevision != 0 || r.MaxModRevision != 0 ||
		r.MinCreateRevision != 0 || r.MaxCreateRevision != 0
}

// clearFullReadOptions strips the options fullRange applies itself from r,
// leaving a plain key-ascending read of the whole range.
func clearFullReadOptions(r *etcdserverpb.RangeRequest) {
	r.Limit, r.KeysOnly = 0, false
	r.SortOrder, r.SortTarget = etcdserverpb.RangeRequest_NONE, etcdserverpb.RangeRequest_KEY
	r.MinModRevision, r.MaxModRevision = 0, 0
	r.MinCreateRevision, r.MaxCreateRevision = 0, 0
}

// fullRange serves a range that must be read whole. Like etcd, it filters
// the range by revision, sorts it, and applies Limit afterwards, so a
// descending read with a limit returns the last keys. Count is taken before
// the filters, as etcd does, so it stays the number of keys in the range.
func (s *Server) fullRange(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	all := proto.Clone(r).(*etcdserverpb.RangeRequest)
	clearFullReadOptions(all)
	resp, err := s.Range(ctx, all)
	if err != nil {
		return nil, err
	}
	resp.Kvs = filterKVs(resp.Kvs, r)
	sortKVs(resp.Kvs, r)
	if r.Limit > 0 && int64(len(resp.Kvs)) > r.Limit {
		resp.Kvs, resp.More = resp.Kvs[:r.Limit], true
	}
	if r.KeysOnly {
		for _, kv := range resp.Kvs {
			applyKeysOnly(kv, r)
		}
	}
	return resp, nil
}

// filterKVs drops the kvs outside r's mod and create revision bounds; a zero
// bound is unset.
func filterKVs(kvs []*mvccpb.KeyValue, r *etcdserverpb.RangeRequest) []*mvccpb.KeyValue {
	return slices.DeleteFunc(kvs, func(kv *mvccpb.KeyValue) bool {
		return (r.MinModRevision != 0 && kv.ModRevision < r.MinModRevision) ||
			(r.MaxModRevision != 0 && kv.ModRevision > r.MaxModRevision) ||
			(r.MinCreateRevision != 0 && kv.CreateRevision < r.MinCreateRevision) ||
			(r.MaxCreateRevision != 0 && kv.CreateRevision > r.MaxCreateRevision)
	})
}

// sortKVs re-sorts key-ascending kvs as r asks. The sort is stable, so ties
// stay in key order; etcd leaves their order unspecified.
func sortKVs(kvs []*mvccpb.KeyValue, r *etcdserverpb.RangeRequest) {
	order := rangeSortOrder(r)
	if order == etcdserverpb.RangeRequest_NONE {
		return
	}
	var compare func(a, b *mvccpb.KeyValue) int
	switch r.SortTarget {
	case etcdserverpb.RangeRequest_KEY:
		compare = func(a, b *mvccpb.KeyValue) int { return bytes.Compare(a.Key, b.Key) }
	case etcdserverpb.RangeRequest_VERSION:
		compare = func(a, b *mvccpb.KeyValue) int { return cmp.Compare(a.Version, b.Version) }
	case etcdserverpb.RangeRequest_CREATE:
		compare = func(a, b *mvccpb.KeyValue) int { return cmp.Compare(a.CreateRevision, b.CreateRevision) }
	case etcdserverpb.RangeRequest_MOD:
		compare = func(a, b *mvccpb.KeyValue) int { return cmp.Compare(a.ModRevision, b.ModRevision) }
	case etcdserverpb.RangeRequest_VALUE:
		compare = func(a, b *mvccpb.KeyValue) int { return bytes.Compare(a.Value, b.Value) }
	default:
		return
	}
	if order == etcdserverpb.RangeRequest_DESCEND {
		asc := compare
		compare = func(a, b *mvccpb.KeyValue) int { return asc(b, a) }
	}
	slices.SortStableFunc(kvs, compare)
}

func applyKeysOnly(pb *mvccpb.KeyValue, r *etcdserverpb.RangeRequest) {
	pb.Value = nil
	// etcd serves keys-only reads from its in-memory index, which has no
	// lease, unless the results must be sorted by value.
	if r.SortTarget != etcdserverpb.RangeRequest_VALUE {
		pb.Lease = 0
	}
}

// rangeScan maps an etcd [key, rangeEnd) range onto a t4 prefix scan seeked
// to key.
//
// Whenever rangeEnd is the exclusive upper bound of some prefix P — that is,
// P with its last byte incremented — the keys in [key, rangeEnd) are exactly
// the keys under P that are >= key. Such a range can be served by seeking
// into P's index (Node.List with WithFromKey/WithLimit) instead of listing
// the whole keyspace and filtering in memory.
//
// Kubernetes paginated LISTs have precisely this shape: the first page sends
// key == P, and every continuation page repeats the same rangeEnd with key
// advanced to the continue token. Without the seek, page two onwards would
// scan every object in the database.
//
// ok is false for ranges that are not prefix-shaped (rangeEnd == "\x00",
// explicit non-prefix bounds, key below the prefix) and for the reserved
// internal keyspace; those callers fall back to a full scan filtered by
// matchRange.
func rangeScan(key, rangeEnd string) (prefix string, ok bool) {
	if key == "" || key[0] == '\x00' || rangeEnd == "" {
		return "", false
	}
	// Invert prefixRangeEnd. A trailing 0x00 has no predecessor, so such an
	// end (notably "\x00", meaning "all keys >= key") is not prefix-shaped.
	last := rangeEnd[len(rangeEnd)-1]
	if last == 0 {
		return "", false
	}
	prefix = rangeEnd[:len(rangeEnd)-1] + string([]byte{last - 1})
	if key < prefix {
		return "", false
	}
	return prefix, true
}

func matchRange(kv *t4.KeyValue, key, rangeEnd string) bool {
	if kv == nil || isInternalKey(kv.Key) {
		return false
	}
	if rangeEnd == "\x00" {
		return kv.Key >= key
	}
	return kv.Key >= key && kv.Key < rangeEnd
}

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
