// Package peer implements the leader→follower WAL streaming gRPC service,
// plus write forwarding (follower→leader).
//
// To avoid a protoc dependency the service descriptor is written by hand and
// messages are encoded with a JSON codec forced on both the peer server and
// client via grpc.ForceCodec.  The kine gRPC server (port 3379) keeps the
// default proto codec; only the peer server (port 3380) uses JSON.
package peer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/t4db/t4/internal/jsonbytes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4/internal/wal"
)

// ── Sentinel errors ───────────────────────────────────────────────────────────

// ErrResyncRequired is returned when the follower's fromRevision predates the
// leader's buffer window. The follower must re-bootstrap from S3.
var ErrResyncRequired = status.Error(codes.FailedPrecondition, "resync_required")

// ErrLeaderUnreachable is returned by Client.Follow after maxRetries
// consecutive connection failures. The follower should attempt a TakeOver.
var ErrLeaderUnreachable = status.Error(codes.Unavailable, "leader_unreachable")

// ErrLeaderShutdown is returned by Client.Follow when the leader sends a
// graceful shutdown signal. The follower should start a TakeOver immediately
// without waiting for the normal retry cycle to exhaust.
var ErrLeaderShutdown = status.Error(codes.Unavailable, "leader_shutdown")

// IsResyncRequired reports whether err is an ErrResyncRequired signal.
func IsResyncRequired(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}

// IsLeaderUnreachable reports whether err is an ErrLeaderUnreachable signal.
func IsLeaderUnreachable(err error) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.Unavailable && s.Message() == "leader_unreachable"
}

// IsLeaderShutdown reports whether err is an ErrLeaderShutdown signal.
func IsLeaderShutdown(err error) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.Unavailable && s.Message() == "leader_shutdown"
}

// ── Binary codec ─────────────────────────────────────────────────────────────

// Codec encodes peer gRPC messages. Hot-path messages (WalEntryMsg, AckMsg)
// use a compact binary format; all other messages fall back to JSON.
//
// WalEntryMsg wire layout (little-endian, 84-byte fixed header):
//
//	[revision  : int64  ]  @0
//	[term      : uint64 ]  @8
//	[createRev : int64  ]  @16
//	[prevRev   : int64  ]  @24
//	[lease     : int64  ]  @32
//	[op        : uint8  ]  @40
//	[flags     : uint8  ]  @41  bit0 = shutdown, bit1 = commit, bit2 = heartbeat
//	[reserved  : uint16 ]  @42
//	[keyLen      : uint32 ]  @44
//	[commitRev   : int64  ]  @48
//	[commitStart : int64  ]  @56
//	[id          : int64  ]  @64
//	[version     : int64  ]  @72
//	[key bytes   ]           @80
//	[valueLen    : uint32 ]  @80+keyLen
//	[value bytes ]           @84+keyLen
//
// AckMsg wire layout:
//
//	[revision  : int64  ]  8 bytes
type Codec struct{}

func (Codec) Name() string { return "t4-bin" }

func (Codec) Marshal(v interface{}) ([]byte, error) {
	switch m := v.(type) {
	case *ForwardRequest:
		if m == nil {
			return nil, fmt.Errorf("peer: nil forward request")
		}
		// Stamp a shallow copy: callers retain ownership of the request and
		// nested slices; encoding never mutates them or copies key payloads.
		stamped := *m
		stamped.WireVersion = m.requiredWireVersion()
		return json.Marshal(&stamped)
	case *WalEntryMsg:
		return marshalWalEntryMsg(m)
	case *AckMsg:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(m.Revision))
		return buf, nil
	default:
		return json.Marshal(v)
	}
}

func (Codec) Unmarshal(data []byte, v interface{}) error {
	switch m := v.(type) {
	case *WalEntryMsg:
		return unmarshalWalEntryMsg(data, m)
	case *AckMsg:
		if len(data) < 8 {
			return fmt.Errorf("peer: AckMsg too short (%d bytes)", len(data))
		}
		m.Revision = int64(binary.LittleEndian.Uint64(data))
		return nil
	default:
		return json.Unmarshal(data, v)
	}
}

const (
	walEntryMsgFixedSize = 84
)

func marshalWalEntryMsg(m *WalEntryMsg) ([]byte, error) {
	kl := len(m.Key)
	vl := len(m.Value)
	buf := make([]byte, walEntryMsgFixedSize+kl+vl)
	binary.LittleEndian.PutUint64(buf[0:], uint64(m.Revision))
	binary.LittleEndian.PutUint64(buf[8:], m.Term)
	binary.LittleEndian.PutUint64(buf[16:], uint64(m.CreateRevision))
	binary.LittleEndian.PutUint64(buf[24:], uint64(m.PrevRevision))
	binary.LittleEndian.PutUint64(buf[32:], uint64(m.Lease))
	buf[40] = m.Op
	if m.Shutdown {
		buf[41] = 1
	}
	if m.Commit {
		buf[41] |= 1 << 1
	}
	if m.Heartbeat {
		buf[41] |= 1 << 2
	}
	binary.LittleEndian.PutUint32(buf[44:], uint32(kl))
	binary.LittleEndian.PutUint64(buf[48:], uint64(m.CommitRevision))
	binary.LittleEndian.PutUint64(buf[56:], uint64(m.CommitStartRevision))
	binary.LittleEndian.PutUint64(buf[64:], uint64(m.ID))
	binary.LittleEndian.PutUint64(buf[72:], uint64(m.Version))
	copy(buf[80:], m.Key)
	binary.LittleEndian.PutUint32(buf[80+kl:], uint32(vl))
	copy(buf[84+kl:], m.Value)
	return buf, nil
}

func unmarshalWalEntryMsg(data []byte, m *WalEntryMsg) error {
	if len(data) < walEntryMsgFixedSize {
		return fmt.Errorf("peer: WalEntryMsg too short (%d bytes)", len(data))
	}
	m.Revision = int64(binary.LittleEndian.Uint64(data[0:]))
	m.Term = binary.LittleEndian.Uint64(data[8:])
	m.CreateRevision = int64(binary.LittleEndian.Uint64(data[16:]))
	m.PrevRevision = int64(binary.LittleEndian.Uint64(data[24:]))
	m.Lease = int64(binary.LittleEndian.Uint64(data[32:]))
	m.Op = data[40]
	m.Shutdown = data[41]&1 != 0
	m.Commit = data[41]&(1<<1) != 0
	m.Heartbeat = data[41]&(1<<2) != 0
	kl := int(binary.LittleEndian.Uint32(data[44:]))
	m.CommitRevision = int64(binary.LittleEndian.Uint64(data[48:]))
	m.CommitStartRevision = int64(binary.LittleEndian.Uint64(data[56:]))
	m.ID = int64(binary.LittleEndian.Uint64(data[64:]))
	m.Version = int64(binary.LittleEndian.Uint64(data[72:]))
	if len(data) < walEntryMsgFixedSize+kl {
		return fmt.Errorf("peer: WalEntryMsg truncated at key (need %d have %d)", walEntryMsgFixedSize+kl, len(data))
	}
	m.Key = string(data[80 : 80+kl])
	vl := int(binary.LittleEndian.Uint32(data[80+kl:]))
	if len(data) < walEntryMsgFixedSize+kl+vl {
		return fmt.Errorf("peer: WalEntryMsg truncated at value (need %d have %d)", walEntryMsgFixedSize+kl+vl, len(data))
	}
	m.Value = make([]byte, vl)
	copy(m.Value, data[84+kl:])
	return nil
}

// ── WAL stream message types ──────────────────────────────────────────────────

// FollowRequest is the single message sent by a follower to open a WAL stream.
// FromRevision is a legacy field name; after the sequence/revision split it
// carries the next WAL sequence requested by the follower.
//
// Heartbeats asks the leader to exchange heartbeats on the stream (see
// HeartbeatInterval). Leaders that predate heartbeats ignore it and never
// send one, and a leader never sends one to a follower that did not ask.
type FollowRequest struct {
	FromRevision int64  `json:"from_revision"`
	NodeID       string `json:"node_id"`
	Heartbeats   bool   `json:"heartbeats,omitempty"`
	// WALFormat is the highest WAL format version the follower can apply
	// (wal.WALFormatVersion). Zero means the follower predates this field.
	// A leader must not stream entries in a newer format to that follower.
	WALFormat int `json:"wal_format,omitempty"`
}

// AckMsg is sent by a follower to the leader on the bidi Follow stream to
// acknowledge that it has durably written all entries up to the sequence carried
// in Revision. The leader waits for ACKs from all connected followers before
// committing each batch to Pebble (quorum commit).
type AckMsg struct {
	Revision int64 `json:"revision"`
}

// WalEntryMsg is the wire representation of a wal.Entry. Receivers must use ID
// for ordering/ACK semantics and Revision only as the user-visible revision.
// Shutdown is a special flag: when true the leader is shutting down gracefully
// and the follower should start a TakeOver election immediately. Heartbeat
// marks a message that carries nothing but the leader's liveness.
//
// Its json tags are vestigial: Codec owns this type on every path, so Key is
// never JSON-encoded and keeps arbitrary bytes as a plain string. Anything
// that routes it through encoding/json instead would silently replace invalid
// UTF-8 in Key with U+FFFD, which is why forwarded keys use WireKey.
type WalEntryMsg struct {
	ID                  int64  `json:"id,omitempty"`
	Revision            int64  `json:"revision"`
	Term                uint64 `json:"term"`
	Op                  uint8  `json:"op"`
	Key                 string `json:"key"`
	Value               []byte `json:"value"`
	Lease               int64  `json:"lease"`
	CreateRevision      int64  `json:"create_revision"`
	PrevRevision        int64  `json:"prev_revision"`
	Version             int64  `json:"version,omitempty"`
	CommitRevision      int64  `json:"commit_revision,omitempty"`
	CommitStartRevision int64  `json:"commit_start_revision,omitempty"`
	Commit              bool   `json:"commit,omitempty"`
	Shutdown            bool   `json:"shutdown,omitempty"`
	Heartbeat           bool   `json:"heartbeat,omitempty"`
}

func EntryToMsg(e *wal.Entry) *WalEntryMsg {
	return &WalEntryMsg{
		ID: e.Sequence(), Revision: e.Revision, Term: e.Term, Op: uint8(e.Op),
		Key: e.Key, Value: e.Value, Lease: e.Lease,
		CreateRevision: e.CreateRevision, PrevRevision: e.PrevRevision, Version: e.Version,
	}
}

func MsgToEntry(m *WalEntryMsg) wal.Entry {
	return wal.Entry{
		ID: m.ID, Revision: m.Revision, Term: m.Term, Op: wal.Op(m.Op),
		Key: m.Key, Value: m.Value, Lease: m.Lease,
		CreateRevision: m.CreateRevision, PrevRevision: m.PrevRevision, Version: m.Version,
	}
}

// ── Write-forwarding message types ───────────────────────────────────────────

// ForwardOp identifies the write operation being forwarded.
type ForwardOp uint8

const (
	ForwardPut              ForwardOp = iota
	ForwardCreate                     // create-only (fails if key exists)
	ForwardUpdate                     // CAS update by revision
	ForwardDeleteIfRevision           // CAS delete by revision (revision=0 = unconditional)
	ForwardCompact                    // compact up to Revision
	ForwardGetRevision                // ReadIndex: returns the leader's current revision
	ForwardTxn                        // multi-key atomic transaction
	ForwardMetaPut                    // set a meta keyspace key
	ForwardMetaDelete                 // delete a meta keyspace key
	ForwardGetSequence                // ReadIndex for the meta keyspace: returns the leader's applied WAL sequence
	ForwardLeaseRenew                 // renew client lease Lease, granted with TTL Revision (nanoseconds), in the leader's memory
	ForwardLeaseRenewedAt             // read how long ago each of Leases was last renewed (ForwardResponse.Ages)
)

// WireKey is a key on the peer wire: arbitrary bytes that survive JSON.
// See internal/jsonbytes for why a plain string will not do.
type WireKey = jsonbytes.String

// Independent of persisted WAL/checkpoint versions. Version 0 is legacy;
// its strings may already have lost bytes before reaching the leader.
//
// A request is stamped with the lowest version that can represent it, not
// with the newest this binary knows, because the leader's gate is an exact
// match on the versions it understands: stamping every request with the
// newest would make a leader one release behind refuse all of a new
// follower's writes rather than only those using a feature it lacks. So a
// forward carrying a feature added after version 2 stamps the version that
// feature needs, and everything else keeps stamping 2.
const forwardWireVersion uint8 = 2

// forwardWireVersionTxn3 is the version a forwarded txn needs when it carries
// either of the two branch features added after version 2: a nested
// transaction, or a delete carrying a range end.
//
// Both fail quietly on a leader that predates them, which is why they need a
// version rather than just a new field. A nested op's type is unknown there,
// so its branch would be dropped and the transaction would commit without it.
// A range delete is worse: its op type is still an ordinary single-key
// delete, so an older leader would decode the op, ignore RangeEnd and delete
// exactly one key out of the range.
const forwardWireVersionTxn3 uint8 = 3

// TxnOpNested mirrors t4.TxnNested. The wire carries op types as numbers, so
// the two have to agree; peer cannot import the root package, hence the copy.
// The root package asserts the two match at compile time.
const TxnOpNested uint8 = 5

// requiredWireVersion is the lowest version that can represent r.
func (r *ForwardRequest) requiredWireVersion() uint8 {
	if r.TxnReq != nil && r.TxnReq.needsTxn3() {
		return forwardWireVersionTxn3
	}
	return forwardWireVersion
}

// wireVersionUnderstood reports whether this binary can interpret a forward
// stamped v.
func wireVersionUnderstood(v uint8) bool {
	return v == forwardWireVersion || v == forwardWireVersionTxn3
}

// needsTxn3 reports whether either branch holds a nested transaction or a
// range delete. A nested transaction is only reachable through a top-level
// nested op, and a range end inside one travels with it, so a nested op is
// enough to answer for everything below it.
func (m *TxnReqMsg) needsTxn3() bool {
	for _, ops := range [][]TxnOpMsg{m.Success, m.Failure} {
		for _, op := range ops {
			if op.Type == TxnOpNested || op.RangeEnd != "" {
				return true
			}
		}
	}
	return false
}

// KVMsg is the wire representation of a key-value record.
type KVMsg struct {
	Key            WireKey `json:"key"`
	Value          []byte  `json:"value"`
	Revision       int64   `json:"revision"`
	CreateRevision int64   `json:"create_revision"`
	PrevRevision   int64   `json:"prev_revision"`
	Version        int64   `json:"version,omitempty"`
	Lease          int64   `json:"lease"`
}

// TxnCondMsg is the wire representation of a TxnCondition.
type TxnCondMsg struct {
	Key            WireKey `json:"key"`
	Target         uint8   `json:"target"`
	Result         uint8   `json:"result"`
	ModRevision    int64   `json:"mod_revision,omitempty"`
	CreateRevision int64   `json:"create_revision,omitempty"`
	Version        int64   `json:"version,omitempty"`
	Value          []byte  `json:"value,omitempty"`
	Lease          int64   `json:"lease,omitempty"`
}

// TxnOpMsg is the wire representation of a TxnOp (one branch operation).
type TxnOpMsg struct {
	Type  uint8   `json:"type"` // 0=put, 1=delete, 2=meta put, 3=meta delete, 4=delete lease keys, 5=nested txn
	Key   WireKey `json:"key"`
	Value []byte  `json:"value,omitempty"`
	Lease int64   `json:"lease,omitempty"`
	// RangeEnd, on a delete, makes it delete [Key, RangeEnd) rather than Key
	// alone. The leader resolves the range under its write lock.
	RangeEnd WireKey `json:"range_end,omitempty"`
	// Txn is set only for type 5, and carries the nested transaction the
	// leader evaluates in the same atomic step as the enclosing one.
	Txn *TxnReqMsg `json:"txn,omitempty"`
}

// TxnReqMsg is the wire representation of a TxnRequest.
type TxnReqMsg struct {
	Guards     []TxnCondMsg `json:"guards,omitempty"`
	Conditions []TxnCondMsg `json:"conditions,omitempty"`
	Success    []TxnOpMsg   `json:"success,omitempty"`
	Failure    []TxnOpMsg   `json:"failure,omitempty"`
}

// ForwardRequest encodes a write operation for forwarding to the leader.
type ForwardRequest struct {
	WireVersion uint8      `json:"wire_version,omitempty"`
	Op          ForwardOp  `json:"op"`
	Key         WireKey    `json:"key"`
	Value       []byte     `json:"value,omitempty"`
	Revision    int64      `json:"revision,omitempty"` // CAS revision / compact target
	Lease       int64      `json:"lease,omitempty"`
	TxnReq      *TxnReqMsg `json:"txn,omitempty"`    // ForwardTxn only
	Leases      []int64    `json:"leases,omitempty"` // ForwardLeaseRenewedAt only
}

// ForwardResponse encodes the leader's reply to a forwarded write.
type ForwardResponse struct {
	Revision    int64     `json:"revision"`
	OldKV       *KVMsg    `json:"old_kv,omitempty"` // Update / DeleteIfRevision
	Succeeded   bool      `json:"succeeded"`        // CAS result
	ErrCode     string    `json:"err_code,omitempty"`
	ErrMsg      string    `json:"err_msg,omitempty"`
	DeletedKeys []WireKey `json:"deleted_keys,omitempty"` // ForwardTxn: keys actually removed
	Ages        []int64   `json:"ages,omitempty"`         // ForwardLeaseRenewedAt: nanoseconds since each lease was renewed
	// Nested reports one result per nested transaction in the branch the
	// leader took, in order. ForwardTxn only.
	Nested []TxnResultMsg `json:"nested,omitempty"`
}

// TxnResultMsg is the wire representation of a nested transaction's outcome.
type TxnResultMsg struct {
	Succeeded bool           `json:"succeeded"`
	Nested    []TxnResultMsg `json:"nested,omitempty"`
}

// carriesKey reports whether req carries key bytes that a legacy sender could
// already have mangled. A forward without any — a read index, a compaction, a
// lease renewal — cannot carry a corrupted key, and no response to one carries
// a key either, so the version gate lets it through.
func (r *ForwardRequest) carriesKey() bool {
	return r.Key != "" || r.TxnReq != nil
}

// ForwardHandler is implemented by the Node to handle forwarded writes on the
// leader side. The peer.Server delegates Forward RPCs to this interface.
type ForwardHandler interface {
	HandleForward(ctx context.Context, req *ForwardRequest) (*ForwardResponse, error)
}

// GoodByeRequest is sent by a follower to the leader on graceful shutdown.
// The leader uses this to skip split-brain fencing for an intentional disconnect.
type GoodByeRequest struct {
	NodeID string `json:"node_id"`
}

// GoodByeResponse is the leader's acknowledgement of a follower's GoodBye.
type GoodByeResponse struct{}

// ── gRPC service interfaces ───────────────────────────────────────────────────

// WalStreamServer is implemented by the leader (peer/server.go).
type WalStreamServer interface {
	Follow(*FollowRequest, WalStream_FollowServer) error
	Forward(context.Context, *ForwardRequest) (*ForwardResponse, error)
	GoodBye(context.Context, *GoodByeRequest) (*GoodByeResponse, error)
}

// WalStream_FollowServer is the server-side send/receive stream.
// The server sends WalEntryMsgs and receives AckMsgs.
type WalStream_FollowServer interface {
	Send(*WalEntryMsg) error
	grpc.ServerStream
}

type walStream_FollowServer struct{ grpc.ServerStream }

func (x *walStream_FollowServer) Send(m *WalEntryMsg) error { return x.ServerStream.SendMsg(m) }

// WalStreamClient is used by followers.
type WalStreamClient interface {
	Follow(ctx context.Context, req *FollowRequest, opts ...grpc.CallOption) (WalStream_FollowClient, error)
	Forward(ctx context.Context, req *ForwardRequest, opts ...grpc.CallOption) (*ForwardResponse, error)
	GoodBye(ctx context.Context, req *GoodByeRequest, opts ...grpc.CallOption) (*GoodByeResponse, error)
}

// WalStream_FollowClient is the client-side send/receive stream.
// The client receives WalEntryMsgs and sends AckMsgs.
type WalStream_FollowClient interface {
	Recv() (*WalEntryMsg, error)
	SendAck(rev int64) error
	grpc.ClientStream
}

type walStream_FollowClient struct{ grpc.ClientStream }

func (x *walStream_FollowClient) Recv() (*WalEntryMsg, error) {
	m := new(WalEntryMsg)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (x *walStream_FollowClient) SendAck(rev int64) error {
	return x.ClientStream.SendMsg(&AckMsg{Revision: rev})
}

// ── gRPC service registration ─────────────────────────────────────────────────

const (
	followMethod  = "/peer.WalStream/Follow"
	forwardMethod = "/peer.WalStream/Forward"
	goodByeMethod = "/peer.WalStream/GoodBye"
)

// NewWalStreamClient wraps a gRPC ClientConn.
func NewWalStreamClient(cc grpc.ClientConnInterface) WalStreamClient {
	return &walStreamClientImpl{cc}
}

type walStreamClientImpl struct{ cc grpc.ClientConnInterface }

func (c *walStreamClientImpl) Follow(ctx context.Context, req *FollowRequest, opts ...grpc.CallOption) (WalStream_FollowClient, error) {
	stream, err := c.cc.NewStream(ctx, &walStreamServiceDesc.Streams[0], followMethod, opts...)
	if err != nil {
		return nil, err
	}
	x := &walStream_FollowClient{stream}
	if err := x.ClientStream.SendMsg(req); err != nil {
		return nil, err
	}
	// Do NOT call CloseSend: the follower sends AckMsgs back on this stream.
	return x, nil
}

func (c *walStreamClientImpl) Forward(ctx context.Context, req *ForwardRequest, opts ...grpc.CallOption) (*ForwardResponse, error) {
	out := new(ForwardResponse)
	if err := c.cc.Invoke(ctx, forwardMethod, req, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *walStreamClientImpl) GoodBye(ctx context.Context, req *GoodByeRequest, opts ...grpc.CallOption) (*GoodByeResponse, error) {
	out := new(GoodByeResponse)
	if err := c.cc.Invoke(ctx, goodByeMethod, req, out, opts...); err != nil {
		return nil, err
	}
	return out, nil
}

// RegisterWalStreamServer registers srv with a grpc.Server.
func RegisterWalStreamServer(s *grpc.Server, srv WalStreamServer) {
	s.RegisterService(&walStreamServiceDesc, srv)
}

func walStreamFollowHandler(srv interface{}, stream grpc.ServerStream) error {
	m := new(FollowRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(WalStreamServer).Follow(m, &walStream_FollowServer{stream})
}

func walStreamForwardHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ForwardRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WalStreamServer).Forward(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: forwardMethod}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WalStreamServer).Forward(ctx, req.(*ForwardRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func walStreamGoodByeHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GoodByeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WalStreamServer).GoodBye(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: goodByeMethod}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WalStreamServer).GoodBye(ctx, req.(*GoodByeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var walStreamServiceDesc = grpc.ServiceDesc{
	ServiceName: "peer.WalStream",
	HandlerType: (*WalStreamServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Forward", Handler: walStreamForwardHandler},
		{MethodName: "GoodBye", Handler: walStreamGoodByeHandler},
	},
	Streams: []grpc.StreamDesc{
		{StreamName: "Follow", Handler: walStreamFollowHandler, ServerStreams: true, ClientStreams: true},
	},
	Metadata: "peer/peer.proto",
}
