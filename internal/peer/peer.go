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
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/makhov/strata/internal/wal"
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

// ── JSON codec ────────────────────────────────────────────────────────────────

// Codec is the JSON codec used for all peer gRPC messages.
type Codec struct{}

func (Codec) Marshal(v interface{}) ([]byte, error)      { return json.Marshal(v) }
func (Codec) Unmarshal(data []byte, v interface{}) error { return json.Unmarshal(data, v) }
func (Codec) Name() string                               { return "strata-json" }

// ── WAL stream message types ──────────────────────────────────────────────────

// FollowRequest is the single message sent by a follower to open a WAL stream.
type FollowRequest struct {
	FromRevision int64  `json:"from_revision"`
	NodeID       string `json:"node_id"`
}

// AckMsg is sent by a follower to the leader on the bidi Follow stream to
// acknowledge that it has durably written all entries up to Revision to its
// local WAL. The leader waits for ACKs from all connected followers before
// committing each batch to Pebble (quorum commit).
type AckMsg struct {
	Revision int64 `json:"revision"`
}

// WalEntryMsg is the wire representation of a wal.Entry.
// Shutdown is a special flag: when true the leader is shutting down gracefully
// and the follower should start a TakeOver election immediately.
type WalEntryMsg struct {
	Revision       int64  `json:"revision"`
	Term           uint64 `json:"term"`
	Op             uint8  `json:"op"`
	Key            string `json:"key"`
	Value          []byte `json:"value"`
	Lease          int64  `json:"lease"`
	CreateRevision int64  `json:"create_revision"`
	PrevRevision   int64  `json:"prev_revision"`
	Shutdown       bool   `json:"shutdown,omitempty"`
}

func EntryToMsg(e *wal.Entry) *WalEntryMsg {
	return &WalEntryMsg{
		Revision: e.Revision, Term: e.Term, Op: uint8(e.Op),
		Key: e.Key, Value: e.Value, Lease: e.Lease,
		CreateRevision: e.CreateRevision, PrevRevision: e.PrevRevision,
	}
}

func MsgToEntry(m *WalEntryMsg) wal.Entry {
	return wal.Entry{
		Revision: m.Revision, Term: m.Term, Op: wal.Op(m.Op),
		Key: m.Key, Value: m.Value, Lease: m.Lease,
		CreateRevision: m.CreateRevision, PrevRevision: m.PrevRevision,
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
)

// KVMsg is the wire representation of a key-value record.
type KVMsg struct {
	Key            string `json:"key"`
	Value          []byte `json:"value"`
	Revision       int64  `json:"revision"`
	CreateRevision int64  `json:"create_revision"`
	PrevRevision   int64  `json:"prev_revision"`
	Lease          int64  `json:"lease"`
}

// ForwardRequest encodes a write operation for forwarding to the leader.
type ForwardRequest struct {
	Op       ForwardOp `json:"op"`
	Key      string    `json:"key"`
	Value    []byte    `json:"value,omitempty"`
	Revision int64     `json:"revision,omitempty"` // CAS revision / compact target
	Lease    int64     `json:"lease,omitempty"`
}

// ForwardResponse encodes the leader's reply to a forwarded write.
type ForwardResponse struct {
	Revision  int64  `json:"revision"`
	OldKV     *KVMsg `json:"old_kv,omitempty"` // Update / DeleteIfRevision
	Succeeded bool   `json:"succeeded"`        // CAS result
	ErrCode   string `json:"err_code,omitempty"`
	ErrMsg    string `json:"err_msg,omitempty"`
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
