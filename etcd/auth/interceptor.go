package auth

import (
	"context"
	"strings"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// openMethods are always allowed regardless of auth state (auth bootstrapping
// and health endpoints).
var openMethods = map[string]bool{
	"/etcdserverpb.Auth/Authenticate": true,
	"/etcdserverpb.Auth/AuthEnable":   true,
	"/etcdserverpb.Auth/AuthDisable":  true,
	"/etcdserverpb.Auth/AuthStatus":   true,
	"/grpc.health.v1.Health/Check":    true,
	"/grpc.health.v1.Health/Watch":    true,
}

// Interceptors returns gRPC server options that enforce token validation and
// RBAC. If store is nil, the interceptors are no-ops (auth disabled at compile
// time).
func Interceptors(store *Store, tokens *TokenStore) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	return unaryInterceptor(store, tokens), streamInterceptor(store, tokens)
}

func unaryInterceptor(store *Store, tokens *TokenStore) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := checkAuth(ctx, store, tokens, info.FullMethod, req)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// streamInterceptor validates the token when the stream opens. The requests
// arrive later: a single-request stream is checked when its request is
// received, and Watch checks each watch it creates itself, since denying
// one must cancel that watch rather than the stream carrying the others.
func streamInterceptor(store *Store, tokens *TokenStore) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := checkAuth(ss.Context(), store, tokens, info.FullMethod, nil)
		if err != nil {
			return err
		}
		return handler(srv, &checkedStream{ServerStream: ss, ctx: ctx, check: func(req any) error {
			_, err := checkAuth(ctx, store, tokens, info.FullMethod, req)
			return err
		}})
	}
}

// checkedStream gives a stream the context checkAuth returned and checks
// every request it receives.
type checkedStream struct {
	grpc.ServerStream
	ctx   context.Context
	check func(req any) error
}

func (s *checkedStream) Context() context.Context { return s.ctx }

func (s *checkedStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	if _, isWatch := m.(*etcdserverpb.WatchRequest); isWatch {
		return nil // checked per watch by the Watch handler
	}
	return s.check(m)
}

type userKey struct{}

// UserFromContext returns the user a request authenticated as, if auth is
// enabled and it did.
func UserFromContext(ctx context.Context) (string, bool) {
	u, ok := ctx.Value(userKey{}).(string)
	return u, ok
}

// checkAuth validates the token (when auth is enabled) and enforces RBAC on
// every range req touches, returning ctx with the user it authenticated as.
// A nil req is checked for the token only.
func checkAuth(ctx context.Context, store *Store, tokens *TokenStore, method string, req any) (context.Context, error) {
	// Auth service + health endpoints are always open.
	if openMethods[method] {
		return ctx, nil
	}
	checks := RequestChecks(req)

	if !store.IsEnabled() {
		// Block access to auth-reserved keys even when auth is off, to prevent
		// clients from pre-loading \x00auth/ keys that would be mistaken for auth
		// data when auth is later enabled.
		return ctx, checkAuthKeyAccess(method, checks)
	}

	// Extract token from gRPC metadata.
	username, err := tokenFromCtx(ctx, tokens)
	if err != nil {
		return ctx, err
	}
	ctx = context.WithValue(ctx, userKey{}, username)

	// Block access to the reserved auth namespace from the KV service.
	if err := checkAuthKeyAccess(method, checks); err != nil {
		return ctx, err
	}
	if err := store.CheckRequest(username, checks); err != nil {
		return ctx, err
	}
	return ctx, nil
}

// CheckRequest returns etcd's permission denied error unless userName may
// make every check.
func (s *Store) CheckRequest(userName string, checks []RangeCheck) error {
	for _, c := range checks {
		if err := s.CheckRangePermission(userName, c.Key, c.RangeEnd, c.PermType); err != nil {
			return rpctypes.ErrGRPCPermissionDenied
		}
	}
	return nil
}

// tokenFromCtx extracts the bearer token from gRPC metadata and looks up the
// username.
func tokenFromCtx(ctx context.Context, tokens *TokenStore) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get("token")
	if len(vals) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing token")
	}
	username, ok := tokens.Lookup(vals[0])
	if !ok {
		return "", status.Error(codes.Unauthenticated, "invalid or expired token")
	}
	return username, nil
}

// checkAuthKeyAccess returns PermissionDenied if a KV request touches a key
// in the reserved \x00auth/ namespace.
func checkAuthKeyAccess(method string, checks []RangeCheck) error {
	if !strings.HasPrefix(method, "/etcdserverpb.KV/") {
		return nil
	}
	for _, c := range checks {
		if IsAuthPrefix(c.Key) {
			return status.Error(codes.PermissionDenied, "access to internal auth namespace is not allowed")
		}
	}
	return nil
}

// RangeCheck is a range of keys a request touches and the access it needs:
// Key alone if RangeEnd is empty, every key from Key on if it is "\x00".
type RangeCheck struct {
	Key, RangeEnd string
	PermType      PermType
}

// RequestChecks returns every range req touches, with the access each needs,
// as etcd checks them: a read for Range and RangeStream, a write for Put and
// DeleteRange and a read too when they return the previous value, a read for
// each compare of a Txn and its operations' checks, and a read for a Watch
// create request.
func RequestChecks(req any) []RangeCheck {
	switch r := req.(type) {
	case *etcdserverpb.RangeRequest:
		return []RangeCheck{{string(r.Key), string(r.RangeEnd), READ}}
	case *etcdserverpb.PutRequest:
		return putChecks(r)
	case *etcdserverpb.DeleteRangeRequest:
		return deleteChecks(r)
	case *etcdserverpb.TxnRequest:
		return txnChecks(r)
	case *etcdserverpb.WatchRequest:
		if cr := r.GetCreateRequest(); cr != nil {
			return WatchChecks(cr)
		}
	}
	return nil
}

// WatchChecks returns the checks a watch create request needs.
func WatchChecks(cr *etcdserverpb.WatchCreateRequest) []RangeCheck {
	return []RangeCheck{{string(cr.Key), string(cr.RangeEnd), READ}}
}

func putChecks(r *etcdserverpb.PutRequest) []RangeCheck {
	checks := []RangeCheck{{string(r.Key), "", WRITE}}
	if r.PrevKv {
		checks = append(checks, RangeCheck{string(r.Key), "", READ})
	}
	return checks
}

func deleteChecks(r *etcdserverpb.DeleteRangeRequest) []RangeCheck {
	checks := []RangeCheck{{string(r.Key), string(r.RangeEnd), WRITE}}
	if r.PrevKv {
		checks = append(checks, RangeCheck{string(r.Key), string(r.RangeEnd), READ})
	}
	return checks
}

func txnChecks(r *etcdserverpb.TxnRequest) []RangeCheck {
	var checks []RangeCheck
	for _, c := range r.Compare {
		checks = append(checks, RangeCheck{string(c.Key), string(c.RangeEnd), READ})
	}
	for _, ops := range [][]*etcdserverpb.RequestOp{r.Success, r.Failure} {
		for _, op := range ops {
			switch v := op.Request.(type) {
			case *etcdserverpb.RequestOp_RequestRange:
				if v.RequestRange != nil {
					checks = append(checks, RequestChecks(v.RequestRange)...)
				}
			case *etcdserverpb.RequestOp_RequestPut:
				if v.RequestPut != nil {
					checks = append(checks, putChecks(v.RequestPut)...)
				}
			case *etcdserverpb.RequestOp_RequestDeleteRange:
				if v.RequestDeleteRange != nil {
					checks = append(checks, deleteChecks(v.RequestDeleteRange)...)
				}
			case *etcdserverpb.RequestOp_RequestTxn:
				if v.RequestTxn != nil {
					checks = append(checks, txnChecks(v.RequestTxn)...)
				}
			}
		}
	}
	return checks
}
