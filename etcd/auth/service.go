package auth

import (
	"context"

	"go.etcd.io/etcd/api/v3/authpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the etcd v3 Auth gRPC service.
type Service struct {
	etcdserverpb.UnimplementedAuthServer
	store  *Store
	tokens *TokenStore

	// Header, when set, supplies the response header for every RPC, as etcd
	// includes one in all Auth responses. Auth changes do not advance the
	// revision in etcd, so the header reports the current revision.
	Header func() *etcdserverpb.ResponseHeader
}

func (s *Service) header() *etcdserverpb.ResponseHeader {
	if s.Header == nil {
		return nil
	}
	return s.Header()
}

// NewService returns a Service backed by store and tokens.
func NewService(store *Store, tokens *TokenStore) *Service {
	return &Service{store: store, tokens: tokens}
}

// ── Enable / Disable / Status ────────────────────────────────────────────────

func (s *Service) AuthEnable(ctx context.Context, _ *etcdserverpb.AuthEnableRequest) (*etcdserverpb.AuthEnableResponse, error) {
	if err := s.store.Enable(ctx); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "auth enable: %v", err)
	}
	return &etcdserverpb.AuthEnableResponse{Header: s.header()}, nil
}

func (s *Service) AuthDisable(ctx context.Context, _ *etcdserverpb.AuthDisableRequest) (*etcdserverpb.AuthDisableResponse, error) {
	if err := s.store.Disable(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "auth disable: %v", err)
	}
	return &etcdserverpb.AuthDisableResponse{Header: s.header()}, nil
}

func (s *Service) AuthStatus(_ context.Context, _ *etcdserverpb.AuthStatusRequest) (*etcdserverpb.AuthStatusResponse, error) {
	return &etcdserverpb.AuthStatusResponse{
		Header:  s.header(),
		Enabled: s.store.IsEnabled(),
	}, nil
}

// ── Authenticate ─────────────────────────────────────────────────────────────

func (s *Service) Authenticate(_ context.Context, req *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
	if !s.store.IsEnabled() {
		return nil, status.Error(codes.FailedPrecondition, "auth is not enabled")
	}
	if err := s.store.CheckPassword(req.Name, req.Password); err != nil {
		return nil, status.Error(codes.Unauthenticated, "authentication failed")
	}
	tok, err := s.tokens.Generate(req.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate token: %v", err)
	}
	return &etcdserverpb.AuthenticateResponse{Header: s.header(), Token: tok}, nil
}

// ── Users ────────────────────────────────────────────────────────────────────

func (s *Service) UserAdd(ctx context.Context, req *etcdserverpb.AuthUserAddRequest) (*etcdserverpb.AuthUserAddResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "user name is empty")
	}
	// Fail if user already exists.
	if _, err := s.store.GetUser(req.Name); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "user %q already exists", req.Name)
	}
	u := User{Name: req.Name}
	password := req.Password
	if req.Options != nil && req.Options.NoPassword {
		password = ""
	}
	if err := s.store.PutUser(ctx, u, password); err != nil {
		return nil, status.Errorf(codes.Internal, "add user: %v", err)
	}
	return &etcdserverpb.AuthUserAddResponse{Header: s.header()}, nil
}

func (s *Service) UserGet(_ context.Context, req *etcdserverpb.AuthUserGetRequest) (*etcdserverpb.AuthUserGetResponse, error) {
	u, err := s.store.GetUser(req.Name)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	return &etcdserverpb.AuthUserGetResponse{Header: s.header(), Roles: u.Roles}, nil
}

func (s *Service) UserList(_ context.Context, _ *etcdserverpb.AuthUserListRequest) (*etcdserverpb.AuthUserListResponse, error) {
	users, err := s.store.ListUsers()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list users: %v", err)
	}
	names := make([]string, len(users))
	for i, u := range users {
		names[i] = u.Name
	}
	return &etcdserverpb.AuthUserListResponse{Header: s.header(), Users: names}, nil
}

func (s *Service) UserDelete(ctx context.Context, req *etcdserverpb.AuthUserDeleteRequest) (*etcdserverpb.AuthUserDeleteResponse, error) {
	if err := s.store.DeleteUser(ctx, req.Name); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "delete user: %v", err)
	}
	return &etcdserverpb.AuthUserDeleteResponse{Header: s.header()}, nil
}

func (s *Service) UserChangePassword(ctx context.Context, req *etcdserverpb.AuthUserChangePasswordRequest) (*etcdserverpb.AuthUserChangePasswordResponse, error) {
	u, err := s.store.GetUser(req.Name)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	if err := s.store.PutUser(ctx, u, req.Password); err != nil {
		return nil, status.Errorf(codes.Internal, "change password: %v", err)
	}
	return &etcdserverpb.AuthUserChangePasswordResponse{Header: s.header()}, nil
}

func (s *Service) UserGrantRole(ctx context.Context, req *etcdserverpb.AuthUserGrantRoleRequest) (*etcdserverpb.AuthUserGrantRoleResponse, error) {
	if err := s.store.GrantRole(ctx, req.User, req.Role); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "grant role: %v", err)
	}
	return &etcdserverpb.AuthUserGrantRoleResponse{Header: s.header()}, nil
}

func (s *Service) UserRevokeRole(ctx context.Context, req *etcdserverpb.AuthUserRevokeRoleRequest) (*etcdserverpb.AuthUserRevokeRoleResponse, error) {
	if err := s.store.RevokeRole(ctx, req.Name, req.Role); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "revoke role: %v", err)
	}
	return &etcdserverpb.AuthUserRevokeRoleResponse{Header: s.header()}, nil
}

// ── Roles ────────────────────────────────────────────────────────────────────

func (s *Service) RoleAdd(ctx context.Context, req *etcdserverpb.AuthRoleAddRequest) (*etcdserverpb.AuthRoleAddResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "role name is empty")
	}
	if _, err := s.store.GetRole(req.Name); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "role %q already exists", req.Name)
	}
	if err := s.store.PutRole(ctx, Role{Name: req.Name}); err != nil {
		return nil, status.Errorf(codes.Internal, "add role: %v", err)
	}
	return &etcdserverpb.AuthRoleAddResponse{Header: s.header()}, nil
}

func (s *Service) RoleGet(_ context.Context, req *etcdserverpb.AuthRoleGetRequest) (*etcdserverpb.AuthRoleGetResponse, error) {
	r, err := s.store.GetRole(req.Role)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "%v", err)
	}
	perms := make([]*authpb.Permission, len(r.Permissions))
	for i, p := range r.Permissions {
		perms[i] = &authpb.Permission{
			PermType: p.PermType,
			Key:      []byte(p.Key),
			RangeEnd: []byte(p.RangeEnd),
		}
	}
	return &etcdserverpb.AuthRoleGetResponse{Header: s.header(), Perm: perms}, nil
}

func (s *Service) RoleList(_ context.Context, _ *etcdserverpb.AuthRoleListRequest) (*etcdserverpb.AuthRoleListResponse, error) {
	roles, err := s.store.ListRoles()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list roles: %v", err)
	}
	names := make([]string, len(roles))
	for i, r := range roles {
		names[i] = r.Name
	}
	return &etcdserverpb.AuthRoleListResponse{Header: s.header(), Roles: names}, nil
}

func (s *Service) RoleDelete(ctx context.Context, req *etcdserverpb.AuthRoleDeleteRequest) (*etcdserverpb.AuthRoleDeleteResponse, error) {
	if err := s.store.DeleteRole(ctx, req.Role); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "delete role: %v", err)
	}
	return &etcdserverpb.AuthRoleDeleteResponse{Header: s.header()}, nil
}

func (s *Service) RoleGrantPermission(ctx context.Context, req *etcdserverpb.AuthRoleGrantPermissionRequest) (*etcdserverpb.AuthRoleGrantPermissionResponse, error) {
	if req.Perm == nil {
		return nil, status.Error(codes.InvalidArgument, "permission is nil")
	}
	p := Permission{
		Key:      string(req.Perm.Key),
		RangeEnd: string(req.Perm.RangeEnd),
		PermType: req.Perm.PermType,
	}
	if err := s.store.GrantPermission(ctx, req.Name, p); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "grant permission: %v", err)
	}
	return &etcdserverpb.AuthRoleGrantPermissionResponse{Header: s.header()}, nil
}

func (s *Service) RoleRevokePermission(ctx context.Context, req *etcdserverpb.AuthRoleRevokePermissionRequest) (*etcdserverpb.AuthRoleRevokePermissionResponse, error) {
	if err := s.store.RevokePermission(ctx, req.Role, string(req.Key), string(req.RangeEnd)); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "revoke permission: %v", err)
	}
	return &etcdserverpb.AuthRoleRevokePermissionResponse{Header: s.header()}, nil
}
