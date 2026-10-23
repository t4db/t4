package t4

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestForwardErrorReportsCutShortForwardsAsTimeout checks the error a
// follower reports for a forward its peer connection cut short: not the
// connection's Canceled or DeadlineExceeded, which etcd clients answer by
// sending a write again although the leader may have applied it.
func TestForwardErrorReportsCutShortForwardsAsTimeout(t *testing.T) {
	live := context.Background()
	ended, cancel := context.WithCancel(context.Background())
	cancel()

	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want error
	}{
		{"connection closed", live, status.Error(codes.Canceled, "grpc: the client connection is closing"), ErrTimeout},
		{"connection timed out", live, status.Error(codes.DeadlineExceeded, "context deadline exceeded"), ErrTimeout},
		{"leader unreachable", live, status.Error(codes.Unavailable, "connection refused"), ErrNoLeader},
	} {
		if got := forwardError(tc.ctx, tc.err); !errors.Is(got, tc.want) {
			t.Errorf("%s: forwardError = %v, want %v", tc.name, got, tc.want)
		}
	}

	// The caller gave up: its own error stands, and the client does not
	// send again for an ended context.
	canceled := status.Error(codes.Canceled, "context canceled")
	if got := forwardError(ended, canceled); got != canceled {
		t.Errorf("caller canceled: forwardError = %v, want the caller's own error", got)
	}
}
