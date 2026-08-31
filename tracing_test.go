package t4

import (
	"context"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// recordingProvider returns a TracerProvider that keeps every *ended* span in
// memory. A span only reaches the exporter when End has been called, so
// presence in the recorder is proof of a complete span lifecycle.
func recordingProvider(t *testing.T) (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	return sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)), rec
}

func spanNames(rec *tracetest.SpanRecorder) []string {
	var out []string
	for _, s := range rec.Ended() {
		out = append(out, s.Name())
	}
	return out
}

func hasSpan(rec *tracetest.SpanRecorder, name string) bool {
	for _, n := range spanNames(rec) {
		if n == name {
			return true
		}
	}
	return false
}

// TestTracingWritePathSpans checks the shape a successful write produces: the
// caller's span plus commit-loop children, with the children parented to it.
func TestTracingWritePathSpans(t *testing.T) {
	tp, rec := recordingProvider(t)
	n, err := Open(Config{DataDir: t.TempDir(), TracerProvider: tp})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		_ = n.Close()
	}()

	if _, err := n.Put(context.Background(), "/registry/pods/default/nginx", []byte("v"), 0); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if !hasSpan(rec, "t4.put") {
		t.Fatalf("no t4.put span; got %v", spanNames(rec))
	}
	if !hasSpan(rec, "t4.wal.append") {
		t.Fatalf("no t4.wal.append span; got %v", spanNames(rec))
	}

	var put, walAppend sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		switch s.Name() {
		case "t4.put":
			put = s
		case "t4.wal.append":
			walAppend = s
		}
	}
	if walAppend.Parent().SpanID() != put.SpanContext().SpanID() {
		t.Error("t4.wal.append is not a child of t4.put")
	}
	if !walAppend.StartTime().After(put.StartTime()) {
		t.Error("t4.wal.append should start after its parent")
	}
}

// TestTracingNeverRecordsKeyNames is the security property. Span attributes are
// exported to a collector and retained there; full keys would leak object
// identities (for Kubernetes: secret, namespace and workload names) at
// unbounded cardinality. Only the resource-type scope may appear.
func TestTracingNeverRecordsKeyNames(t *testing.T) {
	tp, rec := recordingProvider(t)
	n, err := Open(Config{DataDir: t.TempDir(), TracerProvider: tp})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		_ = n.Close()
	}()

	const secret = "top-secret-name"
	if _, err := n.Put(context.Background(), "/registry/secrets/prod/"+secret, []byte("v"), 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := n.LinearizableGet(context.Background(), "/registry/secrets/prod/"+secret); err != nil {
		t.Fatalf("LinearizableGet: %v", err)
	}

	for _, s := range rec.Ended() {
		for _, kv := range s.Attributes() {
			v := kv.Value.String()
			if v == "" {
				continue
			}
			if containsStr(v, secret) || containsStr(v, "prod") {
				t.Errorf("span %q attribute %s=%q leaks a key identity", s.Name(), kv.Key, v)
			}
		}
	}
}

func containsStr(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// TestTracingSpanEndedWhenWriteAbandoned is the regression test for the failure
// mode that matters most: a write whose caller gives up while the commit loop
// is still busy. If the entry-point span is not ended on that path the whole
// trace never reaches the exporter — losing exactly the traces that were worth
// collecting. Run under -race, which also covers the commit loop writing the
// request's phase timings while the caller has already returned.
func TestTracingSpanEndedWhenWriteAbandoned(t *testing.T) {
	tp, rec := recordingProvider(t)
	n, err := Open(Config{DataDir: t.TempDir(), TracerProvider: tp})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	ctx := context.Background()
	if _, err := n.Put(ctx, "/registry/pods/default/warmup", []byte("v"), 0); err != nil {
		t.Fatalf("warmup Put: %v", err)
	}

	// Wedge the commit loop so the next write cannot complete.
	fw := newFakeWAL(n)
	blockC := make(chan struct{})
	fw.setBlockChan(blockC)
	defer func() {
		_ = n.Close()
	}()
	defer close(blockC)

	writeCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err := n.Put(writeCtx, "/registry/pods/default/abandoned", []byte("v"), 0); err == nil {
		t.Fatal("expected the abandoned write to fail")
	}

	// The span must have been ended despite the caller bailing out.
	var puts int
	for _, s := range rec.Ended() {
		if s.Name() == "t4.put" {
			puts++
		}
	}
	if puts < 2 {
		t.Errorf("abandoned write did not end its span: only %d t4.put spans ended, want 2 (%v)",
			puts, spanNames(rec))
	}
}

func TestKeyScope(t *testing.T) {
	for _, tc := range []struct{ key, want string }{
		{"/registry/pods/default/nginx-abc123", "/registry/pods"},
		{"/registry/secrets/kube-system/token", "/registry/secrets"},
		{"/registry/pods/", "/registry/pods"},
		{"/a/b", "/a"},
		{"/a", ""},
		{"a", ""},
		{"", ""},
	} {
		if got := keyScope(tc.key); got != tc.want {
			t.Errorf("keyScope(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}
