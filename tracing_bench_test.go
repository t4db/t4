package t4_test

// Benchmarks that measure the overhead of OTel tracing at different levels of
// provider fidelity:
//
//   - noop:     trace.NewNoopTracerProvider() — spans never created; minimum overhead
//   - sdk_noop: real OTel SDK with a discard exporter — full span lifecycle, no export I/O
//
// Run and compare:
//
//	go test -bench=BenchmarkTracingOverhead -benchtime=5s -count=6 . | tee tracing.txt
//	benchstat tracing.txt   # within-file comparison by sub-benchmark name

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/t4db/t4"
)

// discardExporter implements sdktrace.SpanExporter by discarding every span.
type discardExporter struct{}

func (discardExporter) ExportSpans(_ context.Context, _ []sdktrace.ReadOnlySpan) error { return nil }
func (discardExporter) Shutdown(_ context.Context) error                               { return nil }

func openBenchNodeTP(b *testing.B, tp trace.TracerProvider) *t4.Node {
	b.Helper()
	n, err := t4.Open(t4.Config{DataDir: b.TempDir(), TracerProvider: tp})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = n.Close() })
	return n
}

func tracerProviders(b *testing.B) map[string]trace.TracerProvider {
	b.Helper()
	sdkTP := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(discardExporter{}),
	)
	b.Cleanup(func() { _ = sdkTP.Shutdown(context.Background()) })
	return map[string]trace.TracerProvider{
		"noop":     noop.NewTracerProvider(),
		"sdk_noop": sdkTP,
	}
}

func BenchmarkTracingOverheadPut(b *testing.B) {
	for name, tp := range tracerProviders(b) {
		b.Run(name, func(b *testing.B) {
			n := openBenchNodeTP(b, tp)
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := n.Put(ctx, fmt.Sprintf("/bench/put/%d", i), []byte("v"), 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTracingOverheadPutParallel(b *testing.B) {
	for name, tp := range tracerProviders(b) {
		b.Run(name, func(b *testing.B) {
			n := openBenchNodeTP(b, tp)
			ctx := context.Background()
			var i atomic.Int64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					k := i.Add(1)
					if _, err := n.Put(ctx, fmt.Sprintf("/bench/pp/%d", k), []byte("v"), 0); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkTracingOverheadGet(b *testing.B) {
	for name, tp := range tracerProviders(b) {
		b.Run(name, func(b *testing.B) {
			n := openBenchNodeTP(b, tp)
			ctx := context.Background()
			if _, err := n.Put(ctx, "/bench/get", []byte("value"), 0); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := n.Get("/bench/get"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTracingOverheadLinearizableGet(b *testing.B) {
	for name, tp := range tracerProviders(b) {
		b.Run(name, func(b *testing.B) {
			n := openBenchNodeTP(b, tp)
			ctx := context.Background()
			if _, err := n.Put(ctx, "/bench/lget", []byte("value"), 0); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := n.LinearizableGet(ctx, "/bench/lget"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
