package strata_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/makhov/strata"
)

func openBenchNode(b *testing.B) *strata.Node {
	b.Helper()
	n, err := strata.Open(strata.Config{DataDir: b.TempDir()})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { n.Close() })
	return n
}

func BenchmarkPut(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.Put(ctx, fmt.Sprintf("/bench/put/%d", i), []byte("value"), 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutSameKey(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.Put(ctx, "/bench/same", []byte("value"), 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	n := openBenchNode(b)
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
}

func BenchmarkCreate(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.Create(ctx, fmt.Sprintf("/bench/create/%d", i), []byte("v"), 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	rev, err := n.Put(ctx, "/bench/update", []byte("v0"), 0)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		newRev, _, _, err := n.Update(ctx, "/bench/update", []byte("v"), rev, 0)
		if err != nil {
			b.Fatal(err)
		}
		rev = newRev
	}
}

func BenchmarkDelete(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		key := fmt.Sprintf("/bench/del/%d", i)
		if _, err := n.Put(ctx, key, []byte("v"), 0); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := n.Delete(ctx, key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkList(b *testing.B) {
	n := openBenchNode(b)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if _, err := n.Put(ctx, fmt.Sprintf("/bench/list/%04d", i), []byte("v"), 0); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := n.List("/bench/list/"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWatch(b *testing.B) {
	n := openBenchNode(b)
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	ch, err := n.Watch(ctx, "/bench/watch/", 0)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n.Put(ctx, fmt.Sprintf("/bench/watch/%d", i), []byte("v"), 0)
		<-ch
	}
}
