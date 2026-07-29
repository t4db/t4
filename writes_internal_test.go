package t4

import "testing"

func TestTxnStatsKind(t *testing.T) {
	tests := []struct {
		name  string
		stats txnStats
		want  string
	}{
		{name: "none", want: "none"},
		{name: "create", stats: txnStats{creates: 1}, want: "create"},
		{name: "update", stats: txnStats{updates: 1}, want: "update"},
		{name: "delete", stats: txnStats{deletes: 1}, want: "delete"},
		{name: "mixed_create_delete", stats: txnStats{creates: 1, deletes: 1}, want: "mixed"},
		{name: "mixed_create_update", stats: txnStats{creates: 2, updates: 1}, want: "mixed"},
		{name: "mixed_all", stats: txnStats{creates: 1, updates: 1, deletes: 1}, want: "mixed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.stats.kind(); got != tt.want {
				t.Fatalf("kind() = %q, want %q", got, tt.want)
			}
		})
	}
}
