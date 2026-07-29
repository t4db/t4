package t4

import (
	"testing"
	"time"
)

func TestAutoCompactSampleIntervalDefaults(t *testing.T) {
	tests := []struct {
		name      string
		retention time.Duration
		want      time.Duration
	}{
		{name: "minimum", retention: 10 * time.Second, want: time.Minute},
		{name: "retention fraction", retention: 7 * time.Hour, want: time.Hour},
		{name: "daily for weekly retention", retention: 7 * 24 * time.Hour, want: 24 * time.Hour},
		{name: "maximum", retention: 30 * 24 * time.Hour, want: 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{AutoCompactRetention: tt.retention}
			cfg.setDefaults()
			if cfg.AutoCompactMode != AutoCompactTime {
				t.Fatalf("AutoCompactMode: want %q got %q", AutoCompactTime, cfg.AutoCompactMode)
			}
			if cfg.AutoCompactSampleInterval != tt.want {
				t.Fatalf("AutoCompactSampleInterval: want %s got %s", tt.want, cfg.AutoCompactSampleInterval)
			}
			if cfg.AutoCompactInterval != tt.want {
				t.Fatalf("AutoCompactInterval: want %s got %s", tt.want, cfg.AutoCompactInterval)
			}
		})
	}
}

func TestAutoCompactRevisionDefaults(t *testing.T) {
	cfg := Config{AutoCompactRevisionRetention: 1000}
	cfg.setDefaults()
	if cfg.AutoCompactMode != AutoCompactRevision {
		t.Fatalf("AutoCompactMode: want %q got %q", AutoCompactRevision, cfg.AutoCompactMode)
	}
	if cfg.AutoCompactInterval != time.Minute {
		t.Fatalf("AutoCompactInterval: want %s got %s", time.Minute, cfg.AutoCompactInterval)
	}
}
