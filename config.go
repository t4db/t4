package strata

import (
	"time"

	"github.com/makhov/strata/internal/object"
)

// Config holds all configuration for a Node.
type Config struct {
	// DataDir is the directory used for local Pebble data and WAL segments.
	// Required.
	DataDir string

	// ObjectStore is used to archive WAL segments and checkpoints.
	// If nil, durability is local-only (useful for development/testing).
	ObjectStore object.Store

	// SegmentMaxSize is the byte threshold that triggers WAL segment rotation.
	// Default: 50 MB.
	SegmentMaxSize int64

	// SegmentMaxAge is the time threshold that triggers WAL segment rotation.
	// Default: 10 s.
	SegmentMaxAge time.Duration

	// CheckpointInterval controls how often the node writes a full checkpoint.
	// Default: 15 minutes.
	CheckpointInterval time.Duration

	// CheckpointEntries triggers a checkpoint after this many WAL entries
	// regardless of time. 0 means disabled.
	CheckpointEntries int64
}

func (c *Config) setDefaults() {
	if c.SegmentMaxSize == 0 {
		c.SegmentMaxSize = 50 << 20
	}
	if c.SegmentMaxAge == 0 {
		c.SegmentMaxAge = 10 * time.Second
	}
	if c.CheckpointInterval == 0 {
		c.CheckpointInterval = 15 * time.Minute
	}
}
