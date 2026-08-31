// Package t4 provides an embeddable, S3-durable key-value store.
package t4

import istore "github.com/t4db/t4/internal/store"

// KeyValue is a versioned key-value pair.
type KeyValue = istore.KeyValue

// EventType classifies a watch event.
type EventType = istore.EventType

const (
	EventPut      = istore.EventPut
	EventDelete   = istore.EventDelete
	EventProgress = istore.EventProgress
)

// Event is a single watch notification.
type Event = istore.Event
