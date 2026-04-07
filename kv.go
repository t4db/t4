// Package t4 provides an embeddable, S3-durable key-value store.
package t4

// KeyValue is a versioned key-value pair.
type KeyValue struct {
	Key            string
	Value          []byte
	Revision       int64
	CreateRevision int64
	PrevRevision   int64
	Lease          int64
}

// EventType classifies a watch event.
type EventType int

const (
	EventPut    EventType = iota // create or update
	EventDelete                  // deletion
)

// Event is a single watch notification.
type Event struct {
	Type   EventType
	KV     *KeyValue
	PrevKV *KeyValue // nil for creates
}
