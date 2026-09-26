// Package wal implements the write-ahead log.
//
// Each write is appended to a local segment file (fsync'd) before being
// applied to the state machine. Sealed segments are uploaded to object storage
// asynchronously; they are kept locally until the upload is confirmed.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Op identifies the type of a WAL entry.
type Op uint8

const (
	OpCreate  Op = 1
	OpUpdate  Op = 2
	OpDelete  Op = 3
	OpCompact Op = 4
	// OpTxn is a multi-key atomic transaction. Key is empty; sub-operations
	// are encoded in Value using EncodeTxnOps / DecodeTxnOps.
	OpTxn Op = 5
	// OpMetaPut sets Key to Value in the meta keyspace: unversioned node
	// metadata that has no history, is never watched, and does not advance
	// the user-visible revision. Requires WAL format 3.
	OpMetaPut Op = 6
	// OpMetaDelete removes Key from the meta keyspace. Requires WAL format 3.
	OpMetaDelete Op = 7
)

// ErrUnknownOp is returned when a WAL entry or txn sub-operation carries an op
// code this binary does not understand. It usually means the entry was written
// by a newer T4 release. Callers must fail closed: guessing the semantics of an
// unknown op (for example, treating it as a put) silently corrupts state.
var ErrUnknownOp = errors.New("wal: unknown op (written by a newer T4 release?)")

// Known reports whether o is an entry-level op code understood by this binary.
func (o Op) Known() bool { return o >= OpCreate && o <= OpMetaDelete }

// IsMeta reports whether o writes the meta keyspace.
func (o Op) IsMeta() bool { return o == OpMetaPut || o == OpMetaDelete }

// knownTxnSubOp reports whether o is valid inside an OpTxn payload.
func knownTxnSubOp(o Op) bool {
	return o == OpCreate || o == OpUpdate || o == OpDelete || o.IsMeta()
}

// ConsumesRevision reports whether applying e advances the user-visible
// revision: true for data writes, false for compaction, meta ops, and
// transactions whose sub-operations are all meta ops. Entries that do not
// consume a revision carry the revision of the preceding data write, so
// anything keyed by revision (log records, pending reads, term-conflict
// cleanup) must skip them.
func (e *Entry) ConsumesRevision() bool {
	switch e.Op {
	case OpCreate, OpUpdate, OpDelete:
		return true
	case OpTxn:
		s, err := scanTxnOps(e.Value)
		// An undecodable payload is rejected on apply; treat it as data so
		// callers never skip revision-keyed safety work for it.
		return err != nil || s.data > 0
	default:
		return false
	}
}

// RequiredFormat returns the lowest WAL segment format that may contain e.
// Meta ops, standalone or inside a transaction, require format 3 so that
// binaries predating them refuse the segment instead of misapplying it.
func RequiredFormat(e *Entry) int {
	switch e.Op {
	case OpMetaPut, OpMetaDelete:
		return formatMeta
	case OpTxn:
		if s, err := scanTxnOps(e.Value); err == nil && s.meta > 0 {
			return formatMeta
		}
	}
	return formatBase
}

// ValidateEntry checks that e and, for OpTxn, every sub-operation carry op
// codes understood by this binary. It returns an error wrapping ErrUnknownOp
// otherwise.
func ValidateEntry(e *Entry) error {
	if !e.Op.Known() {
		return fmt.Errorf("%w: op=%d seq=%d rev=%d", ErrUnknownOp, e.Op, e.Sequence(), e.Revision)
	}
	if e.Op == OpTxn {
		if _, err := scanTxnOps(e.Value); err != nil {
			return fmt.Errorf("seq=%d rev=%d: %w", e.Sequence(), e.Revision, err)
		}
	}
	return nil
}

// Entry is one record in the write-ahead log.
type Entry struct {
	// ID is the WAL/peer-stream identity. It is unique for every WAL entry,
	// including metadata-only entries such as compaction. Older WAL entries have
	// ID==0; in that case Revision is also the identity.
	ID             int64
	Revision       int64
	Term           uint64
	Op             Op
	Key            string
	Value          []byte
	Lease          int64
	CreateRevision int64 // meaningful for Update/Delete
	PrevRevision   int64 // meaningful for Update/Delete
	Version        int64 // etcd-style per-key modification count
}

// Sequence returns the identity used for WAL ordering and peer-stream ACKs.
func (e Entry) Sequence() int64 {
	if e.ID != 0 {
		return e.ID
	}
	return e.Revision
}

// TxnSubOp is one operation within an OpTxn entry.
// Op must be OpCreate, OpUpdate, or OpDelete.
type TxnSubOp struct {
	Op             Op
	Key            string
	Value          []byte
	Lease          int64
	CreateRevision int64
	PrevRevision   int64
	Version        int64
}

// txnSubOpFixedSizeV1: op(1) + lease(8) + createRev(8) + prevRev(8) + keyLen(4) + valLen(4) = 33
// txnSubOpFixedSize: txnSubOpFixedSizeV1 + version(8) = 41
const (
	txnOpsVersionedFlag = uint32(1 << 31)
	txnSubOpFixedSizeV1 = 33
	txnSubOpFixedSize   = txnSubOpFixedSizeV1 + 8
)

// EncodeTxnOps encodes a slice of TxnSubOp into a byte slice for storage in
// an OpTxn Entry's Value field.
func EncodeTxnOps(ops []TxnSubOp) []byte {
	total := 4 // uint32 count prefix
	for i := range ops {
		total += txnSubOpFixedSize + len(ops[i].Key) + len(ops[i].Value)
	}
	buf := make([]byte, total)
	binary.BigEndian.PutUint32(buf[0:4], txnOpsVersionedFlag|uint32(len(ops)))
	off := 4
	for i := range ops {
		o := &ops[i]
		buf[off] = byte(o.Op)
		binary.BigEndian.PutUint64(buf[off+1:], uint64(o.Lease))
		binary.BigEndian.PutUint64(buf[off+9:], uint64(o.CreateRevision))
		binary.BigEndian.PutUint64(buf[off+17:], uint64(o.PrevRevision))
		binary.BigEndian.PutUint64(buf[off+25:], uint64(o.Version))
		binary.BigEndian.PutUint32(buf[off+33:], uint32(len(o.Key)))
		binary.BigEndian.PutUint32(buf[off+37:], uint32(len(o.Value)))
		off += txnSubOpFixedSize
		copy(buf[off:], o.Key)
		off += len(o.Key)
		copy(buf[off:], o.Value)
		off += len(o.Value)
	}
	return buf
}

// DecodeTxnOps decodes a byte slice produced by EncodeTxnOps. It returns an
// error wrapping ErrUnknownOp if any sub-operation has an unknown op code.
func DecodeTxnOps(b []byte) ([]TxnSubOp, error) {
	ops, _, err := decodeTxnOps(b, true)
	return ops, err
}

// txnSummary counts the data and meta sub-operations in a txn payload.
type txnSummary struct{ data, meta int }

// scanTxnOps validates framing and op codes of a txn payload and counts its
// sub-operations without allocating them.
func scanTxnOps(b []byte) (txnSummary, error) {
	_, s, err := decodeTxnOps(b, false)
	return s, err
}

// decodeTxnOps parses and validates a txn payload. When keep is false it only
// validates framing and op codes, without allocating the decoded ops.
func decodeTxnOps(b []byte, keep bool) ([]TxnSubOp, txnSummary, error) {
	var sum txnSummary
	if len(b) < 4 {
		return nil, sum, fmt.Errorf("wal: txn ops payload too short (%d bytes)", len(b))
	}
	rawCount := binary.BigEndian.Uint32(b[0:4])
	versioned := rawCount&txnOpsVersionedFlag != 0
	count := int(rawCount &^ txnOpsVersionedFlag)
	fixedSize := txnSubOpFixedSizeV1
	if versioned {
		fixedSize = txnSubOpFixedSize
	}
	var ops []TxnSubOp
	if keep {
		// Bound the allocation by what the payload can actually hold, so a
		// corrupt count cannot trigger a huge allocation.
		ops = make([]TxnSubOp, 0, min(count, (len(b)-4)/fixedSize))
	}
	off := 4
	for i := 0; i < count; i++ {
		if len(b)-off < fixedSize {
			return nil, sum, fmt.Errorf("wal: txn sub-op %d header truncated", i)
		}
		op := Op(b[off])
		if !knownTxnSubOp(op) {
			return nil, sum, fmt.Errorf("%w: txn sub-op %d has op=%d", ErrUnknownOp, i, op)
		}
		if op.IsMeta() {
			sum.meta++
		} else {
			sum.data++
		}
		keyLenOffset := off + 25
		if versioned {
			keyLenOffset = off + 33
		}
		keyLen := int(binary.BigEndian.Uint32(b[keyLenOffset:]))
		valLen := int(binary.BigEndian.Uint32(b[keyLenOffset+4:]))
		hdr := off
		off += fixedSize
		if len(b)-off < keyLen+valLen {
			return nil, sum, fmt.Errorf("wal: txn sub-op %d payload truncated (need %d, have %d)", i, keyLen+valLen, len(b)-off)
		}
		if !keep {
			off += keyLen + valLen
			continue
		}
		o := TxnSubOp{Op: op}
		o.Lease = int64(binary.BigEndian.Uint64(b[hdr+1:]))
		o.CreateRevision = int64(binary.BigEndian.Uint64(b[hdr+9:]))
		o.PrevRevision = int64(binary.BigEndian.Uint64(b[hdr+17:]))
		if versioned {
			o.Version = int64(binary.BigEndian.Uint64(b[hdr+25:]))
		}
		o.Key = string(b[off : off+keyLen])
		off += keyLen
		if valLen > 0 {
			o.Value = make([]byte, valLen)
			copy(o.Value, b[off:])
		}
		off += valLen
		ops = append(ops, o)
	}
	return ops, sum, nil
}

// Wire layout:
//
//	[4: payload_len uint32 BE]
//	[4: crc32c      uint32 BE]
//	[payload_len bytes: entry data]
//
// Entry data:
//
//	[1:  op]
//	[8:  revision        int64  BE]
//	[8:  term            uint64 BE]
//	[8:  lease           int64  BE]
//	[8:  create_revision int64  BE]
//	[8:  prev_revision   int64  BE]
//	[8:  id              int64  BE] // v2+
//	[8:  version         int64  BE] // v2+
//	[4:  key_len         uint32 BE]
//	[4:  val_len         uint32 BE]
//	[key_len: key bytes]
//	[val_len: value bytes]
const (
	entryFixedSizeV1 = 1 + 8 + 8 + 8 + 8 + 8 + 4 + 4 // 49 bytes
	entryFixedSize   = entryFixedSizeV1 + 8          // 57 bytes
	entryFixedSizeV3 = entryFixedSize + 8            // 65 bytes
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// AppendEntry encodes e as a framed record and writes it to w.
func AppendEntry(w io.Writer, e *Entry) error {
	payload := marshalEntry(e)
	frame := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.Checksum(payload, crcTable))
	copy(frame[8:], payload)
	_, err := w.Write(frame)
	return err
}

// ReadEntry reads and decodes the next framed record from r.
// Returns nil, io.EOF when the stream is cleanly exhausted.
// Returns nil, io.EOF on a truncated last frame (crash before fsync completed).
func ReadEntry(r io.Reader) (*Entry, error) {
	return ReadEntryVersion(r, WALFormatVersion)
}

// ReadEntryVersion reads an entry encoded for a specific WAL segment format.
func ReadEntryVersion(r io.Reader, version int) (*Entry, error) {
	var hdr [8]byte
	_, err := io.ReadFull(r, hdr[:])
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, err
	}

	payloadLen := binary.BigEndian.Uint32(hdr[0:4])
	wantCRC := binary.BigEndian.Uint32(hdr[4:8])

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		// Truncated entry — treat as clean EOF (crash during append).
		return nil, io.EOF
	}

	gotCRC := crc32.Checksum(payload, crcTable)
	if gotCRC != wantCRC {
		return nil, fmt.Errorf("wal: CRC mismatch (want %08x got %08x)", wantCRC, gotCRC)
	}

	return unmarshalEntryVersion(payload, version)
}

func marshalEntry(e *Entry) []byte {
	buf := make([]byte, entryFixedSizeV3+len(e.Key)+len(e.Value))
	buf[0] = byte(e.Op)
	binary.BigEndian.PutUint64(buf[1:9], uint64(e.Revision))
	binary.BigEndian.PutUint64(buf[9:17], e.Term)
	binary.BigEndian.PutUint64(buf[17:25], uint64(e.Lease))
	binary.BigEndian.PutUint64(buf[25:33], uint64(e.CreateRevision))
	binary.BigEndian.PutUint64(buf[33:41], uint64(e.PrevRevision))
	binary.BigEndian.PutUint64(buf[41:49], uint64(e.Sequence()))
	binary.BigEndian.PutUint64(buf[49:57], uint64(e.Version))
	binary.BigEndian.PutUint32(buf[57:61], uint32(len(e.Key)))
	binary.BigEndian.PutUint32(buf[61:65], uint32(len(e.Value)))
	copy(buf[65:], e.Key)
	copy(buf[65+len(e.Key):], e.Value)
	return buf
}

func unmarshalEntry(b []byte) (*Entry, error) {
	return unmarshalEntryVersion(b, WALFormatVersion)
}

func unmarshalEntryVersion(b []byte, version int) (*Entry, error) {
	if len(b) < entryFixedSizeV1 {
		return nil, fmt.Errorf("wal: entry too short: %d bytes", len(b))
	}
	e := &Entry{}
	e.Op = Op(b[0])
	if !e.Op.Known() {
		return nil, fmt.Errorf("%w: op=%d", ErrUnknownOp, e.Op)
	}
	e.Revision = int64(binary.BigEndian.Uint64(b[1:9]))
	e.Term = binary.BigEndian.Uint64(b[9:17])
	e.Lease = int64(binary.BigEndian.Uint64(b[17:25]))
	e.CreateRevision = int64(binary.BigEndian.Uint64(b[25:33]))
	e.PrevRevision = int64(binary.BigEndian.Uint64(b[33:41]))
	fixedSize := entryFixedSizeV1
	if version >= 2 {
		if len(b) < entryFixedSizeV3 {
			return nil, fmt.Errorf("wal: v2 entry too short: %d bytes", len(b))
		}
		e.ID = int64(binary.BigEndian.Uint64(b[41:49]))
		e.Version = int64(binary.BigEndian.Uint64(b[49:57]))
		fixedSize = entryFixedSizeV3
	}
	keyLen := int(binary.BigEndian.Uint32(b[fixedSize-8 : fixedSize-4]))
	valLen := int(binary.BigEndian.Uint32(b[fixedSize-4 : fixedSize]))

	tail := b[fixedSize:]
	if len(tail) < keyLen+valLen {
		return nil, fmt.Errorf("wal: entry payload truncated (need %d, have %d)", keyLen+valLen, len(tail))
	}
	e.Key = string(tail[:keyLen])
	if valLen > 0 {
		e.Value = make([]byte, valLen)
		copy(e.Value, tail[keyLen:])
	}
	return e, nil
}
