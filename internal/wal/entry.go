// Package wal implements the write-ahead log.
//
// Each write is appended to a local segment file (fsync'd) before being
// applied to the state machine. Sealed segments are uploaded to object storage
// asynchronously; they are kept locally until the upload is confirmed.
package wal

import (
	"encoding/binary"
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
)

// Entry is one record in the write-ahead log.
type Entry struct {
	Revision       int64
	Term           uint64
	Op             Op
	Key            string
	Value          []byte
	Lease          int64
	CreateRevision int64 // meaningful for Update/Delete
	PrevRevision   int64 // meaningful for Update/Delete
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
}

// txnSubOpFixedSize: op(1) + lease(8) + createRev(8) + prevRev(8) + keyLen(4) + valLen(4) = 33
const txnSubOpFixedSize = 33

// EncodeTxnOps encodes a slice of TxnSubOp into a byte slice for storage in
// an OpTxn Entry's Value field.
func EncodeTxnOps(ops []TxnSubOp) []byte {
	total := 4 // uint32 count prefix
	for i := range ops {
		total += txnSubOpFixedSize + len(ops[i].Key) + len(ops[i].Value)
	}
	buf := make([]byte, total)
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(ops)))
	off := 4
	for i := range ops {
		o := &ops[i]
		buf[off] = byte(o.Op)
		binary.BigEndian.PutUint64(buf[off+1:], uint64(o.Lease))
		binary.BigEndian.PutUint64(buf[off+9:], uint64(o.CreateRevision))
		binary.BigEndian.PutUint64(buf[off+17:], uint64(o.PrevRevision))
		binary.BigEndian.PutUint32(buf[off+25:], uint32(len(o.Key)))
		binary.BigEndian.PutUint32(buf[off+29:], uint32(len(o.Value)))
		off += txnSubOpFixedSize
		copy(buf[off:], o.Key)
		off += len(o.Key)
		copy(buf[off:], o.Value)
		off += len(o.Value)
	}
	return buf
}

// DecodeTxnOps decodes a byte slice produced by EncodeTxnOps.
func DecodeTxnOps(b []byte) ([]TxnSubOp, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("wal: txn ops payload too short (%d bytes)", len(b))
	}
	count := int(binary.BigEndian.Uint32(b[0:4]))
	ops := make([]TxnSubOp, 0, count)
	off := 4
	for i := 0; i < count; i++ {
		if len(b)-off < txnSubOpFixedSize {
			return nil, fmt.Errorf("wal: txn sub-op %d header truncated", i)
		}
		o := TxnSubOp{}
		o.Op = Op(b[off])
		o.Lease = int64(binary.BigEndian.Uint64(b[off+1:]))
		o.CreateRevision = int64(binary.BigEndian.Uint64(b[off+9:]))
		o.PrevRevision = int64(binary.BigEndian.Uint64(b[off+17:]))
		keyLen := int(binary.BigEndian.Uint32(b[off+25:]))
		valLen := int(binary.BigEndian.Uint32(b[off+29:]))
		off += txnSubOpFixedSize
		if len(b)-off < keyLen+valLen {
			return nil, fmt.Errorf("wal: txn sub-op %d payload truncated (need %d, have %d)", i, keyLen+valLen, len(b)-off)
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
	return ops, nil
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
//	[4:  key_len         uint32 BE]
//	[4:  val_len         uint32 BE]
//	[key_len: key bytes]
//	[val_len: value bytes]
const entryFixedSize = 1 + 8 + 8 + 8 + 8 + 8 + 4 + 4 // 49 bytes

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

	return unmarshalEntry(payload)
}

func marshalEntry(e *Entry) []byte {
	buf := make([]byte, entryFixedSize+len(e.Key)+len(e.Value))
	buf[0] = byte(e.Op)
	binary.BigEndian.PutUint64(buf[1:9], uint64(e.Revision))
	binary.BigEndian.PutUint64(buf[9:17], e.Term)
	binary.BigEndian.PutUint64(buf[17:25], uint64(e.Lease))
	binary.BigEndian.PutUint64(buf[25:33], uint64(e.CreateRevision))
	binary.BigEndian.PutUint64(buf[33:41], uint64(e.PrevRevision))
	binary.BigEndian.PutUint32(buf[41:45], uint32(len(e.Key)))
	binary.BigEndian.PutUint32(buf[45:49], uint32(len(e.Value)))
	copy(buf[49:], e.Key)
	copy(buf[49+len(e.Key):], e.Value)
	return buf
}

func unmarshalEntry(b []byte) (*Entry, error) {
	if len(b) < entryFixedSize {
		return nil, fmt.Errorf("wal: entry too short: %d bytes", len(b))
	}
	e := &Entry{}
	e.Op = Op(b[0])
	e.Revision = int64(binary.BigEndian.Uint64(b[1:9]))
	e.Term = binary.BigEndian.Uint64(b[9:17])
	e.Lease = int64(binary.BigEndian.Uint64(b[17:25]))
	e.CreateRevision = int64(binary.BigEndian.Uint64(b[25:33]))
	e.PrevRevision = int64(binary.BigEndian.Uint64(b[33:41]))
	keyLen := int(binary.BigEndian.Uint32(b[41:45]))
	valLen := int(binary.BigEndian.Uint32(b[45:49]))

	tail := b[49:]
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
