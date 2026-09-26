package object

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	encMagic     = "T4E1"
	encHeaderLen = len(encMagic) + 1 + 12 // magic + key ID + file nonce
	encChunkSize = 64 * 1024
	encTagSize   = 16
)

// KeyProvider supplies the AES-256 key used to encrypt and decrypt objects.
type KeyProvider interface {
	// Key returns the 32-byte AES-256 encryption key.
	Key(ctx context.Context) ([32]byte, error)

	// KeyID returns a small identifier stored in every encrypted object header.
	// V1 uses a single key, but keeping this byte in the format leaves room for
	// future key rotation without changing the object wrapper API.
	KeyID() uint8
}

// StaticKeyProvider is a KeyProvider backed by a fixed 32-byte AES-256 key.
// It is safe for concurrent use.
type StaticKeyProvider struct {
	key [32]byte
}

// NewStaticKeyProvider returns a KeyProvider that always returns key.
func NewStaticKeyProvider(key []byte) (*StaticKeyProvider, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("object: AES-256 key must be 32 bytes, got %d", len(key))
	}
	p := &StaticKeyProvider{}
	copy(p.key[:], key)
	return p, nil
}

func (p *StaticKeyProvider) Key(context.Context) ([32]byte, error) { return p.key, nil }
func (p *StaticKeyProvider) KeyID() uint8                          { return 0 }

type encryptedStore struct {
	inner Store
	kp    KeyProvider
}

type encryptedConditionalStore struct {
	*encryptedStore
	inner ConditionalStore
}

type encryptedVersionedStore struct {
	*encryptedStore
	inner VersionedStore
}

type encryptedConditionalVersionedStore struct {
	*encryptedStore
	conditional *encryptedConditionalStore
	versioned   *encryptedVersionedStore
}

// NewEncryptedStore wraps s with transparent AES-256-GCM encryption.
//
// Object keys remain plaintext so List, Delete, DeleteMany, checkpoint GC, and
// leader-election lock discovery keep their existing behavior. Object bodies
// are encrypted, and the logical object key is authenticated as AEAD associated
// data so ciphertext copied to a different key is rejected on read.
func NewEncryptedStore(s Store, kp KeyProvider) Store {
	base := &encryptedStore{inner: s, kp: kp}
	cs, hasConditional := s.(ConditionalStore)
	vs, hasVersioned := s.(VersionedStore)
	switch {
	case hasConditional && hasVersioned:
		return &encryptedConditionalVersionedStore{
			encryptedStore: base,
			conditional:    &encryptedConditionalStore{encryptedStore: base, inner: cs},
			versioned:      &encryptedVersionedStore{encryptedStore: base, inner: vs},
		}
	case hasConditional:
		return &encryptedConditionalStore{encryptedStore: base, inner: cs}
	case hasVersioned:
		return &encryptedVersionedStore{encryptedStore: base, inner: vs}
	default:
		return base
	}
}

func (s *encryptedStore) Inner() Store { return s.inner }

func (s *encryptedStore) Put(ctx context.Context, key string, r io.Reader) error {
	return s.putEncrypted(ctx, key, r, func(er io.Reader) error {
		return s.inner.Put(ctx, key, er)
	})
}

func (s *encryptedStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := s.inner.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	k, err := s.kp.Key(ctx)
	if err != nil {
		_ = rc.Close()
		return nil, fmt.Errorf("object: decrypt key: %w", err)
	}
	return newDecryptReader(key, rc, k)
}

func (s *encryptedStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}

func (s *encryptedStore) DeleteMany(ctx context.Context, keys []string) error {
	return s.inner.DeleteMany(ctx, keys)
}

func (s *encryptedStore) List(ctx context.Context, prefix string) ([]string, error) {
	return s.inner.List(ctx, prefix)
}

func (s *encryptedStore) putEncrypted(ctx context.Context, key string, r io.Reader, put func(io.Reader) error) error {
	k, err := s.kp.Key(ctx)
	if err != nil {
		return fmt.Errorf("object: encrypt key: %w", err)
	}

	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := encryptStream(key, pw, r, k, s.kp.KeyID())
		_ = pw.CloseWithError(err)
		done <- err
	}()

	putErr := put(pr)
	if putErr != nil {
		_ = pr.CloseWithError(putErr)
		encErr := <-done
		if encErr != nil {
			return fmt.Errorf("object: put encrypted %q: %w", key, putErr)
		}
		return putErr
	}
	if encErr := <-done; encErr != nil {
		return fmt.Errorf("object: encrypt %q: %w", key, encErr)
	}
	return nil
}

func (s *encryptedConditionalStore) GetETag(ctx context.Context, key string) (*GetWithETag, error) {
	res, err := s.inner.GetETag(ctx, key)
	if err != nil {
		return nil, err
	}
	k, err := s.kp.Key(ctx)
	if err != nil {
		_ = res.Body.Close()
		return nil, fmt.Errorf("object: decrypt key: %w", err)
	}
	dec, err := newDecryptReader(key, res.Body, k)
	if err != nil {
		return nil, err
	}
	return &GetWithETag{Body: dec, ETag: res.ETag}, nil
}

func (s *encryptedConditionalStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	body, err := s.encryptToBuffer(ctx, key, r)
	if err != nil {
		return err
	}
	return s.inner.PutIfAbsent(ctx, key, body)
}

func (s *encryptedConditionalStore) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	body, err := s.encryptToBuffer(ctx, key, r)
	if err != nil {
		return err
	}
	return s.inner.PutIfMatch(ctx, key, body, matchETag)
}

// encryptToBuffer encrypts r fully in memory. Conditional puts must not stream:
// an unknown-length body makes S3 clients fall back to a multipart upload,
// where If-Match / If-None-Match is not enforced atomically at completion, so
// two racing writers could both succeed. Conditional objects (leader lock,
// manifests, branch registry) are small, so buffering is cheap.
func (s *encryptedStore) encryptToBuffer(ctx context.Context, key string, r io.Reader) (*bytes.Reader, error) {
	k, err := s.kp.Key(ctx)
	if err != nil {
		return nil, fmt.Errorf("object: encrypt key: %w", err)
	}
	var buf bytes.Buffer
	if err := encryptStream(key, &buf, r, k, s.kp.KeyID()); err != nil {
		return nil, fmt.Errorf("object: encrypt %q: %w", key, err)
	}
	return bytes.NewReader(buf.Bytes()), nil
}

func (s *encryptedVersionedStore) GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error) {
	rc, err := s.inner.GetVersioned(ctx, key, versionID)
	if err != nil {
		return nil, err
	}
	k, err := s.kp.Key(ctx)
	if err != nil {
		_ = rc.Close()
		return nil, fmt.Errorf("object: decrypt key: %w", err)
	}
	return newDecryptReader(key, rc, k)
}

func (s *encryptedConditionalVersionedStore) GetETag(ctx context.Context, key string) (*GetWithETag, error) {
	return s.conditional.GetETag(ctx, key)
}

func (s *encryptedConditionalVersionedStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	return s.conditional.PutIfAbsent(ctx, key, r)
}

func (s *encryptedConditionalVersionedStore) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	return s.conditional.PutIfMatch(ctx, key, r, matchETag)
}

func (s *encryptedConditionalVersionedStore) GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error) {
	return s.versioned.GetVersioned(ctx, key, versionID)
}

func encryptStream(key string, w io.Writer, r io.Reader, aesKey [32]byte, keyID uint8) error {
	gcm, err := newGCM(aesKey)
	if err != nil {
		return err
	}

	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fmt.Errorf("object: generate nonce: %w", err)
	}

	var hdr [encHeaderLen]byte
	copy(hdr[:], encMagic)
	hdr[len(encMagic)] = keyID
	copy(hdr[len(encMagic)+1:], nonce[:])
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}

	plain := make([]byte, encChunkSize)
	var idx uint64
	for {
		n, readErr := io.ReadFull(r, plain)
		if n > 0 {
			if err := writeEncryptedFrame(key, w, gcm, hdr[:], nonce, idx, false, plain[:n]); err != nil {
				return err
			}
			idx++
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	return writeEncryptedFrame(key, w, gcm, hdr[:], nonce, idx, true, nil)
}

func writeEncryptedFrame(key string, w io.Writer, gcm cipher.AEAD, header []byte, baseNonce [12]byte, idx uint64, final bool, plain []byte) error {
	if len(plain) > encChunkSize {
		return fmt.Errorf("object: plaintext frame too large: %d", len(plain))
	}
	var lbuf [4]byte
	binary.BigEndian.PutUint32(lbuf[:], uint32(len(plain)))
	cn := chunkNonce(baseNonce, idx)
	aad := frameAAD(key, header, idx, final, lbuf[:])
	sealed := gcm.Seal(nil, cn[:], plain, aad)
	if _, err := w.Write(lbuf[:]); err != nil {
		return err
	}
	_, err := w.Write(sealed)
	return err
}

func newGCM(key [32]byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func chunkNonce(base [12]byte, idx uint64) [12]byte {
	var n [12]byte
	copy(n[:4], base[:4])
	binary.BigEndian.PutUint64(n[4:], binary.BigEndian.Uint64(base[4:])^idx)
	return n
}

func frameAAD(key string, header []byte, idx uint64, final bool, length []byte) []byte {
	aad := bytes.NewBuffer(make([]byte, 0, len("t4-object-encryption-v1")+1+len(key)+len(header)+8+1+4))
	aad.WriteString("t4-object-encryption-v1")
	aad.WriteByte(0)
	aad.WriteString(key)
	aad.Write(header)
	var ibuf [8]byte
	binary.BigEndian.PutUint64(ibuf[:], idx)
	aad.Write(ibuf[:])
	if final {
		aad.WriteByte(1)
	} else {
		aad.WriteByte(0)
	}
	aad.Write(length)
	return aad.Bytes()
}

type decryptReader struct {
	inner  io.ReadCloser
	key    string
	gcm    cipher.AEAD
	header [encHeaderLen]byte
	nonce  [12]byte
	idx    uint64
	buf    []byte
	pos    int
	done   bool
}

func newDecryptReader(key string, rc io.ReadCloser, aesKey [32]byte) (*decryptReader, error) {
	gcm, err := newGCM(aesKey)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}
	d := &decryptReader{inner: rc, key: key, gcm: gcm}
	if _, err := io.ReadFull(rc, d.header[:]); err != nil {
		_ = rc.Close()
		return nil, fmt.Errorf("object: read encrypted header: %w", err)
	}
	if string(d.header[:len(encMagic)]) != encMagic {
		_ = rc.Close()
		return nil, fmt.Errorf("object: unsupported encrypted object header %q", d.header[:len(encMagic)])
	}
	copy(d.nonce[:], d.header[len(encMagic)+1:])
	return d, nil
}

func (d *decryptReader) Read(p []byte) (int, error) {
	for {
		if d.pos < len(d.buf) {
			n := copy(p, d.buf[d.pos:])
			d.pos += n
			return n, nil
		}
		if d.done {
			return 0, io.EOF
		}
		if err := d.readNextFrame(); err != nil {
			return 0, err
		}
	}
}

func (d *decryptReader) Close() error { return d.inner.Close() }

func (d *decryptReader) readNextFrame() error {
	var lbuf [4]byte
	if _, err := io.ReadFull(d.inner, lbuf[:]); err != nil {
		return fmt.Errorf("object: read encrypted frame length: %w", err)
	}
	plainLen := binary.BigEndian.Uint32(lbuf[:])
	if plainLen > encChunkSize {
		return fmt.Errorf("object: encrypted frame length %d exceeds max %d", plainLen, encChunkSize)
	}

	final := plainLen == 0
	cipherLen := int(plainLen) + encTagSize
	cipherBuf := make([]byte, cipherLen)
	if _, err := io.ReadFull(d.inner, cipherBuf); err != nil {
		return fmt.Errorf("object: read encrypted frame: %w", err)
	}

	cn := chunkNonce(d.nonce, d.idx)
	aad := frameAAD(d.key, d.header[:], d.idx, final, lbuf[:])
	plain, err := d.gcm.Open(cipherBuf[:0], cn[:], cipherBuf, aad)
	if err != nil {
		return fmt.Errorf("object: decrypt frame %d: authentication failed", d.idx)
	}
	d.idx++
	if final {
		d.done = true
		return nil
	}
	d.buf = plain
	d.pos = 0
	return nil
}
