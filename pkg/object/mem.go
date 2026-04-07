// Package object provides a small interface for object storage operations
// used by T4 (WAL archive, checkpoints, manifest).
package object

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// Mem is an in-memory Store implementation for tests.
// It also implements ConditionalStore using a mutex-protected version counter.
type Mem struct {
	mu      sync.Mutex
	objects map[string]memObj
}

type memObj struct {
	data []byte
	etag string // hex(md5(data)), recomputed on every write
}

// NewMem returns an empty in-memory Store.
func NewMem() *Mem { return &Mem{objects: make(map[string]memObj)} }

func memETag(data []byte) string {
	sum := md5.Sum(data)
	return fmt.Sprintf("%x", sum)
}

func (m *Mem) Put(_ context.Context, key string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[key] = memObj{data: data, etag: memETag(data)}
	m.mu.Unlock()
	return nil
}

func (m *Mem) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	obj, ok := m.objects[key]
	m.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(obj.data)), nil
}

func (m *Mem) GetETag(_ context.Context, key string) (*GetWithETag, error) {
	m.mu.Lock()
	obj, ok := m.objects[key]
	m.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	return &GetWithETag{
		Body: io.NopCloser(bytes.NewReader(obj.data)),
		ETag: obj.etag,
	}, nil
}

func (m *Mem) PutIfAbsent(_ context.Context, key string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.objects[key]; exists {
		return ErrPreconditionFailed
	}
	m.objects[key] = memObj{data: data, etag: memETag(data)}
	return nil
}

func (m *Mem) PutIfMatch(_ context.Context, key string, r io.Reader, matchETag string) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, exists := m.objects[key]
	if !exists || obj.etag != matchETag {
		return ErrPreconditionFailed
	}
	m.objects[key] = memObj{data: data, etag: memETag(data)}
	return nil
}

func (m *Mem) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	return nil
}

func (m *Mem) DeleteMany(_ context.Context, keys []string) error {
	m.mu.Lock()
	for _, k := range keys {
		delete(m.objects, k)
	}
	m.mu.Unlock()
	return nil
}

func (m *Mem) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}
