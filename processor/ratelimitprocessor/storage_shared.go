package ratelimitprocessor

import (
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// storageRegistry shares one Storage per component ID across the signal
// pipelines (traces / metrics / logs). Without this, the factory would build
// an independent backend per signal, which has two production problems:
//
//   - memory backend: each signal gets its own bucket per key, so a
//     "100 rps" limit silently becomes 300 rps per key across signals;
//   - redis backend: all signals already share one bucket (same key), so
//     swapping backends silently changes enforcement semantics.
//
// Sharing the backend per component ID makes both backends behave the same:
// one bucket per key, shared by every signal the processor is wired into.
type storageRegistry struct {
	mu      sync.Mutex
	entries map[component.ID]*storageEntry
}

type storageEntry struct {
	storage  Storage
	refs     int
	gaugeReg metric.Registration // bucket-gauge callback, registered once
	gaugeSet bool
}

func newStorageRegistry() *storageRegistry {
	return &storageRegistry{entries: make(map[component.ID]*storageEntry)}
}

// acquire returns a shared handle on the storage for id, creating the backend
// on first use. Every returned handle must be Closed; the underlying backend
// is closed when the last handle goes away.
func (r *storageRegistry) acquire(id component.ID, cfg *Config, logger *zap.Logger) (Storage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.entries[id]; ok {
		e.refs++
		return &sharedStorage{Storage: e.storage, registry: r, id: id}, nil
	}
	s, err := newStorage(cfg, logger)
	if err != nil {
		return nil, err
	}
	r.entries[id] = &storageEntry{storage: s, refs: 1}
	return &sharedStorage{Storage: s, registry: r, id: id}, nil
}

func (r *storageRegistry) release(id component.ID) error {
	r.mu.Lock()
	e, ok := r.entries[id]
	if !ok {
		r.mu.Unlock()
		return nil
	}
	e.refs--
	if e.refs > 0 {
		r.mu.Unlock()
		return nil
	}
	delete(r.entries, id)
	r.mu.Unlock()
	// Unregister the gauge callback before closing the backend so a scrape
	// can't race a closed storage.
	if e.gaugeReg != nil {
		_ = e.gaugeReg.Unregister()
	}
	return e.storage.Close()
}

// registerGauges runs `register` for the first pipeline only; the resulting
// registration is unregistered when the last handle is closed. Registering the
// same observable callback once per signal would produce duplicate datapoints
// for the same attribute set.
func (r *storageRegistry) registerGauges(id component.ID, register func() (metric.Registration, error)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[id]
	if !ok || e.gaugeSet {
		return nil
	}
	reg, err := register()
	if err != nil {
		return err
	}
	e.gaugeReg = reg
	e.gaugeSet = true
	return nil
}

// sharedStorage is a per-pipeline handle on a registry-owned backend. Close
// releases the reference (idempotently) instead of closing the backend.
type sharedStorage struct {
	Storage
	registry  *storageRegistry
	id        component.ID
	closeOnce sync.Once
	closeErr  error
}

func (s *sharedStorage) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.registry.release(s.id) })
	return s.closeErr
}

func (s *sharedStorage) registerGauges(register func() (metric.Registration, error)) error {
	return s.registry.registerGauges(s.id, register)
}

// defaultStorageRegistry backs the factory. Package-level because the
// collector creates one processor instance per signal pipeline and the only
// shared scope across those calls is the package.
var defaultStorageRegistry = newStorageRegistry()
