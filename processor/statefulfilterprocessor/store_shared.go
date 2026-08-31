// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package statefulfilterprocessor

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"
)

// storeRegistry shares one ruleStore per component ID across the traces,
// metrics and logs pipelines. The collector instantiates a processor per
// signal, so without this a single `statefulfilter/x` in the config would open
// three Redis connections and run three identical polling goroutines — triple
// the Redis load for the same rule set, and three chances to disagree about
// which version is current mid-update.
type storeRegistry struct {
	mu      sync.Mutex
	entries map[component.ID]*storeEntry
}

type storeEntry struct {
	store    ruleStore
	refs     int
	loaded   bool
	started  bool
	gaugeReg metric.Registration
	gaugeSet bool
}

func newStoreRegistry() *storeRegistry {
	return &storeRegistry{entries: make(map[component.ID]*storeEntry)}
}

// acquire returns a shared handle for id, building the store on first use.
// Every handle must be closed; the store is closed when the last one goes.
func (r *storeRegistry) acquire(id component.ID, build func() (ruleStore, error)) (ruleStore, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.entries[id]; ok {
		e.refs++
		return &sharedStore{ruleStore: e.store, registry: r, id: id}, nil
	}
	s, err := build()
	if err != nil {
		return nil, err
	}
	r.entries[id] = &storeEntry{store: s, refs: 1}
	return &sharedStore{ruleStore: s, registry: r, id: id}, nil
}

// initialLoadOnce performs the synchronous first fetch for whichever signal
// pipeline starts first; the others get nil because the rules are already
// there. Kept outside the mutex so a slow Redis can't block a concurrent
// acquire/release.
func (r *storeRegistry) initialLoadOnce(id component.ID, ctx context.Context) error {
	r.mu.Lock()
	e, ok := r.entries[id]
	if !ok || e.loaded {
		r.mu.Unlock()
		return nil
	}
	e.loaded = true
	r.mu.Unlock()
	return e.store.initialLoad(ctx)
}

// startOnce runs the refresh loop for the first pipeline that starts; later
// signals reuse the already-running loop.
func (r *storeRegistry) startOnce(id component.ID) {
	r.mu.Lock()
	e, ok := r.entries[id]
	if !ok || e.started {
		r.mu.Unlock()
		return
	}
	e.started = true
	r.mu.Unlock()
	e.store.start()
}

func (r *storeRegistry) release(id component.ID) error {
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
	// Unregister the gauge callback before closing the store so a scrape can't
	// race a closed client.
	if e.gaugeReg != nil {
		_ = e.gaugeReg.Unregister()
	}
	return e.store.close()
}

// registerGauges runs `register` for the first pipeline only. Registering the
// same observable callback per signal would emit duplicate datapoints for one
// attribute set.
func (r *storeRegistry) registerGauges(id component.ID, register func() (metric.Registration, error)) error {
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

// sharedStore is a per-pipeline handle on a registry-owned store. start/close
// are routed through the registry so only the first/last pipeline acts.
type sharedStore struct {
	ruleStore
	registry  *storeRegistry
	id        component.ID
	closeOnce sync.Once
	closeErr  error
}

func (s *sharedStore) initialLoad(ctx context.Context) error {
	return s.registry.initialLoadOnce(s.id, ctx)
}

func (s *sharedStore) start() { s.registry.startOnce(s.id) }

// registerGauges satisfies gaugeRegistrar so the rule-state callback is
// registered once per underlying store rather than once per signal pipeline —
// three identical callbacks would emit three data points for the same
// (attribute-free) gauge on every scrape.
func (s *sharedStore) registerGauges(register func() (metric.Registration, error)) error {
	return s.registry.registerGauges(s.id, register)
}

func (s *sharedStore) close() error {
	s.closeOnce.Do(func() { s.closeErr = s.registry.release(s.id) })
	return s.closeErr
}

// defaultStoreRegistry backs the factory. Package-level because the collector
// creates one processor instance per signal pipeline and the package is the
// only scope shared across those calls.
var defaultStoreRegistry = newStoreRegistry()
