package ratelimitprocessor

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// defaultMaxBuckets bounds the in-process bucket map out of the box, so a hostile
// or buggy producer spraying unique keys (service.name is client-controlled and
// spoofable) can't grow the map without limit and OOM the collector.
const defaultMaxBuckets = 100_000

// memoryStorage keeps token buckets in-process. This is the default and
// zero-dependency backend: no network hop, no extra infra. Trade-off: each
// collector replica keeps an independent view, so rate limits are replica-local,
// not fleet-wide.
//
// Two mechanisms bound memory: a background goroutine evicts idle buckets, and a
// hard `maxBuckets` cap evicts the oldest buckets when the map is full.
type memoryStorage struct {
	buckets    map[string]*TokenBucket
	mu         sync.RWMutex
	stopCh     chan struct{}
	closeOnce  sync.Once
	logger     *zap.Logger
	maxBuckets int

	evictCounter metric.Int64Counter // optional; counts cap-triggered evictions
}

func newMemoryStorage(cleanupInterval, maxAge time.Duration, maxBuckets int, logger *zap.Logger) *memoryStorage {
	if maxBuckets <= 0 {
		maxBuckets = defaultMaxBuckets
	}
	ms := &memoryStorage{
		buckets:    make(map[string]*TokenBucket),
		stopCh:     make(chan struct{}),
		logger:     logger,
		maxBuckets: maxBuckets,
	}
	go ms.cleanupLoop(cleanupInterval, maxAge)
	return ms
}

func (ms *memoryStorage) Name() string { return "memory" }

func (ms *memoryStorage) getOrCreate(key string, limit int, period time.Duration) *TokenBucket {
	ms.mu.RLock()
	bucket, ok := ms.buckets[key]
	ms.mu.RUnlock()
	if ok {
		return bucket
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	if bucket, ok = ms.buckets[key]; ok {
		return bucket
	}
	// Enforce the hard cap before inserting. Evict in a batch (down to 90%) so
	// the O(n) eviction scan is amortized across many inserts rather than run on
	// every new key while at the ceiling.
	if len(ms.buckets) >= ms.maxBuckets {
		ms.evictOldestLocked(ms.maxBuckets * 9 / 10)
	}
	bucket = NewTokenBucket(limit, period)
	ms.buckets[key] = bucket
	return bucket
}

// evictOldestLocked removes the oldest buckets (by last refill) until at most
// `target` remain. Caller must hold ms.mu for writing.
//
// Selection uses a bounded max-heap (size = number of evictions) instead of
// sorting the whole map: O(n log k) with a k-sized allocation rather than
// O(n log n) with an n-sized one. This runs with the write lock held and
// blocks the hot path, so the scan must stay as cheap as possible.
func (ms *memoryStorage) evictOldestLocked(target int) {
	if target < 0 {
		target = 0
	}
	remove := len(ms.buckets) - target
	if remove <= 0 {
		return
	}
	type entry struct {
		key string
		ts  time.Time
	}
	// Max-heap on ts holding the `remove` oldest entries seen so far: the root
	// is the newest of the candidates and is displaced by anything older.
	h := make([]entry, 0, remove)
	siftDown := func(i int) {
		for {
			l, r := 2*i+1, 2*i+2
			newest := i
			if l < len(h) && h[l].ts.After(h[newest].ts) {
				newest = l
			}
			if r < len(h) && h[r].ts.After(h[newest].ts) {
				newest = r
			}
			if newest == i {
				return
			}
			h[i], h[newest] = h[newest], h[i]
			i = newest
		}
	}
	for k, b := range ms.buckets {
		b.mu.Lock()
		ts := b.lastRefillTime
		b.mu.Unlock()
		if len(h) < remove {
			h = append(h, entry{k, ts})
			// Sift up.
			for i := len(h) - 1; i > 0; {
				parent := (i - 1) / 2
				if !h[i].ts.After(h[parent].ts) {
					break
				}
				h[i], h[parent] = h[parent], h[i]
				i = parent
			}
		} else if ts.Before(h[0].ts) {
			h[0] = entry{k, ts}
			siftDown(0)
		}
	}
	for _, e := range h {
		delete(ms.buckets, e.key)
	}
	if ms.evictCounter != nil {
		ms.evictCounter.Add(context.Background(), int64(len(h)),
			metric.WithAttributes(attribute.String("reason", "cap")))
	}
	if ms.logger != nil {
		ms.logger.Warn("rate-limit bucket cap reached; evicted oldest buckets",
			zap.Int("evicted", len(h)), zap.Int("max_buckets", ms.maxBuckets))
	}
}

func (ms *memoryStorage) AllowUpToN(_ context.Context, key string, n, limit int, period time.Duration) int {
	if n <= 0 {
		return 0
	}
	return ms.getOrCreate(key, limit, period).AllowUpToN(n)
}

func (ms *memoryStorage) Refund(_ context.Context, key string, n, limit int, period time.Duration) {
	if n <= 0 {
		return
	}
	ms.getOrCreate(key, limit, period).Refund(n)
}

// AvailableTokens returns the current (refilled) token count for key without
// creating the bucket if it doesn't exist yet.
func (ms *memoryStorage) AvailableTokens(_ context.Context, key string) (float64, bool) {
	ms.mu.RLock()
	bucket, ok := ms.buckets[key]
	ms.mu.RUnlock()
	if !ok {
		return 0, false
	}
	bucket.mu.Lock()
	bucket.refillLocked()
	tokens := bucket.tokens
	bucket.mu.Unlock()
	return tokens, true
}

// SetErrorCounter is a no-op: the in-process backend cannot fail transiently.
func (ms *memoryStorage) SetErrorCounter(metric.Int64Counter) {}

// SetFailOpenCounter is a no-op: the in-process backend never fails open.
func (ms *memoryStorage) SetFailOpenCounter(metric.Int64Counter) {}

// SetEvictionCounter wires the metric counter incremented on cap-triggered
// bucket evictions, so operators can tell when the cap is being hit.
func (ms *memoryStorage) SetEvictionCounter(counter metric.Int64Counter) {
	ms.mu.Lock()
	ms.evictCounter = counter
	ms.mu.Unlock()
}

// bucketCount returns the current number of live buckets (for tests/visibility).
func (ms *memoryStorage) bucketCount() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.buckets)
}

// Close is idempotent: the processor and the rate limiter may both own a
// reference, and a double close must not panic on the channel.
func (ms *memoryStorage) Close() error {
	ms.closeOnce.Do(func() { close(ms.stopCh) })
	return nil
}

func (ms *memoryStorage) cleanupLoop(interval, maxAge time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ms.cleanup(maxAge)
		case <-ms.stopCh:
			return
		}
	}
}

func (ms *memoryStorage) cleanup(maxAge time.Duration) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	now := time.Now()
	for key, bucket := range ms.buckets {
		bucket.mu.Lock()
		age := now.Sub(bucket.lastRefillTime)
		bucket.mu.Unlock()

		if age > maxAge {
			delete(ms.buckets, key)
		}
	}
}
