// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// discardTraces is a no-op consumer for benchmarks (no slice growth).
type discardTraces struct{}

func (discardTraces) ConsumeTraces(context.Context, ptrace.Traces) error { return nil }
func (discardTraces) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// --- Concurrency correctness ---

// TestConcurrent_SingleBucketExact: under many concurrent callers, a single
// bucket grants exactly its capacity and not one more (per-bucket atomicity, no
// over-grant, race-free under -race).
func TestConcurrent_SingleBucketExact(t *testing.T) {
	ms := newMemoryStorage(time.Hour, time.Hour, 0, zap.NewNop())
	defer func() { _ = ms.Close() }()

	const capacity = 1000
	const callers = 4000
	var granted int64
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// period 1h => refill is negligible during the test, so the budget
			// is a fixed `capacity`.
			if ms.AllowUpToN(context.Background(), "k", 1, capacity, time.Hour) == 1 {
				atomic.AddInt64(&granted, 1)
			}
		}()
	}
	wg.Wait()
	if granted != capacity {
		t.Errorf("granted %d, want exactly %d (concurrent over/under-grant)", granted, capacity)
	}
}

// TestConcurrent_GlobalCeilingHolds: with a global ceiling and non-binding
// per-key limits, the total granted across all keys never exceeds the global
// budget under concurrency (the global bucket is atomic).
func TestConcurrent_GlobalCeilingHolds(t *testing.T) {
	ms := newMemoryStorage(time.Hour, time.Hour, 0, zap.NewNop())
	defer func() { _ = ms.Close() }()

	const global = 500
	cfg := &Config{
		LimitType:               "service_name",
		GlobalRequestsPerMinute: global,    // period = 1 min => refill negligible in-test
		RequestsPerMinute:       1_000_000, // per-key not binding
		SpecificLimits:          map[string]LimitConfig{},
	}
	rl := newRateLimiterWithStorage(cfg, ms)

	const callers = 5000
	var granted int64
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		key := "svc-" + strconv.Itoa(i%50) // spread across 50 keys
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			if rl.AllowNCtx(context.Background(), k, 1) {
				atomic.AddInt64(&granted, 1)
			}
		}(key)
	}
	wg.Wait()
	if granted > global+2 { // +2 tolerance for any sub-second refill
		t.Errorf("granted %d exceeded the global ceiling %d under concurrency", granted, global)
	}
	if granted < global-5 {
		t.Errorf("granted %d, expected to use ~all of the global budget %d", granted, global)
	}
}

// --- Benchmarks (run with: go test -bench . -benchmem) ---

func BenchmarkMemoryAllowUpToN(b *testing.B) {
	ms := newMemoryStorage(time.Hour, time.Hour, 0, zap.NewNop())
	defer func() { _ = ms.Close() }()
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ms.AllowUpToN(ctx, "svc", 1, 1_000_000, time.Second)
		}
	})
}

func BenchmarkMemoryAllowUpToN_ManyKeys(b *testing.B) {
	ms := newMemoryStorage(time.Hour, time.Hour, 0, zap.NewNop())
	defer func() { _ = ms.Close() }()
	ctx := context.Background()
	var n int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := "svc-" + strconv.Itoa(int(atomic.AddInt64(&n, 1))%1000)
			ms.AllowUpToN(ctx, k, 1, 1_000_000, time.Second)
		}
	})
}

func BenchmarkConsumeTraces(b *testing.B) {
	mp, _ := newTestMeterProvider()
	tp, err := newTracesProcessor(testConfig(1_000_000), zap.NewNop(), mp, testStorage(b), discardTraces{})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// fresh batch each iter: ConsumeTraces may mutate the pdata.
		tp.ConsumeTraces(ctx, newTestTraces("svc", 10))
	}
}
