package ratelimitprocessor

import (
	"context"
	"strconv"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestMemoryStorage_DefaultCap: an unset cap falls back to the safe default.
func TestMemoryStorage_DefaultCap(t *testing.T) {
	ms := newMemoryStorage(time.Hour, time.Hour, 0, zap.NewNop())
	defer func() { _ = ms.Close() }()
	if ms.maxBuckets != defaultMaxBuckets {
		t.Fatalf("default cap = %d, want %d", ms.maxBuckets, defaultMaxBuckets)
	}
}

// TestMemoryStorage_CapBounded: spraying many distinct keys (a hostile cardinality
// pattern) never grows the map past the cap.
func TestMemoryStorage_CapBounded(t *testing.T) {
	const cap = 10
	ms := newMemoryStorage(time.Hour, time.Hour, cap, zap.NewNop())
	defer func() { _ = ms.Close() }()

	for i := 0; i < 500; i++ {
		ms.AllowUpToN(context.Background(), "k"+strconv.Itoa(i), 1, 5, time.Second)
		if c := ms.bucketCount(); c > cap {
			t.Fatalf("bucket count %d exceeded cap %d after %d unique keys", c, cap, i)
		}
	}
	if ms.bucketCount() == 0 {
		t.Fatal("expected live buckets after the spray")
	}
}

// TestMemoryStorage_EvictionCounter: cap-triggered evictions increment the wired
// counter so operators can alert on hitting the cap.
func TestMemoryStorage_EvictionCounter(t *testing.T) {
	mp, reader := newTestMeterProvider()
	tel, err := newProcessorTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	ms := newMemoryStorage(time.Hour, time.Hour, 10, zap.NewNop())
	defer func() { _ = ms.Close() }()
	ms.SetEvictionCounter(tel.bucketEvictions)

	for i := 0; i < 100; i++ {
		ms.AllowUpToN(context.Background(), "k"+strconv.Itoa(i), 1, 5, time.Second)
	}

	rm := collectMetrics(t, reader)
	if v := getCounterTotal(findMetric(rm, "otelcol_processor_ratelimit_bucket_evictions")); v <= 0 {
		t.Errorf("bucket_evictions = %d, want > 0 after spraying 100 keys past cap 10", v)
	}
}
