// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"context"
	"reflect"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

// gaugeValue reads a float64 observable-gauge datapoint by its `key` attribute.
func gaugeValue(m *metricdata.Metrics, key string) (float64, bool) {
	if m == nil {
		return 0, false
	}
	g, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		return 0, false
	}
	for _, dp := range g.DataPoints {
		v, exists := dp.Attributes.Value(attribute.Key("key"))
		if exists && v.AsString() == key {
			return dp.Value, true
		}
	}
	return 0, false
}

// TestMetricKey_Unbounded: with no allowlist, every key is exported verbatim
// (backward-compatible behavior).
func TestMetricKey_Unbounded(t *testing.T) {
	rp := &rateLimitProcessor{} // metricKeyAllow nil = unbounded
	for _, k := range []string{"payment-service", "anything", "default"} {
		if got := rp.metricKey(k); got != k {
			t.Errorf("metricKey(%q) = %q, want %q (unbounded must pass through)", k, got, k)
		}
	}
}

// TestMetricKey_Allowlist: only listed keys keep their value; every other key
// collapses to "other" so cardinality stays bounded at len(allowlist)+1.
func TestMetricKey_Allowlist(t *testing.T) {
	rp := &rateLimitProcessor{
		metricKeyAllow: map[string]struct{}{
			"payment-service": {},
			"legacy-service":  {},
		},
	}
	// Listed keys survive verbatim.
	for _, k := range []string{"payment-service", "legacy-service"} {
		if got := rp.metricKey(k); got != k {
			t.Errorf("metricKey(%q) = %q, want %q (allowlisted)", k, got, k)
		}
	}
	// Everything else folds into a single "other" series.
	for _, k := range []string{"checkout-service", "svc-19999", "default"} {
		if got := rp.metricKey(k); got != "other" {
			t.Errorf("metricKey(%q) = %q, want \"other\" (not allowlisted)", k, got)
		}
	}
}

// TestMetricsKeyAllowlist_BuiltFromConfig: the constructor turns the config
// slice into the lookup set, and an empty slice stays unbounded (nil set).
func TestMetricsKeyAllowlist_BuiltFromConfig(t *testing.T) {
	mp, _ := newTestMeterProvider()
	storage := testStorage(t)
	defer func() { _ = storage.Close() }()

	withList := createDefaultConfig().(*Config)
	withList.MetricsKeyAllowlist = []string{"a", "b"}
	rp, err := newRateLimitProcessor(withList, zap.NewNop(), mp, storage)
	if err != nil {
		t.Fatalf("newRateLimitProcessor: %v", err)
	}
	if rp.metricKeyAllow == nil {
		t.Fatal("metricKeyAllow must be non-nil when allowlist is configured")
	}
	if got := rp.metricKey("a"); got != "a" {
		t.Errorf("metricKey(\"a\") = %q, want \"a\"", got)
	}
	if got := rp.metricKey("c"); got != "other" {
		t.Errorf("metricKey(\"c\") = %q, want \"other\"", got)
	}

	empty := createDefaultConfig().(*Config) // no allowlist
	rp2, err := newRateLimitProcessor(empty, zap.NewNop(), mp, storage)
	if err != nil {
		t.Fatalf("newRateLimitProcessor (empty): %v", err)
	}
	if rp2.metricKeyAllow != nil {
		t.Error("empty allowlist must stay unbounded (nil set)")
	}
	if got := rp2.metricKey("c"); got != "c" {
		t.Errorf("metricKey(\"c\") = %q, want \"c\" (unbounded)", got)
	}
}

// TestKeyLabelSet_Default: with key_labels_on unset, only denied + preserved
// carry the `key` label. received/allowed are aggregated so the active-series
// count stays bounded regardless of how many distinct keys flow through.
func TestKeyLabelSet_Default(t *testing.T) {
	cfg := createDefaultConfig().(*Config) // KeyLabelsOn unset
	want := map[string]bool{"denied": true, "preserved": true}
	if got := cfg.keyLabelSet(); !reflect.DeepEqual(got, want) {
		t.Errorf("keyLabelSet() default = %v, want %v", got, want)
	}
}

// TestKeyLabelSet_Explicit: an explicit list is honored verbatim, including the
// legacy "key on every counter" configuration.
func TestKeyLabelSet_Explicit(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.KeyLabelsOn = []string{"received", "allowed", "denied", "preserved"}
	got := cfg.keyLabelSet()
	for _, c := range []string{"received", "allowed", "denied", "preserved"} {
		if !got[c] {
			t.Errorf("keyLabelSet() missing %q for legacy all-on config", c)
		}
	}
}

// TestKeyLabelsOn_Validation: unknown counter names are rejected.
func TestKeyLabelsOn_Validation(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.KeyLabelsOn = []string{"denied", "bogus"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() must reject unknown key_labels_on entries")
	}

	cfg.KeyLabelsOn = []string{"received", "denied"}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected valid key_labels_on: %v", err)
	}
}

// TestKeyLabelOn_BuiltFromConfig: the constructor wires keyLabelSet() into the
// processor so the per-counter decision is available on the hot path.
func TestKeyLabelOn_BuiltFromConfig(t *testing.T) {
	mp, _ := newTestMeterProvider()
	storage := testStorage(t)
	defer func() { _ = storage.Close() }()

	cfg := createDefaultConfig().(*Config) // default: denied + preserved
	rp, err := newRateLimitProcessor(cfg, zap.NewNop(), mp, storage)
	if err != nil {
		t.Fatalf("newRateLimitProcessor: %v", err)
	}
	if rp.keyLabelOn[counterReceived] || rp.keyLabelOn[counterAllowed] {
		t.Error("received/allowed must be aggregated (no key) by default")
	}
	if !rp.keyLabelOn[counterDenied] || !rp.keyLabelOn[counterPreserved] {
		t.Error("denied/preserved must keep the key label by default")
	}
}

// TestDropLogThrottle: the first call emits, calls within the window are
// suppressed and counted, and the next emission reports the suppressed total.
func TestDropLogThrottle(t *testing.T) {
	var d dropLogThrottle
	if _, emit := d.next(); !emit {
		t.Fatal("first call must emit")
	}
	for i := 0; i < 5; i++ {
		if _, emit := d.next(); emit {
			t.Fatal("calls within the interval must be suppressed")
		}
	}
	// Force the throttle window open without sleeping.
	d.last = time.Now().Add(-2 * dropLogInterval)
	sup, emit := d.next()
	if !emit {
		t.Fatal("call after the interval must emit")
	}
	if sup != 5 {
		t.Errorf("suppressed = %d, want 5", sup)
	}
}

// TestAvailableTokens_Memory: the memory backend reports free tokens only after
// a bucket exists, and the count drops as the bucket is drained.
func TestAvailableTokens_Memory(t *testing.T) {
	ms := newMemoryStorage(time.Hour, time.Hour, 0, zap.NewNop())
	defer func() { _ = ms.Close() }()

	if _, ok := ms.AvailableTokens(context.Background(), "svc"); ok {
		t.Fatal("AvailableTokens must report ok=false before any traffic")
	}

	// Capacity 5; consume 3 -> ~2 free.
	ms.AllowUpToN(context.Background(), "svc", 3, 5, time.Second)
	free, ok := ms.AvailableTokens(context.Background(), "svc")
	if !ok {
		t.Fatal("AvailableTokens must report ok=true after the bucket is created")
	}
	if free < 1.5 || free > 2.5 {
		t.Errorf("free tokens = %.2f, want ~2 (capacity 5 minus 3 consumed)", free)
	}
}

// TestBucketGauges_Exported: with an allowlist set, the capacity/available
// gauges are observed for the watched key, and occupied = capacity - available
// reflects a drained bucket.
func TestBucketGauges_Exported(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockTracesConsumer{}

	cfg := &Config{
		LimitType:           "service_name",
		RequestsPerSecond:   2, // capacity = 2 tokens
		DropOnLimit:         true,
		MetricsKeyAllowlist: []string{"my-svc"},
		SpecificLimits:      map[string]LimitConfig{},
	}
	tp, err := newTracesProcessor(cfg, zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	// 5 spans against a 2-token bucket -> 2 allowed, bucket drained to ~0.
	_ = tp.ConsumeTraces(context.Background(), newTestTraces("my-svc", 5))

	rm := collectMetrics(t, reader)
	capM := findMetric(rm, "otelcol_processor_ratelimit_bucket_capacity_tokens")
	availM := findMetric(rm, "otelcol_processor_ratelimit_bucket_available_tokens")

	capacity, ok := gaugeValue(capM, "my-svc")
	if !ok || capacity != 2 {
		t.Errorf("bucket capacity for my-svc = %.2f (ok=%v), want 2", capacity, ok)
	}
	avail, ok := gaugeValue(availM, "my-svc")
	if !ok {
		t.Fatal("bucket available gauge missing for my-svc")
	}
	if avail > 1 {
		t.Errorf("bucket available = %.2f, want near 0 (drained)", avail)
	}
	// occupied = capacity - available should be high (bucket mostly consumed).
	if occupied := capacity - avail; occupied < 1 {
		t.Errorf("occupied = %.2f, want > 1 (bucket drained)", occupied)
	}
}

// TestBucketGauges_OffWithoutAllowlist: no allowlist => no bounded watch set =>
// the bucket gauges are not exported at all.
func TestBucketGauges_OffWithoutAllowlist(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockTracesConsumer{}

	tp, err := newTracesProcessor(testConfig(2), zap.NewNop(), mp, testStorage(t), sink) // no allowlist
	if err != nil {
		t.Fatal(err)
	}
	_ = tp.ConsumeTraces(context.Background(), newTestTraces("my-svc", 5))

	rm := collectMetrics(t, reader)
	if m := findMetric(rm, "otelcol_processor_ratelimit_bucket_capacity_tokens"); m != nil {
		t.Error("bucket gauges must be off when no metrics_key_allowlist is set")
	}
}
