// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/nochaosio/opentelemetry-collector-gateway/processor/ratelimitprocessor/internal/metadata"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

// gaugeDataPointCount counts float64-gauge datapoints carrying key=<key>.
func gaugeDataPointCount(m *metricdata.Metrics, key string) int {
	if m == nil {
		return 0
	}
	g, ok := m.Data.(metricdata.Gauge[float64])
	if !ok {
		return 0
	}
	n := 0
	for _, dp := range g.DataPoints {
		v, exists := dp.Attributes.Value(attribute.Key("key"))
		if exists && v.AsString() == key {
			n++
		}
	}
	return n
}

// --- AllowN refund semantics (denied batches must not burn budget) ---

func TestAllowN_PartialDenyRefundsTokens(t *testing.T) {
	rl := NewRateLimiter(testConfig(10))
	defer func() { _ = rl.Close() }()
	defer func() { _ = rl.Close() }()

	if !rl.AllowN("k", 6) {
		t.Fatal("first 6 of 10 must be allowed")
	}
	// Only 4 tokens left: a 6-item batch is denied, but the 4 partially
	// granted tokens must be refunded, not burned.
	if rl.AllowN("k", 6) {
		t.Fatal("second 6 must be denied (only 4 tokens left)")
	}
	if got := rl.AllowUpToN("k", 4); got != 4 {
		t.Errorf("the 4 remaining tokens must survive the denied batch; got %d", got)
	}
}

func TestAllowN_OverCapacityBatchDoesNotStarve(t *testing.T) {
	rl := NewRateLimiter(testConfig(5))
	defer func() { _ = rl.Close() }()
	defer func() { _ = rl.Close() }()

	// A batch larger than the bucket capacity can never be admitted, but it
	// must not drain the bucket either (the starvation bug: repeated
	// over-capacity batches used to zero the budget for everyone else).
	for i := 0; i < 3; i++ {
		if rl.AllowN("k", 10) {
			t.Fatal("over-capacity batch must be denied")
		}
	}
	if got := rl.AllowUpToN("k", 5); got != 5 {
		t.Errorf("bucket must still be full after denied over-capacity batches; got %d", got)
	}
}

func TestAllowN_PartialDenyRefundsGlobal(t *testing.T) {
	cfg := testConfig(10)
	cfg.GlobalRequestsPerSecond = 8
	rl := NewRateLimiter(cfg)
	defer func() { _ = rl.Close() }()
	defer func() { _ = rl.Close() }()

	// Global grants 8 of 10 → key grants 8 → denied; both buckets refunded.
	if rl.AllowN("k", 10) {
		t.Fatal("10 must be denied (global=8)")
	}
	if got := rl.AllowUpToN("k", 8); got != 8 {
		t.Errorf("global budget must survive the denied batch; got %d", got)
	}
}

// --- Multi-resource batches billed per key ---

// newMultiResourceTraces builds one batch with two resources (different
// service.name) of spansA / spansB spans.
func newMultiResourceTraces(svcA string, spansA int, svcB string, spansB int) ptrace.Traces {
	td := newTestTraces(svcA, spansA)
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", svcB)
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < spansB; i++ {
		ss.Spans().AppendEmpty()
	}
	return td
}

func TestTraces_MultiResourceBatch_PerKeyLimits(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockTracesConsumer{}

	cfg := testConfig(100)
	cfg.SpecificLimits = map[string]LimitConfig{
		"svc-b": {RequestsPerSecond: 1},
	}
	tp, err := newTracesProcessor(cfg, zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	// svc-a: 3 spans (limit 100, all pass). svc-b: 3 spans (limit 1 → 1 kept).
	err = tp.ConsumeTraces(context.Background(), newMultiResourceTraces("svc-a", 3, "svc-b", 3))
	if err != nil {
		t.Fatalf("partially-admitted batch must be forwarded, got: %v", err)
	}
	if len(sink.received) != 1 {
		t.Fatalf("expected 1 forwarded batch, got %d", len(sink.received))
	}
	if got := sink.received[0].SpanCount(); got != 4 {
		t.Errorf("expected 4 spans forwarded (3 svc-a + 1 svc-b), got %d", got)
	}

	rm := collectMetrics(t, reader)
	denied := findMetric(rm, "otelcol_processor_ratelimit_denied_items")
	if v := getCounterValue(denied, "key", "svc-b"); v != 2 {
		t.Errorf("denied key=svc-b: expected 2, got %d", v)
	}
	if v := getCounterValue(denied, "key", "svc-a"); v != 0 {
		t.Errorf("denied key=svc-a: expected 0, got %d", v)
	}
}

func TestMetrics_MultiResourceBatch_DropsOnlyDeniedKey(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockMetricsConsumer{}

	cfg := testConfig(100)
	cfg.SpecificLimits = map[string]LimitConfig{
		"svc-b": {RequestsPerSecond: 2},
	}
	proc, err := newMetricsProcessor(cfg, zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	md := newTestMetrics("svc-a", 2)
	rmB := md.ResourceMetrics().AppendEmpty()
	rmB.Resource().Attributes().PutStr("service.name", "svc-b")
	m := rmB.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetEmptyGauge()
	for i := 0; i < 5; i++ {
		m.Gauge().DataPoints().AppendEmpty()
	}

	// svc-b asks 5 with limit 2 → denied whole (metrics are all-or-nothing per
	// key); svc-a passes. Batch is forwarded with only svc-a's resource.
	if err := proc.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("partially-admitted batch must be forwarded, got: %v", err)
	}
	if len(sink.received) != 1 {
		t.Fatalf("expected 1 forwarded batch, got %d", len(sink.received))
	}
	out := sink.received[0]
	if out.ResourceMetrics().Len() != 1 {
		t.Fatalf("expected 1 resource left (svc-a), got %d", out.ResourceMetrics().Len())
	}
	if got := countDataPoints(out); got != 2 {
		t.Errorf("expected 2 datapoints forwarded, got %d", got)
	}

	rm := collectMetrics(t, reader)
	denied := findMetric(rm, "otelcol_processor_ratelimit_denied_items")
	if v := getCounterValue(denied, "key", "svc-b"); v != 5 {
		t.Errorf("denied key=svc-b: expected 5, got %d", v)
	}
}

func TestLogs_MultiResourceBatch_PerKeyLimits(t *testing.T) {
	mp, _ := newTestMeterProvider()
	sink := &mockLogsConsumer{}

	cfg := testConfig(100)
	cfg.SpecificLimits = map[string]LimitConfig{
		"svc-b": {RequestsPerSecond: 1},
	}
	proc, err := newLogsProcessor(cfg, zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	ld := newTestLogs("svc-a", 3)
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svc-b")
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < 3; i++ {
		sl.LogRecords().AppendEmpty()
	}

	if err := proc.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("partially-admitted batch must be forwarded, got: %v", err)
	}
	if got := sink.received[0].LogRecordCount(); got != 4 {
		t.Errorf("expected 4 records forwarded (3 svc-a + 1 svc-b), got %d", got)
	}
}

// --- Shared storage across signal pipelines (factory path) ---

func TestFactory_SharesStorageAcrossSignals(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.RequestsPerSecond = 5
	cfg.DefaultLimit = 0

	set := processortest.NewNopSettings(metadata.Type)
	set.ID = component.NewIDWithName(metadata.Type, "shared-test")

	ctx := context.Background()
	tp, err := factory.CreateTraces(ctx, set, cfg, consumertest.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	lp, err := factory.CreateLogs(ctx, set, cfg, consumertest.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// Drain the key's budget via the traces pipeline...
	if err := tp.ConsumeTraces(ctx, newTestTraces("svc", 5)); err != nil {
		t.Fatalf("first traces batch must pass: %v", err)
	}
	// ...and the logs pipeline must see the SAME bucket (one budget per key
	// across signals, not one per signal).
	if err := lp.ConsumeLogs(ctx, newTestLogs("svc", 5)); err == nil {
		t.Error("logs batch must be denied: traces already consumed the shared budget")
	}

	// Refcounted shutdown: first shutdown must not close the shared backend.
	if err := tp.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := lp.ConsumeLogs(ctx, newTestLogs("other-svc", 1)); err != nil {
		t.Errorf("storage must stay usable until the last pipeline shuts down: %v", err)
	}
	if err := lp.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFactory_SharedGaugesRegisteredOnce(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.MetricsKeyAllowlist = []string{"watched-svc"}

	mp, reader := newTestMeterProvider()
	set := processortest.NewNopSettings(metadata.Type)
	set.ID = component.NewIDWithName(metadata.Type, "gauge-test")
	set.MeterProvider = mp

	ctx := context.Background()
	tp, err := factory.CreateTraces(ctx, set, cfg, consumertest.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	lp, err := factory.CreateLogs(ctx, set, cfg, consumertest.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// Two pipelines, one storage: the bucket gauges must be observed exactly
	// once per key, not once per pipeline (duplicate datapoints).
	rm := collectMetrics(t, reader)
	capMetric := findMetric(rm, "otelcol_processor_ratelimit_bucket_capacity_tokens")
	if capMetric == nil {
		t.Fatal("bucket capacity gauge must be exported when an allowlist is set")
	}
	if v, ok := gaugeValue(capMetric, "watched-svc"); !ok || v <= 0 {
		t.Errorf("expected capacity gauge for watched-svc, got (%v, %v)", v, ok)
	}
	if points := gaugeDataPointCount(capMetric, "watched-svc"); points != 1 {
		t.Errorf("expected exactly 1 datapoint for watched-svc (registered once), got %d", points)
	}

	// After the LAST pipeline shuts down, the callback must be unregistered:
	// no stale observations against a closed storage.
	if err := tp.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := lp.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	rm = collectMetrics(t, reader)
	capMetric = findMetric(rm, "otelcol_processor_ratelimit_bucket_capacity_tokens")
	if capMetric != nil && gaugeDataPointCount(capMetric, "watched-svc") > 0 {
		t.Error("gauge callback must be unregistered after the last pipeline shuts down")
	}
}

// TestShutdown_UnregistersOwnGauges covers the non-shared storage path (tests
// or embedders that hand a Storage directly to the processor).
func TestShutdown_UnregistersOwnGauges(t *testing.T) {
	mp, reader := newTestMeterProvider()
	cfg := testConfig(10)
	cfg.MetricsKeyAllowlist = []string{"svc"}
	tp, err := newTracesProcessor(cfg, zap.NewNop(), mp, testStorage(t), &mockTracesConsumer{})
	if err != nil {
		t.Fatal(err)
	}

	rm := collectMetrics(t, reader)
	if m := findMetric(rm, "otelcol_processor_ratelimit_bucket_capacity_tokens"); m == nil {
		t.Fatal("gauges must be live before shutdown")
	}
	if err := tp.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	rm = collectMetrics(t, reader)
	if m := findMetric(rm, "otelcol_processor_ratelimit_bucket_capacity_tokens"); m != nil && gaugeDataPointCount(m, "svc") > 0 {
		t.Error("gauges must stop being observed after shutdown")
	}
}

// --- key label cardinality backstop (no allowlist) ---

func TestMetricKey_AutoCapWithoutAllowlist(t *testing.T) {
	mp, _ := newTestMeterProvider()
	cfg := testConfig(10)
	cfg.MaxMetricKeys = 2
	storage := testStorage(t)
	defer func() { _ = storage.Close() }()
	rp, err := newRateLimitProcessor(cfg, zap.NewNop(), mp, storage)
	if err != nil {
		t.Fatal(err)
	}

	// First-come keys keep their value (stable series)...
	if got := rp.metricKey("a"); got != "a" {
		t.Errorf("metricKey(a) = %q, want a", got)
	}
	if got := rp.metricKey("b"); got != "b" {
		t.Errorf("metricKey(b) = %q, want b", got)
	}
	// ...later keys collapse to "other" (bounded cardinality)...
	if got := rp.metricKey("c"); got != "other" {
		t.Errorf("metricKey(c) = %q, want other", got)
	}
	// ...and already-seen keys stay stable.
	if got := rp.metricKey("a"); got != "a" {
		t.Errorf("metricKey(a) again = %q, want a", got)
	}
}

func TestMetricKey_DefaultCapIs100(t *testing.T) {
	mp, _ := newTestMeterProvider()
	storage := testStorage(t)
	defer func() { _ = storage.Close() }()
	rp, err := newRateLimitProcessor(testConfig(10), zap.NewNop(), mp, storage)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < defaultMaxMetricKeys; i++ {
		k := "svc-" + strconv.Itoa(i)
		if got := rp.metricKey(k); got != k {
			t.Fatalf("key %d within default cap must keep its value, got %q", i, got)
		}
	}
	if got := rp.metricKey("svc-overflow"); got != "other" {
		t.Errorf("key beyond the default cap must collapse to other, got %q", got)
	}
}

func TestMetricKey_NegativeMeansUnlimited(t *testing.T) {
	mp, _ := newTestMeterProvider()
	cfg := testConfig(10)
	cfg.MaxMetricKeys = -1
	storage := testStorage(t)
	defer func() { _ = storage.Close() }()
	rp, err := newRateLimitProcessor(cfg, zap.NewNop(), mp, storage)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("svc-%d", i)
		if got := rp.metricKey(k); got != k {
			t.Fatalf("unlimited mode must pass keys through, got %q for %q", got, k)
		}
	}
}

// --- negative cache stays bounded ---

func TestNegCache_Bounded(t *testing.T) {
	rs := &redisStorage{
		negCacheTTL: time.Hour, // entries never expire during the test
		negCache:    make(map[string]time.Time),
		logger:      zap.NewNop(),
	}
	for i := 0; i < maxNegCacheEntries+500; i++ {
		rs.negCacheSet("key-" + strconv.Itoa(i))
	}
	if got := len(rs.negCache); got > maxNegCacheEntries {
		t.Errorf("negative cache must stay capped at %d entries, got %d", maxNegCacheEntries, got)
	}
}

// --- memory storage hardening ---

func TestMemoryStorage_CloseIdempotent(t *testing.T) {
	ms := newMemoryStorage(time.Hour, time.Hour, 0, zap.NewNop())
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	// A second Close (processor + rate limiter both own a handle) must not panic.
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
}
