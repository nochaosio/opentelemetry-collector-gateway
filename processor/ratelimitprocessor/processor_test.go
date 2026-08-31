// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

// --- Mock consumers ---

type mockTracesConsumer struct {
	received []ptrace.Traces
}

func (m *mockTracesConsumer) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	m.received = append(m.received, td)
	return nil
}

func (m *mockTracesConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

type mockMetricsConsumer struct {
	received []pmetric.Metrics
}

func (m *mockMetricsConsumer) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	m.received = append(m.received, md)
	return nil
}

func (m *mockMetricsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

type mockLogsConsumer struct {
	received []plog.Logs
}

func (m *mockLogsConsumer) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	m.received = append(m.received, ld)
	return nil
}

func (m *mockLogsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// --- Test data helpers ---

func newTestTraces(serviceName string, spanCount int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", serviceName)
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < spanCount; i++ {
		ss.Spans().AppendEmpty()
	}
	return td
}

func newTestMetrics(serviceName string, dataPointCount int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", serviceName)
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("test_metric")
	m.SetEmptyGauge()
	for i := 0; i < dataPointCount; i++ {
		m.Gauge().DataPoints().AppendEmpty()
	}
	return md
}

func newTestLogs(serviceName string, logRecordCount int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", serviceName)
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < logRecordCount; i++ {
		sl.LogRecords().AppendEmpty()
	}
	return ld
}

// --- Metric assertion helpers ---

func newTestMeterProvider() (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return mp, reader
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

func getCounterValue(m *metricdata.Metrics, attrKey, attrVal string) int64 {
	if m == nil {
		return 0
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		return 0
	}
	for _, dp := range sum.DataPoints {
		val, exists := dp.Attributes.Value(attribute.Key(attrKey))
		if exists && val.AsString() == attrVal {
			return dp.Value
		}
	}
	return 0
}

func getCounterTotal(m *metricdata.Metrics) int64 {
	if m == nil {
		return 0
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		return 0
	}
	var total int64
	for _, dp := range sum.DataPoints {
		total += dp.Value
	}
	return total
}

// --- Default test config ---

func testConfig(rps int) *Config {
	return &Config{
		LimitType:         "service_name",
		RequestsPerSecond: rps,
		DropOnLimit:       true,
		SpecificLimits:    make(map[string]LimitConfig),
	}
}

// testStorage spins up a throw-away memory storage for tests that need
// to build a processor directly. Using newMemoryStorage keeps test
// behavior identical to what the factory would hand the processor at
// runtime when `storage.backend` is unset or "memory".
func testStorage(tb testing.TB) Storage {
	tb.Helper()
	ms := newMemoryStorage(time.Hour, time.Hour, 0, zap.NewNop())
	tb.Cleanup(func() { _ = ms.Close() })
	return ms
}

// --- Traces tests ---

func TestTracesProcessor_Allowed(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockTracesConsumer{}

	tp, err := newTracesProcessor(testConfig(100), zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	err = tp.ConsumeTraces(ctx, newTestTraces("test-service", 5))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(sink.received) != 1 {
		t.Fatalf("expected 1 batch forwarded, got %d", len(sink.received))
	}

	rm := collectMetrics(t, reader)
	received := findMetric(rm, "otelcol_processor_ratelimit_received_items")
	allowed := findMetric(rm, "otelcol_processor_ratelimit_allowed_items")
	denied := findMetric(rm, "otelcol_processor_ratelimit_denied_items")

	if v := getCounterTotal(received); v != 5 {
		t.Errorf("received_items: expected 5, got %d", v)
	}
	if v := getCounterTotal(allowed); v != 5 {
		t.Errorf("allowed_items: expected 5, got %d", v)
	}
	if v := getCounterTotal(denied); v != 0 {
		t.Errorf("denied_items: expected 0, got %d", v)
	}
}

func TestTracesProcessor_Denied(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockTracesConsumer{}

	tp, err := newTracesProcessor(testConfig(3), zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// First batch: 3 spans (consumes all tokens)
	err = tp.ConsumeTraces(ctx, newTestTraces("test-service", 3))
	if err != nil {
		t.Fatalf("first batch should be allowed, got: %v", err)
	}

	// Second batch: 5 spans (should be denied)
	err = tp.ConsumeTraces(ctx, newTestTraces("test-service", 5))
	if err == nil {
		t.Fatal("second batch should be denied")
	}

	if len(sink.received) != 1 {
		t.Fatalf("expected 1 batch forwarded, got %d", len(sink.received))
	}

	rm := collectMetrics(t, reader)
	received := findMetric(rm, "otelcol_processor_ratelimit_received_items")
	denied := findMetric(rm, "otelcol_processor_ratelimit_denied_items")

	if v := getCounterTotal(received); v != 8 {
		t.Errorf("received_items: expected 8 (3+5), got %d", v)
	}
	if v := getCounterTotal(denied); v != 5 {
		t.Errorf("denied_items: expected 5, got %d", v)
	}
}

func TestTracesProcessor_DeniedPassThrough(t *testing.T) {
	mp, _ := newTestMeterProvider()
	sink := &mockTracesConsumer{}

	cfg := testConfig(3)
	cfg.DropOnLimit = false

	tp, err := newTracesProcessor(cfg, zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Consume all tokens
	err = tp.ConsumeTraces(ctx, newTestTraces("test-service", 3))
	if err != nil {
		t.Fatalf("first batch should be allowed: %v", err)
	}

	// Exceeds limit but drop_on_limit=false, should pass through
	err = tp.ConsumeTraces(ctx, newTestTraces("test-service", 5))
	if err != nil {
		t.Fatalf("with drop_on_limit=false, data should pass through: %v", err)
	}

	if len(sink.received) != 2 {
		t.Fatalf("expected 2 batches forwarded (pass-through mode), got %d", len(sink.received))
	}
}

func TestTracesProcessor_MetricAttributes(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockTracesConsumer{}

	// Limit 1 rps so 3 spans → 1 allowed, 2 denied. This exercises both an
	// aggregated counter (received) and a key-labeled one (denied) under the
	// default key_labels_on = [denied, preserved].
	tp, err := newTracesProcessor(testConfig(1), zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = tp.ConsumeTraces(ctx, newTestTraces("my-svc", 3))

	rm := collectMetrics(t, reader)
	received := findMetric(rm, "otelcol_processor_ratelimit_received_items")
	denied := findMetric(rm, "otelcol_processor_ratelimit_denied_items")

	// received is aggregated by default: signal is present, key is not.
	if v := getCounterValue(received, "signal", "traces"); v != 3 {
		t.Errorf("received signal=traces: expected 3, got %d", v)
	}
	if v := getCounterValue(received, "key", "my-svc"); v != 0 {
		t.Errorf("received must be aggregated (no key) by default, got key=my-svc=%d", v)
	}
	// denied keeps the key label by default (the misbehaving set is bounded).
	if v := getCounterValue(denied, "key", "my-svc"); v != 2 {
		t.Errorf("denied key=my-svc: expected 2, got %d", v)
	}
}

// --- Metrics processor tests ---

func TestMetricsProcessor_Allowed(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockMetricsConsumer{}

	proc, err := newMetricsProcessor(testConfig(100), zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	err = proc.ConsumeMetrics(ctx, newTestMetrics("test-service", 4))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(sink.received) != 1 {
		t.Fatalf("expected 1 batch forwarded, got %d", len(sink.received))
	}

	rm := collectMetrics(t, reader)
	received := findMetric(rm, "otelcol_processor_ratelimit_received_items")
	allowed := findMetric(rm, "otelcol_processor_ratelimit_allowed_items")

	if v := getCounterTotal(received); v != 4 {
		t.Errorf("received_items: expected 4, got %d", v)
	}
	if v := getCounterTotal(allowed); v != 4 {
		t.Errorf("allowed_items: expected 4, got %d", v)
	}
}

func TestMetricsProcessor_Denied(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockMetricsConsumer{}

	proc, err := newMetricsProcessor(testConfig(2), zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = proc.ConsumeMetrics(ctx, newTestMetrics("test-service", 2))

	err = proc.ConsumeMetrics(ctx, newTestMetrics("test-service", 5))
	if err == nil {
		t.Fatal("second batch should be denied")
	}

	rm := collectMetrics(t, reader)
	denied := findMetric(rm, "otelcol_processor_ratelimit_denied_items")

	if v := getCounterTotal(denied); v != 5 {
		t.Errorf("denied_items: expected 5, got %d", v)
	}
}

// --- Logs processor tests ---

func TestLogsProcessor_Allowed(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockLogsConsumer{}

	proc, err := newLogsProcessor(testConfig(100), zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	err = proc.ConsumeLogs(ctx, newTestLogs("test-service", 6))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if len(sink.received) != 1 {
		t.Fatalf("expected 1 batch forwarded, got %d", len(sink.received))
	}

	rm := collectMetrics(t, reader)
	received := findMetric(rm, "otelcol_processor_ratelimit_received_items")
	allowed := findMetric(rm, "otelcol_processor_ratelimit_allowed_items")

	if v := getCounterTotal(received); v != 6 {
		t.Errorf("received_items: expected 6, got %d", v)
	}
	if v := getCounterTotal(allowed); v != 6 {
		t.Errorf("allowed_items: expected 6, got %d", v)
	}
}

func TestLogsProcessor_Denied(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockLogsConsumer{}

	proc, err := newLogsProcessor(testConfig(2), zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	_ = proc.ConsumeLogs(ctx, newTestLogs("test-service", 2))

	err = proc.ConsumeLogs(ctx, newTestLogs("test-service", 5))
	if err == nil {
		t.Fatal("second batch should be denied")
	}

	rm := collectMetrics(t, reader)
	denied := findMetric(rm, "otelcol_processor_ratelimit_denied_items")

	if v := getCounterTotal(denied); v != 5 {
		t.Errorf("denied_items: expected 5, got %d", v)
	}
}

// --- Config validation tests ---

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "valid service_name config",
			config:  testConfig(100),
			wantErr: false,
		},
		{
			name: "missing limit_type",
			config: &Config{
				RequestsPerSecond: 100,
			},
			wantErr: true,
		},
		{
			name: "invalid limit_type",
			config: &Config{
				LimitType:         "invalid",
				RequestsPerSecond: 100,
			},
			wantErr: true,
		},
		{
			name: "attribute without key",
			config: &Config{
				LimitType:         "attribute",
				RequestsPerSecond: 100,
			},
			wantErr: true,
		},
		{
			name: "header without key",
			config: &Config{
				LimitType:         "header",
				RequestsPerSecond: 100,
			},
			wantErr: true,
		},
		{
			name: "no rate limits configured",
			config: &Config{
				LimitType: "service_name",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// --- Rate limiter tests ---

func TestRateLimiter_AllowN(t *testing.T) {
	cfg := testConfig(5)
	rl := NewRateLimiter(cfg)
	defer func() { _ = rl.Close() }()

	if !rl.AllowN("svc-a", 5) {
		t.Error("first 5 requests should be allowed")
	}
	if rl.AllowN("svc-a", 1) {
		t.Error("6th request should be denied")
	}
}

func TestRateLimiter_IndependentKeys(t *testing.T) {
	cfg := testConfig(3)
	rl := NewRateLimiter(cfg)
	defer func() { _ = rl.Close() }()

	if !rl.AllowN("svc-a", 3) {
		t.Error("svc-a: first 3 should be allowed")
	}
	if rl.AllowN("svc-a", 1) {
		t.Error("svc-a: 4th should be denied")
	}
	if !rl.AllowN("svc-b", 3) {
		t.Error("svc-b: first 3 should be allowed (independent bucket)")
	}
}

func TestRateLimiter_SpecificLimits(t *testing.T) {
	cfg := &Config{
		LimitType:         "service_name",
		RequestsPerSecond: 10,
		DropOnLimit:       true,
		SpecificLimits: map[string]LimitConfig{
			"limited-svc": {RequestsPerSecond: 2},
		},
	}
	rl := NewRateLimiter(cfg)
	defer func() { _ = rl.Close() }()

	if !rl.AllowN("limited-svc", 2) {
		t.Error("limited-svc: first 2 should be allowed")
	}
	if rl.AllowN("limited-svc", 1) {
		t.Error("limited-svc: 3rd should be denied (limit=2)")
	}
	if !rl.AllowN("other-svc", 10) {
		t.Error("other-svc: first 10 should be allowed (global limit=10)")
	}
}

func TestTokenBucket_AllowZero(t *testing.T) {
	cfg := testConfig(1)
	rl := NewRateLimiter(cfg)
	defer func() { _ = rl.Close() }()

	if !rl.AllowN("svc", 0) {
		t.Error("AllowN(0) should always return true")
	}
}

// --- Extract key tests ---

func TestExtractKey_ServiceName(t *testing.T) {
	mp, _ := newTestMeterProvider()
	rp, err := newRateLimitProcessor(testConfig(100), zap.NewNop(), mp, testStorage(t))
	if err != nil {
		t.Fatal(err)
	}

	attrs := map[string]string{"service.name": "my-svc"}
	key := rp.extractKey(context.Background(), attrs)
	if key != "my-svc" {
		t.Errorf("expected 'my-svc', got '%s'", key)
	}
}

func TestExtractKey_Attribute(t *testing.T) {
	cfg := &Config{
		LimitType:         "attribute",
		AttributeKey:      "tenant.id",
		RequestsPerSecond: 100,
		SpecificLimits:    make(map[string]LimitConfig),
	}
	mp, _ := newTestMeterProvider()
	rp, err := newRateLimitProcessor(cfg, zap.NewNop(), mp, testStorage(t))
	if err != nil {
		t.Fatal(err)
	}

	attrs := map[string]string{"tenant.id": "tenant-42"}
	key := rp.extractKey(context.Background(), attrs)
	if key != "tenant-42" {
		t.Errorf("expected 'tenant-42', got '%s'", key)
	}
}

func TestExtractKey_Default(t *testing.T) {
	mp, _ := newTestMeterProvider()
	rp, err := newRateLimitProcessor(testConfig(100), zap.NewNop(), mp, testStorage(t))
	if err != nil {
		t.Fatal(err)
	}

	key := rp.extractKey(context.Background(), map[string]string{})
	if key != "default" {
		t.Errorf("expected 'default', got '%s'", key)
	}
}

func TestExtractKey_HeaderFromClientInfo(t *testing.T) {
	cfg := &Config{
		LimitType:         "header",
		HeaderKey:         "X-Tenant-ID",
		RequestsPerSecond: 100,
		SpecificLimits:    make(map[string]LimitConfig),
	}
	mp, _ := newTestMeterProvider()
	rp, err := newRateLimitProcessor(cfg, zap.NewNop(), mp, testStorage(t))
	if err != nil {
		t.Fatal(err)
	}

	// gRPC normalizes metadata keys to lower-case; client.Info exposes them
	// that way too. The processor must look up the configured key case-
	// insensitively for the rate limit to actually fire.
	ctx := client.NewContext(context.Background(), client.Info{
		Metadata: client.NewMetadata(map[string][]string{
			"x-tenant-id": {"acme"},
		}),
	})

	if got := rp.extractKey(ctx, map[string]string{}); got != "acme" {
		t.Errorf("expected 'acme' from client.Info metadata, got '%s'", got)
	}

	// No client.Info on the context → fall back to default.
	if got := rp.extractKey(context.Background(), map[string]string{}); got != "default" {
		t.Errorf("expected 'default' when no metadata, got '%s'", got)
	}
}
