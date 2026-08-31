// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package statefulfilterprocessor

import (
	"context"
	"testing"
	"time"

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

type mockTracesConsumer struct{ received []ptrace.Traces }

func (m *mockTracesConsumer) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	m.received = append(m.received, td)
	return nil
}
func (m *mockTracesConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

type mockLogsConsumer struct{ received []plog.Logs }

func (m *mockLogsConsumer) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	m.received = append(m.received, ld)
	return nil
}
func (m *mockLogsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

type mockMetricsConsumer struct{ received []pmetric.Metrics }

func (m *mockMetricsConsumer) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	m.received = append(m.received, md)
	return nil
}
func (m *mockMetricsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// --- Static store, so processor tests exercise matching without Redis ---

type staticStore struct {
	rs     *ruleSet
	closed bool
}

func (s *staticStore) current() *ruleSet                 { return s.rs }
func (s *staticStore) initialLoad(context.Context) error { return nil }
func (s *staticStore) start()                            {}
func (s *staticStore) close() error                      { s.closed = true; return nil }
func (s *staticStore) stats() storeStats {
	return storeStats{
		version:     s.rs.version,
		rulesLoaded: s.rs.total,
		rulesBad:    s.rs.invalid,
		lastSuccess: time.Now(),
		everLoaded:  true,
	}
}

func storeWith(docs map[string]string) *staticStore {
	return &staticStore{rs: buildRuleSet(docs, 1, defaultMaxRules, nil)}
}

func testConfig() *Config {
	cfg := createDefaultConfig().(*Config)
	cfg.WaitForInitialLoad = false
	return cfg
}

// --- Test data helpers ---

func tracesWith(service string, spanNames ...string) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", service)
	ss := rs.ScopeSpans().AppendEmpty()
	for _, n := range spanNames {
		sp := ss.Spans().AppendEmpty()
		sp.SetName(n)
		sp.Attributes().PutStr("http.route", n)
	}
	return td
}

func logsWith(service string, severities ...string) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", service)
	sl := rl.ScopeLogs().AppendEmpty()
	for _, s := range severities {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetSeverityText(s)
		lr.Body().SetStr("msg from " + service)
	}
	return ld
}

func metricsWith(service, metricName string, routes ...string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", service)
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(metricName)
	dps := m.SetEmptyGauge().DataPoints()
	for _, r := range routes {
		dp := dps.AppendEmpty()
		dp.SetIntValue(1)
		dp.Attributes().PutStr("http.route", r)
	}
	return md
}

func countSpans(td ptrace.Traces) int {
	n := 0
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			n += rs.ScopeSpans().At(j).Spans().Len()
		}
	}
	return n
}

func countLogs(ld plog.Logs) int {
	n := 0
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			n += rl.ScopeLogs().At(j).LogRecords().Len()
		}
	}
	return n
}

func countDataPoints(md pmetric.Metrics) int {
	n := 0
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)
				if m.Type() == pmetric.MetricTypeGauge {
					n += m.Gauge().DataPoints().Len()
				}
			}
		}
	}
	return n
}

// --- Traces ---

func newTracesTest(t *testing.T, docs map[string]string) (*tracesProcessor, *mockTracesConsumer) {
	t.Helper()
	next := &mockTracesConsumer{}
	tp, err := newTracesProcessor(testConfig(), zap.NewNop(), noopMeterProvider(), storeWith(docs), next)
	if err != nil {
		t.Fatalf("newTracesProcessor: %v", err)
	}
	return tp, next
}

func TestTraces_NoRulesPassThrough(t *testing.T) {
	tp, next := newTracesTest(t, nil)

	if err := tp.ConsumeTraces(context.Background(), tracesWith("checkout", "a", "b")); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if len(next.received) != 1 || countSpans(next.received[0]) != 2 {
		t.Fatalf("expected both spans forwarded untouched, got %d batches", len(next.received))
	}
}

func TestTraces_DropsMatchingSpansOnly(t *testing.T) {
	tp, next := newTracesTest(t, map[string]string{
		"drop-health": `{"signals":["traces"],"conditions":[{"source":"attribute","key":"http.route","op":"prefix","value":"/healthz"}]}`,
	})

	td := tracesWith("checkout", "/healthz", "/checkout", "/healthz/ready")
	if err := tp.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if len(next.received) != 1 {
		t.Fatalf("expected the batch to be forwarded, got %d", len(next.received))
	}
	if got := countSpans(next.received[0]); got != 1 {
		t.Fatalf("expected only /checkout to survive, got %d spans", got)
	}
}

func TestTraces_KeepRuleOverridesDrop(t *testing.T) {
	tp, next := newTracesTest(t, map[string]string{
		"a-drop-all-checkout": `{"signals":["traces"],"conditions":[{"source":"resource","key":"service.name","value":"checkout"}]}`,
		"b-keep-errors":       `{"action":"keep","signals":["traces"],"conditions":[{"source":"attribute","key":"http.route","value":"/pay"}]}`,
	})

	if err := tp.ConsumeTraces(context.Background(), tracesWith("checkout", "/healthz", "/pay")); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if got := countSpans(next.received[0]); got != 1 {
		t.Fatalf("expected the keep rule to rescue exactly 1 span, got %d", got)
	}
}

func TestTraces_FullyDroppedBatchIsSwallowed(t *testing.T) {
	tp, next := newTracesTest(t, map[string]string{
		"drop-all-noisy": `{"signals":["traces"],"conditions":[{"source":"resource","key":"service.name","value":"noisy"}]}`,
	})

	// No error: the producer did nothing wrong, and an error would make a
	// compliant SDK retry data we deliberately drop.
	if err := tp.ConsumeTraces(context.Background(), tracesWith("noisy", "a", "b")); err != nil {
		t.Fatalf("expected nil error for a fully dropped batch, got %v", err)
	}
	if len(next.received) != 0 {
		t.Fatalf("expected nothing forwarded, got %d batches", len(next.received))
	}
}

func TestTraces_ExpiredRuleNoLongerDrops(t *testing.T) {
	tp, next := newTracesTest(t, map[string]string{
		"expired": `{"signals":["traces"],"expires_at":"2020-01-01T00:00:00Z","conditions":[{"source":"resource","key":"service.name","value":"noisy"}]}`,
	})

	if err := tp.ConsumeTraces(context.Background(), tracesWith("noisy", "a", "b")); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if got := countSpans(next.received[0]); got != 2 {
		t.Fatalf("expected an expired rule to drop nothing, got %d spans", got)
	}
}

func TestTraces_RuleScopedToOtherSignalIsInert(t *testing.T) {
	tp, next := newTracesTest(t, map[string]string{
		"logs-only": `{"signals":["logs"],"conditions":[{"source":"resource","key":"service.name","value":"noisy"}]}`,
	})

	if err := tp.ConsumeTraces(context.Background(), tracesWith("noisy", "a")); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if got := countSpans(next.received[0]); got != 1 {
		t.Fatalf("a logs-scoped rule must not touch traces, got %d spans", got)
	}
}

// --- Logs ---

func TestLogs_DropsBySeverityAndBody(t *testing.T) {
	next := &mockLogsConsumer{}
	lp, err := newLogsProcessor(testConfig(), zap.NewNop(), noopMeterProvider(), storeWith(map[string]string{
		"drop-debug": `{"signals":["logs"],"conditions":[{"source":"severity","value":"DEBUG"}]}`,
	}), next)
	if err != nil {
		t.Fatalf("newLogsProcessor: %v", err)
	}

	if err := lp.ConsumeLogs(context.Background(), logsWith("api", "DEBUG", "INFO", "DEBUG")); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}
	if got := countLogs(next.received[0]); got != 1 {
		t.Fatalf("expected only the INFO record to survive, got %d", got)
	}
}

func TestLogs_BodyConditionIsEvaluated(t *testing.T) {
	next := &mockLogsConsumer{}
	lp, err := newLogsProcessor(testConfig(), zap.NewNop(), noopMeterProvider(), storeWith(map[string]string{
		"drop-body": `{"signals":["logs"],"conditions":[{"source":"body","op":"contains","value":"from noisy"}]}`,
	}), next)
	if err != nil {
		t.Fatalf("newLogsProcessor: %v", err)
	}

	if err := lp.ConsumeLogs(context.Background(), logsWith("noisy", "INFO", "INFO")); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}
	if len(next.received) != 0 {
		t.Fatalf("expected all records dropped by the body rule, got %d batches", len(next.received))
	}
}

// --- Metrics ---

func TestMetrics_DropsDataPointsNotWholeSeries(t *testing.T) {
	next := &mockMetricsConsumer{}
	mp, err := newMetricsProcessor(testConfig(), zap.NewNop(), noopMeterProvider(), storeWith(map[string]string{
		"drop-health-dp": `{"signals":["metrics"],"conditions":[{"source":"attribute","key":"http.route","value":"/healthz"}]}`,
	}), next)
	if err != nil {
		t.Fatalf("newMetricsProcessor: %v", err)
	}

	md := metricsWith("api", "http.server.duration", "/healthz", "/orders", "/healthz")
	if err := mp.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	if got := countDataPoints(next.received[0]); got != 1 {
		t.Fatalf("expected only the /orders data point to survive, got %d", got)
	}
}

func TestMetrics_DropsByMetricName(t *testing.T) {
	next := &mockMetricsConsumer{}
	mp, err := newMetricsProcessor(testConfig(), zap.NewNop(), noopMeterProvider(), storeWith(map[string]string{
		"drop-metric": `{"signals":["metrics"],"conditions":[{"source":"name","op":"prefix","value":"jvm."}]}`,
	}), next)
	if err != nil {
		t.Fatalf("newMetricsProcessor: %v", err)
	}

	if err := mp.ConsumeMetrics(context.Background(), metricsWith("api", "jvm.gc.count", "/a", "/b")); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	if len(next.received) != 0 {
		t.Fatalf("expected the whole jvm.* metric to be removed, got %d batches", len(next.received))
	}
}

// --- Telemetry ---

func TestTelemetry_DroppedItemsLabelledByRule(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	next := &mockTracesConsumer{}
	tp, err := newTracesProcessor(testConfig(), zap.NewNop(), mp, storeWith(map[string]string{
		"drop-health": `{"signals":["traces"],"conditions":[{"source":"attribute","key":"http.route","op":"prefix","value":"/healthz"}]}`,
	}), next)
	if err != nil {
		t.Fatalf("newTracesProcessor: %v", err)
	}

	if err := tp.ConsumeTraces(context.Background(), tracesWith("api", "/healthz", "/orders", "/healthz")); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	dropped := sumCounter(t, &rm, "otelcol_processor_statefulfilter_dropped_items", attribute.String("rule", "drop-health"))
	if dropped != 2 {
		t.Fatalf("expected 2 dropped items attributed to drop-health, got %d", dropped)
	}
	evaluated := sumCounter(t, &rm, "otelcol_processor_statefulfilter_evaluated_items")
	if evaluated != 3 {
		t.Fatalf("expected 3 evaluated items, got %d", evaluated)
	}
}

func TestTelemetry_RuleStateGaugesExported(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	store := storeWith(map[string]string{
		"good": `{"conditions":[{"source":"name","value":"x"}]}`,
		"bad":  `{"conditions":[{"source":"nonsense"}]}`,
	})
	if _, err := newTracesProcessor(testConfig(), zap.NewNop(), mp, store, &mockTracesConsumer{}); err != nil {
		t.Fatalf("newTracesProcessor: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	if got := gaugeValue(t, &rm, "otelcol_processor_statefulfilter_rules_loaded"); got != 1 {
		t.Fatalf("expected rules_loaded=1, got %d", got)
	}
	if got := gaugeValue(t, &rm, "otelcol_processor_statefulfilter_rules_invalid"); got != 1 {
		t.Fatalf("expected rules_invalid=1, got %d", got)
	}
	if got := gaugeValue(t, &rm, "otelcol_processor_statefulfilter_rules_version"); got != 1 {
		t.Fatalf("expected rules_version=1, got %d", got)
	}
}

// --- Lifecycle ---

func TestStart_FailOpenWhenInitialLoadFails(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.applyDefaults()
	cfg.InitialLoadTimeout = 50 * time.Millisecond

	tp, err := newTracesProcessor(cfg, zap.NewNop(), noopMeterProvider(), &failingStore{}, &mockTracesConsumer{})
	if err != nil {
		t.Fatalf("newTracesProcessor: %v", err)
	}

	// Default posture: a rule-store outage must not stop the gateway from
	// accepting telemetry.
	if err := tp.start(context.Background(), nil); err != nil {
		t.Fatalf("expected fail-open start, got %v", err)
	}
}

func TestStart_FailClosedWhenConfigured(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.applyDefaults()
	cfg.InitialLoadTimeout = 50 * time.Millisecond
	cfg.FailClosedOnEmpty = true

	tp, err := newTracesProcessor(cfg, zap.NewNop(), noopMeterProvider(), &failingStore{}, &mockTracesConsumer{})
	if err != nil {
		t.Fatalf("newTracesProcessor: %v", err)
	}
	if err := tp.start(context.Background(), nil); err == nil {
		t.Fatal("expected startup to fail when fail_closed_on_empty is set")
	}
}

func TestShutdown_ClosesStore(t *testing.T) {
	store := storeWith(nil)
	tp, err := newTracesProcessor(testConfig(), zap.NewNop(), noopMeterProvider(), store, &mockTracesConsumer{})
	if err != nil {
		t.Fatalf("newTracesProcessor: %v", err)
	}
	if err := tp.shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !store.closed {
		t.Fatal("expected the rule store to be closed on shutdown")
	}
}

type failingStore struct{ staticStore }

func (s *failingStore) current() *ruleSet { return emptyRuleSet() }
func (s *failingStore) initialLoad(context.Context) error {
	return context.DeadlineExceeded
}
func (s *failingStore) stats() storeStats { return storeStats{} }
