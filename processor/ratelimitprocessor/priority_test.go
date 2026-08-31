// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// --- Helpers for building traces/logs with mixed priorities ---

func newTracesWithErrors(serviceName string, normal, errors int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", serviceName)
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < normal; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetName("normal")
		sp.Status().SetCode(ptrace.StatusCodeOk)
	}
	for i := 0; i < errors; i++ {
		sp := ss.Spans().AppendEmpty()
		sp.SetName("error")
		sp.Status().SetCode(ptrace.StatusCodeError)
	}
	return td
}

func countTdSpans(td ptrace.Traces) int {
	n := 0
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			n += rs.ScopeSpans().At(j).Spans().Len()
		}
	}
	return n
}

func newLogsWithSeverity(serviceName string, info, errs int) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", serviceName)
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < info; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
	}
	for i := 0; i < errs; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetSeverityNumber(plog.SeverityNumberError)
	}
	return ld
}

func countLdRecords(ld plog.Logs) int {
	n := 0
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			n += rl.ScopeLogs().At(j).LogRecords().Len()
		}
	}
	return n
}

// --- Priority preservation tests ---

func TestTraces_PreserveErrors_DropsOnlyNormal(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockTracesConsumer{}

	cfg := testConfig(3)
	cfg.PreserveErrors = true

	tp, err := newTracesProcessor(cfg, zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// Batch: 10 normal + 5 error spans, but budget is only 3 tokens.
	// Expected: 5 errors preserved + 3 normal allowed = 8 forwarded, 7 normal dropped.
	err = tp.ConsumeTraces(ctx, newTracesWithErrors("svc-a", 10, 5))
	if err != nil {
		t.Fatalf("batch with errors should forward via priority bypass, got: %v", err)
	}
	if len(sink.received) != 1 {
		t.Fatalf("expected 1 forwarded batch, got %d", len(sink.received))
	}

	// Sink should have exactly 8 spans (5 err + 3 normal).
	got := countTdSpans(sink.received[0])
	if got != 8 {
		t.Errorf("expected 8 spans forwarded (5 critical + 3 normal), got %d", got)
	}

	// Verify all 5 error spans survived.
	errCount := 0
	for i := 0; i < sink.received[0].ResourceSpans().Len(); i++ {
		rs := sink.received[0].ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				if ss.Spans().At(k).Status().Code() == ptrace.StatusCodeError {
					errCount++
				}
			}
		}
	}
	if errCount != 5 {
		t.Errorf("expected 5 error spans preserved, got %d", errCount)
	}

	// Telemetry: denied=7 (normal overflow), preserved=5.
	rm := collectMetrics(t, reader)
	denied := findMetric(rm, "otelcol_processor_ratelimit_denied_items")
	preserved := findMetric(rm, "otelcol_processor_ratelimit_preserved_items")

	if v := getCounterTotal(denied); v != 7 {
		t.Errorf("denied: expected 7, got %d", v)
	}
	if v := getCounterTotal(preserved); v != 5 {
		t.Errorf("preserved: expected 5, got %d", v)
	}
}

func TestTraces_PreserveErrors_Disabled(t *testing.T) {
	mp, _ := newTestMeterProvider()
	sink := &mockTracesConsumer{}

	cfg := testConfig(3)
	cfg.PreserveErrors = false

	tp, err := newTracesProcessor(cfg, zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	// 2 normal + 5 errors = 7 total, budget=3, preserve=off → 3 forwarded (mixed).
	err = tp.ConsumeTraces(context.Background(), newTracesWithErrors("svc", 2, 5))
	if err != nil {
		t.Fatalf("expected partial allow, got: %v", err)
	}
	got := countTdSpans(sink.received[0])
	if got != 3 {
		t.Errorf("expected 3 spans forwarded (no priority), got %d", got)
	}
}

func TestLogs_PreserveErrors_DropsOnlyNormal(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockLogsConsumer{}

	cfg := testConfig(2)
	cfg.PreserveErrors = true

	lp, err := newLogsProcessor(cfg, zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	// 8 info + 4 error, budget=2 → 4 error preserved + 2 info allowed = 6 forwarded.
	if err := lp.ConsumeLogs(context.Background(), newLogsWithSeverity("svc", 8, 4)); err != nil {
		t.Fatalf("expected forward, got: %v", err)
	}
	got := countLdRecords(sink.received[0])
	if got != 6 {
		t.Errorf("expected 6 logs forwarded, got %d", got)
	}

	rm := collectMetrics(t, reader)
	preserved := findMetric(rm, "otelcol_processor_ratelimit_preserved_items")
	if v := getCounterTotal(preserved); v != 4 {
		t.Errorf("preserved logs: expected 4, got %d", v)
	}
}

// --- Fair-share / noisy-neighbor tests ---

func TestFairShare_NoisyNeighborCannotStarveOthers(t *testing.T) {
	// Global = 100 rps, max_share_ratio = 0.2 → each key capped at 20 rps.
	cfg := &Config{
		LimitType:               "service_name",
		GlobalRequestsPerSecond: 100,
		MaxShareRatio:           0.2,
		DropOnLimit:             true,
		PreserveErrors:          true,
		SpecificLimits:          make(map[string]LimitConfig),
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	rl := NewRateLimiter(cfg)
	defer func() { _ = rl.Close() }()

	// Noisy service asks for 200: should be capped at 20 (its per-key ceiling).
	noisy := rl.AllowUpToN("noisy-svc", 200)
	if noisy != 20 {
		t.Errorf("noisy-svc expected 20 allowed (20%% of 100), got %d", noisy)
	}

	// Other services still have global budget available (100 - 20 = 80).
	// Each of 4 quiet services asks for 20: all should succeed.
	quiet := []string{"svc-b", "svc-c", "svc-d", "svc-e"}
	for _, s := range quiet {
		got := rl.AllowUpToN(s, 20)
		if got != 20 {
			t.Errorf("%s expected 20, got %d", s, got)
		}
	}

	// Global budget now exhausted (20+80=100). Another quiet service gets 0.
	if got := rl.AllowUpToN("svc-f", 20); got != 0 {
		t.Errorf("svc-f after global exhaustion: expected 0, got %d", got)
	}
}

func TestFairShare_SpecificLimitAlsoCapped(t *testing.T) {
	cfg := &Config{
		LimitType:               "service_name",
		GlobalRequestsPerSecond: 100,
		MaxShareRatio:           0.1,
		DropOnLimit:             true,
		SpecificLimits: map[string]LimitConfig{
			// User asked for 50, but fair-share caps at 10.
			"greedy": {RequestsPerSecond: 50},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	limit, period := cfg.GetLimit("greedy")
	if limit != 10 {
		t.Errorf("greedy capped limit: expected 10, got %d (period=%v)", limit, period)
	}
}

func TestFairShare_GlobalCeilingRespected(t *testing.T) {
	cfg := &Config{
		LimitType:               "service_name",
		GlobalRequestsPerSecond: 50,
		RequestsPerSecond:       100, // per-key higher than global
		DropOnLimit:             true,
		PreserveErrors:          true,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	rl := NewRateLimiter(cfg)
	defer func() { _ = rl.Close() }()

	// Single service tries 100, global is 50 → should get 50.
	if got := rl.AllowUpToN("svc-x", 100); got != 50 {
		t.Errorf("expected 50 (global cap), got %d", got)
	}
	// Next request: global empty → 0.
	if got := rl.AllowUpToN("svc-y", 10); got != 0 {
		t.Errorf("expected 0 after global exhaust, got %d", got)
	}
}

func TestFairShare_Validation(t *testing.T) {
	// max_share_ratio without global → error.
	cfg := &Config{
		LimitType:         "service_name",
		RequestsPerSecond: 100,
		MaxShareRatio:     0.5,
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error: max_share_ratio without global")
	}

	// ratio > 1 → error.
	cfg2 := &Config{
		LimitType:               "service_name",
		GlobalRequestsPerSecond: 100,
		MaxShareRatio:           1.5,
	}
	if err := cfg2.Validate(); err == nil {
		t.Error("expected validation error: ratio > 1")
	}
}

// --- Integration: priority + fair share together ---

func TestIntegration_PriorityAndFairShare(t *testing.T) {
	// global=20 rps, each key capped at 50% = 10.
	cfg := &Config{
		LimitType:               "service_name",
		GlobalRequestsPerSecond: 20,
		MaxShareRatio:           0.5,
		DropOnLimit:             true,
		PreserveErrors:          true,
		SpecificLimits:          make(map[string]LimitConfig),
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	mp, _ := newTestMeterProvider()
	sink := &mockTracesConsumer{}
	tp, err := newTracesProcessor(cfg, zap.NewNop(), mp, testStorage(t), sink)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// svc-a: 100 normal + 3 errors. Per-key cap = 10. Should forward 10 normal + 3 errors = 13.
	if err := tp.ConsumeTraces(ctx, newTracesWithErrors("svc-a", 100, 3)); err != nil {
		t.Fatalf("svc-a: %v", err)
	}
	gotA := countTdSpans(sink.received[0])
	if gotA != 13 {
		t.Errorf("svc-a: expected 13 forwarded (10 normal + 3 err), got %d", gotA)
	}

	// svc-b: 50 normal + 2 errors. Global budget remaining ≈ 10. svc-b per-key also 10.
	// Forward: 10 normal + 2 errors = 12.
	if err := tp.ConsumeTraces(ctx, newTracesWithErrors("svc-b", 50, 2)); err != nil {
		t.Fatalf("svc-b: %v", err)
	}
	gotB := countTdSpans(sink.received[1])
	if gotB != 12 {
		t.Errorf("svc-b: expected 12 forwarded, got %d", gotB)
	}

	// svc-c: 5 normal + 1 error. Global exhausted.
	// Critical bypass always forwards error. Normal allowed = 0.
	// Forward: 1 error only.
	if err := tp.ConsumeTraces(ctx, newTracesWithErrors("svc-c", 5, 1)); err != nil {
		t.Fatalf("svc-c: error path with critical should still forward, got: %v", err)
	}
	gotC := countTdSpans(sink.received[2])
	if gotC != 1 {
		t.Errorf("svc-c: expected 1 (critical only), got %d", gotC)
	}
}
