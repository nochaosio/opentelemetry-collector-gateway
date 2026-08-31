package ratelimitprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// newTestTracesMulti builds a batch grouped by trace_id: traceSpans[i] is the
// span count for trace i (given a distinct non-zero trace_id). Traces listed in
// errorTraces get one error span (status=Error).
func newTestTracesMulti(serviceName string, traceSpans []int, errorTraces map[int]bool) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", serviceName)
	ss := rs.ScopeSpans().AppendEmpty()
	for ti, n := range traceSpans {
		var tid pcommon.TraceID
		tid[0] = byte(ti + 1) // distinct, non-zero
		for s := 0; s < n; s++ {
			sp := ss.Spans().AppendEmpty()
			sp.SetTraceID(tid)
			if errorTraces[ti] && s == 0 {
				sp.Status().SetCode(ptrace.StatusCodeError)
			}
		}
	}
	return td
}

func traceAwareConfig(rps int) *Config {
	c := testConfig(rps)
	c.TraceIDAware = true
	c.PreserveErrors = true
	return c
}

// spansByTrace returns trace_id -> span count for everything the sink received.
func spansByTrace(sink *mockTracesConsumer) map[pcommon.TraceID]int {
	out := map[pcommon.TraceID]int{}
	for _, td := range sink.received {
		forEachSpan(td, func(sp ptrace.Span) { out[sp.TraceID()]++ })
	}
	return out
}

// TestTraceAware_NoPartialTraces: every trace that survives keeps ALL its spans,
// and dropped traces vanish entirely. No trace is ever split.
func TestTraceAware_NoPartialTraces(t *testing.T) {
	mp, _ := newTestMeterProvider()
	sink := &mockTracesConsumer{}
	// capacity 5; three traces of 3 spans each (9 normal spans).
	tp, err := newTracesProcessor(traceAwareConfig(5), zap.NewNop(), mp, testStorage(), sink)
	if err != nil {
		t.Fatal(err)
	}
	orig := []int{3, 3, 3}
	_ = tp.ConsumeTraces(context.Background(), newTestTracesMulti("svc", orig, nil))

	got := spansByTrace(sink)
	// Whatever survived must match its original full size (never partial).
	for tid, n := range got {
		ti := int(tid[0]) - 1
		if n != orig[ti] {
			t.Errorf("trace %d forwarded with %d spans, want full %d (partial trace!)", ti, n, orig[ti])
		}
	}
	// With capacity 5 and 3-span traces, exactly one whole trace (3 spans) fits.
	totalKept := 0
	for _, n := range got {
		totalKept += n
	}
	if totalKept != 3 {
		t.Errorf("kept %d spans, want 3 (one whole trace within budget 5)", totalKept)
	}
}

// TestTraceAware_ErrorTraceKeptWhole: a trace with an error span bypasses the
// limit in full, even when the bucket is empty, and the normal trace is dropped
// whole (not split).
func TestTraceAware_ErrorTraceKeptWhole(t *testing.T) {
	mp, reader := newTestMeterProvider()
	sink := &mockTracesConsumer{}
	// capacity 1; trace0 normal (3 spans), trace1 has an error (3 spans).
	tp, err := newTracesProcessor(traceAwareConfig(1), zap.NewNop(), mp, testStorage(), sink)
	if err != nil {
		t.Fatal(err)
	}
	_ = tp.ConsumeTraces(context.Background(), newTestTracesMulti("svc", []int{3, 3}, map[int]bool{1: true}))

	got := spansByTrace(sink)
	var errTID pcommon.TraceID
	errTID[0] = 2 // trace1
	if got[errTID] != 3 {
		t.Errorf("error trace forwarded with %d spans, want full 3 (kept whole)", got[errTID])
	}
	var normalTID pcommon.TraceID
	normalTID[0] = 1 // trace0
	if got[normalTID] != 0 {
		t.Errorf("normal trace should be dropped whole, got %d spans", got[normalTID])
	}

	rm := collectMetrics(t, reader)
	if v := getCounterValue(findMetric(rm, "otelcol_processor_ratelimit_preserved_items"), "key", "svc"); v != 3 {
		t.Errorf("preserved = %d, want 3 (whole error trace)", v)
	}
	if v := getCounterValue(findMetric(rm, "otelcol_processor_ratelimit_denied_items"), "key", "svc"); v != 3 {
		t.Errorf("denied = %d, want 3 (whole normal trace)", v)
	}
}

// TestTraceAware_AllDroppedReturnsError: when nothing fits and there are no error
// traces, drop mode returns a permanent error (empty batch is not forwarded).
func TestTraceAware_AllDroppedReturnsError(t *testing.T) {
	mp, _ := newTestMeterProvider()
	sink := &mockTracesConsumer{}
	tp, err := newTracesProcessor(traceAwareConfig(1), zap.NewNop(), mp, testStorage(), sink)
	if err != nil {
		t.Fatal(err)
	}
	// One trace of 5 spans against capacity 1: cannot fit, no error span.
	gotErr := tp.ConsumeTraces(context.Background(), newTestTracesMulti("svc", []int{5}, nil))
	if gotErr == nil {
		t.Fatal("expected a permanent error when the whole batch is dropped")
	}
	if len(sink.received) != 0 {
		t.Errorf("nothing should be forwarded, got %d batches", len(sink.received))
	}
}

// TestTraceAware_UnderLimitForwardsAll: when everything fits, all traces pass
// untouched.
func TestTraceAware_UnderLimitForwardsAll(t *testing.T) {
	mp, _ := newTestMeterProvider()
	sink := &mockTracesConsumer{}
	tp, err := newTracesProcessor(traceAwareConfig(100), zap.NewNop(), mp, testStorage(), sink)
	if err != nil {
		t.Fatal(err)
	}
	_ = tp.ConsumeTraces(context.Background(), newTestTracesMulti("svc", []int{2, 2, 1}, nil))
	got := spansByTrace(sink)
	total := 0
	for _, n := range got {
		total += n
	}
	if total != 5 {
		t.Errorf("kept %d spans, want all 5 (under limit)", total)
	}
}

// TestPlanTraceDrops keeps the greedy keep/drop math honest.
func TestPlanTraceDrops(t *testing.T) {
	traces := []traceTally{
		{spans: 3}, {spans: 3}, {spans: 3},
	}
	traces[0].id[0] = 1
	traces[1].id[0] = 2
	traces[2].id[0] = 3
	drop, dropped := planTraceDrops(traces, 5)
	if dropped != 6 || len(drop) != 2 {
		t.Errorf("allowed=5 over [3,3,3]: dropped=%d (%d traces), want 6 (2 traces)", dropped, len(drop))
	}
	if drop[traces[0].id] {
		t.Error("first 3-span trace should fit within budget 5 and be kept")
	}
}
