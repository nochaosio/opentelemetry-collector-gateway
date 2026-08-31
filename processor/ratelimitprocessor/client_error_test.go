package ratelimitprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// What the client sees when a whole batch is rejected depends on
// rejection_mode:
//
//   - "throttle" (default): gRPC RESOURCE_EXHAUSTED with RetryInfo. The OTLP
//     receiver propagates the status as-is on gRPC and maps it to HTTP 429 —
//     the throttling signal prescribed by the OTLP spec, which SDK exporters
//     honor with backoff-and-retry.
//   - "permanent": a collector-permanent error (HTTP ~400, gRPC non-retryable),
//     so a compliant producer drops the batch and moves on instead of retrying.

// assertThrottle verifies the throttle-mode contract: RESOURCE_EXHAUSTED,
// not permanent, carrying a RetryInfo detail.
func assertThrottle(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error when the whole batch is dropped")
	}
	if consumererror.IsPermanent(err) {
		t.Errorf("throttle rejection must NOT be permanent; got: %v", err)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("throttle rejection must be a gRPC status error; got: %v", err)
	}
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("expected RESOURCE_EXHAUSTED, got %v", st.Code())
	}
	hasRetryInfo := false
	for _, d := range st.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			hasRetryInfo = true
			if ri.RetryDelay.AsDuration() <= 0 {
				t.Errorf("RetryInfo delay must be positive, got %v", ri.RetryDelay.AsDuration())
			}
		}
	}
	if !hasRetryInfo {
		t.Error("throttle rejection must carry RetryInfo so SDKs can pace retries")
	}
}

func drainAndOverflowTraces(t *testing.T, cfg *Config) error {
	t.Helper()
	mp, _ := newTestMeterProvider()
	tp, err := newTracesProcessor(cfg, zap.NewNop(), mp, testStorage(), &mockTracesConsumer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = tp.ConsumeTraces(ctx, newTestTraces("svc", 1)) // drains the single token
	return tp.ConsumeTraces(ctx, newTestTraces("svc", 3))
}

func TestFullDrop_ThrottleError_Traces(t *testing.T) {
	assertThrottle(t, drainAndOverflowTraces(t, testConfig(1)))
}

func TestFullDrop_ThrottleError_TracesTraceAware(t *testing.T) {
	cfg := testConfig(1)
	cfg.TraceIDAware = true
	assertThrottle(t, drainAndOverflowTraces(t, cfg))
}

func TestFullDrop_ThrottleError_Metrics(t *testing.T) {
	mp, _ := newTestMeterProvider()
	proc, err := newMetricsProcessor(testConfig(1), zap.NewNop(), mp, testStorage(), &mockMetricsConsumer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = proc.ConsumeMetrics(ctx, newTestMetrics("svc", 1))
	assertThrottle(t, proc.ConsumeMetrics(ctx, newTestMetrics("svc", 3)))
}

func TestFullDrop_ThrottleError_Logs(t *testing.T) {
	mp, _ := newTestMeterProvider()
	proc, err := newLogsProcessor(testConfig(1), zap.NewNop(), mp, testStorage(), &mockLogsConsumer{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_ = proc.ConsumeLogs(ctx, newTestLogs("svc", 1))
	assertThrottle(t, proc.ConsumeLogs(ctx, newTestLogs("svc", 3)))
}

// rejection_mode: permanent keeps the legacy hardening behavior: clients are
// told the data is rejected for good and must not retry.
func TestFullDrop_PermanentMode(t *testing.T) {
	cfg := testConfig(1)
	cfg.RejectionMode = rejectionPermanent
	err := drainAndOverflowTraces(t, cfg)
	if err == nil {
		t.Fatal("expected an error when the whole batch is dropped")
	}
	if !consumererror.IsPermanent(err) {
		t.Errorf("permanent-mode rejection must be permanent (non-retryable); got: %v", err)
	}
}

func TestRejectionModeValidation(t *testing.T) {
	cfg := testConfig(1)
	cfg.RejectionMode = "bogus"
	if err := cfg.Validate(); err == nil {
		t.Error("rejection_mode=bogus must fail validation")
	}
	for _, mode := range []string{"", rejectionThrottle, rejectionPermanent} {
		cfg.RejectionMode = mode
		if err := cfg.Validate(); err != nil {
			t.Errorf("rejection_mode=%q must validate, got: %v", mode, err)
		}
	}
}
