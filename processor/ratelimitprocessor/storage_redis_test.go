package ratelimitprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"go.uber.org/zap"
)

// startMiniRedis boots an in-process Redis-compatible server for the test
// and returns the storage configured against it. Using the real client
// and Lua path keeps coverage representative: if the Lua script has a bug
// it will blow up here, not just in a prod cluster.
func startMiniRedis(t *testing.T) (*redisStorage, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	cfg := &RedisConfig{
		Addr:      mr.Addr(),
		KeyPrefix: "test:rl",
		Timeout:   500 * time.Millisecond,
		OnError:   "open",
	}
	rs, err := newRedisStorage(cfg, zap.NewNop())
	if err != nil {
		mr.Close()
		t.Fatalf("newRedisStorage: %v", err)
	}
	t.Cleanup(func() {
		_ = rs.Close()
		mr.Close()
	})
	return rs, mr
}

func TestRedisStorage_AllowUpToN_UnderLimit(t *testing.T) {
	rs, _ := startMiniRedis(t)
	ctx := context.Background()

	got := rs.AllowUpToN(ctx, "svc-a", 5, 10, time.Second)
	if got != 5 {
		t.Fatalf("expected 5 granted, got %d", got)
	}
}

func TestRedisStorage_AllowUpToN_PartialAllow(t *testing.T) {
	rs, _ := startMiniRedis(t)
	ctx := context.Background()

	// Capacity=10, take 7 then ask for 10 — Lua should grant only 3.
	if got := rs.AllowUpToN(ctx, "svc-b", 7, 10, time.Second); got != 7 {
		t.Fatalf("first call: expected 7, got %d", got)
	}
	if got := rs.AllowUpToN(ctx, "svc-b", 10, 10, time.Second); got != 3 {
		t.Fatalf("second call: expected partial 3, got %d", got)
	}
}

func TestRedisStorage_AllowUpToN_ZeroWhenEmpty(t *testing.T) {
	rs, _ := startMiniRedis(t)
	ctx := context.Background()

	rs.AllowUpToN(ctx, "svc-c", 10, 10, time.Second)
	if got := rs.AllowUpToN(ctx, "svc-c", 1, 10, time.Second); got != 0 {
		t.Fatalf("expected 0 after bucket drained, got %d", got)
	}
}

func TestRedisStorage_IndependentKeys(t *testing.T) {
	rs, _ := startMiniRedis(t)
	ctx := context.Background()

	rs.AllowUpToN(ctx, "svc-a", 10, 10, time.Second)
	// Separate key must start with a full bucket.
	if got := rs.AllowUpToN(ctx, "svc-b", 10, 10, time.Second); got != 10 {
		t.Fatalf("svc-b expected fresh 10, got %d", got)
	}
}

func TestRedisStorage_Refund(t *testing.T) {
	rs, _ := startMiniRedis(t)
	ctx := context.Background()

	rs.AllowUpToN(ctx, "svc-r", 10, 10, time.Second)
	rs.Refund(ctx, "svc-r", 5, 10, time.Second)
	// After refund we should be able to take 5 again.
	if got := rs.AllowUpToN(ctx, "svc-r", 5, 10, time.Second); got != 5 {
		t.Fatalf("after refund of 5, expected 5 granted, got %d", got)
	}
}

func TestRedisStorage_FailOpenOnDisconnect(t *testing.T) {
	rs, mr := startMiniRedis(t)
	ctx := context.Background()

	// Kill Redis — the next call must fail OPEN (grant=n) because
	// on_error=open is the default we configured in startMiniRedis.
	mr.Close()

	got := rs.AllowUpToN(ctx, "svc-x", 7, 100, time.Second)
	if got != 7 {
		t.Fatalf("fail-open: expected 7, got %d", got)
	}
	if rs.ErrorCount() == 0 {
		t.Error("expected ErrorCount > 0 after Redis outage")
	}
}

func TestRedisStorage_FailClosedOnDisconnect(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	cfg := &RedisConfig{
		Addr:      mr.Addr(),
		KeyPrefix: "test:rl",
		Timeout:   500 * time.Millisecond,
		OnError:   "closed",
	}
	rs, err := newRedisStorage(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rs.Close() })
	mr.Close()

	got := rs.AllowUpToN(context.Background(), "svc-z", 7, 100, time.Second)
	if got != 0 {
		t.Fatalf("fail-closed: expected 0, got %d", got)
	}
}

func TestRedisStorage_NegativeCache(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mr.Close() })

	cfg := &RedisConfig{
		Addr:             mr.Addr(),
		KeyPrefix:        "test:rl",
		Timeout:          500 * time.Millisecond,
		OnError:          "open",
		NegativeCacheTTL: 2 * time.Second,
	}
	rs, err := newRedisStorage(cfg, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rs.Close() })

	ctx := context.Background()
	// Exhaust the bucket.
	rs.AllowUpToN(ctx, "svc-n", 10, 10, time.Second)
	// First 0 result should seed the negative cache.
	if got := rs.AllowUpToN(ctx, "svc-n", 1, 10, time.Second); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}

	// Simulate Redis being unreachable during the cooldown: we should
	// still get 0 (served from the negative cache) without a Redis call.
	mr.Close()
	if got := rs.AllowUpToN(ctx, "svc-n", 1, 10, time.Second); got != 0 {
		t.Fatalf("neg cache: expected 0 even with Redis down, got %d", got)
	}
}

// Integration: RateLimiter orchestrating on top of Redis storage with
// specific_limits. Proves the memory <-> redis swap is transparent.
func TestRedisStorage_WithRateLimiter_SpecificLimits(t *testing.T) {
	rs, _ := startMiniRedis(t)

	cfg := &Config{
		LimitType:         "service_name",
		RequestsPerSecond: 10,
		DropOnLimit:       true,
		SpecificLimits: map[string]LimitConfig{
			"legacy-service": {RequestsPerSecond: 2},
		},
	}
	rl := newRateLimiterWithStorage(cfg, rs)

	if got := rl.AllowUpToN("normal-service", 10); got != 10 {
		t.Errorf("normal-service: expected 10, got %d", got)
	}
	if got := rl.AllowUpToN("legacy-service", 2); got != 2 {
		t.Errorf("legacy-service: expected 2, got %d", got)
	}
	if got := rl.AllowUpToN("legacy-service", 1); got != 0 {
		t.Errorf("legacy-service: expected 0 after cap, got %d", got)
	}
}
