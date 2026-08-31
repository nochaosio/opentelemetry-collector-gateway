package ratelimitprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"go.opentelemetry.io/collector/config/configtls"
	"go.uber.org/zap"
)

// TestBuildRedisOptions_TLS: when TLS is configured, the client dials over TLS
// (TLSConfig is set); with no TLS it stays plaintext.
func TestBuildRedisOptions_TLS(t *testing.T) {
	ctx := context.Background()

	plain, err := buildRedisOptions(ctx, &RedisConfig{Addr: "localhost:6379"})
	if err != nil {
		t.Fatalf("plain: %v", err)
	}
	if plain.TLSConfig != nil {
		t.Error("no TLS configured but TLSConfig was set")
	}

	withTLS, err := buildRedisOptions(ctx, &RedisConfig{
		Addr: "localhost:6379",
		TLS:  &configtls.ClientConfig{Config: configtls.Config{}},
	})
	if err != nil {
		t.Fatalf("tls: %v", err)
	}
	if withTLS.TLSConfig == nil {
		t.Error("TLS configured but TLSConfig is nil (client would dial in clear)")
	}
}

// TestRedis_FailOpenMetric: when Redis becomes unreachable, traffic is allowed
// through (fail-open) and the fail_open_items counter records exactly how many
// items bypassed the limit.
func TestRedis_FailOpenMetric(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	rs, err := newRedisStorage(&RedisConfig{
		Addr:    mr.Addr(),
		Timeout: 200 * time.Millisecond,
		OnError: "open",
	}, zap.NewNop())
	if err != nil {
		mr.Close()
		t.Fatalf("newRedisStorage: %v", err)
	}
	defer func() { _ = rs.Close() }()

	mp, reader := newTestMeterProvider()
	tel, err := newProcessorTelemetry(mp)
	if err != nil {
		t.Fatal(err)
	}
	rs.SetFailOpenCounter(tel.failOpenItems)

	// Kill Redis so the next call fails and must fail open.
	mr.Close()

	got := rs.AllowUpToN(context.Background(), "svc", 7, 10, time.Second)
	if got != 7 {
		t.Errorf("fail-open should allow all 7, got %d", got)
	}

	rm := collectMetrics(t, reader)
	if v := getCounterTotal(findMetric(rm, "otelcol_processor_ratelimit_fail_open_items")); v != 7 {
		t.Errorf("fail_open_items = %d, want 7 (items that bypassed the limit)", v)
	}
}

// TestBuildRedisOptions_Modes: single / Sentinel / Cluster are selected from the
// config the way go-redis NewUniversalClient expects.
func TestBuildRedisOptions_Modes(t *testing.T) {
	ctx := context.Background()

	single, err := buildRedisOptions(ctx, &RedisConfig{Addr: "h:6379"})
	if err != nil {
		t.Fatal(err)
	}
	if len(single.Addrs) != 1 || single.Addrs[0] != "h:6379" || single.MasterName != "" {
		t.Errorf("single: addrs=%v master=%q", single.Addrs, single.MasterName)
	}

	sentinel, err := buildRedisOptions(ctx, &RedisConfig{
		Addrs:      []string{"s1:26379", "s2:26379"},
		MasterName: "mymaster",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sentinel.MasterName != "mymaster" || len(sentinel.Addrs) != 2 {
		t.Errorf("sentinel: addrs=%v master=%q", sentinel.Addrs, sentinel.MasterName)
	}

	cluster, err := buildRedisOptions(ctx, &RedisConfig{
		Addrs: []string{"n1:6379", "n2:6379", "n3:6379"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cluster.MasterName != "" || len(cluster.Addrs) != 3 {
		t.Errorf("cluster: addrs=%v master=%q", cluster.Addrs, cluster.MasterName)
	}
}

// TestRedis_FailOpenRecovery is the behavior the user asked for: while Redis is
// down, everything passes (fail-open); when Redis comes back, limiting resumes
// on its own. Full cycle: up (limits) -> down (all pass) -> up (limits again).
func TestRedis_FailOpenRecovery(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	addr := mr.Addr()
	rs, err := newRedisStorage(&RedisConfig{
		Addr:    addr,
		Timeout: 200 * time.Millisecond,
		OnError: "open",
	}, zap.NewNop())
	if err != nil {
		mr.Close()
		t.Fatalf("newRedisStorage: %v", err)
	}
	defer func() { _ = rs.Close() }()
	ctx := context.Background()

	// 1) Redis up: the limit is enforced (capacity 2 over a 1-min window).
	if got := rs.AllowUpToN(ctx, "svc", 2, 2, time.Minute); got != 2 {
		t.Fatalf("up: expected 2 granted, got %d", got)
	}
	if got := rs.AllowUpToN(ctx, "svc", 5, 2, time.Minute); got != 0 {
		t.Fatalf("up: bucket should be empty, got %d granted", got)
	}

	// 2) Redis down: fail-open lets everything through despite a capacity of 2.
	mr.Close()
	passed := false
	for i := 0; i < 5; i++ {
		if rs.AllowUpToN(ctx, "svc", 5, 2, time.Minute) == 5 {
			passed = true
			break
		}
	}
	if !passed {
		t.Fatal("down: fail-open should allow all 5 while Redis is unreachable")
	}

	// 3) Redis recovers on the same address: limiting must resume by itself.
	mr2 := miniredis.NewMiniRedis()
	if err := mr2.StartAddr(addr); err != nil {
		t.Fatalf("restart miniredis on %s: %v", addr, err)
	}
	defer mr2.Close()

	limited := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		// Fresh server => fresh full bucket of 2; a request for 5 should grant
		// at most 2 once Redis is answering again (not 5 = fail-open).
		if got := rs.AllowUpToN(ctx, "svc-recovered", 5, 2, time.Minute); got <= 2 {
			limited = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !limited {
		t.Fatal("recovery: limiting did not resume after Redis came back")
	}
}

// TestRedis_FailClosedDenies: with on_error=closed, an unreachable Redis denies
// instead of allowing.
func TestRedis_FailClosedDenies(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	rs, err := newRedisStorage(&RedisConfig{
		Addr:    mr.Addr(),
		Timeout: 200 * time.Millisecond,
		OnError: "closed",
	}, zap.NewNop())
	if err != nil {
		mr.Close()
		t.Fatalf("newRedisStorage: %v", err)
	}
	defer func() { _ = rs.Close() }()

	mr.Close()
	if got := rs.AllowUpToN(context.Background(), "svc", 7, 10, time.Second); got != 0 {
		t.Errorf("fail-closed should deny (0), got %d", got)
	}
}
