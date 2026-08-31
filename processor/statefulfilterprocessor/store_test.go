package statefulfilterprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"go.uber.org/zap"
)

// newTestStore boots an in-process Redis-compatible server and returns a store
// wired to it through the real go-redis client, so the key layout and the
// version-gating logic are exercised end to end rather than mocked away.
func newTestStore(t *testing.T, mutate func(*Config)) (*redisRuleStore, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	cfg := &Config{
		Redis:              &RedisConfig{Addr: mr.Addr(), KeyPrefix: "test:filter", Timeout: time.Second},
		RefreshInterval:    20 * time.Millisecond,
		FullResyncEvery:    defaultFullResyncEvery,
		InitialLoadTimeout: time.Second,
		MaxRules:           defaultMaxRules,
	}
	if mutate != nil {
		mutate(cfg)
	}
	s, err := newRedisRuleStore(cfg, zap.NewNop())
	if err != nil {
		mr.Close()
		t.Fatalf("newRedisRuleStore: %v", err)
	}
	t.Cleanup(func() {
		_ = s.close()
		mr.Close()
	})
	return s, mr
}

func putRule(t *testing.T, mr *miniredis.Miniredis, id, doc string) {
	t.Helper()
	mr.HSet("test:filter:rules", id, doc)
}

func bumpVersion(t *testing.T, mr *miniredis.Miniredis, v string) {
	t.Helper()
	mr.Set("test:filter:rules:version", v)
}

const dropNoisyTraces = `{"signals":["traces"],"conditions":[{"source":"resource","key":"service.name","value":"noisy"}]}`

func TestStore_InitialLoad(t *testing.T) {
	s, mr := newTestStore(t, nil)
	putRule(t, mr, "drop-noisy", dropNoisyTraces)
	bumpVersion(t, mr, "1")

	if err := s.initialLoad(context.Background()); err != nil {
		t.Fatalf("initialLoad: %v", err)
	}

	rs := s.current()
	if rs.total != 1 || rs.version != 1 {
		t.Fatalf("expected 1 rule at version 1, got total=%d version=%d", rs.total, rs.version)
	}
	if len(rs.traces) != 1 {
		t.Fatalf("expected the rule to land in the traces list, got %d", len(rs.traces))
	}
}

func TestStore_EmptyRedisIsPassThroughNotAnError(t *testing.T) {
	s, _ := newTestStore(t, nil)

	if err := s.initialLoad(context.Background()); err != nil {
		t.Fatalf("an empty rule store is a valid state, got error: %v", err)
	}
	if got := s.current().total; got != 0 {
		t.Fatalf("expected 0 rules, got %d", got)
	}
}

func TestStore_VersionGating_SkipsFetchWhenUnchanged(t *testing.T) {
	// full_resync_every high enough that only the version gate is in play.
	s, mr := newTestStore(t, func(c *Config) { c.FullResyncEvery = 1000 })
	putRule(t, mr, "drop-noisy", dropNoisyTraces)
	bumpVersion(t, mr, "1")

	if err := s.initialLoad(context.Background()); err != nil {
		t.Fatalf("initialLoad: %v", err)
	}

	// Write a second rule WITHOUT bumping the version: the version gate must
	// keep the old snapshot, which is exactly the cheap-poll behaviour that
	// lets 20 replicas poll without hammering Redis.
	putRule(t, mr, "drop-other", `{"conditions":[{"source":"name","value":"x"}]}`)
	if err := s.refresh(context.Background(), false); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if got := s.current().total; got != 1 {
		t.Fatalf("expected the version gate to skip the fetch, got %d rules", got)
	}

	// Bumping the version publishes both rules.
	bumpVersion(t, mr, "2")
	if err := s.refresh(context.Background(), false); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	rs := s.current()
	if rs.total != 2 || rs.version != 2 {
		t.Fatalf("expected 2 rules at version 2, got total=%d version=%d", rs.total, rs.version)
	}
}

func TestStore_FullResyncRecoversRuleWrittenWithoutVersionBump(t *testing.T) {
	// full_resync_every=2 → the second poll fetches regardless of the version.
	s, mr := newTestStore(t, func(c *Config) { c.FullResyncEvery = 2 })
	bumpVersion(t, mr, "1")
	if err := s.initialLoad(context.Background()); err != nil {
		t.Fatalf("initialLoad: %v", err)
	}

	putRule(t, mr, "hand-written", dropNoisyTraces) // HSET without INCR

	// First poll: version unchanged, gated out.
	_ = s.refresh(context.Background(), false)
	if got := s.current().total; got != 0 {
		t.Fatalf("expected the version gate to skip the first poll, got %d", got)
	}
	// Second poll: periodic resync converges anyway.
	_ = s.refresh(context.Background(), false)
	if got := s.current().total; got != 1 {
		t.Fatalf("expected the full resync to pick up the hand-written rule, got %d", got)
	}
}

func TestStore_RedisOutageKeepsLastKnownRules(t *testing.T) {
	s, mr := newTestStore(t, nil)
	putRule(t, mr, "drop-noisy", dropNoisyTraces)
	bumpVersion(t, mr, "1")
	if err := s.initialLoad(context.Background()); err != nil {
		t.Fatalf("initialLoad: %v", err)
	}

	mr.Close() // Redis goes away

	if err := s.refresh(context.Background(), false); err == nil {
		t.Fatal("expected a refresh error while Redis is down")
	}
	// Fail-stale, not fail-empty: the rules someone deliberately installed must
	// not silently stop applying because the rule store hiccuped.
	if got := s.current().total; got != 1 {
		t.Fatalf("expected the last known rule set to stay active, got %d rules", got)
	}
	if st := s.stats(); st.errors != 1 {
		t.Fatalf("expected 1 refresh error recorded, got %d", st.errors)
	}
}

func TestStore_RemovingARuleStopsDropping(t *testing.T) {
	s, mr := newTestStore(t, nil)
	putRule(t, mr, "drop-noisy", dropNoisyTraces)
	bumpVersion(t, mr, "1")
	if err := s.initialLoad(context.Background()); err != nil {
		t.Fatalf("initialLoad: %v", err)
	}

	mr.HDel("test:filter:rules", "drop-noisy")
	bumpVersion(t, mr, "2")
	if err := s.refresh(context.Background(), false); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if got := s.current().total; got != 0 {
		t.Fatalf("expected the rule to be gone, got %d", got)
	}
}

func TestStore_BackgroundLoopPicksUpNewRules(t *testing.T) {
	s, mr := newTestStore(t, nil)
	if err := s.initialLoad(context.Background()); err != nil {
		t.Fatalf("initialLoad: %v", err)
	}
	s.start()

	putRule(t, mr, "drop-noisy", dropNoisyTraces)
	bumpVersion(t, mr, "1")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.current().total == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("background refresh loop never picked up the new rule")
}

func TestStore_StatsReflectLoadState(t *testing.T) {
	s, mr := newTestStore(t, nil)
	if st := s.stats(); st.everLoaded {
		t.Fatal("everLoaded must be false before the first successful load")
	}

	putRule(t, mr, "drop-noisy", dropNoisyTraces)
	putRule(t, mr, "broken", `{"conditions":[{"source":"nonsense"}]}`)
	bumpVersion(t, mr, "5")
	if err := s.initialLoad(context.Background()); err != nil {
		t.Fatalf("initialLoad: %v", err)
	}

	st := s.stats()
	if !st.everLoaded || st.rulesLoaded != 1 || st.rulesBad != 1 || st.version != 5 {
		t.Fatalf("unexpected stats: %+v", st)
	}
	if st.lastSuccess.IsZero() {
		t.Fatal("lastSuccess must be set after a successful load")
	}
}
