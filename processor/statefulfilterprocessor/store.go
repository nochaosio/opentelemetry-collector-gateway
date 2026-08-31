package statefulfilterprocessor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ruleStore publishes rule snapshots to the data path.
type ruleStore interface {
	// current returns the active snapshot. Never nil — before the first
	// successful load it is the empty (pass-through) set.
	current() *ruleSet
	// initialLoad performs one synchronous fetch, bounded by ctx. Optional:
	// the refresh loop fetches on its own. Used by wait_for_initial_load so a
	// restarted collector does not accept traffic in a no-rules state.
	initialLoad(ctx context.Context) error
	// start begins the background refresh loop.
	start()
	// close stops the refresh loop and releases the connection.
	close() error
	// stats returns the counters backing the exported gauges.
	stats() storeStats
}

type storeStats struct {
	version     int64
	rulesLoaded int
	rulesBad    int
	errors      uint64
	lastSuccess time.Time
	everLoaded  bool
}

// redisRuleStore keeps every collector replica converged on one rule set.
//
// Design:
//   - Off the data path. ConsumeTraces never talks to Redis; it reads an
//     atomic pointer. A Redis outage costs propagation delay, not latency.
//   - Version-gated polling. Each cycle is a single GET of the version key;
//     the full HGETALL only runs when the version moved (or on the periodic
//     resync). 20 replicas at 10s cost Redis ~2 GET/s.
//   - Fail-stale, not fail-empty. On any refresh error the last known rule set
//     stays active. Losing Redis must not silently un-drop the traffic
//     someone deliberately muted.
type redisRuleStore struct {
	client     redis.UniversalClient
	rulesKey   string
	versionKey string
	timeout    time.Duration
	interval   time.Duration
	fullEvery  int
	maxRules   int
	logger     *zap.Logger

	snapshot atomic.Pointer[ruleSet]

	// pollsSinceFull counts version-only cycles since the last HGETALL; drives
	// the periodic full resync that recovers from a rule written without
	// bumping the version key.
	pollsSinceFull int

	errors      atomic.Uint64
	lastSuccess atomic.Int64 // unix nanos; 0 == never
	everLoaded  atomic.Bool

	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

func buildRedisOptions(ctx context.Context, cfg *RedisConfig) (*redis.UniversalOptions, error) {
	addrs := cfg.Addrs
	if len(addrs) == 0 && cfg.Addr != "" {
		addrs = []string{cfg.Addr}
	}
	opts := &redis.UniversalOptions{
		Addrs:        addrs,
		MasterName:   cfg.MasterName, // non-empty => Sentinel failover mode
		Password:     string(cfg.Password),
		DB:           cfg.DB,
		DialTimeout:  cfg.Timeout,
		ReadTimeout:  cfg.Timeout,
		WriteTimeout: cfg.Timeout,
		MaxRetries:   1,
		MinIdleConns: 1,
	}
	if cfg.TLS != nil {
		tlsCfg, err := cfg.TLS.LoadTLSConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("redis.tls: %w", err)
		}
		opts.TLSConfig = tlsCfg
	}
	return opts, nil
}

func newRedisRuleStore(cfg *Config, logger *zap.Logger) (*redisRuleStore, error) {
	opts, err := buildRedisOptions(context.Background(), cfg.Redis)
	if err != nil {
		return nil, err
	}
	return newRuleStoreWithClient(redis.NewUniversalClient(opts), cfg, logger), nil
}

// newRuleStoreWithClient is the injection seam used by the tests (miniredis).
func newRuleStoreWithClient(client redis.UniversalClient, cfg *Config, logger *zap.Logger) *redisRuleStore {
	s := &redisRuleStore{
		client:     client,
		rulesKey:   cfg.Redis.KeyPrefix + ":rules",
		versionKey: cfg.Redis.KeyPrefix + ":rules:version",
		timeout:    cfg.Redis.Timeout,
		interval:   cfg.RefreshInterval,
		fullEvery:  cfg.FullResyncEvery,
		maxRules:   cfg.MaxRules,
		logger:     logger,
		done:       make(chan struct{}),
	}
	s.snapshot.Store(emptyRuleSet())
	return s
}

func (s *redisRuleStore) current() *ruleSet { return s.snapshot.Load() }

func (s *redisRuleStore) initialLoad(ctx context.Context) error {
	return s.refresh(ctx, true)
}

func (s *redisRuleStore) start() {
	loopCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go s.loop(loopCtx)
}

func (s *redisRuleStore) loop(ctx context.Context) {
	defer close(s.done)
	// Fetch immediately: with wait_for_initial_load off this is the first load,
	// and with it on a duplicate fetch is cheap and version-gated anyway.
	_ = s.refresh(ctx, !s.everLoaded.Load())

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Errors are counted and logged in refresh; the previous snapshot
			// stays active, so there is nothing to do here but keep polling.
			_ = s.refresh(ctx, false)
		}
	}
}

// refresh runs one poll cycle. force skips the version gate (used for the
// initial load). Returns the error so the caller can decide whether an initial
// failure is fatal; the background loop just keeps the stale snapshot.
func (s *redisRuleStore) refresh(ctx context.Context, force bool) error {
	callCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	version, err := s.readVersion(callCtx)
	if err != nil {
		s.recordError("read rule version", err)
		return err
	}

	// Skip the fetch only when the version is unchanged AND no periodic resync
	// is due. A version bump alone is enough to refetch; the periodic resync
	// exists for the human who ran HSET without INCR.
	cur := s.snapshot.Load()
	fullDue := force || s.fullEvery <= 1 || s.pollsSinceFull >= s.fullEvery-1
	if !fullDue && version == cur.version {
		s.pollsSinceFull++
		s.markSuccess()
		return nil
	}

	docs, err := s.client.HGetAll(callCtx, s.rulesKey).Result()
	if err != nil {
		s.recordError("fetch rules", err)
		return err
	}
	s.pollsSinceFull = 0

	next := buildRuleSet(docs, version, s.maxRules, func(id string, err error) {
		s.logger.Warn("Ignoring invalid filter rule",
			zap.String("rule_id", id),
			zap.String("rules_key", s.rulesKey),
			zap.Error(err),
		)
	})

	changed := !s.everLoaded.Load() || next.version != cur.version || next.total != cur.total
	s.snapshot.Store(next)
	s.everLoaded.Store(true)
	s.markSuccess()

	if changed {
		s.logger.Info("Filter rules loaded from Redis",
			zap.Int64("version", next.version),
			zap.Int("rules", next.total),
			zap.Int("invalid", next.invalid),
			zap.Int("traces_rules", len(next.traces)),
			zap.Int("metrics_rules", len(next.metrics)),
			zap.Int("logs_rules", len(next.logs)),
		)
	}
	return nil
}

// readVersion returns the rule version counter. A missing key is version 0, not
// an error: an empty Redis is a valid "no rules yet" state.
func (s *redisRuleStore) readVersion(ctx context.Context) (int64, error) {
	v, err := s.client.Get(ctx, s.versionKey).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return v, nil
}

func (s *redisRuleStore) recordError(what string, err error) {
	s.errors.Add(1)
	s.logger.Warn("Filter rule refresh failed; keeping last known rules",
		zap.String("operation", what),
		zap.String("rules_key", s.rulesKey),
		zap.Bool("ever_loaded", s.everLoaded.Load()),
		zap.Error(err),
	)
}

func (s *redisRuleStore) markSuccess() {
	s.lastSuccess.Store(time.Now().UnixNano())
}

func (s *redisRuleStore) stats() storeStats {
	rs := s.snapshot.Load()
	var last time.Time
	if ns := s.lastSuccess.Load(); ns != 0 {
		last = time.Unix(0, ns)
	}
	return storeStats{
		version:     rs.version,
		rulesLoaded: rs.total,
		rulesBad:    rs.invalid,
		errors:      s.errors.Load(),
		lastSuccess: last,
		everLoaded:  s.everLoaded.Load(),
	}
}

func (s *redisRuleStore) close() error {
	var err error
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
			<-s.done
		}
		err = s.client.Close()
	})
	return err
}
