// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// allowLuaScript atomically refills and consumes tokens in a single Redis
// round-trip. Returning "how many were granted" keeps the partial-allow
// semantics of the memory backend intact.
//
//	KEYS[1] = full bucket key (prefix:<user-key>)
//	ARGV[1] = requested tokens (n)
//	ARGV[2] = max tokens (bucket capacity == configured limit)
//	ARGV[3] = refill rate (tokens per second, float)
//	ARGV[4] = now (microseconds since epoch)
//	ARGV[5] = key TTL in seconds (so idle buckets expire automatically)
//
// Returns: integer granted in [0, n].
const allowLuaScript = `
local key = KEYS[1]
local requested = tonumber(ARGV[1])
local maxTokens = tonumber(ARGV[2])
local refillRate = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local data = redis.call('HMGET', key, 't', 'r')
local tokens = tonumber(data[1])
local lastRefill = tonumber(data[2])

if tokens == nil then
    tokens = maxTokens
    lastRefill = now
end

local elapsed = (now - lastRefill) / 1000000.0
if elapsed < 0 then elapsed = 0 end
tokens = tokens + elapsed * refillRate
if tokens > maxTokens then tokens = maxTokens end

local granted = math.floor(math.min(requested, tokens))
if granted < 0 then granted = 0 end
tokens = tokens - granted

redis.call('HMSET', key, 't', tokens, 'r', now)
redis.call('EXPIRE', key, ttl)

return granted
`

// refundLuaScript returns tokens to a bucket without going over capacity.
//
//	KEYS[1] = full bucket key
//	ARGV[1] = amount to refund
//	ARGV[2] = max tokens (cap)
//	ARGV[3] = key TTL seconds
const refundLuaScript = `
local key = KEYS[1]
local amount = tonumber(ARGV[1])
local maxTokens = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local tokens = tonumber(redis.call('HGET', key, 't'))
if tokens == nil then return 0 end

tokens = tokens + amount
if tokens > maxTokens then tokens = maxTokens end

redis.call('HSET', key, 't', tokens)
redis.call('EXPIRE', key, ttl)
return tokens
`

// redisStorage keeps token-bucket state in Redis, giving all collector
// replicas a shared view. Use when an L4 LB (F5, GCLB, etc.) fans OTLP
// traffic across replicas — without shared state, N replicas allow N×
// the configured rate.
//
// Design choices:
//   - Atomic Lua: one RTT per decision, no race between refill and consume.
//   - Fail-open: if Redis is unreachable or slow, allow the traffic and log.
//     An edge collector that drops telemetry because its state store hiccuped
//     is worse than briefly imprecise rate limiting.
//   - Negative cache: optional per-key cooldown after a 0-grant reply, so
//     a service that's clearly over-limit doesn't hammer Redis with every
//     single batch.
type redisStorage struct {
	client       redis.UniversalClient
	allowScript  *redis.Script
	refundScript *redis.Script
	prefix       string
	timeout      time.Duration
	failOpen     bool
	logger       *zap.Logger

	// Fail-open accounting. errorCount is the in-process running total (used by
	// tests); errCounter, when wired via SetErrorCounter, exports the same
	// signal as the otelcol_processor_ratelimit_backend_errors metric so operators can
	// alert on "Redis is silently broken".
	errorCount    atomic.Uint64
	errCounter    metric.Int64Counter
	failOpenItems metric.Int64Counter // items allowed because Redis failed open

	// Optional in-process negative cache. Empty map when negCacheTTL == 0.
	negCache    map[string]time.Time
	negCacheMu  sync.Mutex
	negCacheTTL time.Duration
}

// buildRedisOptions translates RedisConfig into go-redis universal options,
// covering single-node, Sentinel failover (MasterName set), and Cluster
// (multiple addrs). NewUniversalClient picks the mode from these options.
// Split out so it can be unit-tested without a live server.
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
		MaxRetries:   0, // fail fast — fail-open logic handles retries at the app layer
		MinIdleConns: 1,
	}
	if cfg.TLS != nil {
		tlsCfg, err := cfg.TLS.LoadTLSConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("storage.redis.tls: %w", err)
		}
		opts.TLSConfig = tlsCfg
	}
	return opts, nil
}

func newRedisStorage(cfg *RedisConfig, logger *zap.Logger) (*redisStorage, error) {
	if cfg.Addr == "" && len(cfg.Addrs) == 0 {
		return nil, fmt.Errorf("storage.redis requires addr or addrs")
	}

	opts, err := buildRedisOptions(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	client := redis.NewUniversalClient(opts)

	// Ping once at startup so a misconfigured endpoint surfaces immediately
	// instead of silently fail-opening every request.
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping failed at %v: %w", opts.Addrs, err)
	}

	rs := &redisStorage{
		client:       client,
		allowScript:  redis.NewScript(allowLuaScript),
		refundScript: redis.NewScript(refundLuaScript),
		prefix:       cfg.KeyPrefix,
		timeout:      cfg.Timeout,
		failOpen:     cfg.OnError != "closed",
		logger:       logger,
		negCacheTTL:  cfg.NegativeCacheTTL,
	}
	if rs.negCacheTTL > 0 {
		rs.negCache = make(map[string]time.Time)
	}
	logger.Info("Rate limit Redis storage ready",
		zap.String("addr", cfg.Addr),
		zap.String("prefix", rs.prefix),
		zap.Duration("timeout", rs.timeout),
		zap.Bool("fail_open", rs.failOpen),
		zap.Duration("negative_cache_ttl", rs.negCacheTTL),
	)
	return rs, nil
}

func (rs *redisStorage) Name() string { return "redis" }

// SetErrorCounter wires the metric counter incremented on every transient
// backend error (alongside the internal errorCount).
func (rs *redisStorage) SetErrorCounter(counter metric.Int64Counter) {
	rs.errCounter = counter
}

// SetEvictionCounter is a no-op: Redis evicts via key TTL, not a local cap.
func (rs *redisStorage) SetEvictionCounter(metric.Int64Counter) {}

// SetFailOpenCounter wires the counter incremented (by the item count) whenever
// Redis is unreachable and traffic is allowed through fail-open, so operators
// can see exactly how much traffic bypassed the limit during a Redis outage.
func (rs *redisStorage) SetFailOpenCounter(counter metric.Int64Counter) {
	rs.failOpenItems = counter
}

// recordError bumps both the in-process total and the exported metric (if set).
func (rs *redisStorage) recordError() {
	rs.errorCount.Add(1)
	if rs.errCounter != nil {
		rs.errCounter.Add(context.Background(), 1, metric.WithAttributes(attribute.String("backend", "redis")))
	}
}

func (rs *redisStorage) bucketKey(key string) string {
	return rs.prefix + ":" + key
}

// negCacheHit reports whether `key` is within its deny cooldown window.
// Only used when negCacheTTL > 0; returning false when disabled keeps the
// hot path a single map lookup under a mutex — skipping the lookup
// entirely when no cache is configured.
func (rs *redisStorage) negCacheHit(key string) bool {
	if rs.negCacheTTL == 0 {
		return false
	}
	rs.negCacheMu.Lock()
	defer rs.negCacheMu.Unlock()
	expiry, ok := rs.negCache[key]
	if !ok {
		return false
	}
	if time.Now().After(expiry) {
		delete(rs.negCache, key)
		return false
	}
	return true
}

// maxNegCacheEntries caps the negative cache. Entries are otherwise only
// removed lazily on lookup, so a hostile producer spraying unique over-limit
// keys would grow the map without bound.
const maxNegCacheEntries = 10_000

func (rs *redisStorage) negCacheSet(key string) {
	if rs.negCacheTTL == 0 {
		return
	}
	rs.negCacheMu.Lock()
	defer rs.negCacheMu.Unlock()
	if len(rs.negCache) >= maxNegCacheEntries {
		now := time.Now()
		for k, exp := range rs.negCache {
			if now.After(exp) {
				delete(rs.negCache, k)
			}
		}
		if len(rs.negCache) >= maxNegCacheEntries {
			// Full of live entries: skip caching this key rather than grow.
			return
		}
	}
	rs.negCache[key] = time.Now().Add(rs.negCacheTTL)
}

// failOpenGrant returns the fail-open verdict: allow all `n` when
// FailOpen is true, deny when false. Logs error and bumps the counter
// so operators can tell Redis is degraded.
func (rs *redisStorage) failOpenGrant(key string, n int, err error) int {
	rs.recordError()
	if rs.failOpen {
		if rs.failOpenItems != nil && n > 0 {
			rs.failOpenItems.Add(context.Background(), int64(n),
				metric.WithAttributes(attribute.String("backend", "redis")))
		}
		rs.logger.Warn("Redis unavailable: failing OPEN (traffic allowed)",
			zap.String("key", key),
			zap.Error(err),
		)
		return n
	}
	rs.logger.Warn("Redis unavailable: failing CLOSED (traffic denied)",
		zap.String("key", key),
		zap.Error(err),
	)
	return 0
}

func (rs *redisStorage) AllowUpToN(ctx context.Context, key string, n, limit int, period time.Duration) int {
	if n <= 0 {
		return 0
	}

	if rs.negCacheHit(key) {
		return 0
	}

	callCtx, cancel := context.WithTimeout(ctx, rs.timeout)
	defer cancel()

	maxTokens := limit
	refillRate := float64(limit) / period.Seconds()
	nowMicros := time.Now().UnixMicro()
	// TTL: enough to cover several refill periods so long-lived low-traffic
	// keys don't get evicted mid-window, bounded to 1h to cap memory.
	ttlSeconds := int64(period.Seconds() * 10)
	if ttlSeconds < 60 {
		ttlSeconds = 60
	}
	if ttlSeconds > 3600 {
		ttlSeconds = 3600
	}

	res, err := rs.allowScript.Run(
		callCtx,
		rs.client,
		[]string{rs.bucketKey(key)},
		n, maxTokens, refillRate, nowMicros, ttlSeconds,
	).Int64()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return rs.failOpenGrant(key, n, err)
		}
		return rs.failOpenGrant(key, n, err)
	}

	granted := int(res)
	if granted == 0 && n > 0 {
		rs.negCacheSet(key)
	}
	return granted
}

func (rs *redisStorage) Refund(ctx context.Context, key string, n, limit int, period time.Duration) {
	if n <= 0 {
		return
	}

	callCtx, cancel := context.WithTimeout(ctx, rs.timeout)
	defer cancel()

	ttlSeconds := int64(period.Seconds() * 10)
	if ttlSeconds < 60 {
		ttlSeconds = 60
	}
	if ttlSeconds > 3600 {
		ttlSeconds = 3600
	}

	if err := rs.refundScript.Run(
		callCtx,
		rs.client,
		[]string{rs.bucketKey(key)},
		n, limit, ttlSeconds,
	).Err(); err != nil {
		rs.recordError()
		rs.logger.Debug("Redis refund failed (non-fatal)",
			zap.String("key", key),
			zap.Error(err),
		)
	}
}

// AvailableTokens reads the stored token count for key from Redis. The value
// is as-of the last refill write (not continuously refilled), so it's a close
// approximation suitable for a gauge. ok=false when the bucket key is absent
// or Redis is unreachable.
func (rs *redisStorage) AvailableTokens(ctx context.Context, key string) (float64, bool) {
	callCtx, cancel := context.WithTimeout(ctx, rs.timeout)
	defer cancel()

	v, err := rs.client.HGet(callCtx, rs.bucketKey(key), "t").Float64()
	if err != nil {
		return 0, false
	}
	return v, true
}

func (rs *redisStorage) Close() error {
	return rs.client.Close()
}

// ErrorCount exposes the running count of Redis errors for observability
// glue (tests today, Prometheus in a follow-up).
func (rs *redisStorage) ErrorCount() uint64 {
	return rs.errorCount.Load()
}
