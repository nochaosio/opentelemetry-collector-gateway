// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// TokenBucket implements a token bucket rate limiter.
type TokenBucket struct {
	tokens         float64
	maxTokens      float64
	refillRate     float64
	lastRefillTime time.Time
	mu             sync.Mutex
}

func NewTokenBucket(limit int, period time.Duration) *TokenBucket {
	refillRate := float64(limit) / period.Seconds()
	return &TokenBucket{
		tokens:         float64(limit),
		maxTokens:      float64(limit),
		refillRate:     refillRate,
		lastRefillTime: time.Now(),
	}
}

func (tb *TokenBucket) refillLocked() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefillTime).Seconds()
	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
	tb.lastRefillTime = now
}

func (tb *TokenBucket) Allow() bool {
	return tb.AllowN(1)
}

func (tb *TokenBucket) AllowN(n int) bool {
	if n <= 0 {
		return true
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refillLocked()

	requiredTokens := float64(n)
	if tb.tokens >= requiredTokens {
		tb.tokens -= requiredTokens
		return true
	}
	return false
}

// AllowUpToN consumes up to n tokens and returns how many were granted.
// Enables partial-allow semantics: instead of dropping a full batch when
// over-limit, the processor can forward as many items as the bucket can
// afford and drop only the overflow.
func (tb *TokenBucket) AllowUpToN(n int) int {
	if n <= 0 {
		return 0
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refillLocked()

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return n
	}
	granted := int(tb.tokens)
	if granted <= 0 {
		return 0
	}
	tb.tokens -= float64(granted)
	return granted
}

// Refund adds tokens back to the bucket, capped at max. Used after a
// multi-bucket coordination when the downstream bucket granted less than
// the upstream pre-consumed.
func (tb *TokenBucket) Refund(n int) {
	if n <= 0 {
		return
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.tokens += float64(n)
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
}

// globalKey is the reserved bucket name used when a global budget is set.
// It lives in the same keyspace as regular keys — no practical collision
// risk because service.name / header / attribute values are user-provided
// and anyone literally naming a service "__global__" has bigger problems.
const globalKey = "__global__"

// RateLimiter manages rate limits for multiple keys on top of a pluggable
// Storage backend, optionally coordinated with a global bucket that puts
// a ceiling on total throughput across all keys.
//
// Semantics vs. the underlying Storage:
//   - Memory backend → each replica has its own view (stateless gateway).
//   - Redis backend  → all replicas share one view (stateful, fleet-wide).
//
// The RateLimiter itself doesn't know or care which is in use; it just
// coordinates global↔per-key accounting via Storage calls.
type RateLimiter struct {
	storage   Storage
	config    *Config
	hasGlobal bool
}

// NewRateLimiter builds a rate limiter with the default (in-process)
// memory storage. Used by tests and by callers that want the simple path.
//
// The caller owns the returned limiter and must Close it: the memory storage
// runs a background eviction goroutine that lives until then.
func NewRateLimiter(config *Config) *RateLimiter {
	storage := newMemoryStorage(5*time.Minute, 10*time.Minute, 0, zap.NewNop())
	return newRateLimiterWithStorage(config, storage)
}

// newRateLimiterWithStorage injects a specific storage backend. This is
// what the factory uses so it can hand in a Redis-backed storage when the
// operator opts in via config.
func newRateLimiterWithStorage(config *Config, storage Storage) *RateLimiter {
	_, hasGlobal := configHasGlobal(config)
	return &RateLimiter{
		storage:   storage,
		config:    config,
		hasGlobal: hasGlobal,
	}
}

func configHasGlobal(cfg *Config) (int, bool) {
	if cfg.GlobalRequestsPerSecond > 0 || cfg.GlobalRequestsPerMinute > 0 {
		return 1, true
	}
	return 0, false
}

func (rl *RateLimiter) Allow(key string) bool {
	return rl.AllowN(key, 1)
}

// AllowN is a boolean all-or-nothing admission check.
func (rl *RateLimiter) AllowN(key string, n int) bool {
	return rl.AllowNCtx(context.Background(), key, n)
}

// AllowNCtx is the context-aware variant of AllowN. It is atomic in effect:
// when the budget cannot cover all n items, any partially granted tokens are
// refunded, so a denied batch never drains budget it didn't use. Without the
// refund, a steady stream of over-budget batches would burn the bucket dry and
// starve traffic that is actually under the limit.
func (rl *RateLimiter) AllowNCtx(ctx context.Context, key string, n int) bool {
	if n <= 0 {
		return true
	}
	granted := rl.AllowUpToNCtx(ctx, key, n)
	if granted == n {
		return true
	}
	if granted > 0 {
		keyLimit, keyPeriod := rl.config.GetLimit(key)
		rl.storage.Refund(ctx, key, granted, keyLimit, keyPeriod)
		if rl.hasGlobal {
			// AllowUpToNCtx already refunded the global bucket down to the
			// key-granted amount, so the net global charge is exactly `granted`.
			gLimit, gPeriod := rl.config.GlobalLimit()
			rl.storage.Refund(ctx, globalKey, granted, gLimit, gPeriod)
		}
	}
	return false
}

// AllowUpToN consumes up to n tokens from the per-key bucket AND the global
// bucket (if configured), and returns the number of items that fit.
// Coordination: take from global first, then from key, refund the
// unused-by-key portion back to global to avoid over-charging.
func (rl *RateLimiter) AllowUpToN(key string, n int) int {
	return rl.AllowUpToNCtx(context.Background(), key, n)
}

// AllowUpToNCtx is the context-aware variant. The context is threaded into
// the Storage layer so Redis calls can respect the consumer's deadline.
func (rl *RateLimiter) AllowUpToNCtx(ctx context.Context, key string, n int) int {
	if n <= 0 {
		return 0
	}

	globalGranted := n
	if rl.hasGlobal {
		gLimit, gPeriod := rl.config.GlobalLimit()
		globalGranted = rl.storage.AllowUpToN(ctx, globalKey, n, gLimit, gPeriod)
		if globalGranted == 0 {
			return 0
		}
	}

	keyLimit, keyPeriod := rl.config.GetLimit(key)
	keyGranted := rl.storage.AllowUpToN(ctx, key, globalGranted, keyLimit, keyPeriod)

	if rl.hasGlobal && keyGranted < globalGranted {
		gLimit, gPeriod := rl.config.GlobalLimit()
		rl.storage.Refund(ctx, globalKey, globalGranted-keyGranted, gLimit, gPeriod)
	}
	return keyGranted
}

// Close releases the storage backend's resources.
func (rl *RateLimiter) Close() error {
	return rl.storage.Close()
}
