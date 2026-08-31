package ratelimitprocessor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Storage abstracts the token-bucket state store so the processor can run
// with either an in-process backend (default) or a shared backend like
// Redis (opt-in, fleet-wide accuracy + cross-replica visibility).
//
// Implementations MUST be safe for concurrent use.
// Implementations SHOULD be fail-open on transient errors (return `n`, the
// requested amount) — an edge collector dropping traffic because the state
// store hiccupped is worse than the rate limit being briefly imprecise.
type Storage interface {
	// AllowUpToN tries to consume up to n tokens from the bucket identified
	// by key, refilling it first based on (limit, period). Returns the
	// number of tokens granted in [0, n].
	//
	// limit/period are passed per-call (not fixed at construction) so the
	// processor can apply different budgets to different keys — e.g. a
	// specific_limits override or a fair-share cap.
	AllowUpToN(ctx context.Context, key string, n, limit int, period time.Duration) int

	// Refund returns n tokens to the bucket (bounded by `limit`). Used by
	// the fair-share coordinator to undo over-charging when the downstream
	// per-key bucket granted less than the upstream global bucket consumed.
	Refund(ctx context.Context, key string, n, limit int, period time.Duration)

	// AvailableTokens reports how many tokens are currently free in the
	// bucket for key, and whether that bucket exists yet. Used only for the
	// observable bucket gauges (capacity vs free). ok=false means no traffic
	// has created the bucket — callers treat that as "idle / full".
	AvailableTokens(ctx context.Context, key string) (tokens float64, ok bool)

	// SetErrorCounter wires a counter that the backend increments on transient
	// errors (e.g. Redis unreachable while fail-open). Lets operators alert on a
	// silently-degraded state store. No-op for backends that can't fail.
	SetErrorCounter(counter metric.Int64Counter)

	// SetEvictionCounter wires a counter incremented when the backend evicts
	// buckets to stay under a memory cap. No-op for backends without a local cap.
	SetEvictionCounter(counter metric.Int64Counter)

	// SetFailOpenCounter wires a counter incremented (by item count) when the
	// backend is unreachable and traffic is allowed through fail-open. No-op for
	// backends that cannot fail open.
	SetFailOpenCounter(counter metric.Int64Counter)

	// Close releases any resources (background goroutines, connections).
	Close() error

	// Name identifies the backend for logs/observability.
	Name() string
}

// newStorage picks a backend from config. "" and "memory" → in-process;
// "redis" → shared Redis bucket. Unknown values fail validation upstream.
func newStorage(cfg *Config, logger *zap.Logger) (Storage, error) {
	backend := "memory"
	if cfg.Storage != nil && cfg.Storage.Backend != "" {
		backend = cfg.Storage.Backend
	}
	switch backend {
	case "memory":
		maxBuckets := 0
		if cfg.Storage != nil {
			maxBuckets = cfg.Storage.MaxBuckets
		}
		return newMemoryStorage(5*time.Minute, 10*time.Minute, maxBuckets, logger), nil
	case "redis":
		if cfg.Storage == nil || cfg.Storage.Redis == nil {
			return nil, fmt.Errorf("storage.redis must be configured when backend=redis")
		}
		return newRedisStorage(cfg.Storage.Redis, logger)
	default:
		return nil, fmt.Errorf("unsupported storage backend: %q (valid: memory, redis)", backend)
	}
}
