// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
)

type Config struct {
	// LimitType defines how to identify the client for rate limiting
	// Options: "attribute", "header", "service_name"
	LimitType string `mapstructure:"limit_type"`

	// AttributeKey is the attribute key to use when limit_type is "attribute"
	// Example: "service.name", "client.id", etc.
	AttributeKey string `mapstructure:"attribute_key"`

	// HeaderKey is the header key to use when limit_type is "header"
	// Example: "X-Client-ID", "X-Rate-Limit-Key", etc.
	HeaderKey string `mapstructure:"header_key"`

	// RequestsPerSecond defines the per-key maximum number of requests per second
	// If set, this takes precedence over RequestsPerMinute
	RequestsPerSecond int `mapstructure:"requests_per_second"`

	// RequestsPerMinute defines the per-key maximum number of requests per minute
	RequestsPerMinute int `mapstructure:"requests_per_minute"`

	// DefaultLimit is used when no specific limit is configured
	DefaultLimit int `mapstructure:"default_limit"`

	// DefaultLimitPeriod is the period for the default limit (second or minute)
	DefaultLimitPeriod string `mapstructure:"default_limit_period"`

	// SpecificLimits allows setting different limits for different keys
	// Key is the attribute/header/service name value
	// Value is the limit for that specific key
	SpecificLimits map[string]LimitConfig `mapstructure:"specific_limits"`

	// DropOnLimit defines if data should be dropped when limit is exceeded
	// If false, data will be passed through (useful for monitoring)
	DropOnLimit bool `mapstructure:"drop_on_limit"`

	// GlobalRequestsPerSecond is an optional ceiling on *total* throughput
	// across all keys. When set together with MaxShareRatio, it bounds how
	// much of the global budget any single key can take, preventing one
	// noisy service from starving all others.
	GlobalRequestsPerSecond int `mapstructure:"global_requests_per_second"`

	// GlobalRequestsPerMinute — same as above but in per-minute units.
	GlobalRequestsPerMinute int `mapstructure:"global_requests_per_minute"`

	// MaxShareRatio caps any single key at this fraction of the global budget.
	// Range (0, 1]. Example: global=1000 rps, MaxShareRatio=0.2 →
	// each key gets at most 200 rps even if SpecificLimits says otherwise.
	// Requires Global*PerSecond/Minute to be set.
	MaxShareRatio float64 `mapstructure:"max_share_ratio"`

	// PreserveErrors, when true, forces error spans (StatusCode=Error) and
	// error-severity logs (SeverityNumber >= ERROR) to bypass the rate limit.
	// These high-value items are always forwarded even when the bucket is empty.
	// Default: true (we'd rather keep the signal that matters).
	PreserveErrors bool `mapstructure:"preserve_errors"`

	// TraceIDAware makes trace dropping all-or-nothing per trace_id. The default
	// (false) drops individual spans, which corrupts traces (orphan spans,
	// missing parents). When true, whole traces are kept or dropped together, so
	// a downstream backend never sees a partial trace; a trace containing any
	// error span is kept in full when PreserveErrors is on. Only affects traces.
	// Recommended for any tracing-critical deployment.
	TraceIDAware bool `mapstructure:"trace_id_aware"`

	// MetricsKeyAllowlist bounds the cardinality of the `key` label on the
	// exported Prometheus metrics. Only keys listed here keep their real value
	// in the `key` label; every other key is reported as key="other".
	//
	// This has NO effect on rate limiting itself — buckets are always per-key
	// (full service.name/attribute/header value). It only caps how many
	// distinct `key` label values reach the metrics backend.
	//
	// Empty (default) = unbounded: every key is exported verbatim. That is fine
	// for a small, known set of keys but a cardinality bomb at high key counts
	// (e.g. 20k services → ~20k × signals × priorities active series). When you
	// have many keys, list only the handful you actually want to watch here and
	// everything else collapses into the single "other" series.
	MetricsKeyAllowlist []string `mapstructure:"metrics_key_allowlist"`

	// KeyLabelsOn selects which exported counters carry the high-cardinality
	// `key` label. Counters NOT listed are emitted aggregated (no `key` label
	// at all), which is the primary lever that keeps active-series count bounded
	// when you have many distinct keys.
	//
	// Default (empty): ["denied", "preserved"]. Rationale: received/allowed are
	// produced for EVERY key on every batch (unbounded cardinality), while
	// denied/preserved are produced only by the few keys that actually hit the
	// limit — naturally bounded to the misbehaving set. So you get per-key
	// visibility exactly where it matters (who is being throttled / who is
	// shedding errors) without paying for a series per idle service.
	//
	// Valid entries: received, allowed, denied, preserved. Set all four to
	// restore the legacy "key on everything" behavior. Composes with
	// metrics_key_allowlist: wherever a `key` label is kept, its value is still
	// collapsed to "other" outside the allowlist.
	KeyLabelsOn []string `mapstructure:"key_labels_on"`

	// MetricsVerbosity is the single-switch cardinality control for the
	// processor's own metrics:
	//
	//   "detailed" (default): current behavior — the `key` label is emitted on
	//     the counters selected by key_labels_on (bounded by
	//     metrics_key_allowlist / max_metric_keys), and the per-key token-bucket
	//     gauges are exported for allowlisted keys.
	//
	//   "basic": no `key` label on ANY counter (everything aggregated by
	//     signal/priority/limit_type only) and the token-bucket gauges are not
	//     exported. Worst-case active series drops to a handful per signal,
	//     regardless of key count. Use when the metrics backend is
	//     cardinality-sensitive; per-key attribution is still available in the
	//     over-limit warn logs.
	//
	// "basic" makes key_labels_on and the bucket gauges inert; it does not
	// affect rate limiting itself (each key keeps its own bucket).
	MetricsVerbosity string `mapstructure:"metrics_verbosity"`

	// RejectionMode selects the error returned when an entire batch is dropped
	// (drop_on_limit: true and nothing in the batch fit the budget):
	//
	//   "throttle" (default): gRPC RESOURCE_EXHAUSTED with RetryInfo, which the
	//     OTLP receiver maps to HTTP 429 Too Many Requests. This is the
	//     throttling signal prescribed by the OTLP spec; SDK exporters honor it
	//     with backoff-and-retry, so data is delayed rather than lost.
	//   "permanent": non-retryable error (mapped to HTTP ~400). Compliant
	//     clients drop the batch and move on. Choose this when client retries
	//     are worse than data loss (e.g. abusive producers you don't control).
	RejectionMode string `mapstructure:"rejection_mode"`

	// MaxMetricKeys bounds how many distinct values the `key` label on this
	// processor's own metrics can take when metrics_key_allowlist is NOT set.
	// The first MaxMetricKeys distinct keys keep their real value; every key
	// after that is reported as "other". First-come retention keeps series
	// stable for Prometheus (no label churn).
	//
	// This is the always-on backstop against a producer-controlled cardinality
	// bomb (key values come from service.name / attributes / headers): even
	// with no allowlist configured, exported series stay bounded.
	//
	// 0 (default) = 100. Negative = unlimited (not recommended; only safe when
	// the key population is small and trusted). Ignored when
	// metrics_key_allowlist is set (the allowlist already bounds cardinality).
	MaxMetricKeys int `mapstructure:"max_metric_keys"`

	// Storage selects the token-bucket backend. When nil or Backend="memory"
	// (default), each collector replica maintains its own view — stateless,
	// zero-dependency. When Backend="redis", all replicas share one view via
	// Redis — "stateful" rate limiting. Useful when an L4 LB (F5, GCLB, etc.)
	// spreads traffic across replicas and the per-replica limit × N is not
	// the limit you actually want to enforce.
	Storage *StorageConfig `mapstructure:"storage"`
}

type LimitConfig struct {
	RequestsPerSecond int `mapstructure:"requests_per_second"`
	RequestsPerMinute int `mapstructure:"requests_per_minute"`
}

// StorageConfig selects a rate-limit state backend. The zero value
// (unspecified) means "memory" — no new operational dependency.
type StorageConfig struct {
	// Backend: "memory" (default, in-process) or "redis" (shared state).
	Backend string `mapstructure:"backend"`

	// MaxBuckets caps the in-process bucket map (memory backend) so a hostile or
	// buggy producer spraying unique keys can't grow it without bound and OOM the
	// collector. When the cap is hit, the oldest buckets are evicted. 0 uses a
	// safe default (100000). Ignored by the Redis backend (keys expire via TTL).
	MaxBuckets int `mapstructure:"max_buckets"`

	// Redis is required iff Backend == "redis".
	Redis *RedisConfig `mapstructure:"redis"`
}

// RedisConfig controls the Redis-backed storage. All fields have
// production-reasonable defaults; only `addr` is mandatory.
type RedisConfig struct {
	// Addr: "host:port" of a single Redis node. Use this for the simple,
	// single-node case. For HA, use Addrs + MasterName (Sentinel) or Addrs
	// (Cluster) instead.
	Addr string `mapstructure:"addr"`

	// Addrs: multiple endpoints for high availability. With MasterName set,
	// these are the Sentinel addresses (the client follows primary failover).
	// Without MasterName and len>1, the client runs in Redis Cluster mode.
	// Takes precedence over Addr when non-empty.
	Addrs []string `mapstructure:"addrs"`

	// MasterName: when set, run in Sentinel failover mode against Addrs (the
	// sentinels). The client automatically follows a primary failover, so a
	// single Redis node dying does not take rate limiting down with it.
	MasterName string `mapstructure:"master_name"`

	// Password: optional AUTH password. Stored opaque so it is redacted from
	// config dumps (zpages /debug/configz), logs, and error messages.
	Password configopaque.String `mapstructure:"password"`

	// DB: Redis logical database index. Default 0.
	DB int `mapstructure:"db"`

	// KeyPrefix: namespace applied to every rate-limit key so this processor
	// can share a Redis instance with other workloads. Default:
	// "otelcol:ratelimit".
	KeyPrefix string `mapstructure:"key_prefix"`

	// Timeout: per-call deadline for dial/read/write. If Redis doesn't
	// answer within this window, the storage fails open (see FailOpen).
	// Default 100ms — small on purpose: an edge collector must not block
	// the receive path on a slow state store.
	Timeout time.Duration `mapstructure:"timeout"`

	// OnError selects the behavior when Redis is unreachable/slow:
	//   "open"   → allow traffic (default — safer for edge collectors).
	//   "closed" → deny traffic (stricter; only choose this if dropping
	//              telemetry during a Redis outage is preferable to
	//              exceeding the configured rate).
	OnError string `mapstructure:"on_error"`

	// NegativeCacheTTL: after a key gets 0 tokens granted, skip Redis
	// for this duration and return 0 locally. Reduces Redis load under
	// attack at the cost of very short-lived imprecision when the
	// bucket refills. Default 0 (disabled). Set to e.g. 50ms under
	// high-volume deny patterns.
	NegativeCacheTTL time.Duration `mapstructure:"negative_cache_ttl"`

	// TLS: optional TLS for the Redis connection. When set (even empty), the
	// client dials over TLS. In a critical/regulated environment the rate-limit
	// state (who is talking, at what rate) should not cross the network in clear.
	// Standard collector TLS config: ca_file, cert_file, key_file, etc.
	TLS *configtls.ClientConfig `mapstructure:"tls"`
}

func (cfg *Config) Validate() error {
	if cfg.LimitType == "" {
		return errors.New("limit_type must be specified")
	}

	if cfg.LimitType != "attribute" && cfg.LimitType != "header" && cfg.LimitType != "service_name" {
		return errors.New("limit_type must be one of: attribute, header, service_name")
	}

	if cfg.LimitType == "attribute" && cfg.AttributeKey == "" {
		return errors.New("attribute_key must be specified when limit_type is 'attribute'")
	}

	if cfg.LimitType == "header" && cfg.HeaderKey == "" {
		return errors.New("header_key must be specified when limit_type is 'header'")
	}

	if cfg.RequestsPerSecond == 0 && cfg.RequestsPerMinute == 0 && cfg.DefaultLimit == 0 && cfg.GlobalRequestsPerSecond == 0 && cfg.GlobalRequestsPerMinute == 0 {
		return errors.New("at least one of requests_per_second, requests_per_minute, default_limit or global_requests_per_second/minute must be set")
	}

	if cfg.DefaultLimit > 0 && cfg.DefaultLimitPeriod != "second" && cfg.DefaultLimitPeriod != "minute" {
		return errors.New("default_limit_period must be 'second' or 'minute'")
	}

	if cfg.MaxShareRatio < 0 || cfg.MaxShareRatio > 1 {
		return errors.New("max_share_ratio must be in (0, 1]")
	}

	if cfg.MaxShareRatio > 0 && cfg.GlobalRequestsPerSecond == 0 && cfg.GlobalRequestsPerMinute == 0 {
		return errors.New("max_share_ratio requires global_requests_per_second or global_requests_per_minute to be set")
	}

	for _, c := range cfg.KeyLabelsOn {
		if _, ok := validKeyLabelCounters[c]; !ok {
			return fmt.Errorf("key_labels_on entries must be one of received, allowed, denied, preserved; got %q", c)
		}
	}

	if cfg.RejectionMode != "" && cfg.RejectionMode != rejectionThrottle && cfg.RejectionMode != rejectionPermanent {
		return fmt.Errorf("rejection_mode must be %q or %q, got %q", rejectionThrottle, rejectionPermanent, cfg.RejectionMode)
	}

	if cfg.MetricsVerbosity != "" && cfg.MetricsVerbosity != metricsVerbosityDetailed && cfg.MetricsVerbosity != metricsVerbosityBasic {
		return fmt.Errorf("metrics_verbosity must be %q or %q, got %q", metricsVerbosityDetailed, metricsVerbosityBasic, cfg.MetricsVerbosity)
	}

	if cfg.Storage != nil {
		switch cfg.Storage.Backend {
		case "", "memory":
			// OK — Redis block (if any) is ignored in memory mode.
		case "redis":
			if cfg.Storage.Redis == nil {
				return errors.New("storage.redis must be set when storage.backend is 'redis'")
			}
			if cfg.Storage.Redis.Addr == "" && len(cfg.Storage.Redis.Addrs) == 0 {
				return errors.New("storage.redis requires addr (single node) or addrs (sentinel/cluster)")
			}
			if cfg.Storage.Redis.MasterName != "" && len(cfg.Storage.Redis.Addrs) == 0 {
				return errors.New("storage.redis.master_name (sentinel mode) requires addrs (the sentinel endpoints)")
			}
			if r := cfg.Storage.Redis.OnError; r != "" && r != "open" && r != "closed" {
				return fmt.Errorf("storage.redis.on_error must be 'open' or 'closed', got %q", r)
			}
		default:
			return fmt.Errorf("storage.backend must be 'memory' or 'redis', got %q", cfg.Storage.Backend)
		}
	}

	return nil
}

// applyStorageDefaults fills in reasonable defaults on the Redis config so
// operators only have to set `addr` in the common case.
func (cfg *Config) applyStorageDefaults() {
	if cfg.Storage == nil || cfg.Storage.Backend != "redis" || cfg.Storage.Redis == nil {
		return
	}
	r := cfg.Storage.Redis
	if r.KeyPrefix == "" {
		r.KeyPrefix = "otelcol:ratelimit"
	}
	if r.Timeout == 0 {
		r.Timeout = 100 * time.Millisecond
	}
	if r.OnError == "" {
		r.OnError = "open"
	}
}

func createDefaultConfig() component.Config {
	return &Config{
		LimitType:          "service_name",
		RequestsPerSecond:  100,
		DefaultLimit:       100,
		DefaultLimitPeriod: "second",
		DropOnLimit:        true,
		PreserveErrors:     true,
		SpecificLimits:     make(map[string]LimitConfig),
	}
}

// validKeyLabelCounters is the set of counter names accepted by key_labels_on.
var validKeyLabelCounters = map[string]struct{}{
	"received": {}, "allowed": {}, "denied": {}, "preserved": {},
}

// Rejection modes accepted by rejection_mode. Empty defaults to throttle.
const (
	rejectionThrottle  = "throttle"
	rejectionPermanent = "permanent"
)

// Verbosity levels accepted by metrics_verbosity. Empty defaults to detailed.
const (
	metricsVerbosityDetailed = "detailed"
	metricsVerbosityBasic    = "basic"
)

// metricsBasic reports whether the low-cardinality metrics mode is on.
func (cfg *Config) metricsBasic() bool {
	return cfg.MetricsVerbosity == metricsVerbosityBasic
}

// defaultMaxMetricKeys is the `key` label cardinality backstop applied when
// neither metrics_key_allowlist nor max_metric_keys is configured.
const defaultMaxMetricKeys = 100

// keyLabelSet returns the set of counters that should carry the `key` label,
// applying the default ([denied, preserved]) when key_labels_on is unset.
// In basic verbosity no counter carries the label.
func (cfg *Config) keyLabelSet() map[string]bool {
	if cfg.metricsBasic() {
		return map[string]bool{}
	}
	src := cfg.KeyLabelsOn
	if len(src) == 0 {
		src = []string{"denied", "preserved"}
	}
	set := make(map[string]bool, len(src))
	for _, c := range src {
		set[c] = true
	}
	return set
}

// GlobalLimit returns the configured global (limit, period) or (0, 0) if unset.
func (cfg *Config) GlobalLimit() (int, time.Duration) {
	if cfg.GlobalRequestsPerSecond > 0 {
		return cfg.GlobalRequestsPerSecond, time.Second
	}
	if cfg.GlobalRequestsPerMinute > 0 {
		return cfg.GlobalRequestsPerMinute, time.Minute
	}
	return 0, 0
}

func (cfg *Config) GetLimit(key string) (int, time.Duration) {
	// Check for specific limit first
	if specificLimit, ok := cfg.SpecificLimits[key]; ok {
		if specificLimit.RequestsPerSecond > 0 {
			return cfg.capToShare(specificLimit.RequestsPerSecond, time.Second)
		}
		if specificLimit.RequestsPerMinute > 0 {
			return cfg.capToShare(specificLimit.RequestsPerMinute, time.Minute)
		}
	}

	// If global + share ratio is set and no explicit per-key limit, derive from share.
	if cfg.MaxShareRatio > 0 {
		if gLimit, gPeriod := cfg.GlobalLimit(); gLimit > 0 {
			share := int(float64(gLimit) * cfg.MaxShareRatio)
			if share < 1 {
				share = 1
			}
			return share, gPeriod
		}
	}

	// Use global per-key configuration
	if cfg.RequestsPerSecond > 0 {
		return cfg.capToShare(cfg.RequestsPerSecond, time.Second)
	}
	if cfg.RequestsPerMinute > 0 {
		return cfg.capToShare(cfg.RequestsPerMinute, time.Minute)
	}
	if cfg.DefaultLimit > 0 {
		period := time.Second
		if cfg.DefaultLimitPeriod == "minute" {
			period = time.Minute
		}
		return cfg.capToShare(cfg.DefaultLimit, period)
	}

	// Fall back to global * share if only global is configured.
	if gLimit, gPeriod := cfg.GlobalLimit(); gLimit > 0 {
		return gLimit, gPeriod
	}

	// Fallback
	return 100, time.Second
}

// capToShare clips a per-key limit to global*MaxShareRatio when configured.
// Keeps units consistent by converting between second/minute as needed.
func (cfg *Config) capToShare(limit int, period time.Duration) (int, time.Duration) {
	if cfg.MaxShareRatio <= 0 {
		return limit, period
	}
	gLimit, gPeriod := cfg.GlobalLimit()
	if gLimit == 0 {
		return limit, period
	}
	// Normalize to per-second for comparison.
	keyRPS := float64(limit) / period.Seconds()
	globalRPS := float64(gLimit) / gPeriod.Seconds()
	capRPS := globalRPS * cfg.MaxShareRatio
	if keyRPS <= capRPS {
		return limit, period
	}
	// Apply cap while keeping the requested period.
	capped := int(capRPS * period.Seconds())
	if capped < 1 {
		capped = 1
	}
	return capped, period
}
