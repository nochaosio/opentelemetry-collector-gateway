// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package statefulfilterprocessor

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
)

// Config wires the processor to the Redis rule set. Unlike the contrib
// `filter` processor — whose OTTL expressions are frozen into the collector
// config and only change on a restart/reload — the rules here live in Redis
// and are picked up by every replica within refresh_interval. One write,
// N collectors, no rollout.
type Config struct {
	// Redis is the rule source. Mandatory: without it there is nothing to read.
	Redis *RedisConfig `mapstructure:"redis"`

	// RefreshInterval is how often each replica checks Redis for a new rule
	// version. The check is a single GET of the version key — HGETALL only
	// happens when the version actually moved, so a 20-replica fleet polling
	// every 5s costs Redis ~4 trivial GETs per second.
	//
	// This is also the worst-case propagation delay of a new drop rule.
	// Default 10s.
	RefreshInterval time.Duration `mapstructure:"refresh_interval"`

	// FullResyncEvery forces a full HGETALL every N refreshes regardless of the
	// version key, so a rule written by hand (HSET without bumping the version)
	// still converges instead of being invisible forever. Default 6
	// (== every minute at the default interval). 1 = always full sync.
	FullResyncEvery int `mapstructure:"full_resync_every"`

	// WaitForInitialLoad blocks component startup until the first rule fetch
	// succeeds (or InitialLoadTimeout elapses). Prevents the window right after
	// a restart where the collector is already accepting OTLP but has no rules
	// yet and therefore forwards traffic that should be dropped.
	// Default true.
	WaitForInitialLoad bool `mapstructure:"wait_for_initial_load"`

	// InitialLoadTimeout bounds the startup fetch. On timeout the processor
	// starts anyway with an empty rule set (pass-through) and keeps retrying in
	// the background — an unreachable Redis must not stop the gateway from
	// accepting telemetry. Default 5s.
	InitialLoadTimeout time.Duration `mapstructure:"initial_load_timeout"`

	// MaxRules caps how many rules are compiled from Redis. A rule hash that
	// grew unbounded (bug in whatever writes it) would otherwise turn into
	// per-item CPU cost on the hot path. Rules beyond the cap are ignored, in
	// stable (sorted-by-id) order, and reported via the invalid_rules metric.
	// Default 500. Negative = unlimited.
	MaxRules int `mapstructure:"max_rules"`

	// FailClosedOnEmpty inverts the default fail-open posture: when Redis has
	// never been reachable since startup, drop *nothing* (default, false) or
	// treat that as a fatal misconfiguration (true) and refuse to start.
	//
	// Default false: an edge collector that refuses telemetry because its rule
	// store hiccuped is worse than briefly forwarding data it would have
	// dropped. Set true only when the drop rules are a compliance control
	// (e.g. PII scrubbing) and forwarding-by-accident is unacceptable.
	FailClosedOnEmpty bool `mapstructure:"fail_closed_on_empty"`
}

// RedisConfig points at the Redis holding the rules. Shape mirrors the
// ratelimit processor's storage.redis block on purpose — in the common
// deployment both components talk to the same Redis, so the two config
// snippets read identically.
type RedisConfig struct {
	// Addr: "host:port" of a single node. Use Addrs for HA instead.
	Addr string `mapstructure:"addr"`

	// Addrs: multiple endpoints. With MasterName set these are the Sentinel
	// addresses (client follows primary failover); without it and len>1 the
	// client runs in Redis Cluster mode. Takes precedence over Addr.
	Addrs []string `mapstructure:"addrs"`

	// MasterName: enables Sentinel failover mode against Addrs.
	MasterName string `mapstructure:"master_name"`

	// Password: optional AUTH password. Opaque so it is redacted from config
	// dumps (zpages /debug/configz), logs and error messages.
	Password configopaque.String `mapstructure:"password"`

	// DB: logical database index. Default 0.
	DB int `mapstructure:"db"`

	// KeyPrefix namespaces the rule keys, so this processor can share a Redis
	// instance with the rate limiter and anything else. The processor reads
	// "<prefix>:rules" (hash) and "<prefix>:rules:version" (counter).
	// Default "otelcol:filter".
	KeyPrefix string `mapstructure:"key_prefix"`

	// Timeout bounds each rule fetch. Generous compared to the rate limiter's
	// 100ms because this call is off the data path — it runs in the background
	// refresh goroutine, never in ConsumeTraces. Default 2s.
	Timeout time.Duration `mapstructure:"timeout"`

	// TLS: optional TLS for the Redis connection. Rules describe which
	// services and attributes exist in your estate; in a regulated environment
	// that should not cross the network in clear.
	TLS *configtls.ClientConfig `mapstructure:"tls"`
}

const (
	defaultKeyPrefix          = "otelcol:filter"
	defaultRefreshInterval    = 10 * time.Second
	defaultFullResyncEvery    = 6
	defaultInitialLoadTimeout = 5 * time.Second
	defaultRedisTimeout       = 2 * time.Second
	defaultMaxRules           = 500
)

func createDefaultConfig() component.Config {
	return &Config{
		RefreshInterval:    defaultRefreshInterval,
		FullResyncEvery:    defaultFullResyncEvery,
		WaitForInitialLoad: true,
		InitialLoadTimeout: defaultInitialLoadTimeout,
		MaxRules:           defaultMaxRules,
	}
}

func (cfg *Config) Validate() error {
	if cfg.Redis == nil {
		return errors.New("redis must be configured: this processor reads its rules from Redis")
	}
	if cfg.Redis.Addr == "" && len(cfg.Redis.Addrs) == 0 {
		return errors.New("redis requires addr (single node) or addrs (sentinel/cluster)")
	}
	if cfg.Redis.MasterName != "" && len(cfg.Redis.Addrs) == 0 {
		return errors.New("redis.master_name (sentinel mode) requires addrs (the sentinel endpoints)")
	}
	if cfg.RefreshInterval < 0 {
		return fmt.Errorf("refresh_interval must be positive, got %s", cfg.RefreshInterval)
	}
	if cfg.Redis.Timeout < 0 {
		return fmt.Errorf("redis.timeout must be positive, got %s", cfg.Redis.Timeout)
	}
	if cfg.InitialLoadTimeout < 0 {
		return fmt.Errorf("initial_load_timeout must be positive, got %s", cfg.InitialLoadTimeout)
	}
	if cfg.FullResyncEvery < 0 {
		return fmt.Errorf("full_resync_every must be >= 0, got %d", cfg.FullResyncEvery)
	}
	return nil
}

// applyDefaults fills in the zero values left by an explicit (non-factory)
// config so operators only have to set `redis.addr` in the common case.
// Called after unmarshal, before the store is built.
func (cfg *Config) applyDefaults() {
	if cfg.RefreshInterval == 0 {
		cfg.RefreshInterval = defaultRefreshInterval
	}
	if cfg.FullResyncEvery == 0 {
		cfg.FullResyncEvery = defaultFullResyncEvery
	}
	if cfg.InitialLoadTimeout == 0 {
		cfg.InitialLoadTimeout = defaultInitialLoadTimeout
	}
	if cfg.MaxRules == 0 {
		cfg.MaxRules = defaultMaxRules
	}
	if cfg.Redis == nil {
		return
	}
	if cfg.Redis.KeyPrefix == "" {
		cfg.Redis.KeyPrefix = defaultKeyPrefix
	}
	if cfg.Redis.Timeout == 0 {
		cfg.Redis.Timeout = defaultRedisTimeout
	}
}
