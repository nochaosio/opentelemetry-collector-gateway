// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package statefulfilterprocessor

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name:    "no redis block",
			cfg:     &Config{},
			wantErr: true,
		},
		{
			name:    "redis without addr",
			cfg:     &Config{Redis: &RedisConfig{}},
			wantErr: true,
		},
		{
			name: "single node",
			cfg:  &Config{Redis: &RedisConfig{Addr: "redis:6379"}},
		},
		{
			name: "cluster via addrs",
			cfg:  &Config{Redis: &RedisConfig{Addrs: []string{"a:6379", "b:6379"}}},
		},
		{
			name:    "sentinel without addrs",
			cfg:     &Config{Redis: &RedisConfig{Addr: "redis:6379", MasterName: "mymaster"}},
			wantErr: true,
		},
		{
			name: "sentinel with addrs",
			cfg:  &Config{Redis: &RedisConfig{Addrs: []string{"s1:26379"}, MasterName: "mymaster"}},
		},
		{
			name:    "negative refresh interval",
			cfg:     &Config{Redis: &RedisConfig{Addr: "redis:6379"}, RefreshInterval: -time.Second},
			wantErr: true,
		},
		{
			name:    "negative full_resync_every",
			cfg:     &Config{Redis: &RedisConfig{Addr: "redis:6379"}, FullResyncEvery: -1},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr && err == nil {
				t.Fatal("expected a validation error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestApplyDefaults(t *testing.T) {
	cfg := &Config{Redis: &RedisConfig{Addr: "redis:6379"}}
	cfg.applyDefaults()

	if cfg.Redis.KeyPrefix != defaultKeyPrefix {
		t.Fatalf("key_prefix default = %q", cfg.Redis.KeyPrefix)
	}
	if cfg.Redis.Timeout != defaultRedisTimeout {
		t.Fatalf("redis timeout default = %s", cfg.Redis.Timeout)
	}
	if cfg.RefreshInterval != defaultRefreshInterval {
		t.Fatalf("refresh_interval default = %s", cfg.RefreshInterval)
	}
	if cfg.FullResyncEvery != defaultFullResyncEvery {
		t.Fatalf("full_resync_every default = %d", cfg.FullResyncEvery)
	}
	if cfg.MaxRules != defaultMaxRules {
		t.Fatalf("max_rules default = %d", cfg.MaxRules)
	}
	if cfg.InitialLoadTimeout != defaultInitialLoadTimeout {
		t.Fatalf("initial_load_timeout default = %s", cfg.InitialLoadTimeout)
	}
}

func TestApplyDefaults_DoesNotOverrideExplicitValues(t *testing.T) {
	cfg := &Config{
		Redis:           &RedisConfig{Addr: "redis:6379", KeyPrefix: "custom", Timeout: 5 * time.Second},
		RefreshInterval: time.Minute,
		MaxRules:        10,
	}
	cfg.applyDefaults()

	if cfg.Redis.KeyPrefix != "custom" || cfg.Redis.Timeout != 5*time.Second {
		t.Fatalf("explicit redis settings were overwritten: %+v", cfg.Redis)
	}
	if cfg.RefreshInterval != time.Minute || cfg.MaxRules != 10 {
		t.Fatalf("explicit settings were overwritten: %+v", cfg)
	}
}

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	// The default config alone is not usable — redis.addr is mandatory — and
	// Validate must say so rather than silently starting a no-op processor.
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected the default config to fail validation without a redis block")
	}
	if !cfg.WaitForInitialLoad {
		t.Fatal("wait_for_initial_load should default to true")
	}
	if cfg.FailClosedOnEmpty {
		t.Fatal("fail_closed_on_empty should default to false (fail-open)")
	}
}

// TestLoadConfig round-trips testdata/config.yaml through confmap. Struct
// literals in the other tests never exercise the mapstructure tags, so this is
// the only place a renamed or missing tag shows up.
func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	if err != nil {
		t.Fatalf("LoadConf: %v", err)
	}
	sub, err := cm.Sub("statefulfilter")
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}

	cfg := createDefaultConfig()
	if err := sub.Unmarshal(cfg); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if err := confmap.Validate(cfg); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	want := &Config{
		RefreshInterval:    15 * time.Second,
		FullResyncEvery:    40,
		WaitForInitialLoad: true,
		InitialLoadTimeout: 7 * time.Second,
		MaxRules:           2000,
		FailClosedOnEmpty:  true,
		Redis: &RedisConfig{
			Addr:      "127.0.0.1:6379",
			DB:        2,
			KeyPrefix: "otelcol:filter:test",
			Timeout:   300 * time.Millisecond,
		},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("config mismatch:\n got: %+v\nwant: %+v", cfg, want)
	}
}
