// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap"
)

// TestLoadConfig round-trips testdata/config.yaml through confmap. Struct
// literals in the other tests never exercise the mapstructure tags, so this is
// the only place a renamed or missing tag shows up.
func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	sub, err := cm.Sub("ratelimit")
	require.NoError(t, err)

	cfg := createDefaultConfig()
	require.NoError(t, sub.Unmarshal(cfg))
	require.NoError(t, confmap.Validate(cfg))

	assert.Equal(t, &Config{
		LimitType:               "attribute",
		AttributeKey:            "tenant.id",
		RequestsPerSecond:       500,
		DefaultLimit:            250,
		DefaultLimitPeriod:      "second",
		DropOnLimit:             true,
		PreserveErrors:          true,
		TraceIDAware:            true,
		GlobalRequestsPerSecond: 10000,
		MaxShareRatio:           0.2,
		RejectionMode:           "permanent",
		MetricsVerbosity:        "basic",
		MaxMetricKeys:           50,
		MetricsKeyAllowlist:     []string{"checkout", "payments"},
		KeyLabelsOn:             []string{"denied", "preserved"},
		SpecificLimits: map[string]LimitConfig{
			"checkout":  {RequestsPerSecond: 10},
			"reporting": {RequestsPerMinute: 600},
		},
		Storage: &StorageConfig{
			Backend:    "redis",
			MaxBuckets: 1000,
			Redis: &RedisConfig{
				Addr:             "127.0.0.1:6379",
				DB:               3,
				KeyPrefix:        "otelcol:ratelimit:test",
				Timeout:          250 * time.Millisecond,
				OnError:          "closed",
				NegativeCacheTTL: 50 * time.Millisecond,
			},
		},
	}, cfg)
}

// The default config must be usable as-is: the generated lifecycle test and
// any operator who drops a bare `ratelimit:` into a pipeline depend on it.
func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()
	require.NotNil(t, cfg)
	require.NoError(t, confmap.Validate(cfg))
}
