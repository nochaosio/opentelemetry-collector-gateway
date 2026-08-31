// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/nochaosio/opentelemetry-collector-gateway/processor/ratelimitprocessor/internal/metadata"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetricsVerbosity_Validation(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"", true},
		{"detailed", true},
		{"basic", true},
		{"verbose", false},
		{"none", false},
	} {
		cfg := createDefaultConfig().(*Config)
		cfg.RequestsPerSecond = 5
		cfg.MetricsVerbosity = tc.value
		err := cfg.Validate()
		if tc.valid && err != nil {
			t.Errorf("metrics_verbosity=%q: unexpected error: %v", tc.value, err)
		}
		if !tc.valid && err == nil {
			t.Errorf("metrics_verbosity=%q: expected validation error, got nil", tc.value)
		}
	}
}

func TestMetricsVerbosity_KeyLabelSet(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if set := cfg.keyLabelSet(); !set["denied"] || !set["preserved"] {
		t.Fatalf("detailed default should keep key on denied+preserved, got %v", set)
	}

	cfg.MetricsVerbosity = "basic"
	cfg.KeyLabelsOn = []string{"received", "allowed", "denied", "preserved"}
	if set := cfg.keyLabelSet(); len(set) != 0 {
		t.Fatalf("basic must return an empty key-label set even with key_labels_on, got %v", set)
	}
}

// Basic mode: over-limit traffic must produce denied datapoints WITHOUT a key
// attribute, and the token-bucket gauges must not be registered even though an
// allowlist (the gauges' watch set) is configured.
func TestMetricsVerbosity_Basic_NoKeyLabelNoGauges(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.RequestsPerSecond = 1
	cfg.DropOnLimit = true
	cfg.MetricsVerbosity = "basic"
	cfg.MetricsKeyAllowlist = []string{"svc-a"}

	mp, reader := newTestMeterProvider()
	set := processortest.NewNopSettings(metadata.Type)
	set.ID = component.NewIDWithName(metadata.Type, "verbosity-basic")
	set.MeterProvider = mp

	ctx := context.Background()
	tp, err := factory.CreateTraces(ctx, set, cfg, consumertest.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := tp.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	// Burst far over the 1/s limit so denied_items is emitted.
	for i := 0; i < 5; i++ {
		_ = tp.ConsumeTraces(ctx, newTestTraces("svc-a", 10))
	}

	rm := collectMetrics(t, reader)

	denied := findMetric(rm, "otelcol_processor_ratelimit_denied_items")
	if denied == nil {
		t.Fatal("expected denied_items to be emitted in basic mode")
	}
	sum, ok := denied.Data.(metricdata.Sum[int64])
	if !ok || len(sum.DataPoints) == 0 {
		t.Fatal("denied_items has no datapoints")
	}
	for _, dp := range sum.DataPoints {
		if _, exists := dp.Attributes.Value(attribute.Key("key")); exists {
			t.Fatalf("basic mode must not emit the key label, got attrs %v", dp.Attributes.ToSlice())
		}
	}

	for _, gauge := range []string{
		"otelcol_processor_ratelimit_bucket_capacity_tokens",
		"otelcol_processor_ratelimit_bucket_available_tokens",
	} {
		if findMetric(rm, gauge) != nil {
			t.Fatalf("basic mode must not export %s", gauge)
		}
	}
}

// Detailed (default) keeps today's behavior: key label present on denied.
func TestMetricsVerbosity_DetailedDefault_KeyLabelPresent(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig().(*Config)
	cfg.RequestsPerSecond = 1
	cfg.DropOnLimit = true
	cfg.MetricsKeyAllowlist = []string{"svc-a"}

	mp, reader := newTestMeterProvider()
	set := processortest.NewNopSettings(metadata.Type)
	set.ID = component.NewIDWithName(metadata.Type, "verbosity-detailed")
	set.MeterProvider = mp

	ctx := context.Background()
	tp, err := factory.CreateTraces(ctx, set, cfg, consumertest.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := tp.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	for i := 0; i < 5; i++ {
		_ = tp.ConsumeTraces(ctx, newTestTraces("svc-a", 10))
	}

	rm := collectMetrics(t, reader)
	denied := findMetric(rm, "otelcol_processor_ratelimit_denied_items")
	if got := getCounterValue(denied, "key", "svc-a"); got == 0 {
		t.Fatal("detailed mode should emit denied_items with key=svc-a")
	}
}
