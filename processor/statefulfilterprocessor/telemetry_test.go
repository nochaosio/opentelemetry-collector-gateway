// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package statefulfilterprocessor

import (
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func noopMeterProvider() metric.MeterProvider { return noop.NewMeterProvider() }

// findMetric locates one instrument in a collected snapshot.
func findMetric(t *testing.T, rm *metricdata.ResourceMetrics, name string) (metricdata.Metrics, bool) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m, true
			}
		}
	}
	return metricdata.Metrics{}, false
}

// sumCounter totals an Int64 counter, optionally restricted to data points
// carrying all of the given attributes.
func sumCounter(t *testing.T, rm *metricdata.ResourceMetrics, name string, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	m, ok := findMetric(t, rm, name)
	if !ok {
		t.Fatalf("metric %q not found", name)
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric %q is not an int64 sum (%T)", name, m.Data)
	}
	var total int64
	for _, dp := range sum.DataPoints {
		if hasAttrs(dp.Attributes, attrs) {
			total += dp.Value
		}
	}
	return total
}

// gaugeValue returns the single value of an Int64 observable gauge.
func gaugeValue(t *testing.T, rm *metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	m, ok := findMetric(t, rm, name)
	if !ok {
		t.Fatalf("metric %q not found", name)
	}
	g, ok := m.Data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatalf("metric %q is not an int64 gauge (%T)", name, m.Data)
	}
	if len(g.DataPoints) != 1 {
		t.Fatalf("metric %q: expected exactly 1 data point, got %d", name, len(g.DataPoints))
	}
	return g.DataPoints[0].Value
}

func hasAttrs(set attribute.Set, want []attribute.KeyValue) bool {
	for _, kv := range want {
		v, ok := set.Value(kv.Key)
		if !ok || v != kv.Value {
			return false
		}
	}
	return true
}
