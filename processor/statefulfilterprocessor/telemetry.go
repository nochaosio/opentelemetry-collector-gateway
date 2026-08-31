// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package statefulfilterprocessor

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"

	"github.com/nochaosio/opentelemetry-collector-gateway/processor/statefulfilterprocessor/internal/metadata"
)

// processorTelemetry adapts the mdatagen-generated TelemetryBuilder to the
// field names the processor uses. Instrument names, units and descriptions
// live in metadata.yaml; run `make generate` after changing them.
//
// rulesVersion is the fleet-convergence signal: alert on
// max(version) != min(version) across replicas and you catch a collector that
// stopped seeing rule updates.
type processorTelemetry struct {
	meter metric.Meter

	evaluatedItems metric.Int64Counter
	droppedItems   metric.Int64Counter
	keptItems      metric.Int64Counter
	refreshErrors  metric.Int64ObservableGauge

	rulesLoaded  metric.Int64ObservableGauge
	rulesInvalid metric.Int64ObservableGauge
	rulesVersion metric.Int64ObservableGauge
	rulesAge     metric.Float64ObservableGauge
}

// newProcessorTelemetry takes a MeterProvider rather than the full
// component.TelemetrySettings because the generated builder only reads the
// meter from it, and every call site already has the provider in hand.
func newProcessorTelemetry(mp metric.MeterProvider) (*processorTelemetry, error) {
	ts := component.TelemetrySettings{MeterProvider: mp}
	tb, err := metadata.NewTelemetryBuilder(ts)
	if err != nil {
		return nil, err
	}
	return &processorTelemetry{
		meter:          metadata.Meter(ts),
		evaluatedItems: tb.ProcessorStatefulfilterEvaluatedItems,
		droppedItems:   tb.ProcessorStatefulfilterDroppedItems,
		keptItems:      tb.ProcessorStatefulfilterKeptItems,
		refreshErrors:  tb.ProcessorStatefulfilterRefreshErrors,
		rulesLoaded:    tb.ProcessorStatefulfilterRulesLoaded,
		rulesInvalid:   tb.ProcessorStatefulfilterRulesInvalid,
		rulesVersion:   tb.ProcessorStatefulfilterRulesVersion,
		rulesAge:       tb.ProcessorStatefulfilterRulesAgeSeconds,
	}, nil
}
