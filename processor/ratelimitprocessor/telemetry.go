// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"

	"github.com/nochaosio/opentelemetry-collector-gateway/processor/ratelimitprocessor/internal/metadata"
)

// processorTelemetry adapts the mdatagen-generated TelemetryBuilder to the
// field names the processor uses. Instrument names, units and descriptions
// live in metadata.yaml; run `make generate` after changing them.
type processorTelemetry struct {
	meter metric.Meter

	receivedItems   metric.Int64Counter
	allowedItems    metric.Int64Counter
	deniedItems     metric.Int64Counter
	preservedItems  metric.Int64Counter
	backendErrors   metric.Int64Counter
	bucketEvictions metric.Int64Counter
	failOpenItems   metric.Int64Counter

	// Observable gauges for token-bucket state, sampled at scrape time —
	// same idea as the collector's exporter queue_size/queue_capacity. Only
	// observed for allowlisted keys (bounded cardinality); see processor.go.
	bucketCapacity  metric.Float64ObservableGauge
	bucketAvailable metric.Float64ObservableGauge
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
		meter:           metadata.Meter(ts),
		receivedItems:   tb.ProcessorRatelimitReceivedItems,
		allowedItems:    tb.ProcessorRatelimitAllowedItems,
		deniedItems:     tb.ProcessorRatelimitDeniedItems,
		preservedItems:  tb.ProcessorRatelimitPreservedItems,
		backendErrors:   tb.ProcessorRatelimitBackendErrors,
		bucketEvictions: tb.ProcessorRatelimitBucketEvictions,
		failOpenItems:   tb.ProcessorRatelimitFailOpenItems,
		bucketCapacity:  tb.ProcessorRatelimitBucketCapacityTokens,
		bucketAvailable: tb.ProcessorRatelimitBucketAvailableTokens,
	}, nil
}
