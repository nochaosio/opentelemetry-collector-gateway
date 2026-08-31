package ratelimitprocessor

import (
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/nochaosio/opentelemetry-collector-gateway/processor/ratelimitprocessor"

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

func newProcessorTelemetry(mp metric.MeterProvider) (*processorTelemetry, error) {
	meter := mp.Meter(meterName)

	receivedItems, err := meter.Int64Counter(
		"otelcol_processor_ratelimit_received_items",
		metric.WithDescription("Number of items received by the rate limit processor"),
		metric.WithUnit("{item}"),
	)
	if err != nil {
		return nil, err
	}

	allowedItems, err := meter.Int64Counter(
		"otelcol_processor_ratelimit_allowed_items",
		metric.WithDescription("Number of items allowed through by the rate limit processor"),
		metric.WithUnit("{item}"),
	)
	if err != nil {
		return nil, err
	}

	deniedItems, err := meter.Int64Counter(
		"otelcol_processor_ratelimit_denied_items",
		metric.WithDescription("Number of items denied by the rate limit processor"),
		metric.WithUnit("{item}"),
	)
	if err != nil {
		return nil, err
	}

	preservedItems, err := meter.Int64Counter(
		"otelcol_processor_ratelimit_preserved_items",
		metric.WithDescription("Number of critical items preserved via priority bypass (error spans, error+ logs)"),
		metric.WithUnit("{item}"),
	)
	if err != nil {
		return nil, err
	}

	backendErrors, err := meter.Int64Counter(
		"otelcol_processor_ratelimit_backend_errors",
		metric.WithDescription("Transient errors from the rate-limit storage backend (e.g. Redis unreachable while fail-open)"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	bucketEvictions, err := meter.Int64Counter(
		"otelcol_processor_ratelimit_bucket_evictions",
		metric.WithDescription("Buckets evicted because the memory backend hit its max_buckets cap"),
		metric.WithUnit("{bucket}"),
	)
	if err != nil {
		return nil, err
	}

	failOpenItems, err := meter.Int64Counter(
		"otelcol_processor_ratelimit_fail_open_items",
		metric.WithDescription("Items allowed through because the storage backend was unreachable (fail-open) and thus NOT rate-limited"),
		metric.WithUnit("{item}"),
	)
	if err != nil {
		return nil, err
	}

	bucketCapacity, err := meter.Float64ObservableGauge(
		"otelcol_processor_ratelimit_bucket_capacity_tokens",
		metric.WithDescription("Token-bucket capacity (max tokens == configured limit) for watched keys"),
		metric.WithUnit("{token}"),
	)
	if err != nil {
		return nil, err
	}

	bucketAvailable, err := meter.Float64ObservableGauge(
		"otelcol_processor_ratelimit_bucket_available_tokens",
		metric.WithDescription("Token-bucket tokens currently available (free) for watched keys; occupied = capacity - available"),
		metric.WithUnit("{token}"),
	)
	if err != nil {
		return nil, err
	}

	return &processorTelemetry{
		meter:           meter,
		receivedItems:   receivedItems,
		allowedItems:    allowedItems,
		deniedItems:     deniedItems,
		preservedItems:  preservedItems,
		backendErrors:   backendErrors,
		bucketEvictions: bucketEvictions,
		failOpenItems:   failOpenItems,
		bucketCapacity:  bucketCapacity,
		bucketAvailable: bucketAvailable,
	}, nil
}
