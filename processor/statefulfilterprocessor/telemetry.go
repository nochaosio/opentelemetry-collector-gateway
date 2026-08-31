package statefulfilterprocessor

import (
	"go.opentelemetry.io/otel/metric"
)

const meterName = "github.com/nochaosio/opentelemetry-collector-gateway/processor/statefulfilterprocessor"

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

func newProcessorTelemetry(mp metric.MeterProvider) (*processorTelemetry, error) {
	meter := mp.Meter(meterName)

	evaluatedItems, err := meter.Int64Counter(
		"otelcol_processor_statefulfilter_evaluated_items",
		metric.WithDescription("Items (spans, log records, metric data points) evaluated against the Redis rule set"),
		metric.WithUnit("{item}"),
	)
	if err != nil {
		return nil, err
	}

	droppedItems, err := meter.Int64Counter(
		"otelcol_processor_statefulfilter_dropped_items",
		metric.WithDescription("Items dropped by a shared filter rule, labelled by the rule that matched"),
		metric.WithUnit("{item}"),
	)
	if err != nil {
		return nil, err
	}

	keptItems, err := meter.Int64Counter(
		"otelcol_processor_statefulfilter_kept_items",
		metric.WithDescription("Items explicitly rescued by a keep rule that would otherwise have matched a drop rule"),
		metric.WithUnit("{item}"),
	)
	if err != nil {
		return nil, err
	}

	// The refresh-error total is exported as a gauge rather than a counter
	// because the store owns the running value; the callback just reads it.
	refreshErrors, err := meter.Int64ObservableGauge(
		"otelcol_processor_statefulfilter_refresh_errors",
		metric.WithDescription("Total failed rule refreshes since start (Redis unreachable, timeout); rules go stale, not empty"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	rulesLoaded, err := meter.Int64ObservableGauge(
		"otelcol_processor_statefulfilter_rules_loaded",
		metric.WithDescription("Filter rules currently active on this replica"),
		metric.WithUnit("{rule}"),
	)
	if err != nil {
		return nil, err
	}

	rulesInvalid, err := meter.Int64ObservableGauge(
		"otelcol_processor_statefulfilter_rules_invalid",
		metric.WithDescription("Rule documents rejected at the last refresh (bad JSON, unknown operator, bad regex)"),
		metric.WithUnit("{rule}"),
	)
	if err != nil {
		return nil, err
	}

	// Version is the fleet-convergence signal: alert on
	// max(version) != min(version) across replicas and you catch a collector
	// that stopped seeing rule updates.
	rulesVersion, err := meter.Int64ObservableGauge(
		"otelcol_processor_statefulfilter_rules_version",
		metric.WithDescription("Rule-set version this replica has applied; diverging values across replicas mean the fleet is out of sync"),
		metric.WithUnit("{version}"),
	)
	if err != nil {
		return nil, err
	}

	rulesAge, err := meter.Float64ObservableGauge(
		"otelcol_processor_statefulfilter_rules_age_seconds",
		metric.WithDescription("Seconds since the last successful rule refresh; climbing means this replica is enforcing stale rules"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	return &processorTelemetry{
		meter:          meter,
		evaluatedItems: evaluatedItems,
		droppedItems:   droppedItems,
		keptItems:      keptItems,
		refreshErrors:  refreshErrors,
		rulesLoaded:    rulesLoaded,
		rulesInvalid:   rulesInvalid,
		rulesVersion:   rulesVersion,
		rulesAge:       rulesAge,
	}, nil
}
