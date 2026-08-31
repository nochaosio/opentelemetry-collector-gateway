package ratelimitprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

const (
	typeStr   = "ratelimit"
	stability = component.StabilityLevelBeta
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, stability),
		processor.WithMetrics(createMetricsProcessor, stability),
		processor.WithLogs(createLogsProcessor, stability),
	)
}

// acquireStorage validates the config and returns the shared storage handle
// for this component ID. Traces/metrics/logs pipelines using the same
// `ratelimit/<name>` instance share one backend, so a key's budget is a
// single bucket across signals — identical semantics for memory and Redis.
func acquireStorage(set processor.Settings, cfg component.Config) (*Config, Storage, error) {
	oCfg := cfg.(*Config)
	if err := oCfg.Validate(); err != nil {
		return nil, nil, err
	}
	oCfg.applyStorageDefaults()

	storage, err := defaultStorageRegistry.acquire(set.ID, oCfg, set.Logger)
	if err != nil {
		return nil, nil, err
	}
	return oCfg, storage, nil
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	oCfg, storage, err := acquireStorage(set, cfg)
	if err != nil {
		return nil, err
	}
	return newTracesProcessor(oCfg, set.Logger, set.MeterProvider, storage, nextConsumer)
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	oCfg, storage, err := acquireStorage(set, cfg)
	if err != nil {
		return nil, err
	}
	return newMetricsProcessor(oCfg, set.Logger, set.MeterProvider, storage, nextConsumer)
}

func createLogsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	oCfg, storage, err := acquireStorage(set, cfg)
	if err != nil {
		return nil, err
	}
	return newLogsProcessor(oCfg, set.Logger, set.MeterProvider, storage, nextConsumer)
}
