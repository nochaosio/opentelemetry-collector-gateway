// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/nochaosio/opentelemetry-collector-gateway/processor/ratelimitprocessor/internal/metadata"
)

func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
		processor.WithLogs(createLogsProcessor, metadata.LogsStability),
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
	p, err := newTracesProcessor(oCfg, set.Logger, set.MeterProvider, storage, nextConsumer)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewTraces(ctx, set, cfg, nextConsumer, p.processTraces,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(p.start),
		processorhelper.WithShutdown(p.shutdown),
	)
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
	p, err := newMetricsProcessor(oCfg, set.Logger, set.MeterProvider, storage, nextConsumer)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewMetrics(ctx, set, cfg, nextConsumer, p.processMetrics,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(p.start),
		processorhelper.WithShutdown(p.shutdown),
	)
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
	p, err := newLogsProcessor(oCfg, set.Logger, set.MeterProvider, storage, nextConsumer)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewLogs(ctx, set, cfg, nextConsumer, p.processLogs,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(p.start),
		processorhelper.WithShutdown(p.shutdown),
	)
}
