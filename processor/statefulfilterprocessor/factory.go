// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package statefulfilterprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/nochaosio/opentelemetry-collector-gateway/processor/statefulfilterprocessor/internal/metadata"
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

// acquireStore validates the config and returns the shared rule store for this
// component ID, so traces/metrics/logs pipelines wired to the same
// `statefulfilter/<name>` instance poll Redis once and always agree on the
// active rule version.
func acquireStore(set processor.Settings, cfg component.Config) (*Config, ruleStore, error) {
	oCfg := cfg.(*Config)
	if err := oCfg.Validate(); err != nil {
		return nil, nil, err
	}
	oCfg.applyDefaults()

	store, err := defaultStoreRegistry.acquire(set.ID, func() (ruleStore, error) {
		return newRedisRuleStore(oCfg, set.Logger)
	})
	if err != nil {
		return nil, nil, err
	}
	return oCfg, store, nil
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	oCfg, store, err := acquireStore(set, cfg)
	if err != nil {
		return nil, err
	}
	p, err := newTracesProcessor(oCfg, set.Logger, set.MeterProvider, store, nextConsumer)
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
	oCfg, store, err := acquireStore(set, cfg)
	if err != nil {
		return nil, err
	}
	p, err := newMetricsProcessor(oCfg, set.Logger, set.MeterProvider, store, nextConsumer)
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
	oCfg, store, err := acquireStore(set, cfg)
	if err != nil {
		return nil, err
	}
	p, err := newLogsProcessor(oCfg, set.Logger, set.MeterProvider, store, nextConsumer)
	if err != nil {
		return nil, err
	}
	return processorhelper.NewLogs(ctx, set, cfg, nextConsumer, p.processLogs,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(p.start),
		processorhelper.WithShutdown(p.shutdown),
	)
}
