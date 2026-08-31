// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package statefulfilterprocessor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// dropLogInterval throttles the "rules dropped data" log line. A drop rule is
// usually installed precisely because something is flooding; logging every
// batch would move the flood from the telemetry pipeline into the collector's
// own logs. Counts stay exact in the metrics.
const dropLogInterval = 30 * time.Second

type dropLogThrottle struct {
	mu   sync.Mutex
	last time.Time
}

func (d *dropLogThrottle) allow() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if !d.last.IsZero() && now.Sub(d.last) < dropLogInterval {
		return false
	}
	d.last = now
	return true
}

// dropTally accumulates per-rule drop counts for one batch, so the metric is
// updated once per rule per batch instead of once per item.
type dropTally map[string]int64

type statefulFilterProcessor struct {
	config    *Config
	store     ruleStore
	logger    *zap.Logger
	telemetry *processorTelemetry
	dropLog   dropLogThrottle

	// gaugeReg is the rule-state callback registration when this processor owns
	// it directly (unshared store, i.e. tests). For shared stores the registry
	// owns it so a config reload can't leak callbacks onto a closed store.
	gaugeReg metric.Registration
}

// gaugeRegistrar is implemented by registry-owned stores: the rule-state gauges
// are registered once per underlying store, not once per signal pipeline.
type gaugeRegistrar interface {
	registerGauges(register func() (metric.Registration, error)) error
}

func newStatefulFilterProcessor(cfg *Config, logger *zap.Logger, mp metric.MeterProvider, store ruleStore) (*statefulFilterProcessor, error) {
	tel, err := newProcessorTelemetry(mp)
	if err != nil {
		return nil, fmt.Errorf("failed to create processor telemetry: %w", err)
	}

	fp := &statefulFilterProcessor{
		config:    cfg,
		store:     store,
		logger:    logger,
		telemetry: tel,
	}

	// Rule-set state is sampled at scrape time rather than pushed on refresh:
	// the values are levels (how many rules, which version, how stale), and a
	// scrape-time read cannot go missing because a refresh was skipped.
	register := func() (metric.Registration, error) {
		return tel.meter.RegisterCallback(
			func(ctx context.Context, o metric.Observer) error {
				st := fp.store.stats()
				o.ObserveInt64(tel.rulesLoaded, int64(st.rulesLoaded))
				o.ObserveInt64(tel.rulesInvalid, int64(st.rulesBad))
				o.ObserveInt64(tel.rulesVersion, st.version)
				o.ObserveInt64(tel.refreshErrors, int64(st.errors))
				age := -1.0 // never loaded: negative is unmistakable on a graph
				if !st.lastSuccess.IsZero() {
					age = time.Since(st.lastSuccess).Seconds()
				}
				o.ObserveFloat64(tel.rulesAge, age)
				return nil
			},
			tel.rulesLoaded, tel.rulesInvalid, tel.rulesVersion, tel.refreshErrors, tel.rulesAge,
		)
	}
	if gr, ok := store.(gaugeRegistrar); ok {
		if err := gr.registerGauges(register); err != nil {
			return nil, fmt.Errorf("failed to register rule gauges: %w", err)
		}
	} else {
		reg, err := register()
		if err != nil {
			return nil, fmt.Errorf("failed to register rule gauges: %w", err)
		}
		fp.gaugeReg = reg
	}

	return fp, nil
}

func (fp *statefulFilterProcessor) start(ctx context.Context, _ component.Host) error {
	if fp.config.WaitForInitialLoad {
		loadCtx, cancel := context.WithTimeout(ctx, fp.config.InitialLoadTimeout)
		defer cancel()
		if err := fp.store.initialLoad(loadCtx); err != nil {
			if fp.config.FailClosedOnEmpty {
				return fmt.Errorf("initial filter-rule load failed and fail_closed_on_empty is set: %w", err)
			}
			// Fail-open: start anyway with no rules and keep retrying in the
			// background. Refusing to start would take telemetry ingestion down
			// for the whole fleet because of a rule-store outage.
			fp.logger.Warn("Initial filter-rule load failed; starting with no rules and retrying in background",
				zap.Duration("retry_interval", fp.config.RefreshInterval),
				zap.Error(err),
			)
		}
	}
	fp.store.start()

	st := fp.store.stats()
	fp.logger.Info("Stateful filter processor started",
		zap.Int("rules", st.rulesLoaded),
		zap.Int("invalid_rules", st.rulesBad),
		zap.Int64("version", st.version),
		zap.Duration("refresh_interval", fp.config.RefreshInterval),
	)
	return nil
}

func (fp *statefulFilterProcessor) shutdown(context.Context) error {
	if fp.gaugeReg != nil {
		if err := fp.gaugeReg.Unregister(); err != nil {
			fp.logger.Warn("gauge callback unregister failed", zap.Error(err))
		}
		fp.gaugeReg = nil
	}
	if err := fp.store.close(); err != nil {
		fp.logger.Warn("rule store close failed", zap.Error(err))
	}
	fp.logger.Info("Stateful filter processor stopped")
	return nil
}

// report publishes the per-batch counters and emits the throttled summary log.
func (fp *statefulFilterProcessor) report(ctx context.Context, signal string, evaluated int64, dropped dropTally, kept int64) {
	if evaluated > 0 {
		fp.telemetry.evaluatedItems.Add(ctx, evaluated,
			metric.WithAttributes(attribute.String("signal", signal)))
	}
	if kept > 0 {
		fp.telemetry.keptItems.Add(ctx, kept,
			metric.WithAttributes(attribute.String("signal", signal)))
	}
	if len(dropped) == 0 {
		return
	}
	var total int64
	for rule, n := range dropped {
		total += n
		// `rule` is operator-authored and bounded by max_rules, so it is safe
		// as a label — unlike anything derived from the telemetry itself.
		fp.telemetry.droppedItems.Add(ctx, n, metric.WithAttributes(
			attribute.String("signal", signal),
			attribute.String("rule", rule),
		))
	}
	if fp.dropLog.allow() {
		fp.logger.Info("Stateful filter rules dropped telemetry",
			zap.String("signal", signal),
			zap.Int64("dropped", total),
			zap.Int64("evaluated", evaluated),
			zap.Int("rules_matched", len(dropped)),
			zap.Int64("rule_version", fp.store.stats().version),
		)
	}
}

// Traces

type tracesProcessor struct {
	*statefulFilterProcessor
	nextConsumer consumer.Traces
}

func newTracesProcessor(cfg *Config, logger *zap.Logger, mp metric.MeterProvider, store ruleStore, next consumer.Traces) (*tracesProcessor, error) {
	fp, err := newStatefulFilterProcessor(cfg, logger, mp, store)
	if err != nil {
		return nil, err
	}
	return &tracesProcessor{statefulFilterProcessor: fp, nextConsumer: next}, nil
}

func (tp *tracesProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (tp *tracesProcessor) Start(ctx context.Context, host component.Host) error {
	return tp.start(ctx, host)
}

func (tp *tracesProcessor) Shutdown(ctx context.Context) error { return tp.shutdown(ctx) }

func (tp *tracesProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	rs := tp.store.current()
	if len(rs.traces) == 0 {
		return tp.nextConsumer.ConsumeTraces(ctx, td)
	}

	now := time.Now()
	dropped := dropTally{}
	var evaluated, kept int64

	td.ResourceSpans().RemoveIf(func(rspans ptrace.ResourceSpans) bool {
		resAttrs := rspans.Resource().Attributes()
		rspans.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			scope := ss.Scope().Name()
			ss.Spans().RemoveIf(func(sp ptrace.Span) bool {
				evaluated++
				e := evalCtx{
					resource: resAttrs,
					attrs:    sp.Attributes(),
					scope:    scope,
					name:     sp.Name(),
				}
				r := rs.match(signalTraces, &e, now)
				if r == nil {
					return false
				}
				if r.action == actionKeep {
					kept++
					return false
				}
				dropped[r.id]++
				return true
			})
			return ss.Spans().Len() == 0
		})
		return rspans.ScopeSpans().Len() == 0
	})

	tp.report(ctx, signalTraces, evaluated, dropped, kept)

	// Everything matched a drop rule: accept and discard, rather than forward
	// an empty batch (which some exporters still turn into a request) or return
	// an error (which would make well-behaved SDKs retry data we mean to drop).
	if td.ResourceSpans().Len() == 0 {
		return nil
	}
	return tp.nextConsumer.ConsumeTraces(ctx, td)
}

// Logs

type logsProcessor struct {
	*statefulFilterProcessor
	nextConsumer consumer.Logs
}

func newLogsProcessor(cfg *Config, logger *zap.Logger, mp metric.MeterProvider, store ruleStore, next consumer.Logs) (*logsProcessor, error) {
	fp, err := newStatefulFilterProcessor(cfg, logger, mp, store)
	if err != nil {
		return nil, err
	}
	return &logsProcessor{statefulFilterProcessor: fp, nextConsumer: next}, nil
}

func (lp *logsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (lp *logsProcessor) Start(ctx context.Context, host component.Host) error {
	return lp.start(ctx, host)
}

func (lp *logsProcessor) Shutdown(ctx context.Context) error { return lp.shutdown(ctx) }

func (lp *logsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	rs := lp.store.current()
	if len(rs.logs) == 0 {
		return lp.nextConsumer.ConsumeLogs(ctx, ld)
	}

	now := time.Now()
	dropped := dropTally{}
	var evaluated, kept int64

	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		resAttrs := rl.Resource().Attributes()
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			scope := sl.Scope().Name()
			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				evaluated++
				e := evalCtx{
					resource: resAttrs,
					attrs:    lr.Attributes(),
					scope:    scope,
					severity: lr.SeverityText(),
				}
				// Stringifying a body is not free, so only pay for it when a
				// loaded rule actually reads it.
				if rs.needsBody {
					e.body = lr.Body().AsString()
				}
				r := rs.match(signalLogs, &e, now)
				if r == nil {
					return false
				}
				if r.action == actionKeep {
					kept++
					return false
				}
				dropped[r.id]++
				return true
			})
			return sl.LogRecords().Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})

	lp.report(ctx, signalLogs, evaluated, dropped, kept)

	if ld.ResourceLogs().Len() == 0 {
		return nil
	}
	return lp.nextConsumer.ConsumeLogs(ctx, ld)
}

// Metrics

type metricsProcessor struct {
	*statefulFilterProcessor
	nextConsumer consumer.Metrics
}

func newMetricsProcessor(cfg *Config, logger *zap.Logger, mp metric.MeterProvider, store ruleStore, next consumer.Metrics) (*metricsProcessor, error) {
	fp, err := newStatefulFilterProcessor(cfg, logger, mp, store)
	if err != nil {
		return nil, err
	}
	return &metricsProcessor{statefulFilterProcessor: fp, nextConsumer: next}, nil
}

func (mp *metricsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (mp *metricsProcessor) Start(ctx context.Context, host component.Host) error {
	return mp.start(ctx, host)
}

func (mp *metricsProcessor) Shutdown(ctx context.Context) error { return mp.shutdown(ctx) }

func (mp *metricsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	rs := mp.store.current()
	if len(rs.metrics) == 0 {
		return mp.nextConsumer.ConsumeMetrics(ctx, md)
	}

	now := time.Now()
	dropped := dropTally{}
	var evaluated, kept int64

	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		resAttrs := rm.Resource().Attributes()
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			scope := sm.Scope().Name()
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				// Filtering per data point (not per metric) is what makes
				// attribute-scoped rules work: "drop the datapoints tagged
				// http.route=/healthz" must not take the whole series with it.
				keep := func(attrs pcommon.Map) bool {
					evaluated++
					e := evalCtx{resource: resAttrs, attrs: attrs, scope: scope, name: m.Name()}
					r := rs.match(signalMetrics, &e, now)
					if r == nil {
						return true
					}
					if r.action == actionKeep {
						kept++
						return true
					}
					dropped[r.id]++
					return false
				}
				return removeDataPoints(m, keep)
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	mp.report(ctx, signalMetrics, evaluated, dropped, kept)

	if md.ResourceMetrics().Len() == 0 {
		return nil
	}
	return mp.nextConsumer.ConsumeMetrics(ctx, md)
}

// removeDataPoints applies `keep` to every data point of m and reports whether
// the metric is now empty (and should itself be removed). Metric types are
// enumerated explicitly because pdata has no generic datapoint accessor.
func removeDataPoints(m pmetric.Metric, keep func(pcommon.Map) bool) bool {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		dps.RemoveIf(func(dp pmetric.NumberDataPoint) bool { return !keep(dp.Attributes()) })
		return dps.Len() == 0
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		dps.RemoveIf(func(dp pmetric.NumberDataPoint) bool { return !keep(dp.Attributes()) })
		return dps.Len() == 0
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		dps.RemoveIf(func(dp pmetric.HistogramDataPoint) bool { return !keep(dp.Attributes()) })
		return dps.Len() == 0
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		dps.RemoveIf(func(dp pmetric.ExponentialHistogramDataPoint) bool { return !keep(dp.Attributes()) })
		return dps.Len() == 0
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		dps.RemoveIf(func(dp pmetric.SummaryDataPoint) bool { return !keep(dp.Attributes()) })
		return dps.Len() == 0
	}
	// Unknown/empty metric type: nothing to evaluate, leave it alone.
	return false
}
