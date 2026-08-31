// Copyright The NOCHAOS Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimitprocessor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Counter names used by key_labels_on / metricAttrs to decide, per counter,
// whether the high-cardinality `key` label is attached.
const (
	counterReceived  = "received"
	counterAllowed   = "allowed"
	counterDenied    = "denied"
	counterPreserved = "preserved"
)

// dropLogInterval caps how often the per-batch "rate limit exceeded" warning is
// emitted. Without this, a sustained overload — exactly when the limiter kicks
// in — would log on every batch and flood the log pipeline. The drop counts
// remain exact in the metrics; only the log line is throttled.
const dropLogInterval = 10 * time.Second

// dropLogThrottle emits at most one warning per dropLogInterval and reports how
// many were suppressed in between.
type dropLogThrottle struct {
	mu         sync.Mutex
	last       time.Time
	suppressed int64
}

func (d *dropLogThrottle) next() (suppressed int64, emit bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if !d.last.IsZero() && now.Sub(d.last) < dropLogInterval {
		d.suppressed++
		return 0, false
	}
	suppressed, d.suppressed = d.suppressed, 0
	d.last = now
	return suppressed, true
}

// gaugeRegistrar is implemented by storages whose lifetime is shared across
// pipelines (sharedStorage): the bucket-gauge callback is registered once per
// underlying backend and unregistered when the last pipeline releases it.
type gaugeRegistrar interface {
	registerGauges(register func() (metric.Registration, error)) error
}

type rateLimitProcessor struct {
	config      *Config
	rateLimiter *RateLimiter
	storage     Storage
	logger      *zap.Logger
	telemetry   *processorTelemetry

	// metricKeyAllow bounds metric-label cardinality. When non-nil, only keys
	// present in this set keep their value in the `key` metric label; all
	// others are reported as "other". Built once from
	// Config.MetricsKeyAllowlist — never used for rate limiting.
	metricKeyAllow map[string]struct{}

	// seenMetricKeys is the always-on cardinality backstop used when no
	// allowlist is configured: the first maxMetricKeys distinct keys keep
	// their value, later ones collapse to "other". nil = unlimited
	// (max_metric_keys < 0).
	seenMetricKeys map[string]struct{}
	seenMu         sync.Mutex
	maxMetricKeys  int

	// keyLabelOn decides, per counter, whether the `key` label is attached.
	// Counters absent from this set are emitted aggregated (no `key`), which is
	// the main guard against unbounded active-series growth. Built once from
	// Config.KeyLabelsOn (default: denied + preserved).
	keyLabelOn map[string]bool

	// gaugeReg is the bucket-gauge callback registration when this processor
	// owns it directly (non-shared storage, e.g. in tests). Unregistered on
	// shutdown so a config reload can't leave a callback observing a closed
	// storage. For shared storages the registry owns the registration instead.
	gaugeReg metric.Registration

	// dropLog throttles the per-batch "rate limit exceeded" warning.
	dropLog dropLogThrottle
}

func newRateLimitProcessor(config *Config, logger *zap.Logger, mp metric.MeterProvider, storage Storage) (*rateLimitProcessor, error) {
	tel, err := newProcessorTelemetry(mp)
	if err != nil {
		return nil, fmt.Errorf("failed to create processor telemetry: %w", err)
	}

	var metricKeyAllow map[string]struct{}
	if len(config.MetricsKeyAllowlist) > 0 {
		metricKeyAllow = make(map[string]struct{}, len(config.MetricsKeyAllowlist))
		for _, k := range config.MetricsKeyAllowlist {
			metricKeyAllow[k] = struct{}{}
		}
	}

	rp := &rateLimitProcessor{
		config:         config,
		rateLimiter:    newRateLimiterWithStorage(config, storage),
		storage:        storage,
		logger:         logger,
		telemetry:      tel,
		metricKeyAllow: metricKeyAllow,
		keyLabelOn:     config.keyLabelSet(),
	}

	// Cardinality backstop for the `key` label when no allowlist bounds it.
	if metricKeyAllow == nil {
		maxKeys := config.MaxMetricKeys
		if maxKeys == 0 {
			maxKeys = defaultMaxMetricKeys
		}
		if maxKeys > 0 {
			rp.maxMetricKeys = maxKeys
			rp.seenMetricKeys = make(map[string]struct{})
		}
	}

	// Export storage backend errors (Redis fail-open, etc.) and bucket evictions
	// (memory cap reached) as metrics.
	storage.SetErrorCounter(tel.backendErrors)
	storage.SetEvictionCounter(tel.bucketEvictions)
	storage.SetFailOpenCounter(tel.failOpenItems)

	// Token-bucket gauges (capacity vs free) are sampled at scrape time, like
	// the collector's exporter queue_size/queue_capacity. They are observed
	// ONLY for allowlisted keys, so per-key bucket state never reintroduces
	// the cardinality problem the allowlist exists to prevent. Without an
	// allowlist there is no bounded "watch set", so the gauges stay off.
	// metrics_verbosity: basic turns them off entirely (they carry `key`).
	if len(rp.metricKeyAllow) > 0 && !config.metricsBasic() {
		register := func() (metric.Registration, error) {
			return tel.meter.RegisterCallback(
				func(ctx context.Context, o metric.Observer) error {
					for key := range rp.metricKeyAllow {
						capacity, _ := rp.config.GetLimit(key)
						capF := float64(capacity)
						free := capF // no traffic yet => bucket idle/full
						if v, ok := rp.storage.AvailableTokens(ctx, key); ok {
							free = v
						}
						attr := metric.WithAttributes(
							attribute.String("key", key),
							attribute.String("limit_type", rp.config.LimitType),
						)
						o.ObserveFloat64(tel.bucketCapacity, capF, attr)
						o.ObserveFloat64(tel.bucketAvailable, free, attr)
					}
					return nil
				},
				tel.bucketCapacity, tel.bucketAvailable,
			)
		}
		if gr, ok := storage.(gaugeRegistrar); ok {
			// Shared storage: register once across all signal pipelines; the
			// registry unregisters when the last pipeline shuts down.
			if err := gr.registerGauges(register); err != nil {
				return nil, fmt.Errorf("failed to register bucket gauges: %w", err)
			}
		} else {
			reg, err := register()
			if err != nil {
				return nil, fmt.Errorf("failed to register bucket gauges: %w", err)
			}
			rp.gaugeReg = reg
		}
	}

	return rp, nil
}

func (rp *rateLimitProcessor) start(ctx context.Context, host component.Host) error {
	rp.logger.Info("Rate limit processor started", zap.String("storage", rp.storage.Name()))
	return nil
}

func (rp *rateLimitProcessor) shutdown(ctx context.Context) error {
	// Unregister the gauge callback before closing the storage so a metrics
	// scrape can't observe a closed backend, and so a config reload doesn't
	// accumulate dead callbacks.
	if rp.gaugeReg != nil {
		if err := rp.gaugeReg.Unregister(); err != nil {
			rp.logger.Warn("gauge callback unregister failed", zap.Error(err))
		}
		rp.gaugeReg = nil
	}
	if err := rp.storage.Close(); err != nil {
		rp.logger.Warn("storage close failed", zap.Error(err))
	}
	rp.logger.Info("Rate limit processor stopped")
	return nil
}

// extractKey returns the rate-limit key for the current request. Attribute and
// service_name modes come from the resource; header mode reads from the gRPC
// metadata / HTTP headers carried in client.Info (case-insensitive lookup —
// gRPC normalizes metadata keys to lower case). NOTE: header mode requires
// `include_metadata: true` on the OTLP receiver, otherwise client.Info carries
// no metadata and every request falls into the "default" bucket.
func (rp *rateLimitProcessor) extractKey(ctx context.Context, attrs map[string]string) string {
	switch rp.config.LimitType {
	case "attribute":
		if val, ok := attrs[rp.config.AttributeKey]; ok {
			return val
		}
	case "header":
		ci := client.FromContext(ctx)
		want := strings.ToLower(rp.config.HeaderKey)
		if vals := ci.Metadata.Get(want); len(vals) > 0 && vals[0] != "" {
			return vals[0]
		}
	case "service_name":
		if val, ok := attrs["service.name"]; ok {
			return val
		}
	}
	return "default"
}

// keyForResource resolves the rate-limit key for one resource entry without
// materializing the full attribute map. Header mode is per-request (same key
// for every resource in the batch).
func (rp *rateLimitProcessor) keyForResource(ctx context.Context, res pcommon.Resource) string {
	switch rp.config.LimitType {
	case "attribute":
		if v, ok := res.Attributes().Get(rp.config.AttributeKey); ok {
			return v.AsString()
		}
	case "service_name":
		if v, ok := res.Attributes().Get("service.name"); ok {
			return v.AsString()
		}
	case "header":
		return rp.extractKey(ctx, nil)
	}
	return "default"
}

// metricKey maps a rate-limit key to the value used in the `key` metric label.
// With an allowlist configured, only listed keys survive and everything else
// collapses to "other". Without an allowlist, the first max_metric_keys
// distinct keys keep their value (first-come, stable series) and later ones
// collapse to "other" — the backstop that keeps a producer-controlled key
// space from becoming unbounded label cardinality.
func (rp *rateLimitProcessor) metricKey(key string) string {
	if rp.metricKeyAllow != nil {
		if _, ok := rp.metricKeyAllow[key]; ok {
			return key
		}
		return "other"
	}
	if rp.seenMetricKeys == nil {
		return key // max_metric_keys < 0: explicitly unlimited
	}
	rp.seenMu.Lock()
	defer rp.seenMu.Unlock()
	if _, ok := rp.seenMetricKeys[key]; ok {
		return key
	}
	if len(rp.seenMetricKeys) < rp.maxMetricKeys {
		rp.seenMetricKeys[key] = struct{}{}
		return key
	}
	return "other"
}

// metricAttrs builds the attribute set for a counter measurement. The `key`
// label is attached only for counters enabled via key_labels_on; the rest are
// emitted aggregated (signal + priority only) so high key counts don't blow up
// the active-series count. Where present, `key` is still routed through
// metricKey so the allowlist / backstop can collapse it to "other".
func (rp *rateLimitProcessor) metricAttrs(counter, key, signal, priority string) metric.MeasurementOption {
	// limit_type is low cardinality (service_name | attribute | header) and lets
	// dashboards separate per-service.name series from per-tenant/per-client ones
	// when several rate-limit processors share these instrument names.
	if rp.keyLabelOn[counter] {
		return metric.WithAttributes(
			attribute.String("key", rp.metricKey(key)),
			attribute.String("signal", signal),
			attribute.String("priority", priority),
			attribute.String("limit_type", rp.config.LimitType),
		)
	}
	return metric.WithAttributes(
		attribute.String("signal", signal),
		attribute.String("priority", priority),
		attribute.String("limit_type", rp.config.LimitType),
	)
}

// limitExceededError builds the rejection error returned when an entire batch
// is dropped. In throttle mode (default) it is a gRPC RESOURCE_EXHAUSTED
// status with RetryInfo — the OTLP receiver propagates the gRPC code as-is
// and maps it to HTTP 429, which is the throttling signal the OTLP spec
// prescribes; SDK exporters back off and retry. In permanent mode it is a
// collector-permanent error (mapped to ~400) so compliant clients drop the
// batch instead of retrying.
func (rp *rateLimitProcessor) limitExceededError(key string, denied int) error {
	msg := fmt.Sprintf("rate limit exceeded: key=%s, limit_type=%s, dropped=%d", key, rp.config.LimitType, denied)
	if rp.config.RejectionMode == rejectionPermanent {
		return consumererror.NewPermanent(errors.New(msg))
	}

	// Suggest a retry delay long enough for the bucket to refill what was
	// denied, clamped to a sane window so clients neither hammer nor stall.
	limit, period := rp.config.GetLimit(key)
	delay := time.Second
	if limit > 0 {
		rate := float64(limit) / period.Seconds()
		delay = time.Duration(float64(denied) / rate * float64(time.Second))
	}
	if delay < 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}
	if delay > 30*time.Second {
		delay = 30 * time.Second
	}
	st := status.New(codes.ResourceExhausted, msg)
	if detailed, err := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(delay)}); err == nil {
		st = detailed
	}
	return st.Err()
}

// keyTally accumulates per-key item counts for one batch.
type keyTally struct {
	key      string
	critical int
	normal   int
}

// Traces Processor
type tracesProcessor struct {
	*rateLimitProcessor
	nextConsumer consumer.Traces
}

func newTracesProcessor(config *Config, logger *zap.Logger, mp metric.MeterProvider, storage Storage, nextConsumer consumer.Traces) (*tracesProcessor, error) {
	rp, err := newRateLimitProcessor(config, logger, mp, storage)
	if err != nil {
		return nil, err
	}
	return &tracesProcessor{
		rateLimitProcessor: rp,
		nextConsumer:       nextConsumer,
	}, nil
}

// ConsumeTraces runs processTraces and forwards the result, mirroring what
// processorhelper does around the same function in the factory path. Tests
// build the processor directly and go through here.
func (tp *tracesProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	out, err := tp.processTraces(ctx, td)
	if err != nil {
		return err
	}
	return tp.nextConsumer.ConsumeTraces(ctx, out)
}

// isCriticalSpan flags spans we always want to keep when PreserveErrors is on.
func isCriticalSpan(sp ptrace.Span) bool {
	return sp.Status().Code() == ptrace.StatusCodeError
}

// forEachSpan visits every span in the batch.
func forEachSpan(td ptrace.Traces, fn func(ptrace.Span)) {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		forEachSpanInResource(td.ResourceSpans().At(i), fn)
	}
}

func forEachSpanInResource(rs ptrace.ResourceSpans, fn func(ptrace.Span)) {
	for j := 0; j < rs.ScopeSpans().Len(); j++ {
		ss := rs.ScopeSpans().At(j)
		for k := 0; k < ss.Spans().Len(); k++ {
			fn(ss.Spans().At(k))
		}
	}
}

// tracesKeyGroups walks the batch once, resolving the rate-limit key of every
// resource entry and tallying critical/normal spans per key. Multi-tenant
// batches (several resources with different keys) are billed per key instead
// of all to the first resource's key. Returns the per-resource key slice
// (index-aligned with ResourceSpans) and the per-key tallies in first-seen
// order.
func (tp *tracesProcessor) tracesKeyGroups(ctx context.Context, td ptrace.Traces) (resKeys []string, groups []*keyTally) {
	rss := td.ResourceSpans()
	resKeys = make([]string, rss.Len())
	byKey := make(map[string]*keyTally)
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		key := tp.keyForResource(ctx, rs.Resource())
		resKeys[i] = key
		t := byKey[key]
		if t == nil {
			t = &keyTally{key: key}
			byKey[key] = t
			groups = append(groups, t)
		}
		forEachSpanInResource(rs, func(sp ptrace.Span) {
			if tp.config.PreserveErrors && isCriticalSpan(sp) {
				t.critical++
			} else {
				t.normal++
			}
		})
	}
	return resKeys, groups
}

// dropNormalSpansByKey removes, per key, exactly excessByKey[key] non-critical
// spans from the resources billed to that key. Critical spans are never
// touched. Empty scope/resource containers are cleaned up so the downstream
// sees a well-formed pdata.
func dropNormalSpansByKey(td ptrace.Traces, resKeys []string, excessByKey map[string]int, preserveErrors bool) {
	idx := -1
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		idx++
		key := resKeys[idx]
		if excessByKey[key] <= 0 {
			return false
		}
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(sp ptrace.Span) bool {
				if excessByKey[key] <= 0 {
					return false
				}
				if preserveErrors && isCriticalSpan(sp) {
					return false
				}
				excessByKey[key]--
				return true
			})
			return ss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})
}

// traceTally is the normal-span count for one trace_id (trace-aware mode).
type traceTally struct {
	id    pcommon.TraceID
	spans int
}

// analyzeTracesIn groups the spans of the given resource entries by trace_id.
// A trace is critical when preserveErrors is on and it has at least one error
// span; critical traces are kept whole and counted as preserved. Returns the
// preserved-span count, the total normal-span count, and the per-trace normal
// counts in first-seen order.
func analyzeTracesIn(td ptrace.Traces, resIdx []int, preserveErrors bool) (preserved, normal int, normalTraces []traceTally) {
	criticalTrace := make(map[pcommon.TraceID]bool)
	if preserveErrors {
		for _, i := range resIdx {
			forEachSpanInResource(td.ResourceSpans().At(i), func(sp ptrace.Span) {
				if isCriticalSpan(sp) {
					criticalTrace[sp.TraceID()] = true
				}
			})
		}
	}
	pos := make(map[pcommon.TraceID]int)
	for _, i := range resIdx {
		forEachSpanInResource(td.ResourceSpans().At(i), func(sp ptrace.Span) {
			tid := sp.TraceID()
			if criticalTrace[tid] {
				preserved++
				return
			}
			normal++
			if p, ok := pos[tid]; ok {
				normalTraces[p].spans++
			} else {
				pos[tid] = len(normalTraces)
				normalTraces = append(normalTraces, traceTally{id: tid, spans: 1})
			}
		})
	}
	return
}

// planTraceDrops keeps whole normal traces while the running kept-span count
// stays within allowed; the rest are dropped whole. Returns the drop set and the
// number of normal spans dropped. Keeping is conservative: it never exceeds the
// granted budget, so a trace that doesn't fit is dropped rather than split.
func planTraceDrops(normalTraces []traceTally, allowed int) (drop map[pcommon.TraceID]bool, dropped int) {
	drop = make(map[pcommon.TraceID]bool)
	kept := 0
	for _, t := range normalTraces {
		if kept+t.spans <= allowed {
			kept += t.spans
		} else {
			drop[t.id] = true
			dropped += t.spans
		}
	}
	return
}

// dropTracesByID removes every span whose trace_id is in drop, cleaning empty
// containers. Whole traces go together, so no trace is left partial.
func dropTracesByID(td ptrace.Traces, drop map[pcommon.TraceID]bool) {
	if len(drop) == 0 {
		return
	}
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(sp ptrace.Span) bool {
				return drop[sp.TraceID()]
			})
			return ss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})
}

// consumeTracesByTrace is the all-or-nothing-per-trace path (TraceIDAware),
// applied per rate-limit key.
func (tp *tracesProcessor) processTracesByTrace(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	rss := td.ResourceSpans()
	if rss.Len() == 0 {
		return td, nil
	}

	resByKey := make(map[string][]int)
	var order []string
	for i := 0; i < rss.Len(); i++ {
		key := tp.keyForResource(ctx, rss.At(i).Resource())
		if _, ok := resByKey[key]; !ok {
			order = append(order, key)
		}
		resByKey[key] = append(resByKey[key], i)
	}

	// A trace spanning resources of different keys is analyzed under each key;
	// if any key's plan drops it, the union drop removes it everywhere, which
	// keeps the "never forward a partial trace" guarantee.
	dropAll := make(map[pcommon.TraceID]bool)
	forwarded := 0
	totalDenied := 0
	deniedKey := ""
	for _, key := range order {
		critical, normal, normalTraces := analyzeTracesIn(td, resByKey[key], tp.config.PreserveErrors)
		total := critical + normal

		tp.telemetry.receivedItems.Add(ctx, int64(total), tp.metricAttrs(counterReceived, key, "traces", "all"))

		normalAllowed := tp.rateLimiter.AllowUpToNCtx(ctx, key, normal)

		if critical > 0 {
			tp.telemetry.preservedItems.Add(ctx, int64(critical), tp.metricAttrs(counterPreserved, key, "traces", "critical"))
		}

		// Keep whole traces up to the granted budget; drop the rest whole. Tokens
		// for a trace that didn't fit stay spent (conservative: never split).
		drop, dropped := planTraceDrops(normalTraces, normalAllowed)
		keptNormal := normal - dropped
		for id := range drop {
			dropAll[id] = true
		}

		if dropped > 0 {
			totalDenied += dropped
			deniedKey = key
			tp.telemetry.deniedItems.Add(ctx, int64(dropped), tp.metricAttrs(counterDenied, key, "traces", "normal"))
			if suppressed, emit := tp.dropLog.next(); emit {
				tp.logger.Warn("Rate limit exceeded for traces; dropping whole traces",
					zap.String("key", key),
					zap.Int("dropped_spans", dropped),
					zap.Int("dropped_traces", len(drop)),
					zap.Int("preserved_critical", critical),
					zap.Int64("suppressed_warnings_since_last", suppressed),
				)
			}
		}

		tp.telemetry.allowedItems.Add(ctx, int64(critical+keptNormal), tp.metricAttrs(counterAllowed, key, "traces", "all"))
		forwarded += critical + keptNormal
	}

	if totalDenied > 0 && tp.config.DropOnLimit {
		if forwarded == 0 {
			return td, tp.limitExceededError(deniedKey, totalDenied)
		}
		dropTracesByID(td, dropAll)
	}

	return td, nil
}

func (tp *tracesProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if tp.config.TraceIDAware {
		return tp.processTracesByTrace(ctx, td)
	}

	if td.ResourceSpans().Len() == 0 {
		return td, nil
	}

	resKeys, groups := tp.tracesKeyGroups(ctx, td)

	forwarded := 0
	totalDenied := 0
	deniedKey := ""
	excessByKey := make(map[string]int)
	for _, g := range groups {
		total := g.critical + g.normal

		tp.telemetry.receivedItems.Add(ctx, int64(total), tp.metricAttrs(counterReceived, g.key, "traces", "all"))

		// Normal spans compete for the budget first; critical spans bypass the
		// limit entirely so an error flood doesn't get silently throttled during
		// an incident (which is exactly when we need the signal most).
		normalAllowed := tp.rateLimiter.AllowUpToNCtx(ctx, g.key, g.normal)

		if g.critical > 0 {
			tp.telemetry.preservedItems.Add(ctx, int64(g.critical), tp.metricAttrs(counterPreserved, g.key, "traces", "critical"))
		}
		excess := g.normal - normalAllowed

		if excess > 0 {
			excessByKey[g.key] = excess
			totalDenied += excess
			deniedKey = g.key
			tp.telemetry.deniedItems.Add(ctx, int64(excess), tp.metricAttrs(counterDenied, g.key, "traces", "normal"))
			if suppressed, emit := tp.dropLog.next(); emit {
				tp.logger.Warn("Rate limit exceeded for traces; dropping non-critical spans",
					zap.String("key", g.key),
					zap.Int("dropped", excess),
					zap.Int("preserved_critical", g.critical),
					zap.Int64("suppressed_warnings_since_last", suppressed),
				)
			}
		}

		// allowed = items admitted by the limiter (critical bypass + granted),
		// in both drop and monitor modes, so received == allowed + denied always
		// holds and dashboards don't need per-mode arithmetic.
		tp.telemetry.allowedItems.Add(ctx, int64(g.critical+normalAllowed), tp.metricAttrs(counterAllowed, g.key, "traces", "all"))
		forwarded += g.critical + normalAllowed
	}

	if totalDenied > 0 && tp.config.DropOnLimit {
		// If nothing at all was admitted, reject the batch instead of
		// forwarding an empty one.
		if forwarded == 0 {
			return td, tp.limitExceededError(deniedKey, totalDenied)
		}
		dropNormalSpansByKey(td, resKeys, excessByKey, tp.config.PreserveErrors)
	}

	return td, nil
}

// Metrics Processor
type metricsProcessor struct {
	*rateLimitProcessor
	nextConsumer consumer.Metrics
}

func newMetricsProcessor(config *Config, logger *zap.Logger, mp metric.MeterProvider, storage Storage, nextConsumer consumer.Metrics) (*metricsProcessor, error) {
	rp, err := newRateLimitProcessor(config, logger, mp, storage)
	if err != nil {
		return nil, err
	}
	return &metricsProcessor{
		rateLimitProcessor: rp,
		nextConsumer:       nextConsumer,
	}, nil
}

// ConsumeMetrics runs processMetrics and forwards the result. See
// tracesProcessor.ConsumeTraces for why this adapter exists.
func (mp *metricsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	out, err := mp.processMetrics(ctx, md)
	if err != nil {
		return err
	}
	return mp.nextConsumer.ConsumeMetrics(ctx, out)
}

func countDataPointsIn(rm pmetric.ResourceMetrics) int {
	count := 0
	for j := 0; j < rm.ScopeMetrics().Len(); j++ {
		sm := rm.ScopeMetrics().At(j)
		for k := 0; k < sm.Metrics().Len(); k++ {
			m := sm.Metrics().At(k)
			switch m.Type() {
			case pmetric.MetricTypeGauge:
				count += m.Gauge().DataPoints().Len()
			case pmetric.MetricTypeSum:
				count += m.Sum().DataPoints().Len()
			case pmetric.MetricTypeHistogram:
				count += m.Histogram().DataPoints().Len()
			case pmetric.MetricTypeExponentialHistogram:
				count += m.ExponentialHistogram().DataPoints().Len()
			case pmetric.MetricTypeSummary:
				count += m.Summary().DataPoints().Len()
			}
		}
	}
	return count
}

func countDataPoints(md pmetric.Metrics) int {
	count := 0
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		count += countDataPointsIn(md.ResourceMetrics().At(i))
	}
	return count
}

func (mp *metricsProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	rms := md.ResourceMetrics()
	if rms.Len() == 0 {
		return md, nil
	}

	// Bill each resource entry to its own key so multi-tenant batches are
	// limited per key, not all charged to the first resource's key.
	resKeys := make([]string, rms.Len())
	counts := make(map[string]int)
	var order []string
	for i := 0; i < rms.Len(); i++ {
		key := mp.keyForResource(ctx, rms.At(i).Resource())
		resKeys[i] = key
		if _, ok := counts[key]; !ok {
			order = append(order, key)
		}
		counts[key] += countDataPointsIn(rms.At(i))
	}

	// Metrics have no natural priority split (gauges/counters aren't
	// "errors"), so admission is all-or-nothing per key.
	deniedKeys := make(map[string]bool)
	forwarded := 0
	totalDenied := 0
	deniedKey := ""
	for _, key := range order {
		c := counts[key]
		mp.telemetry.receivedItems.Add(ctx, int64(c), mp.metricAttrs(counterReceived, key, "metrics", "all"))

		if !mp.rateLimiter.AllowNCtx(ctx, key, c) {
			deniedKeys[key] = true
			totalDenied += c
			deniedKey = key
			mp.telemetry.deniedItems.Add(ctx, int64(c), mp.metricAttrs(counterDenied, key, "metrics", "normal"))
			if suppressed, emit := mp.dropLog.next(); emit {
				mp.logger.Warn("Rate limit exceeded for metrics",
					zap.String("key", key),
					zap.String("limit_type", mp.config.LimitType),
					zap.Int("data_point_count", c),
					zap.Int64("suppressed_warnings_since_last", suppressed),
				)
			}
			continue
		}
		mp.telemetry.allowedItems.Add(ctx, int64(c), mp.metricAttrs(counterAllowed, key, "metrics", "all"))
		forwarded += c
	}

	if totalDenied > 0 && mp.config.DropOnLimit {
		if forwarded == 0 {
			return md, mp.limitExceededError(deniedKey, totalDenied)
		}
		idx := -1
		rms.RemoveIf(func(pmetric.ResourceMetrics) bool {
			idx++
			return deniedKeys[resKeys[idx]]
		})
	}

	return md, nil
}

// Logs Processor
type logsProcessor struct {
	*rateLimitProcessor
	nextConsumer consumer.Logs
}

func newLogsProcessor(config *Config, logger *zap.Logger, mp metric.MeterProvider, storage Storage, nextConsumer consumer.Logs) (*logsProcessor, error) {
	rp, err := newRateLimitProcessor(config, logger, mp, storage)
	if err != nil {
		return nil, err
	}
	return &logsProcessor{
		rateLimitProcessor: rp,
		nextConsumer:       nextConsumer,
	}, nil
}

// ConsumeLogs runs processLogs and forwards the result. See
// tracesProcessor.ConsumeTraces for why this adapter exists.
func (lp *logsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	out, err := lp.processLogs(ctx, ld)
	if err != nil {
		return err
	}
	return lp.nextConsumer.ConsumeLogs(ctx, out)
}

// isCriticalLog flags ERROR-and-above severities as must-keep.
func isCriticalLog(lr plog.LogRecord) bool {
	return lr.SeverityNumber() >= plog.SeverityNumberError
}

func forEachLogInResource(rl plog.ResourceLogs, fn func(plog.LogRecord)) {
	for j := 0; j < rl.ScopeLogs().Len(); j++ {
		sl := rl.ScopeLogs().At(j)
		for k := 0; k < sl.LogRecords().Len(); k++ {
			fn(sl.LogRecords().At(k))
		}
	}
}

// logsKeyGroups mirrors tracesKeyGroups for log records.
func (lp *logsProcessor) logsKeyGroups(ctx context.Context, ld plog.Logs) (resKeys []string, groups []*keyTally) {
	rls := ld.ResourceLogs()
	resKeys = make([]string, rls.Len())
	byKey := make(map[string]*keyTally)
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		key := lp.keyForResource(ctx, rl.Resource())
		resKeys[i] = key
		t := byKey[key]
		if t == nil {
			t = &keyTally{key: key}
			byKey[key] = t
			groups = append(groups, t)
		}
		forEachLogInResource(rl, func(lr plog.LogRecord) {
			if lp.config.PreserveErrors && isCriticalLog(lr) {
				t.critical++
			} else {
				t.normal++
			}
		})
	}
	return resKeys, groups
}

// dropNormalLogsByKey removes, per key, exactly excessByKey[key] non-critical
// records from the resources billed to that key.
func dropNormalLogsByKey(ld plog.Logs, resKeys []string, excessByKey map[string]int, preserveErrors bool) {
	idx := -1
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		idx++
		key := resKeys[idx]
		if excessByKey[key] <= 0 {
			return false
		}
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				if excessByKey[key] <= 0 {
					return false
				}
				if preserveErrors && isCriticalLog(lr) {
					return false
				}
				excessByKey[key]--
				return true
			})
			return sl.LogRecords().Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})
}

func (lp *logsProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	if ld.ResourceLogs().Len() == 0 {
		return ld, nil
	}

	resKeys, groups := lp.logsKeyGroups(ctx, ld)

	forwarded := 0
	totalDenied := 0
	deniedKey := ""
	excessByKey := make(map[string]int)
	for _, g := range groups {
		total := g.critical + g.normal

		lp.telemetry.receivedItems.Add(ctx, int64(total), lp.metricAttrs(counterReceived, g.key, "logs", "all"))

		normalAllowed := lp.rateLimiter.AllowUpToNCtx(ctx, g.key, g.normal)

		if g.critical > 0 {
			lp.telemetry.preservedItems.Add(ctx, int64(g.critical), lp.metricAttrs(counterPreserved, g.key, "logs", "critical"))
		}
		excess := g.normal - normalAllowed

		if excess > 0 {
			excessByKey[g.key] = excess
			totalDenied += excess
			deniedKey = g.key
			lp.telemetry.deniedItems.Add(ctx, int64(excess), lp.metricAttrs(counterDenied, g.key, "logs", "normal"))
			if suppressed, emit := lp.dropLog.next(); emit {
				lp.logger.Warn("Rate limit exceeded for logs; dropping non-critical records",
					zap.String("key", g.key),
					zap.Int("dropped", excess),
					zap.Int("preserved_critical", g.critical),
					zap.Int64("suppressed_warnings_since_last", suppressed),
				)
			}
		}

		lp.telemetry.allowedItems.Add(ctx, int64(g.critical+normalAllowed), lp.metricAttrs(counterAllowed, g.key, "logs", "all"))
		forwarded += g.critical + normalAllowed
	}

	if totalDenied > 0 && lp.config.DropOnLimit {
		if forwarded == 0 {
			return ld, lp.limitExceededError(deniedKey, totalDenied)
		}
		dropNormalLogsByKey(ld, resKeys, excessByKey, lp.config.PreserveErrors)
	}

	return ld, nil
}
