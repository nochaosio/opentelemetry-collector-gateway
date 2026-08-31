# Internal Telemetry: `ratelimitprocessor`

Everything the rate-limit processor emits about **itself** (metrics + logs), what each
signal means, and how to use it operationally. This is the processor's *self*
telemetry: not the user traces/metrics/logs flowing through it.

- Stability: `beta`
- Exposed at the collector's telemetry endpoint (playground default: `http://localhost:8888/metrics`).
- Meter / scope name: `github.com/nochaosio/opentelemetry-collector-gateway/processor/ratelimitprocessor`

> **`_total` suffix.** Names below are the *instrument* names. Whether the Prometheus
> exposition appends `_total` to counters depends on your `service.telemetry.metrics`
> reader (the collector's default internal exporter does **not** append it). Query
> whatever your `/metrics` shows. Examples here use the no-suffix form.

---

## 1. Metrics

### 1.1 Counters

| Metric | Unit | Meaning | Emitted when |
|--------|------|---------|--------------|
| `otelcol_processor_ratelimit_received_items`  | `{item}` | Items (spans / datapoints / log records) that entered the processor | every batch |
| `otelcol_processor_ratelimit_allowed_items`   | `{item}` | Items forwarded downstream | every batch |
| `otelcol_processor_ratelimit_denied_items`    | `{item}` | Items over the limit (dropped if `drop_on_limit: true`; counted-only if `false`) | when something is over-limit |
| `otelcol_processor_ratelimit_preserved_items` | `{item}` | Critical items that bypassed the limit (`preserve_errors`): error spans (`Status=Error`) and error+ logs (`SeverityNumber >= ERROR`) | when critical items are present |
| `otelcol_processor_ratelimit_backend_errors`  | `{error}` | Transient storage-backend errors (e.g. Redis unreachable while fail-open) | on each backend error |
| `otelcol_processor_ratelimit_fail_open_items` | `{item}` | Items allowed through (not rate-limited) because the backend was unreachable; traffic that bypassed the limit during a Redis outage | on each fail-open allow |
| `otelcol_processor_ratelimit_bucket_evictions` | `{bucket}` | Buckets evicted because the memory backend hit `storage.max_buckets` | on each cap-triggered eviction |

Counting model per batch: **`received = allowed + denied` in both modes.**
`allowed` = items admitted by the limiter (including the critical bypass);
`denied` = items over the limit. In monitor mode (`drop_on_limit: false`)
denied items are still *forwarded*, but counted only in `denied`, so the same
dashboard arithmetic works in either mode. `preserved` is a subset of `allowed`
(critical items always admitted).

### 1.2 Observable gauges (token-bucket state)

Sampled at scrape time: same idea as the collector's exporter `queue_size` /
`queue_capacity`. **Emitted only for keys listed in `metrics_key_allowlist`** (the
bounded "watch set"), so they never reintroduce a cardinality problem. With no
allowlist configured, these gauges are off.

| Metric | Unit | Meaning |
|--------|------|---------|
| `otelcol_processor_ratelimit_bucket_capacity_tokens`  | `{token}` | Bucket capacity = configured limit (max tokens) for the key |
| `otelcol_processor_ratelimit_bucket_available_tokens` | `{token}` | Tokens currently free for the key |

Derived: **occupied = capacity - available**; **% occupied = 1 - available/capacity**
(100% = bucket empty = throttling right now; 0% = full = idle).

### 1.3 Labels

| Label | Values | On | Notes |
|-------|--------|----|-------|
| `signal`     | `traces`, `metrics`, `logs`         | counters | which signal |
| `priority`   | `all`, `normal`, `critical`         | counters | `received`/`allowed` use `all`; `denied` uses `normal`; `preserved` uses `critical` |
| `limit_type` | `service_name`, `attribute`, `header` | counters + gauges | separates per-service from per-tenant/per-client series when multiple rate-limit processors share these instruments |
| `backend`    | `redis`                             | `backend_errors` | which backend failed |
| `key`        | the rate-limit key, or `other`      | **conditional** | see cardinality below |

### 1.4 Cardinality controls (how `key` behaves)

The `key` label is client-controlled (service name / attribute / header) → potential
cardinality bomb. Four composable knobs bound it:

- **`metrics_verbosity`** (default `detailed`): the single switch. `basic` removes
  `key` from **all** counters and disables the bucket gauges — a handful of series
  per signal regardless of key count; the knobs below become inert. Rate limiting
  itself is unaffected. Full metrics table: [metric.md](metric.md).
- **`key_labels_on`** (default `[denied, preserved]`): which counters carry `key` at
  all. Counters not listed are emitted **aggregated** (no `key`). Default keeps `key`
  only where it's naturally bounded (the few keys that actually hit the limit / shed
  errors); `received`/`allowed` stay aggregated.
- **`metrics_key_allowlist`**: wherever `key` is kept, only listed keys keep their
  value; everything else collapses to `key="other"`. Caps cardinality at
  `len(allowlist) + 1`.
- **`max_metric_keys`** (default `100`): the always-on backstop when **no**
  allowlist is configured. The first N distinct keys keep their value
  (first-come, so series stay stable); later keys collapse to `key="other"`.
  Even an attacker spraying unique over-limit keys cannot push the exported
  series count past N+1. Negative disables it (not recommended).

Rule of thumb: per-service `received`/`allowed` requires turning `key` on for those
counters **and** allowlisting the (bounded, known) services you care about. For the
unbounded long tail, use the warning logs (§2) instead of metrics.

---

## 2. Logs

| Level | Message | Fields | When |
|-------|---------|--------|------|
| info  | `Rate limit processor started` | `storage` (`memory`/`redis`) | on Start |
| info  | `Rate limit processor stopped` | (none) | on Shutdown |
| info  | `Rate limit Redis storage ready` | `addr`, `prefix`, `timeout`, `fail_open`, `negative_cache_ttl` | redis backend init |
| warn  | `Rate limit exceeded for traces/logs; dropping non-critical ...` | `key`, `dropped`, `preserved_critical`, `suppressed_warnings_since_last` | over-limit (**throttled ≤ 1 / 10s**) |
| warn  | `Rate limit exceeded for metrics` | `key`, `limit_type`, `data_point_count`, `suppressed_warnings_since_last` | over-limit (**throttled ≤ 1 / 10s**) |
| warn  | `Redis unavailable: failing OPEN/CLOSED ...` | `key`, `error` | redis call failed |
| warn  | `storage close failed` | `error` | shutdown error |
| debug | `Redis refund failed (non-fatal)` | `key`, `error` | refund error |

**Throttling:** the per-batch "rate limit exceeded" warnings are emitted at most once
per 10s per processor; `suppressed_warnings_since_last` tells you how many were
collapsed. The exact drop counts live in `otelcol_processor_ratelimit_denied_items`: use the
metric for measurement, the log for spot investigation (it carries the full `key`,
even for non-allowlisted services).

---

## 3. How to use the data (PromQL)

```promql
# Global drop rate (%)
sum(rate(otelcol_processor_ratelimit_denied_items[5m]))
/
sum(rate(otelcol_processor_ratelimit_received_items[5m]))

# Drop rate by signal
sum by (signal) (rate(otelcol_processor_ratelimit_denied_items[5m]))
/ sum by (signal) (rate(otelcol_processor_ratelimit_received_items[5m]))

# Who is being throttled (top 10): `key` lives on denied by default
topk(10, sum by (key) (rate(otelcol_processor_ratelimit_denied_items[5m])))

# Per-service received vs dropped (requires key_labels_on incl. received + allowlist)
sum by (key) (rate(otelcol_processor_ratelimit_received_items{limit_type="service_name"}[5m]))
sum by (key) (rate(otelcol_processor_ratelimit_denied_items{limit_type="service_name"}[5m]))

# Critical items rescued by preserve_errors (per service)
sum by (key) (rate(otelcol_processor_ratelimit_preserved_items[5m]))

# Bucket occupancy (% full-drained) for watched keys
1 - (otelcol_processor_ratelimit_bucket_available_tokens / otelcol_processor_ratelimit_bucket_capacity_tokens)

# Redis health: ANY increase means the state store is degraded (fail-open)
rate(otelcol_processor_ratelimit_backend_errors[5m])
```

### Suggested alerts

| Alert | Expression (sketch) | Why |
|-------|---------------------|-----|
| Backend degraded | `rate(otelcol_processor_ratelimit_backend_errors[5m]) > 0` | Redis unreachable → limits imprecise (fail-open) |
| Sustained heavy drop | `global drop rate > 0.5 for 10m` | a producer is over budget, or a limit is mis-set |
| Bucket pinned empty | `(1 - available/capacity) == 1 for 15m` (watched key) | a service is constantly throttled |

### Reading these metrics locally

`playground/ratelimitprocessor/` runs the processor under docker compose and
exposes every counter and gauge above at `http://localhost:8888/metrics`; its
README maps each scenario to the metrics it moves.

---

## 4. Complementary collector-level telemetry

The processor's own metrics pair well with the collector's built-ins:

- `otelcol_receiver_accepted_spans|metric_points|log_records`: total ingestion (before rate limiting).
- `otelcol_exporter_sent_*` / `otelcol_exporter_send_failed_*`: what left the gateway.
- `otelcol_process_memory_rss`, `otelcol_process_cpu_seconds`, `otelcol_process_uptime`: runtime health.

A useful sanity check: `received_items` (this processor) should track
`otelcol_receiver_accepted_*` for the pipelines the processor sits in.
