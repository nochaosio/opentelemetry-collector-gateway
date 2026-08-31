# Metrics reference — ratelimit processor

Every metric the processor exports, on the collector's self-telemetry endpoint
(`:8888/metrics` in the default config). Deep dive (labels, cardinality
internals, logs): [internal-telemetry.md](internal-telemetry.md).

## Counters

| Metric | Unit | Labels | Description |
|--------|------|--------|-------------|
| `otelcol_processor_ratelimit_received_items` | items | `signal`, `priority="all"`, `limit_type` | Everything that entered the processor, before any decision. Baseline of the funnel: `received = allowed + denied`. |
| `otelcol_processor_ratelimit_allowed_items` | items | `signal`, `priority="all"`, `limit_type` | Items that passed to the next pipeline stage — within-limit traffic plus preserved criticals. |
| `otelcol_processor_ratelimit_denied_items` | items | `key`*, `signal`, `priority="normal"`, `limit_type` | Items rejected/dropped for exceeding the limit. Critical items never land here. In `throttle` mode the client retries (denied ≠ necessarily lost); in `permanent`/drop mode it is a real drop. |
| `otelcol_processor_ratelimit_preserved_items` | items | `key`*, `signal`, `priority="critical"`, `limit_type` | Subset of `allowed` that passed **only** because of the priority bypass (error spans, ERROR+ logs) while over the limit. |
| `otelcol_processor_ratelimit_fail_open_items` | items | `signal`, `limit_type` | Items allowed through **without** rate limiting because the storage backend (Redis) was unreachable and the processor failed open. |
| `otelcol_processor_ratelimit_backend_errors` | errors | `backend` | Transient storage-backend errors (e.g. Redis timeout/unreachable). |
| `otelcol_processor_ratelimit_bucket_evictions` | buckets | — | Token buckets evicted because the memory backend hit its `max_buckets` cap. |

## Gauges (scrape-time, allowlisted keys only)

| Metric | Unit | Labels | Description |
|--------|------|--------|-------------|
| `otelcol_processor_ratelimit_bucket_capacity_tokens` | tokens | `key`, `limit_type` | Token-bucket capacity (= configured limit) for each key in `metrics_key_allowlist`. |
| `otelcol_processor_ratelimit_bucket_available_tokens` | tokens | `key`, `limit_type` | Tokens currently free for each allowlisted key. `capacity - available` = occupancy; pinned at 0 = key saturated. |

\* `key` is emitted only on the counters selected by `key_labels_on`
(default: `denied`, `preserved`) and never in `metrics_verbosity: basic`.

## Cardinality controls

Four composable knobs bound the `key` label; from bluntest to finest:

| Knob | Default | Effect |
|------|---------|--------|
| `metrics_verbosity` | `detailed` | **Single switch.** `basic` = no `key` label on any counter and no bucket gauges — a handful of series per signal no matter how many keys exist. Rate limiting itself is unaffected. |
| `key_labels_on` | `[denied, preserved]` | Which counters carry `key` at all. `received`/`allowed` stay aggregated by default. |
| `metrics_key_allowlist` | empty | Where `key` is kept, only listed keys keep their value; the rest collapse into `key="other"`. Cardinality ≤ len+1. Also the watch set for the bucket gauges. |
| `max_metric_keys` | `100` | Backstop when no allowlist is set: first N distinct keys keep their value, later ones collapse to `"other"`. Negative disables (not recommended). |

```yaml
# lowest cardinality — aggregated counters only
ratelimit:
  metrics_verbosity: basic

# default — per-key visibility where it matters, bounded
ratelimit:
  metrics_key_allowlist: [payment-service, legacy-service]

# maximum visibility — key on all four counters, still bounded by the allowlist
ratelimit:
  key_labels_on: [received, allowed, denied, preserved]
  metrics_key_allowlist: [payment-service, legacy-service]
```

In `basic` mode, per-key attribution moves to the throttled warn logs
(`Rate limit exceeded ... key=X`, ≤ 1/10s per key).
