# Rate Limit Processor

| Status | |
|---|---|
| Stability | `beta` |
| Supported signals | traces, metrics, logs |

A processor for the OpenTelemetry Collector Gateway that implements per-key rate limiting using a token bucket algorithm, similar to how nginx, HAProxy, or Envoy handle traffic shaping.

## Features

- **Rate limiting by:**
  - `service_name`: limits per `service.name` resource attribute
  - `attribute`: limits per any custom resource attribute (e.g. `tenant.id`, `client.id`)
  - `header`: limits per HTTP header / gRPC metadata value (requires `include_metadata: true` on the OTLP receiver — see [Header-based limiting](#header-based-limiting))
- **Rate periods:** per-second (`requests_per_second`) or per-minute (`requests_per_minute`)
- **Alternative period config:** `default_limit` + `default_limit_period` (`second` or `minute`)
- **Per-key overrides:** `specific_limits` map to set different limits for individual keys
- **Drop or monitor mode:** `drop_on_limit: true` drops excess telemetry; `false` lets everything pass but still records the denial in metrics
- **Default key fallback:** when the expected key is absent (e.g. `service.name` is missing), traffic is counted under the `"default"` bucket
- **All signals supported:** traces (spans), metrics (data points), and logs (log records)
- **Pluggable storage backend:** in-process `memory` (default, zero-dependency, replica-local) or shared `redis` (opt-in, fleet-wide accuracy, cross-replica visibility)
- **Auto-cleanup:** background eviction for the memory backend (every 5 min, evicts buckets idle >10 min); automatic TTL for the Redis backend
- **Built-in observability metrics:** Prometheus counters (received/allowed/denied/preserved, backend errors, fail-open items, bucket evictions) and token-bucket gauges, exported via the collector's telemetry endpoint

## Configuration Reference

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `limit_type` | string | Key extraction strategy: `service_name`, `attribute`, or `header` | Yes |
| `attribute_key` | string | Resource attribute to use as key (required when `limit_type: attribute`) | Conditional |
| `header_key` | string | HTTP header to use as key (required when `limit_type: header`) | Conditional |
| `requests_per_second` | int | Global rate limit in items per second | No |
| `requests_per_minute` | int | Global rate limit in items per minute | No |
| `default_limit` | int | Rate limit value when using `default_limit_period` | No |
| `default_limit_period` | string | Period for `default_limit`: `second` or `minute` | No |
| `specific_limits` | map | Per-key overrides; each entry supports `requests_per_second` or `requests_per_minute` | No |
| `drop_on_limit` | bool | `true` = drop telemetry when limit exceeded; `false` = pass through but record in metrics | Yes |
| `preserve_errors` | bool | When `true` (default), error spans (status=Error) and error+ logs bypass the limit and are always forwarded | No |
| `trace_id_aware` | bool | Traces only. When `true`, drop whole traces by `trace_id` instead of individual spans, so a backend never sees a partial trace; an error trace is kept in full. Recommended for tracing-critical deployments. Default `false` | No |
| `global_requests_per_second` | int | Ceiling on *total* throughput across all keys | No |
| `global_requests_per_minute` | int | Same as above in per-minute units | No |
| `max_share_ratio` | float | Caps any single key at this fraction `(0,1]` of the global budget (requires a `global_*` limit) | No |
| `rejection_mode` | string | Error returned when a batch is fully dropped: `throttle` (default; gRPC `RESOURCE_EXHAUSTED` + `RetryInfo`, HTTP 429 — the OTLP-spec throttling signal, SDKs back off and retry) or `permanent` (non-retryable, clients drop the batch). See [Client response on rejection](#client-response-on-rejection) | No |
| `metrics_key_allowlist` | []string | Bounds the *values* of the `key` metric label: only listed keys keep their value, the rest collapse to `other`. See [Controlling metric cardinality](#controlling-metric-cardinality) | No |
| `max_metric_keys` | int | Cardinality backstop when `metrics_key_allowlist` is **not** set: the first N distinct keys keep their value in the `key` label, later ones collapse to `other`. `0` = default (`100`); negative = unlimited (not recommended) | No |
| `key_labels_on` | []string | Which counters carry the `key` label: subset of `received`, `allowed`, `denied`, `preserved` (default `[denied, preserved]`). Others are emitted aggregated. See [Controlling metric cardinality](#controlling-metric-cardinality) | No |
| `metrics_verbosity` | string | Single-switch cardinality control for the processor's own metrics: `detailed` (default; `key` label per `key_labels_on`, bucket gauges on) or `basic` (no `key` label anywhere, no bucket gauges — a handful of series per signal regardless of key count). See [metric.md](metric.md) | No |
| `storage.backend` | string | `memory` (default, in-process) or `redis` (shared across replicas) | No |
| `storage.max_buckets` | int | Memory backend only. Hard cap on the number of live buckets; evicts the oldest when full, so a hostile/buggy producer spraying unique keys can't OOM the collector. Default `100000` | No |
| `storage.redis.tls` | object | Optional TLS for the Redis connection (standard collector TLS config: `ca_file`, `cert_file`, `key_file`, ...). Keeps rate-limit state off the wire in clear | No |
| `storage.redis.addr` | string | `host:port` of a single Redis node (one of `addr`/`addrs` required when `backend: redis`) | Conditional |
| `storage.redis.addrs` | []string | Endpoints for HA: Sentinel addresses (with `master_name`) or Cluster nodes. Takes precedence over `addr` | No |
| `storage.redis.master_name` | string | Set to run in **Sentinel failover** mode against `addrs`; the client follows primary failover so a node dying doesn't take rate limiting down | No |
| `storage.redis.password` | string (opaque) | AUTH password: stored opaque, so it is redacted from config dumps (`/debug/configz`), logs, and errors | No |
| `storage.redis.db` | int | Logical DB index (default 0) | No |
| `storage.redis.key_prefix` | string | Namespace for bucket keys (default `otelcol:ratelimit`) | No |
| `storage.redis.timeout` | duration | Per-call deadline; on expiry the storage fails per `on_error` (default `100ms`) | No |
| `storage.redis.on_error` | string | `open` (allow traffic, default) or `closed` (deny) when Redis is unreachable | No |
| `storage.redis.negative_cache_ttl` | duration | Short-lived local cache of "bucket empty" results to reduce Redis load under attack (default `0`, disabled) | No |

> **Note on precedence:** `requests_per_second` takes precedence over `requests_per_minute`, which takes precedence over `default_limit`. The processor's default configuration sets `requests_per_second: 100`, so if you only want to configure `requests_per_minute` or `default_limit`, you must explicitly set `requests_per_second: 0` in your YAML to prevent the default from overriding your setting.

> **Note on `specific_limits`:** within a specific limit entry, `requests_per_second` takes precedence over `requests_per_minute`.

## Storage backend

The token-bucket state lives behind a pluggable `Storage` interface. The choice is a design decision, not just a knob:

> **One bucket per key, across signals.** The traces, metrics and logs pipelines that share the same `ratelimit` instance share **one** storage backend, so a key's budget (e.g. `payment-service: 100 rps`) is a single bucket consumed by all three signals together. This holds identically for `memory` and `redis` — swapping backends never changes enforcement semantics. To give each signal its own budget, declare separate processor instances (`ratelimit/traces`, `ratelimit/logs`, ...) with their own limits.

| Backend | Accuracy across replicas | Extra infra | When to pick |
|---------|--------------------------|-------------|--------------|
| `memory` (default) | Replica-local: N replicas allow N × configured rps in total | None | Single-replica edge, or when an upstream routing layer already shards by key |
| `redis` | Fleet-wide: all replicas share one bucket per key | Redis (Sentinel or a managed equivalent recommended in prod) | Collectors behind an L4 LB (F5, GCLB, AWS NLB) that fans traffic randomly across replicas |

### How the Redis backend works

- **One round-trip per decision.** A Lua script atomically refills the bucket from elapsed time, consumes up to `n` tokens, and returns the number granted. No read-modify-write race.
- **Keys are auto-expiring hashes.** Each key (`<prefix>:<service>`) stores `t` (tokens) and `r` (last refill µs) with a TTL of `10 × period`, bounded to `[60s, 1h]`. Idle services evict themselves: no manual cleanup required.
- **Fail-open by default, with automatic recovery.** Any Redis error (timeout, connection, unavailable) returns "allow all `n`" and increments `backend_errors` (and `fail_open_items` by the bypassed count). While Redis is down, **everything passes**; the moment Redis answers again, limiting **resumes on its own** (the client reconnects, no restart needed). An edge collector that stops accepting telemetry because its state store hiccupped is worse than briefly loose rate limiting. Flip to `on_error: closed` if your priority is strict enforcement over availability.
- **High availability.** Point `addrs` + `master_name` at Redis Sentinel (the client follows primary failover automatically), or list Cluster nodes in `addrs`. A single node dying no longer takes shared-state rate limiting down with it.
- **Optional negative cache.** Setting `negative_cache_ttl: 50ms` makes a key that just got 0 tokens return 0 locally for 50ms, skipping Redis. Turns on under attack, off by default.

### Operational visibility

Every active key is a hash in Redis: so the state store doubles as a live inventory of who is talking to the fleet:

```bash
# List every service currently holding a bucket
redis-cli --scan --pattern 'otelcol:ratelimit:*'

# Inspect one bucket: tokens remaining and last refill (µs epoch)
redis-cli HGETALL otelcol:ratelimit:premium-svc
```

This is the practical answer to "is a single service bombarding my collectors behind the F5?": regardless of which replica that traffic hits, the bucket for `service.name=<X>` is one key in Redis.

## Manual testing

For copy-pasteable recipes that reproduce every scenario on a laptop in under a minute (basic limit, overrides, monitor mode, priority bypass, fair share, token refill, memory vs Redis storage, fail-open, negative cache, attribute/header limits), see [`TESTING.md`](TESTING.md).

## Examples

### Example 1: Rate limit by service name

```yaml
processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 100
    drop_on_limit: true
```

### Example 2: Rate limit by custom attribute

```yaml
processors:
  ratelimit:
    limit_type: attribute
    attribute_key: tenant.id
    requests_per_second: 50
    drop_on_limit: true
```

### Example 3: Per-key overrides with `specific_limits`

```yaml
processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 100       # default for all services
    drop_on_limit: true
    specific_limits:
      payment-service:
        requests_per_second: 500   # higher limit for critical service
      analytics-service:
        requests_per_minute: 1000  # minute-based limit for batch service
      legacy-service:
        requests_per_second: 10    # lower limit for noisy service
```

### Example 4: Per-minute rate limit

```yaml
processors:
  ratelimit:
    limit_type: attribute
    attribute_key: client.id
    requests_per_second: 0         # must be 0 to let requests_per_minute take effect
    requests_per_minute: 300
    drop_on_limit: true
```

### Example 5: Using `default_limit` with period

```yaml
processors:
  ratelimit:
    limit_type: attribute
    attribute_key: tenant.id
    requests_per_second: 0         # must be 0 to let default_limit take effect
    default_limit: 200
    default_limit_period: second
    drop_on_limit: true
```

### Example 6: Monitor mode (observe without dropping)

```yaml
processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 50
    drop_on_limit: false           # all data passes through; excess is only recorded in metrics
```

### Example 7: Rate limit by HTTP header

```yaml
processors:
  ratelimit:
    limit_type: header
    header_key: X-API-Key
    requests_per_second: 0
    requests_per_minute: 1000
    drop_on_limit: true
```

> **Note:** Header-based limiting requires the OTLP receiver to forward HTTP headers as resource attributes. When the header is not present, traffic falls through to the `"default"` bucket.

### Example 8: Distributed rate limit via Redis

Use this when multiple collector replicas sit behind an L4 load balancer (F5, GCLB, NLB, etc.) that can't pin a given `service.name` to a specific replica. The Redis-backed bucket makes the configured rate a true fleet-wide limit, and the bucket hashes in Redis double as a live inventory of which services are talking to the collectors.

```yaml
processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 100
    drop_on_limit: true
    specific_limits:
      premium-service:
        requests_per_second: 500
    storage:
      backend: redis
      redis:
        addr: redis.internal:6379
        # password: "${env:REDIS_PASSWORD}"
        key_prefix: otelcol:ratelimit
        timeout: 100ms
        on_error: open        # "closed" for strict enforcement over availability
        # negative_cache_ttl: 50ms  # enable under attack to reduce Redis QPS
```

With this config running on N replicas, the total traffic accepted from any service still respects `requests_per_second`, not `N × requests_per_second`.

## Token Bucket Algorithm

Each unique key gets its own independent token bucket:

- The bucket starts **full** at the configured limit (e.g. a 5 RPS limit starts with 5 tokens)
- Tokens refill continuously at the configured rate
- **Cost model:** each item consumes 1 token, where an item is:
  - 1 span (for traces)
  - 1 log record (for logs)
  - 1 data point (for metrics)
- An entire OTLP batch is evaluated atomically with `AllowN(batchSize)`: if the bucket doesn't have enough tokens, the whole batch is denied
- If the key is missing from the incoming telemetry, the `"default"` bucket is used

### Token consumption example

```
OTLP request with 10 spans  → consumes 10 tokens
OTLP request with 1 span    → consumes 1 token
OTLP request with 100 logs  → consumes 100 tokens
```

This ensures rate limiting is fair and volume-based, not request-count-based.

## Observability Metrics

The processor exports four Prometheus counters:

Counters:

| Metric | Description |
|--------|-------------|
| `otelcol_processor_ratelimit_received_items`  | Total items received by the processor |
| `otelcol_processor_ratelimit_allowed_items`   | Items that passed the rate limit check |
| `otelcol_processor_ratelimit_denied_items`    | Items that exceeded the rate limit |
| `otelcol_processor_ratelimit_preserved_items` | Critical items forwarded via priority bypass (error spans / error+ logs) |
| `otelcol_processor_ratelimit_backend_errors`  | Transient storage-backend errors (e.g. Redis unreachable while fail-open): **alert on this** when using Redis |
| `otelcol_processor_ratelimit_fail_open_items` | Items allowed through because the backend was unreachable (fail-open) and thus **not** rate-limited: shows how much traffic bypassed the limit during a Redis outage |
| `otelcol_processor_ratelimit_bucket_evictions` | Buckets evicted because the memory backend hit `storage.max_buckets`: a nonzero rate means the cap is being reached |

Observable gauges (sampled at scrape time, like the collector's exporter `queue_size`/`queue_capacity`): emitted **only for keys in `metrics_key_allowlist`**, so cardinality stays bounded:

| Metric | Description |
|--------|-------------|
| `otelcol_processor_ratelimit_bucket_capacity_tokens`  | Token-bucket capacity (= configured limit) per watched key |
| `otelcol_processor_ratelimit_bucket_available_tokens` | Tokens currently free; **occupied = capacity - available** (drained ⇒ throttling now) |

Each measurement carries these labels:

| Label | Values | Notes |
|-------|--------|-------|
| `signal`     | `traces`, `metrics`, `logs` | always present (counters) |
| `priority`   | `all`, `normal`, `critical` | always present (counters) |
| `limit_type` | `service_name`, `attribute`, `header` | always present: lets you separate per-service from per-tenant/per-client series |
| `key`        | the extracted rate-limit key (or `other`) | **conditional**: see [Controlling metric cardinality](#controlling-metric-cardinality) |

> Counting model: `received = allowed + denied` holds in **both** modes. `allowed` counts items *admitted by the limiter* (including the `preserved` critical bypass); `denied` counts items over the limit. In monitor mode (`drop_on_limit: false`) denied items are still forwarded downstream, but they are counted in `denied` only — so the same dashboards work in either mode.

Metrics are available at the collector's built-in telemetry endpoint (default: `http://localhost:8888/metrics`).

> **Naming note:** the names above are the instrument names. Whether the Prometheus exposition appends a `_total` suffix to counters depends on your `service.telemetry.metrics` reader configuration (the collector's default internal exporter does **not** append it). Query whichever form your `/metrics` endpoint shows.

### Controlling metric cardinality

The `key` label carries a client-controlled value (service name / attribute / header). With many distinct keys (e.g. 20k services) that is a cardinality bomb: each key × signal × priority becomes its own active series, *per replica*. Composable levers keep this bounded:

**0. `metrics_verbosity`: the single on/off switch.**

```yaml
metrics_verbosity: basic     # default: detailed
```

`basic` removes the `key` label from **all** counters (aggregated by signal/priority/limit_type only) and disables the per-key token-bucket gauges — worst-case series count drops to a handful per signal, no matter how many keys exist. Rate limiting itself is unaffected (every key keeps its own bucket), and per-key attribution remains available in the throttled over-limit warn logs. Use the finer levers below when you want per-key visibility with bounded cost; use `basic` when the metrics backend is cardinality-sensitive, and see [metric.md](metric.md) for the full metrics reference.

**1. `key_labels_on`: which counters carry the `key` label at all.**

```yaml
key_labels_on: [denied, preserved]   # default
```

Counters **not** in this list are emitted aggregated (no `key` label), so their series count is fixed no matter how many keys flow through. The default keeps `key` only on `denied` and `preserved`: the counters produced solely by the *few* keys that actually hit the limit or shed errors (a naturally bounded set), while `received`/`allowed` (produced by *every* key on *every* batch) stay aggregated. You get "who is being throttled / who is shedding errors" without a series per idle service.

Valid entries: `received`, `allowed`, `denied`, `preserved`. Set all four to restore the legacy "`key` on every counter" behavior:

```yaml
key_labels_on: [received, allowed, denied, preserved]
```

**2. `metrics_key_allowlist`: bound the values of the `key` label.**

Wherever a `key` label *is* kept, only listed keys keep their real value; every other key collapses to `key="other"`. This caps cardinality at `len(allowlist) + 1` even on counters that carry `key`:

```yaml
metrics_key_allowlist:
  - payment-service
  - legacy-service
```

**3. `max_metric_keys`: the always-on backstop.**

When no allowlist is configured, the processor still refuses to export unbounded label values: the first `max_metric_keys` distinct keys (default **100**, first-come so series stay stable) keep their value, and every key after that is reported as `key="other"`. This is the defense-in-depth guard against a producer-controlled cardinality bomb — key values come from `service.name` / attributes / headers, all of which a client can spray with unique values. Set it negative to disable (only safe when the key population is small and trusted); it is ignored when `metrics_key_allowlist` is set, since the allowlist is already a bound.

> For the long tail you still want to investigate, lean on the warning logs below (they always include the full `key`) rather than metrics: high cardinality belongs in logs/traces, not time series.

### Header-based limiting

`limit_type: header` reads the key from gRPC metadata / HTTP headers via the collector's `client.Info`. The OTLP receiver only propagates request metadata to processors when **`include_metadata: true`** is set; without it, every request silently falls into the `"default"` bucket:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
        include_metadata: true
      http:
        endpoint: 0.0.0.0:4318
        include_metadata: true

processors:
  ratelimit:
    limit_type: header
    header_key: X-Tenant-ID
    requests_per_second: 100
    drop_on_limit: true
```

## Warning Logs

When the rate limit is exceeded, the processor emits a warning log:

```
warn  Rate limit exceeded for traces; dropping non-critical spans  {"key": "payment-service", "dropped": 15, "preserved_critical": 0, "suppressed_warnings_since_last": 0}
```

These warnings are **throttled to at most one per 10s** per processor: under a sustained overload (exactly when the limiter is active) logging every batch would flood the log pipeline. `suppressed_warnings_since_last` reports how many warnings were collapsed since the previous line. The drop **counts stay exact in the metrics** (`otelcol_processor_ratelimit_denied_items`): use those, not the logs, for measurement.

## Client response on rejection

When `drop_on_limit: true` and a batch is **fully** dropped, the error returned
to the producer depends on `rejection_mode`:

- **`throttle` (default)**: a gRPC `RESOURCE_EXHAUSTED` status carrying
  `RetryInfo` with a suggested delay (derived from the key's refill rate,
  clamped to `[100ms, 30s]`). The collector's OTLP receivers propagate the gRPC
  code as-is and map it to **HTTP 429 Too Many Requests**. This is the
  throttling signal prescribed by the OTLP specification: compliant SDK
  exporters back off and retry, so telemetry is *delayed* rather than lost.
- **`permanent`**: a permanent consumer error, mapped to a **non-retryable**
  status (HTTP ~400). Compliant producers drop the batch and move on. Choose
  this when client retries are worse than data loss, e.g. abusive or
  uncontrolled producers where any retry amplifies the overload.

Partial drops (some items fit) return success, so the producer is not signaled;
the dropped overflow is visible in `otelcol_processor_ratelimit_denied_items`.

## Full Pipeline Example

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 100
    drop_on_limit: true
    specific_limits:
      high-priority-service:
        requests_per_second: 1000
      low-priority-service:
        requests_per_second: 10

  batch:
    timeout: 10s
    send_batch_size: 1024

exporters:
  otlp:
    endpoint: backend:4317

service:
  telemetry:
    metrics:
      level: detailed
      readers:
        - pull:
            exporter:
              prometheus:
                host: 0.0.0.0
                port: 8888
  pipelines:
    traces:
      receivers: [otlp]
      processors: [ratelimit, batch]
      exporters: [otlp]
    metrics:
      receivers: [otlp]
      processors: [ratelimit, batch]
      exporters: [otlp]
    logs:
      receivers: [otlp]
      processors: [ratelimit, batch]
      exporters: [otlp]
```

## Performance

- **Memory overhead:** ~200 bytes per active bucket
- **CPU overhead:** O(1) per request (token bucket check with mutex)
- **Thread-safe:** all bucket operations are protected by mutexes
- **Auto-cleanup:** idle buckets are removed after 10 minutes of inactivity (checked every 5 minutes)

## E2E Tests with telemetrygen

The `e2e/` folder contains a fast test suite that starts the collector locally, uses [telemetrygen](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/cmd/telemetrygen) to generate load, and validates the Prometheus metrics to confirm rate limiting behaviour.

### Prerequisites

```bash
# 1. Build the collector binary (from project root)
make build

# 2. Install telemetrygen
go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest

# 3. Create Python virtualenv and install dependencies
cd processor/ratelimitprocessor/e2e
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Running the tests

```bash
cd processor/ratelimitprocessor/e2e
source .venv/bin/activate

# Run all scenarios
python3 run_tests.py

# Run a single scenario by ID
python3 run_tests.py --scenario svc_rps_drop

# Skip saving the Markdown report
python3 run_tests.py --no-report

# Save the report to a custom directory
python3 run_tests.py --results-dir /tmp/ratelimit-results
```

### Scenarios covered

| # | ID | Configuration | What is validated |
|---|----|---------------|-------------------|
| 1 | `svc_rps_drop` | `service_name`, 5 RPS, `drop_on_limit: true` | denied > 0, drop works |
| 2 | `svc_specific_lower` | `specific_limits` reduces `legacy-service` to 2 RPS | denied > 0, small allowed |
| 3 | `svc_specific_higher` | `specific_limits` raises `premium-service` to 50 RPS | allowed >> low-limit scenario |
| 4 | `monitor_mode` | `drop_on_limit: false` | all pass through (allowed ≈ received) |
| 5 | `rpm_limit` | `requests_per_minute: 30` | partial allow from initial bucket |
| 6 | `rpm_specific_override` | `specific_limits` per-minute for `slow-service` (10 RPM) | denied > 0 |
| 7 | `default_limit_second` | `default_limit: 5`, `default_limit_period: second` | denied > 0 (same behaviour as RPS) |
| 8 | `default_limit_minute` | `default_limit: 10`, `default_limit_period: minute` | denied > 0 |
| 9 | `attr_missing_fallback` | `limit_type: attribute`, key absent → `"default"` bucket | denied > 0 |

### Output

The runner prints colour-coded results to the terminal and saves a Markdown report to `e2e/results/report_TIMESTAMP.md`:

```
============================================================
  Rate Limit Processor: E2E Tests
============================================================

[1/9] service_name: RPS with drop_on_limit: true
  ID: svc_rps_drop | Port: 4320
  ✓ PASS | received=62 allowed=2 denied=60

[2/9] ...

============================================================
  Summary
============================================================
  Total:   9
  Passed:  9
  Report saved to: e2e/results/report_20260315_103045.md
```

### File structure

```
e2e/
├── scenarios.yaml         # Test scenario definitions
├── collector-config.yaml  # Collector config (9 isolated pipelines, ports 4320-4328)
├── run_tests.py           # Runner: starts collector, runs telemetrygen, checks metrics
├── requirements.txt       # Python dependencies (pyyaml)
└── results/               # Generated Markdown reports (gitignored)
```

### Adding a new scenario

1. Add an entry to `e2e/scenarios.yaml` with a unique `id`, `port`, `signal`, `telemetrygen`, and `expected`
2. Add the corresponding receiver, processor, and pipeline to `e2e/collector-config.yaml`
3. Run `python3 run_tests.py --scenario <id>` to validate

## Roadmap

- [x] Redis backend for distributed rate limiting across collector replicas
- [ ] Redis Cluster / Sentinel topology support (current backend targets a single endpoint)
- [ ] IP-based rate limiting
- [ ] Configurable burst allowance
- [ ] Pre-built Grafana dashboard
