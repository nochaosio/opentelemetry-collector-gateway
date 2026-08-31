# Testing the `ratelimit` processor

Hands-on recipes to reproduce every supported scenario on your laptop in under a minute. Each scenario gives you a minimal YAML, a traffic command, the Prometheus query, and the number you should see — so you can spot regressions at a glance.

For the automated Python suite (9 canned scenarios) see [`e2e/`](e2e/). This document is for **ad-hoc manual verification**.

---

## 0. Prerequisites

```bash
# Go ≥ 1.24, make install-builder + make build at the repo root.
export PATH="$HOME/go/bin:$PATH"         # picks up builder, telemetrygen
which telemetrygen || go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest
```

Ports used throughout this doc (chosen to avoid clashing with an already-running stack):

| Purpose | Port |
|---|---|
| OTLP/gRPC ingest | 7317 |
| Prometheus scrape (collector self-telemetry) | 9888 |
| Redis (when needed) | 6379 |

Every config below assumes the binary at `./cmd/otelcol-gateway/otelcol-gateway`. Run each config with:

```bash
./cmd/otelcol-gateway/otelcol-gateway --config <file>.yaml
```

and scrape counters with:

```bash
curl -s localhost:9888/metrics | grep -E 'otelcol_processor_ratelimit_(received|allowed|denied|preserved)_items_total\{' | sort
```

---

## Scenario 1 — Basic per-service rate limit

**Goal:** confirm the default path (`limit_type: service_name`) blocks a flood to a single service.

`/tmp/rl-1.yaml`:

```yaml
receivers:
  otlp: {protocols: {grpc: {endpoint: 0.0.0.0:7317}}}
processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 10
    drop_on_limit: true
    preserve_errors: false
exporters: {debug: {verbosity: basic}}
service:
  telemetry: {metrics: {level: detailed, readers: [{pull: {exporter: {prometheus: {host: 0.0.0.0, port: 9888}}}}]}, logs: {level: warn}}
  pipelines: {traces: {receivers: [otlp], processors: [ratelimit], exporters: [debug]}}
```

**Drive:**
```bash
telemetrygen traces --otlp-endpoint localhost:7317 --otlp-insecure \
    --service svc-a --workers 4 --rate 0 --duration 5s
```

**Expect:** `allowed` ≈ `10 (burst) + 10 rps × 5s = 60` for `key="svc-a"`. `denied` in the hundreds of thousands.

```text
…allowed_items_total{key="svc-a", signal="traces",…} 60
…denied_items_total{key="svc-a",  signal="traces",…} 500000+
```

---

## Scenario 2 — Per-key overrides with `specific_limits`

**Goal:** confirm the default budget applies to unknown services while overrides apply to listed ones.

Reuse the same file, replace `processors` with:

```yaml
processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 20           # default for anything not listed
    drop_on_limit: true
    preserve_errors: false
    specific_limits:
      premium-svc: {requests_per_second: 200}
      legacy-svc:  {requests_per_second: 5}
```

**Drive (three services in parallel for 5s):**
```bash
for svc in default-svc premium-svc legacy-svc; do
    telemetrygen traces --otlp-endpoint localhost:7317 --otlp-insecure \
        --service "$svc" --workers 4 --rate 0 --duration 5s &
done
wait
```

**Expect (within a few %):**

| Service | Formula | Allowed |
|---|---|---:|
| `default-svc` | 20 + 20×5 | **120** |
| `premium-svc` | 200 + 200×5 | **1 200** |
| `legacy-svc` | 5 + 5×5 | **30** |

Reference script: `benchmarks/e2e_storage_verify.sh memory`.

---

## Scenario 3 — `default_limit` + `default_limit_period`

**Goal:** confirm the alternative config path works (minute-based limit).

```yaml
processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 0            # MUST zero this; see README note on precedence
    default_limit: 30
    default_limit_period: minute
    drop_on_limit: true
    preserve_errors: false
```

**Drive (short flood — 30 per minute = ~0.5/s):**
```bash
telemetrygen traces --otlp-endpoint localhost:7317 --otlp-insecure \
    --service svc-a --workers 2 --rate 0 --duration 10s
```

**Expect:** `allowed` ≈ `30 (burst) + 30/60 × 10 = 35`.

---

## Scenario 4 — `drop_on_limit: false` (monitor mode)

**Goal:** verify excess is counted as `denied` **and** `allowed` — nothing is dropped, counters tell the story.

```yaml
processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 10
    drop_on_limit: false              # pass-through
    preserve_errors: false
```

**Drive:** same flood as Scenario 1.

**Expect:**
- `allowed_items_total ≈ received_items_total` (everything forwarded)
- `denied_items_total > 0` (the overflow is still *recorded*, just not dropped)
- Exporter `otelcol_exporter_sent_spans_total` matches `received`

This is the mode to use when rolling out a new limit: observe the denial counter for a week before flipping `drop_on_limit: true`.

---

## Scenario 5 — Priority bypass for error spans (`preserve_errors`)

**Goal:** error spans must forward **even when the bucket is empty**. Regular spans get dropped as usual.

```yaml
processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 3            # tiny budget, easy to starve
    drop_on_limit: true
    preserve_errors: true             # default is true; making it explicit
```

**Drive (telemetrygen can't mark errors; use a small Go program or the unit test as proof).** For an end-to-end sanity check, use `go test -run TestTraces_PreserveErrors_DropsOnlyNormal` — it asserts exactly this: 10 normal + 5 error → 3 normal allowed + 5 error preserved = 8 forwarded, 7 dropped.

**Expect in Prometheus when it does fire:**
```text
…preserved_items_total{key="svc-a", priority="critical", signal="traces"} > 0
```

Turn it off (`preserve_errors: false`) and the same test drops errors along with everything else.

---

## Scenario 6 — Fair share (global ceiling + `max_share_ratio`)

**Goal:** a single noisy service cannot starve others. Global budget is capped, and each key is clipped to a fraction of it.

```yaml
processors:
  ratelimit:
    limit_type: service_name
    global_requests_per_second: 100
    max_share_ratio: 0.2              # any key capped at 20 rps
    drop_on_limit: true
    preserve_errors: false
```

**Drive:** flood *one* service heavily:
```bash
telemetrygen traces --otlp-endpoint localhost:7317 --otlp-insecure \
    --service noisy --workers 8 --rate 0 --duration 5s
```

**Expect:** `allowed` for `noisy` ≈ `20 + 20×5 = 120`, not 500. The remaining global budget stays available for other services that show up later. Proven by unit test `TestFairShare_NoisyNeighborCannotStarveOthers`.

---

## Scenario 7 — Token bucket **refills** after a quiet window

**Goal:** confirm the classic token-bucket behavior — ten seconds of silence should refill a 10-rps bucket back to 10 tokens.

```yaml
processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 10
    drop_on_limit: true
    preserve_errors: false
```

**Drive — three phases with silence in between:**
```bash
# Phase A: flood 3s → bucket drains.
telemetrygen traces --otlp-endpoint localhost:7317 --otlp-insecure \
    --service svc-a --workers 4 --rate 0 --duration 3s

sleep 10   # Phase B: silence — bucket refills to full (capped at burst=10)

# Phase C: flood again 3s → should see another ~40 allowed.
telemetrygen traces --otlp-endpoint localhost:7317 --otlp-insecure \
    --service svc-a --workers 4 --rate 0 --duration 3s
```

**Expect:** the per-service `allowed_items_total` increases by roughly `10 + 10×3 = 40` on *each* of phases A and C. No "reset window" — refill is continuous.

---

## Scenario 8 — Storage: `memory` backend (replica-local)

**Goal:** prove the default path is stateless — every collector replica keeps its own view.

Run **two** collectors on different ports, each with:

```yaml
processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 10
    drop_on_limit: true
    storage: {backend: memory}        # explicit; same as unset
```

Drive a service to replica A with one telemetrygen, and simultaneously the same service to replica B with another. Scrape both Prometheus endpoints: you'll see each replica allowed ~60 (burst + rate × duration) *independently*. Real throughput = `N replicas × rps`.

**Takeaway:** memory backend is correct only when the LB in front shards by the rate-limit key, or when you run a single replica. Otherwise use Redis.

---

## Scenario 9 — Storage: `redis` backend (fleet-wide)

**Goal:** two replicas fronted by round-robin, one shared Redis → total allowed = `burst + rate × duration`, **not** 2 × that.

**Start Redis:**
```bash
docker run --rm -d --name otelcol-rl-redis -p 6379:6379 redis:7-alpine
```

**Config** (both replicas use this, different ports):

```yaml
processors:
  ratelimit:
    limit_type: service_name
    requests_per_second: 10
    drop_on_limit: true
    storage:
      backend: redis
      redis:
        addr: localhost:6379
        key_prefix: otelcol:ratelimit
        timeout: 100ms
        on_error: open
```

**Drive both replicas simultaneously:**
```bash
telemetrygen traces --otlp-endpoint localhost:7317 --otlp-insecure \
    --service svc-a --workers 4 --rate 0 --duration 5s &
telemetrygen traces --otlp-endpoint localhost:8317 --otlp-insecure \
    --service svc-a --workers 4 --rate 0 --duration 5s &
wait
```

**Expect:** sum of `allowed_items_total{key="svc-a"}` across both collectors ≈ `10 + 10×5 = 60`. With `memory` backend it would have been ≈120.

**Inspect shared state:**
```bash
docker exec otelcol-rl-redis redis-cli --scan --pattern 'otelcol:ratelimit:*'
# otelcol:ratelimit:svc-a

docker exec otelcol-rl-redis redis-cli HGETALL otelcol:ratelimit:svc-a
# t  <tokens remaining>
# r  <last refill µs>
```

Reference script: `benchmarks/e2e_storage_verify.sh redis`.

---

## Scenario 10 — Redis unreachable: fail-open vs fail-closed

**Goal:** the rate-limiter must degrade gracefully when Redis is down.

**Fail-open (default — allow traffic):**
```yaml
storage:
  backend: redis
  redis: {addr: localhost:6379, timeout: 100ms, on_error: open}
```

```bash
# 1) Start collector with Redis up → traffic is rate-limited normally.
# 2) Kill Redis:
docker stop otelcol-rl-redis
# 3) Flood: telemetrygen… you should see `allowed` ≈ received (fail-open).
# 4) Check collector logs: "Redis unavailable — failing OPEN (traffic allowed)".
```

**Fail-closed (reject traffic):**
```yaml
storage:
  backend: redis
  redis: {addr: localhost:6379, timeout: 100ms, on_error: closed}
```

Same steps — `allowed` drops to ~0 when Redis is unreachable.

Unit coverage: `TestRedisStorage_FailOpenOnDisconnect`, `TestRedisStorage_FailClosedOnDisconnect`.

---

## Scenario 11 — Redis negative cache (under-attack optimization)

**Goal:** under a sustained flood, avoid hammering Redis for a service that's already clearly over-limit.

```yaml
storage:
  backend: redis
  redis:
    addr: localhost:6379
    timeout: 100ms
    negative_cache_ttl: 50ms          # after a 0-grant, skip Redis for 50ms
```

**Drive:** heavy flood from a single service (rate 0, 10s).

**Expect:** the collector's local negative cache absorbs most decisions — Redis QPS drops dramatically compared to the baseline. The tradeoff: a service whose bucket refills mid-cooldown won't be allowed until the cache entry expires (bounded loss: 50ms of latency on first recovery).

Unit coverage: `TestRedisStorage_NegativeCache` — confirms that a 0-granted key stays 0 locally even when Redis is stopped, for the cache window.

---

## Scenario 12 — Attribute-based limiting (tenant / client ID)

**Goal:** confirm `limit_type: attribute` routes traffic to per-tenant buckets.

```yaml
processors:
  ratelimit:
    limit_type: attribute
    attribute_key: tenant.id
    requests_per_second: 10
    drop_on_limit: true
    preserve_errors: false
```

**Drive two tenants in parallel:**
```bash
telemetrygen traces --otlp-endpoint localhost:7317 --otlp-insecure \
    --service app --workers 4 --rate 0 --duration 5s \
    --otlp-attributes 'tenant.id="tenant-a"' &
telemetrygen traces --otlp-endpoint localhost:7317 --otlp-insecure \
    --service app --workers 4 --rate 0 --duration 5s \
    --otlp-attributes 'tenant.id="tenant-b"' &
wait
```

**Expect:** each tenant's bucket is independent — both show `allowed ≈ 60`. If the attribute is missing on a request, traffic falls into the `"default"` bucket (regression guard: `TestExtractKey_Default`).

---

## Scenario 13 — Header-based limiting (API-key flavor)

**Goal:** rate-limit by a gRPC/HTTP header, typical for API-gateway-style deployments.

The processor reads the header from the request's gRPC metadata / HTTP headers via `client.Info`. That requires **`include_metadata: true` on the OTLP receiver** — without it the receiver does not propagate headers to processors and every request silently falls into the `"default"` bucket.

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
    header_key: X-Client-ID
    requests_per_second: 0
    requests_per_minute: 1000
    drop_on_limit: true
```

Drive it with per-client headers, e.g. `telemetrygen traces --otlp-endpoint localhost:4317 --otlp-insecure --otlp-header 'X-Client-ID="client-a"' ...`; each distinct header value gets its own bucket (regression guard: `TestExtractKey_HeaderFromClientInfo`).

---

## Quick cleanup

```bash
# Redis (if you started one):
docker stop otelcol-rl-redis 2>/dev/null

# Collector processes launched during ad-hoc testing:
pgrep -f 'otelcol-gateway --config' | xargs -r kill
```

---

## Where each scenario lives in automated coverage

| Scenario | Unit test | E2E |
|---|---|---|
| 1 Basic | `TestTracesProcessor_Denied` | `svc_rps_drop` |
| 2 Overrides | `TestRateLimiter_SpecificLimits` | `svc_specific_lower`, `svc_specific_higher` |
| 3 `default_limit` | — | `default_limit_second`, `default_limit_minute` |
| 4 Monitor mode | `TestTracesProcessor_DeniedPassThrough` | `monitor_mode` |
| 5 Priority bypass | `TestTraces_PreserveErrors_DropsOnlyNormal`, `TestLogs_PreserveErrors_DropsOnlyNormal` | — |
| 6 Fair share | `TestFairShare_*` | — |
| 7 Token refill | implicit in every RPS assertion | — |
| 8 Memory backend | every memory-backed unit test | `benchmarks/e2e_storage_verify.sh memory` |
| 9 Redis backend | `TestRedisStorage_*`, `TestRedisStorage_WithRateLimiter_SpecificLimits` | `benchmarks/e2e_storage_verify.sh redis` |
| 10 Fail-open / closed | `TestRedisStorage_FailOpenOnDisconnect`, `TestRedisStorage_FailClosedOnDisconnect` | — |
| 11 Negative cache | `TestRedisStorage_NegativeCache` | — |
| 12 Attribute-based | `TestExtractKey_Attribute`, `TestExtractKey_Default` | `attr_missing_fallback` |
| 13 Header-based | — | — |

Every unit test: `go test -race -count=1 ./processor/ratelimitprocessor/...`
