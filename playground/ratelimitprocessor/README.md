# ratelimit processor — test scenarios

Isolated lab for the [`ratelimit`](../../processor/ratelimitprocessor/README.md)
processor: a token bucket per key (service, attribute or header) that drops
telemetry above the configured rate.

Each scenario has its **own receiver, processor and pipeline** in
[`collector-config.yaml`](collector-config.yaml), so a scenario never spends
another one's tokens. The port tells you which scenario you are hitting.

| Port | Scenario | What it proves |
|------|----------|----------------|
| 4318 | [1. Per-service limit](#1-per-service-limit) / [2. Per-service override](#2-per-service-override) | Excess spans are dropped; `specific_limits` beats the global limit |
| 4319 | [3. Monitor mode](#3-monitor-mode) | `drop_on_limit: false` counts the excess but forwards everything |
| 4320 | [4. Limit by attribute](#4-limit-by-attribute) | The key can be any resource attribute (`tenant.id`) |
| 4321 | [5. Error spans bypass](#5-error-spans-bypass-the-limit) | `preserve_errors` keeps error spans on an empty bucket |
| 4322 | [6. Fleet-wide limit via Redis](#6-fleet-wide-limit-via-redis) | Two replicas share one bucket instead of one each |

## Run

```bash
docker compose up -d --build     # first build takes a few minutes (OCB)
curl -s -o /dev/null -w '%{http_code}\n' localhost:13133   # 200 = ready
```

Tear down with `docker compose down`.

## How to read a result

```bash
./send.sh PORT SIGNAL COUNT [resource.key=value ...]
```

Sends **one** OTLP/HTTP request with `COUNT` items and prints the HTTP status:

- **200** — at least one item was accepted (the overflow, if any, was dropped)
- **429** — the bucket was empty, the whole batch was rejected; a compliant SDK
  backs off and retries

The processor counters are the source of truth:

```bash
curl -s localhost:8888/metrics | grep ratelimit_
```

`received = allowed + denied` holds in both modes. What actually left the
collector is in the debug exporter output:

```bash
docker compose logs gateway | grep '"spans"'
```

---

## 1. Per-service limit

`ratelimit/by-service`: 5 spans/s per `service.name`, `drop_on_limit: true`.

```bash
./send.sh 4318 traces 10 service.name=checkout   # 5 fit, 5 dropped -> HTTP 200
./send.sh 4318 traces 3  service.name=checkout   # bucket empty     -> HTTP 429
```

```
otelcol_processor_ratelimit_received_items{key="checkout",...} 13
otelcol_processor_ratelimit_allowed_items{key="checkout",...}  5
otelcol_processor_ratelimit_denied_items{key="checkout",...}   8
```

Note the cost model: **1 item = 1 token**, so a request with 10 spans consumes
10 tokens, not 1. Tokens refill continuously — wait a second and the same
request is accepted again.

## 2. Per-service override

Same processor, but `specific_limits` puts `payment-service` on 2 spans/s.

```bash
./send.sh 4318 traces 3 service.name=payment-service   # 2 fit, 1 dropped -> HTTP 200
./send.sh 4318 traces 2 service.name=payment-service   # bucket empty     -> HTTP 429
```

`checkout` is untouched by this: every key has its own bucket.

## 3. Monitor mode

`ratelimit/monitor`: 3 logs/s with `drop_on_limit: false` — the dry-run you use
before enforcing a limit in production.

```bash
./send.sh 4319 logs 10 service.name=noisy-service   # HTTP 200
```

```
otelcol_processor_ratelimit_received_items{key="noisy-service",signal="logs"} 10
otelcol_processor_ratelimit_allowed_items{key="noisy-service",signal="logs"}   3
otelcol_processor_ratelimit_denied_items{key="noisy-service",signal="logs"}    7
```

7 items are counted as denied, and all 10 still reach the exporter — check
`docker compose logs gateway | grep '"log records"'`. Flip `drop_on_limit` to
`true` and the same dashboards keep working.

## 4. Limit by attribute

`ratelimit/by-tenant`: key is the `tenant.id` resource attribute, 5 spans/s,
`tenant-free` capped at 2.

```bash
./send.sh 4320 traces 3 tenant.id=tenant-free   # 2 fit, 1 dropped
./send.sh 4320 traces 5 tenant.id=tenant-gold   # fits
```

```
otelcol_processor_ratelimit_denied_items{key="tenant-free",limit_type="attribute",...} 1
```

Telemetry without `tenant.id` falls into the shared `key="default"` bucket.

## 5. Error spans bypass the limit

`ratelimit/priority`: 2 spans/s with `preserve_errors: true`. Under overload you
still want the failures.

```bash
for i in 1 2 3; do ./send.sh 4321 traces 4 service.name=checkout; done
#  -> HTTP 200, then 429, then 429: the bucket is empty

STATUS=error ./send.sh 4321 traces 4 service.name=checkout
#  -> HTTP 200 even with an empty bucket
```

```
otelcol_processor_ratelimit_preserved_items{key="checkout",priority="critical",...} 4
```

Error spans are counted in `preserved_items` (and in `allowed_items`), never in
`denied_items`. The same rule applies to logs at severity ERROR and above.

## 6. Fleet-wide limit via Redis

`ratelimit/shared`: same 5 spans/s, but `storage.backend: redis`. With the
default in-memory backend, 2 replicas would allow 2 × 5 spans/s; here they share
one bucket. `gateway-b` runs the same config on port **4323**.

```bash
./send.sh 4322 traces 3 service.name=checkout   # replica A takes 3 tokens
./send.sh 4323 traces 3 service.name=checkout   # replica B finds only 2 left
```

```bash
curl -s localhost:8889/metrics | grep -E 'ratelimit_(allowed|denied)_items.*checkout'
# allowed_items ... 2
# denied_items  ... 1
```

The bucket is a Redis hash — which doubles as a live inventory of who is talking
to the fleet:

```bash
docker compose exec redis redis-cli --scan --pattern 'otelcol:ratelimit:*'
docker compose exec redis redis-cli HGETALL otelcol:ratelimit:checkout
# t = tokens left, r = last refill (µs epoch)
```

Redis is fail-open by default: `docker compose stop redis` and traffic keeps
flowing unlimited, with `otelcol_processor_ratelimit_backend_errors` and
`fail_open_items` climbing. Start it again and limiting resumes on its own.
