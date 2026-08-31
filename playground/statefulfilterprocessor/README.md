# statefulfilter processor — test scenarios

Isolated lab for the
[`statefulfilter`](../../processor/statefulfilterprocessor/README.md) processor:
drop rules that live in **Redis**, not in the collector config. One `HSET` and
every replica converges within `refresh_interval` — no restart, no rollout.

[`collector-config.yaml`](collector-config.yaml) has a single OTLP receiver on
port **4318** feeding traces, metrics and logs through the processor. Everything
interesting happens in Redis.

| Scenario | What it proves |
|----------|----------------|
| [1. Baseline](#1-baseline-no-rules) | No rules = pass-through |
| [2. Drop a service live](#2-drop-a-service-live) | A rule written to Redis applies within seconds, with no restart |
| [3. Item-level filtering](#3-item-level-filtering) | Only the matching spans are removed, the rest of the batch survives |
| [4. keep beats drop](#4-keep-beats-drop) | A `keep` rule short-circuits every drop rule |
| [5. Expiry](#5-expiry) | `expires_at` bounds an incident-response rule automatically |
| [6. Invalid rule](#6-invalid-rule) | A broken rule is rejected on its own, the rest keep working |
| [7. Redis down](#7-redis-down) | Fail-stale: the last known rules stay in force |

## Run

```bash
docker compose up -d --build     # first build takes a few minutes (OCB)
curl -s -o /dev/null -w '%{http_code}\n' localhost:13133   # 200 = ready
```

`refresh_interval` is set to **3s** here (default is 10s), so a new rule applies
almost immediately. Tear down with `docker compose down`.

## How to read a result

```bash
./send.sh 4318 SIGNAL COUNT [resource.key=value ...]     # ATTR=k=v for item attributes
```

The producer always gets **HTTP 200**: dropping is a decision made by the
collector, not an error the client should retry. What survived is visible in the
debug exporter output and in the metrics:

```bash
docker compose logs gateway | grep '"spans"'
curl -s localhost:8888/metrics | grep statefulfilter_
```

Rules are written straight to Redis (`scripts/filter-rules.sh` in the repo root
wraps the same two keys):

```bash
docker compose exec redis redis-cli HSET otelcol:filter:rules <id> '<json>'
docker compose exec redis redis-cli INCR otelcol:filter:rules:version
```

The `INCR` is what makes polling cheap — replicas only re-read the whole hash
when the version moves. Forgetting it is survivable: `full_resync_every: 2`
picks the change up on the next full read.

---

## 1. Baseline: no rules

```bash
./send.sh 4318 traces 3 service.name=checkout   # HTTP 200, 3 spans exported
curl -s localhost:8888/metrics | grep statefulfilter_rules_loaded
# otelcol_processor_statefulfilter_rules_loaded 0
```

## 2. Drop a service live

The collector keeps running while the rule is installed:

```bash
docker compose exec redis redis-cli HSET otelcol:filter:rules drop-noisy '{
  "description":"mute noisy-service",
  "signals":["traces"],
  "conditions":[{"source":"resource","key":"service.name","op":"equals","value":"noisy-service"}]}'
docker compose exec redis redis-cli INCR otelcol:filter:rules:version

sleep 4
./send.sh 4318 traces 5 service.name=noisy-service   # dropped
./send.sh 4318 traces 2 service.name=checkout        # untouched
```

```
otelcol_processor_statefulfilter_dropped_items{rule="drop-noisy",signal="traces"} 5
otelcol_processor_statefulfilter_rules_loaded  1
otelcol_processor_statefulfilter_rules_version 1
```

`rules_version` is the alerting hook: if it differs across replicas, the fleet
is out of sync.

## 3. Item-level filtering

Filtering is per span / log record / metric **data point**, not per batch:

```bash
docker compose exec redis redis-cli HSET otelcol:filter:rules drop-healthz '{
  "description":"kube probes",
  "signals":["traces"],
  "conditions":[{"source":"attribute","key":"http.route","op":"prefix","value":"/healthz"}]}'
docker compose exec redis redis-cli INCR otelcol:filter:rules:version

sleep 4
ATTR=http.route=/healthz/live ./send.sh 4318 traces 4 service.name=checkout   # all 4 dropped
ATTR=http.route=/api/orders   ./send.sh 4318 traces 3 service.name=checkout   # all 3 kept
docker compose logs gateway | grep '"spans"' | tail -1   # "spans": 3
```

Conditions inside a rule are ANDed, rules are ORed. Sources: `resource`,
`attribute`, `name`, `body`, `severity`, `scope`. Operators: `equals`,
`not_equals`, `contains`, `prefix`, `suffix`, `regex`, `exists`, `not_exists`.

## 4. keep beats drop

`keep` rules are evaluated before every drop rule — the exception you add when
one service must never be filtered:

```bash
docker compose exec redis redis-cli HSET otelcol:filter:rules keep-payments '{
  "action":"keep",
  "signals":["traces"],
  "conditions":[{"source":"resource","key":"service.name","op":"equals","value":"payments"}]}'
docker compose exec redis redis-cli INCR otelcol:filter:rules:version

sleep 4
ATTR=http.route=/healthz/live ./send.sh 4318 traces 4 service.name=payments
```

```
otelcol_processor_statefulfilter_kept_items{signal="traces"} 4
```

The spans match `drop-healthz` but are rescued by the keep rule.

## 5. Expiry

An incident-response drop should not outlive the incident. Rewrite the rule with
an `expires_at` in the past and it stops applying:

```bash
docker compose exec redis redis-cli HSET otelcol:filter:rules drop-noisy '{
  "signals":["traces"],
  "expires_at":"2020-01-01T00:00:00Z",
  "conditions":[{"source":"resource","key":"service.name","op":"equals","value":"noisy-service"}]}'
docker compose exec redis redis-cli INCR otelcol:filter:rules:version

sleep 4
./send.sh 4318 traces 3 service.name=noisy-service   # exported again
```

The rule is still counted in `rules_loaded` — it exists, it just no longer
matches. `"enabled":false` does the same thing without a deadline; `HDEL` on the
hash field removes it for good.

## 6. Invalid rule

A rule with no conditions would match everything, so it is refused — and refused
**individually**, without blanking the rest of the set:

```bash
docker compose exec redis redis-cli HSET otelcol:filter:rules broken '{"signals":["traces"],"conditions":[]}'
docker compose exec redis redis-cli INCR otelcol:filter:rules:version

sleep 4
curl -s localhost:8888/metrics | grep statefulfilter_rules_invalid
# otelcol_processor_statefulfilter_rules_invalid 1
```

```
warn  Ignoring invalid filter rule  {"rule_id": "broken",
      "error": "conditions must not be empty (a rule matching everything is refused)"}
```

The other rules keep dropping normally. Alert on `rules_invalid > 0`: a rule
that silently does not apply is the worst failure mode here.

## 7. Redis down

Rule loading is off the data path, so an outage costs propagation delay, not
latency — and the last known rules stay in force (fail-stale, not fail-empty):

```bash
docker compose stop redis
sleep 12
ATTR=http.route=/healthz/live ./send.sh 4318 traces 3 service.name=checkout   # still dropped

curl -s localhost:8888/metrics | grep -E 'statefulfilter_(rules_age_seconds|refresh_errors|rules_loaded) '
# rules_age_seconds 15.1   <- climbing: this replica is enforcing stale rules
# refresh_errors    4
# rules_loaded      3      <- rules were NOT dropped

docker compose start redis
sleep 6
curl -s localhost:8888/metrics | grep statefulfilter_rules_age_seconds
# back to ~0: refresh recovered on its own
```
