# statefulfilter processor

Drops telemetry according to rules that live in **Redis**, not in the collector
config.

The contrib `filter` processor is excellent, but its OTTL expressions are frozen
into `otelcol.yaml`: muting a service that started flooding the pipeline at 3am
means editing a config, running a deploy and rolling every replica. With
`statefulfilter` the rules are shared state — one `HSET` + `INCR` and all N
collectors converge within `refresh_interval`. No restart, no rollout.

It is the filtering counterpart to this repo's `ratelimit` processor with
`storage.backend: redis`: same Redis, same fleet-wide-state idea, different
control — one keeps drop rules, the other keeps token buckets. Both can share a
single Redis instance: their key prefixes are distinct (`otelcol:filter:*` vs
`otelcol:ratelimit:*`).

```
                    ┌──────────────┐
   redis-cli / API ─┤    Redis     ├─ rules hash + version counter
                    └──────┬───────┘
             poll (1 GET)  │  every refresh_interval
        ┌─────────┬────────┼────────┬─────────┐
     gw-1      gw-2      gw-3     ...       gw-20     ← all agree on one rule set
```

## Why the rules are off the data path

`ConsumeTraces` never talks to Redis. A background goroutine polls and publishes
an immutable snapshot through a single atomic pointer; the hot path is one
pointer load. Consequences worth knowing:

- A Redis outage costs **propagation delay, not latency**. Data-path p99 is
  unaffected.
- Polling is **version-gated**: each cycle is one `GET` of the version key, and
  the full `HGETALL` only runs when the version actually moved. A 20-replica
  fleet at the default 10s interval costs Redis ~2 trivial GETs per second.
- On refresh failure the processor is **fail-stale, not fail-empty** — the last
  known rules stay in force. Rules someone deliberately installed must not
  silently stop applying because the rule store hiccuped.

## Configuration

```yaml
processors:
  statefulfilter:
    redis:
      addr: redis:6379            # or addrs: [...] for cluster/sentinel
      key_prefix: otelcol:filter  # reads <prefix>:rules and <prefix>:rules:version
      # password: ${env:REDIS_PASSWORD}
      # db: 0
      # timeout: 2s
      # tls:
      #   ca_file: /etc/otelcol-gateway/tls/redis-ca.crt
    refresh_interval: 10s         # worst-case rule propagation delay
    full_resync_every: 6          # force a full re-read every Nth poll
    wait_for_initial_load: true   # don't accept traffic in a no-rules state
    initial_load_timeout: 5s
    max_rules: 500
    fail_closed_on_empty: false   # refuse to start if the first load fails
```

| Field | Default | Notes |
|-------|---------|-------|
| `redis.addr` / `redis.addrs` | — | **Required.** `addrs` + `master_name` = Sentinel; `addrs` alone (len>1) = Cluster. |
| `redis.key_prefix` | `otelcol:filter` | Namespaces the two keys so this can share a Redis with the rate limiter. |
| `redis.timeout` | `2s` | Per-fetch deadline. Generous because this call is off the data path. |
| `redis.password` | — | `configopaque`, redacted from config dumps and logs. |
| `redis.tls` | — | Standard collector TLS block. Rules describe your estate; encrypt them in transit. |
| `refresh_interval` | `10s` | Also the worst-case time for a new rule to reach every replica. |
| `full_resync_every` | `6` | Full `HGETALL` every Nth poll, so a rule written by hand (`HSET` without `INCR`) still converges. `1` = always full sync. |
| `wait_for_initial_load` | `true` | Blocks startup until the first fetch succeeds or times out. |
| `initial_load_timeout` | `5s` | On timeout the collector starts with no rules and keeps retrying. |
| `max_rules` | `500` | Backstop against an unbounded rule hash becoming per-item CPU cost. Truncation is sorted-by-id, so every replica truncates identically. |
| `fail_closed_on_empty` | `false` | `true` refuses startup when the first load fails. Only for compliance-grade drops (PII scrubbing) where forwarding-by-accident is unacceptable. |

## Rule format

Rules are JSON documents in a Redis **hash**, one field per rule:

```
<key_prefix>:rules          HASH    field = rule id, value = rule JSON
<key_prefix>:rules:version  STRING  counter; bump it on every write
```

```json
{
  "id": "drop-healthz",
  "description": "kube probes flooding the trace backend",
  "enabled": true,
  "action": "drop",
  "signals": ["traces"],
  "conditions": [
    {"source": "resource",  "key": "service.name", "op": "equals", "value": "checkout"},
    {"source": "attribute", "key": "http.route",   "op": "prefix", "value": "/healthz"}
  ],
  "expires_at": "2026-08-06T00:00:00Z"
}
```

| Field | Default | Meaning |
|-------|---------|---------|
| `id` | the hash field name | Used as the `rule` label on the drop metric. |
| `enabled` | `true` | `false` keeps the document around without applying it. |
| `action` | `drop` | `keep` marks an exception — see below. |
| `signals` | all three | Any of `traces`, `metrics`, `logs`. |
| `conditions` | — | **Required, non-empty.** ANDed together. |
| `expires_at` | never | RFC3339. Incident-response drops shouldn't outlive the incident. |

**Semantics**

- Conditions within a rule are **AND**; rules are **OR**.
- `keep` rules are evaluated **before every drop rule** and short-circuit them.
  Order among rules of the same action is sorted-by-id, identical on every
  replica.
- An empty `conditions` list is **rejected**: a rule matching everything is
  almost never intended, and as a drop rule it is an outage.
- One malformed rule is rejected **individually** — it never blanks the rest of
  the set — and shows up in `rules_invalid` plus a warn log.

### Condition sources

| `source` | Reads | Signals |
|----------|-------|---------|
| `resource` | resource attribute at `key` (`service.name`, `k8s.*`) | all |
| `attribute` | span / log record / metric **data point** attribute at `key` | all |
| `name` | span name, metric name | traces, metrics |
| `body` | log record body, stringified | logs |
| `severity` | log `severity_text` | logs |
| `scope` | instrumentation scope name | all |

### Condition operators

| `op` | Matches when |
|------|--------------|
| `equals` *(default)* | value equals `value`, or any entry of `values` |
| `not_equals` | field **exists** and differs from `value`/`values` |
| `contains` / `prefix` / `suffix` | substring / prefix / suffix of `value` |
| `regex` | RE2 match of `value` (use inline `(?i)` for case-insensitivity) |
| `exists` / `not_exists` | field is present / absent |

`ignore_case: true` folds both sides for `equals`, `not_equals`, `contains`,
`prefix` and `suffix`.

> `not_equals` deliberately does **not** match an absent field. Otherwise one
> typo'd attribute key turns a targeted rule into "drop everything". Use
> `not_exists` when absence is what you mean.

### Granularity

Filtering happens per **item**, not per batch: individual spans, individual log
records, and individual metric **data points**. A rule scoped to
`http.route=/healthz` removes those data points and leaves the rest of the
series intact. Containers that end up empty (scope, resource, metric) are
cleaned up, and a batch that loses everything is accepted and discarded rather
than forwarded empty or rejected with an error — the producer did nothing
wrong, and an error would make a compliant SDK retry data you meant to drop.

## Managing rules

`scripts/filter-rules.sh` wraps the two keys:

```bash
export REDIS_CLI="redis-cli -h redis.internal"     # or: docker exec -i redis redis-cli
export KEY_PREFIX=otelcol:filter                    # must match redis.key_prefix

./scripts/filter-rules.sh add drop-healthz '{
  "signals":["traces"],
  "conditions":[{"source":"attribute","key":"http.route","op":"prefix","value":"/healthz"}]}'

./scripts/filter-rules.sh list
./scripts/filter-rules.sh disable drop-healthz
./scripts/filter-rules.sh rm drop-healthz
```

Raw equivalent — note the version bump, which is what lets replicas poll cheaply:

```bash
redis-cli HSET otelcol:filter:rules drop-healthz '{"conditions":[...]}'
redis-cli INCR otelcol:filter:rules:version
```

Forgetting the `INCR` is survivable: `full_resync_every` picks the change up on
the next periodic full read (60s at the defaults).

## Metrics

| Metric | Type | Labels | Use |
|--------|------|--------|-----|
| `otelcol_processor_statefulfilter_evaluated_items` | counter | `signal` | Denominator for "what fraction are we dropping". |
| `otelcol_processor_statefulfilter_dropped_items` | counter | `signal`, `rule` | Per-rule blast radius. Bounded cardinality: `rule` is operator-authored and capped by `max_rules`. |
| `otelcol_processor_statefulfilter_kept_items` | counter | `signal` | Items rescued by a `keep` rule. |
| `otelcol_processor_statefulfilter_rules_loaded` | gauge | — | Active rules on this replica. |
| `otelcol_processor_statefulfilter_rules_invalid` | gauge | — | Rejected documents. **Alert on `> 0`** — a rule that silently doesn't apply is the worst failure mode here. |
| `otelcol_processor_statefulfilter_rules_version` | gauge | — | Applied version. **Alert on `max != min` across replicas** — that is a fleet out of sync. |
| `otelcol_processor_statefulfilter_rules_age_seconds` | gauge | — | Seconds since the last successful refresh; `-1` = never loaded. Climbing means stale enforcement. |
| `otelcol_processor_statefulfilter_refresh_errors` | gauge | — | Running total of failed refreshes. |

Suggested alerts:

```promql
# a replica stopped seeing rule updates
max(otelcol_processor_statefulfilter_rules_version) - min(otelcol_processor_statefulfilter_rules_version) > 0

# someone pushed a rule that does not compile
otelcol_processor_statefulfilter_rules_invalid > 0

# rules are stale (Redis unreachable for >5 min)
otelcol_processor_statefulfilter_rules_age_seconds > 300
```

## Pipeline placement

```yaml
processors: [memory_limiter, statefulfilter, ratelimit, batch]
```

Before `ratelimit`: telemetry you are dropping on purpose should not consume
rate-limit budget that legitimate traffic needs. After `memory_limiter`, which
must always be first.

The traces, metrics and logs pipelines of one `statefulfilter/<name>` instance
share a single store — one Redis connection, one polling goroutine, one agreed
version across signals.

## Operational notes

- **Rules are shared state.** A bad rule reaches 20 collectors as fast as a good
  one. `expires_at` and `enabled: false` exist so a change is easy to bound and
  easy to undo.
- **Rule writes are not authenticated by this processor.** Anyone with write
  access to that Redis key can silence telemetry fleet-wide. Treat it like any
  other control plane: ACL the key prefix, and prefer a service writing rules
  over ambient `redis-cli` access.
- **Trace integrity.** Dropping individual spans can orphan children. For
  trace-shaped decisions prefer `tail_sampling`; use this processor for
  "this data is noise" (probes, debug logs, chatty metrics), which is what it is
  designed for.

## Tests

```bash
cd processor/statefulfilterprocessor && go test -race ./...
```

The store tests run against `miniredis`, so the real go-redis client, the key
layout and the version-gating logic are all exercised rather than mocked.
