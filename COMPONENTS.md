# Components

The selected *receivers, exporters, extensions, connectors*, and *processors* were chosen specifically for gateway-class workloads. Suggestions for additional components are welcome and can be submitted through a pull request.

## Receivers

| Component | Origin | Description |
|-----------|--------|-------------|
| `otlp` | upstream | Ingests traces, metrics, and logs via gRPC/HTTP |
| `kafka` | contrib | Consumes telemetry from Kafka topics |

## Processors

| Component | Origin | Description |
|-----------|--------|-------------|
| `memory_limiter` | upstream | Hard safety valve; must be the first processor in every pipeline |
| **`ratelimit`** | **custom** | Token-bucket rate limiting per service / attribute / header, with optional Redis-backed shared state |
| **`statefulfilter`** | **custom** | Drop rules kept in Redis instead of the config; one write reaches the whole fleet, no restart |
| `filter` | contrib | Drops telemetry matching OTTL expressions (e.g. healthcheck spans) |
| `redaction` | contrib | Strips/masks PII-looking attribute values before export |
| `attributes` | contrib | Cheap span/log/metric attribute mutations (insert/update/delete/hash) |
| `resource` | contrib | Same shape as `attributes`, applied at the resource level |
| `transform` | contrib | Full OTTL expression language; use when `attributes`/`resource` can't express the rule |
| `tail_sampling` | contrib | Samples after the full trace is assembled: keep error/slow traces, downsample the healthy bulk |
| `batch` | upstream | Batches telemetry before export |

## Connectors

| Component | Origin | Description |
|-----------|--------|-------------|
| `routing` | contrib | Per-tenant fan-out by OTTL match on resource/attribute/context. The gateway's signature primitive |
| `forward` | upstream | Composes pipelines: shared receive stage feeding multiple downstream branches |
| `failover` | contrib | Health-based backend failover: level 1 while it accepts data, next level takes over on failure, higher levels retried periodically |
| `round_robin` | contrib | Alternates batches across equivalent downstream pipelines, no external load balancer needed |
| `span_metrics` | contrib | Derives RED metrics (calls, duration) from the spans crossing the gateway, so producers don't have to emit them |
| `count` | contrib | Cheap volume/error counters from any signal; keeps ingestion visible even when the payloads are sampled away |

## Exporters

| Component | Origin | Description |
|-----------|--------|-------------|
| `otlp` | upstream | Forwards to any OTLP-compatible backend (gRPC) |
| `otlphttp` | upstream | Same, over OTLP/HTTP (required by backends that don't speak gRPC) |
| `kafka` | contrib | Publishes telemetry to Kafka topics |
| `loadbalancing` | contrib | Distributes load across multiple OTLP endpoints |
| `prometheusremotewrite` | contrib | Writes metrics to Prometheus-compatible TSDBs (Mimir, Thanos, VictoriaMetrics) |
| `debug` | upstream | Prints telemetry to stdout (dev only) |

## Extensions

| Component | Origin | Description |
|-----------|--------|-------------|
| `health_check` | contrib | HTTP readiness endpoint for LBs (default :13133) |
| `pprof` | contrib | Go runtime profiles for post-hoc analysis (default :1777) |
| `zpages` | upstream | In-process debug pages (`/debug/tracez`, `/debug/pipelinez`) |
| `file_storage` | contrib | Persistent state backend, so exporter `sending_queue` survives restarts |
| `basicauth` | contrib | Server-side HTTP basic-auth for OTLP producers |
| `bearertokenauth` | contrib | Server-side bearer-token auth for OTLP producers |
| `oauth2client` | contrib | Client-credentials OAuth2 for exporters talking to protected backends |
| `opamp` | contrib | Remote management (config/agent lifecycle) via an OpAMP server |
| `headers_setter` | contrib | Injects tenant headers (e.g. `X-Scope-OrgID`) when fanning out per-tenant |
