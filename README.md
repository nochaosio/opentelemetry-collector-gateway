# OpenTelemetry Collector Gateway

[![ci](https://github.com/nochaosio/opentelemetry-collector-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/nochaosio/opentelemetry-collector-gateway/actions/workflows/ci.yml)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/nochaosio/opentelemetry-collector-gateway/badge)](https://scorecard.dev/viewer/?uri=github.com/nochaosio/opentelemetry-collector-gateway)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A contrib-based OpenTelemetry Collector distribution standing at the front door of your observability pipeline.

## Built on

Assembled with the [OpenTelemetry Collector Builder][ocb], from
[opentelemetry-collector][core] and [opentelemetry-collector-contrib][contrib].

## Components

The receivers, exporters, extensions, connectors, and processors bundled in this
distribution, and why each one is here, are listed in [COMPONENTS.md](COMPONENTS.md).

The two processors that are ours, and exist because contrib has no equivalent:

| Component | What it adds |
|-----------|--------------|
| [`ratelimit`](processor/ratelimitprocessor/) | Per-key token-bucket limiting with priority bypass, fair share, and optional Redis-backed shared state |
| [`statefulfilter`](processor/statefulfilterprocessor/) | Drop rules held in Redis, so muting a service does not need a config roll |

## Quick start

Container image (linux/amd64 and linux/arm64, signed with cosign):

```bash
docker run --rm -p 4317:4317 -p 4318:4318 -p 13133:13133 \
  ghcr.io/nochaosio/otelcol-gateway:latest
```

Binaries for Linux, macOS and Windows are attached to each
[release](https://github.com/nochaosio/opentelemetry-collector-gateway/releases).

From source:

```bash
make install-builder
make build
make run
```

## Example configs

| Config | Shows |
|--------|-------|
| [`otelcol-gateway.yaml`](config/otelcol-gateway.yaml) | Default: OTLP in, per-service rate limit |
| [`otelcol-gateway-frontdoor.yaml`](config/otelcol-gateway-frontdoor.yaml) | Token auth, Jaeger/Zipkin/remote-write/Fluent ingest, sampling, redaction |
| [`otelcol-gateway-connectors.yaml`](config/otelcol-gateway-connectors.yaml) | span metrics, count, failover, round robin |
| [`otelcol-gateway-redis.yaml`](config/otelcol-gateway-redis.yaml) | Fleet-wide rate limit through Redis |
| [`otelcol-gateway-statefulfilter.yaml`](config/otelcol-gateway-statefulfilter.yaml) | Drop rules held in Redis |

## Benchmarks

Measured against `otelcol-contrib` on the same host, same cores, same workload:
identical throughput, 57% less memory, and two controls contrib does not have.
Numbers and method in [benchmarks/README.md](benchmarks/README.md).

## Recommended pipeline order

```yaml
processors: [memory_limiter, filter/*, statefulfilter, redaction/*, attributes/*, ratelimit, batch]
```

`statefulfilter` sits before `ratelimit` on purpose: telemetry you are dropping
deliberately should not spend rate-limit budget that legitimate traffic needs.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and the [Code of Conduct](CODE_OF_CONDUCT.md).
Security reports go through [SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE), the same license as the OpenTelemetry projects
this builds on. Third-party attributions are in [NOTICE](NOTICE).

[ocb]: https://github.com/open-telemetry/opentelemetry-collector/tree/main/cmd/builder
[core]: https://github.com/open-telemetry/opentelemetry-collector
[contrib]: https://github.com/open-telemetry/opentelemetry-collector-contrib
