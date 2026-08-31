# OpenTelemetry Collector Gateway

A contrib-based OpenTelemetry Collector distribution standing at the front door of your observability pipeline

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

```bash
make install-builder
make build
make run
```

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

## License

[Apache License 2.0](LICENSE), the same license as the OpenTelemetry projects
this builds on. Third-party attributions are in [NOTICE](NOTICE).

[ocb]: https://github.com/open-telemetry/opentelemetry-collector/tree/main/cmd/builder
[core]: https://github.com/open-telemetry/opentelemetry-collector
[contrib]: https://github.com/open-telemetry/opentelemetry-collector-contrib
