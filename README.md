# OpenTelemetry Collector Gateway

A contrib-based OpenTelemetry Collector distribution standing at the front door of your observability pipeline

## Components

The receivers, exporters, extensions, connectors, and processors bundled in this
distribution, and why each one is here, are listed in [COMPONENTS.md](COMPONENTS.md).

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

[MIT License](LICENSE)
