# Playground

Scenario tests for the custom components of this collector, one folder per
component. Each folder is self-contained — a `docker-compose.yaml`, a collector
config, a `send.sh` load helper and a README with the scenarios and the exact
commands to run them.

| Folder | Component | Scenarios |
|--------|-----------|-----------|
| [ratelimitprocessor/](ratelimitprocessor/) | `ratelimit` | per-service limit, per-key override, monitor mode, limit by attribute, error-span bypass, fleet-wide limit via Redis |
| [statefulfilterprocessor/](statefulfilterprocessor/) | `statefulfilter` | live rules from Redis, item-level filtering, keep exceptions, expiry, invalid rules, fail-stale on a Redis outage |

## Requirements

Docker with Compose v2, `curl` and `openssl`. The collector image is built from
the repo root `Dockerfile`, so no local Go toolchain is needed.

## Usage

```bash
cd ratelimitprocessor        # or statefulfilterprocessor
docker compose up -d --build
# ... run the scenarios from the folder README ...
docker compose down
```

Both stacks bind the same host ports (4318, 8888, 13133): run **one at a time**,
or edit the port mappings in the compose file.

For throughput numbers instead of behaviour, see [`../benchmarks/`](../benchmarks/).
