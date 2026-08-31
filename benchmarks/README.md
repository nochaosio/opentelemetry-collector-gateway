# Benchmarks

Both distributions are built from the same collector core, so raw speed is not
the question. What a gateway is judged on is how much telemetry it keeps out of
the backend (IN vs OUT), how fast that decision can change, and what the change
costs the traffic you care about.

Spans are counted at two independent points. The sink is a third
`otelcol-contrib` process neither side controls, so OUT is never self reported:

```
telemetrygen  --->  collector under test  --->  sink (counts OUT)
   6 cores              4 cores                   2 cores
```

Host: Ryzen 9 7900X, 30 GB RAM. `otelcol-gateway` v0.1.0 vs `otelcol-contrib`
v0.154.0, same collector core, same pinned cores, same workload.

> The numbers below were measured on collector core v0.154.0. The build has
> since moved to v0.159.0; the reproduction steps use that version, so re-run
> `run-comparison.sh` before quoting these figures as current.

## Results

**A. Plain pipeline** (`otlp -> memory_limiter, batch -> otlp`, identical config, 20s)

| | spans/s | Max RSS | Avg CPU |
|---|---|---|---|
| contrib | 386,417 | 226 MB | 142% |
| gateway | 386,747 | **97 MB** | 146% |

Same throughput, 57% less memory. The gateway compiles in what a gateway needs
instead of all 200+ components contrib carries.

**B. Dropping known noise** (half the traffic is `/healthz` probes)

| | IN | OUT | Probe spans delivered |
|---|---|---|---|
| contrib `filter` (OTTL) | 7,761,444 | 5,515,596 | **0** |
| gateway `statefulfilter` (Redis) | 7,775,305 | 5,526,157 | **0** |

A tie, and that is the point: contrib does this well, and reading rules from
Redis costs nothing on the hot path.

**C. Changing the rule while traffic flows.** contrib keeps the rule in its
config, so the process must restart. The gateway keeps it in Redis: one `HSET`.

| | Time to effect | Downtime | Business traffic lost |
|---|---|---|---|
| contrib, config + restart | 0.4s | 0.3s | **17.6%** |
| gateway, `refresh_interval: 5s` | 4.9s | **0s** | **0%** |
| gateway, `refresh_interval: 1s` | 0.9s | **0s** | **0%** |

Restarting one local process is genuinely faster than a 5s poll. It cost 17.6%
of the checkout traffic, and 0.3s is the floor: one binary, one machine, no
rollout. A rolling restart across N replicas is minutes and N restarts. The
gateway's number does not change with N, and propagation is a dial.

**D. One service triples its volume** (`checkout` steady at 12k spans/s,
`legacy-svc` 12k -> 36k halfway, 30s)

| | checkout delivered | legacy-svc delivered |
|---|---|---|
| contrib `filter` | 100% | **0%** |
| contrib `probabilistic_sampler` 15% | **15%** | 14% |
| gateway `ratelimit`, 18k/s cap | 100% | 65% |

contrib can delete the noisy service or sample everything; the sampler has no
per service scope, so cutting `legacy-svc` cuts `checkout` with it. The gateway
caps one service and leaves the rest alone. (A `routing` connector plus a
second pipeline would scope the sampler, at the cost of a pipeline rewrite.)

## Caveats

- One node, one process per arm. Fleet wide shared state is not proven here.
- Traces only.
- Scenario D uses `rejection_mode: permanent` so the generator does not resend
  what was deliberately dropped.
- contrib's restart cost in C ranged 9% to 18% across runs. The gateway
  measured 0% every run.

## Reproducing

```bash
make build
curl -fsSL -o /tmp/contrib.tar.gz \
  https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v0.159.0/otelcol-contrib_0.159.0_linux_amd64.tar.gz
tar -xzf /tmp/contrib.tar.gz -C /tmp otelcol-contrib
go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@v0.159.0
docker run -d --name bench-redis -p 6399:6379 redis:7-alpine

./run-comparison.sh
```

Results land in `results/comparison/`. Knobs: `SCENARIOS=C` runs one scenario,
`LOAD_SECS` / `SURGE_SECS` / `LIVE_SECS` set durations, `SUT_CPUS` /
`SINK_CPUS` / `GEN_CPUS` remap pinning. Configs in [`comparison/`](comparison/).

## Other scripts here

| Script | What it does |
|--------|--------------|
| `run.sh` | gateway vs upstream collector core, single pipeline throughput |
| `run-contrib.sh` | gateway vs contrib on one identical pipeline (superseded by A above) |
| `e2e_storage_verify.sh` | checks `ratelimit` enforces the same limits on both storage backends |
