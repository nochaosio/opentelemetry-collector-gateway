# AGENTS.md

Instructions for AI agents working in this repository.

## Language

**Everything in this repository and around it is written in English.** No
exceptions, regardless of the language the request was made in. That covers:

- Source code: identifiers, comments, log and error messages, test names.
- Documentation: every `.md` file, YAML comments, config samples.
- Commit messages: subject and body.
- Pull requests: title, description, review comments.
- Issues: title, body, labels, and replies.
- Release notes and changelog entries.

A request written in another language does not change the output language.
Answer the person in whichever language they used if you like, but anything
that lands in the repository or on GitHub is English.

## What this repo is

A contrib-based OpenTelemetry Collector distribution for gateway-class
workloads, built with the OpenTelemetry Collector Builder (OCB). It ships two
custom processors alongside the upstream and contrib components:

| Component | Path |
|-----------|------|
| `ratelimit` | `processor/ratelimitprocessor/` |
| `statefulfilter` | `processor/statefulfilterprocessor/` |

Component inventory: [COMPONENTS.md](COMPONENTS.md). Build manifest:
`builder-config.yaml`.

## Always run the tests

**If you touch a component, run its tests before you say you are done.**

```bash
make test          # both processors, with -race
make build         # regenerates the collector binary via OCB
make validate      # validates config/otelcol-gateway.yaml against the binary
```

For a single processor:

```bash
cd processor/ratelimitprocessor && go test -race -count=1 ./...
```

Prefer running the real thing over describing what should happen. The
`benchmarks/` and `playground/` directories exist so changes can be exercised
against a running collector.

## Commit and PR title convention

Follow the [opentelemetry-collector-contrib][contrib] convention: a bracketed
component scope, then a capitalised sentence describing the change.

```
[component] Description of the change
```

Examples, matching how contrib writes them:

```
[processor/ratelimit] Cap in-memory buckets to bound cardinality
[processor/statefulfilter] Require explicit conditions on every rule
[exporter/kafka] Close the producer on shutdown
[chore] Prepare release 0.155.0
[chore] Update code ownership
[docs] Split the components table into COMPONENTS.md
```

Rules:

- Drop the redundant suffix from the scope: `processor/ratelimitprocessor` is
  written `[processor/ratelimit]`, matching contrib's `[receiver/mysql]` and
  `[processor/tailsampling]`.
- Use `[chore]` for anything that needs no changelog entry: CI, tooling,
  dependency bumps, formatting, comment cleanup. It is contrib's most common
  prefix by a wide margin.
- Prefixes can be stacked, as contrib does in `[chore][processor/drain]`.
- There is no `!` marker for breaking changes; say so in the description and in
  the changelog entry.
- One logical change per commit. Do not mix a refactor with a behaviour change.

## Documentation

- Keep it short. State the number, the tradeoff, or the gotcha, and stop.
- Do not use em dashes.
- Reference documents live next to what they document: each processor has its
  own `README.md`; the root `README.md` links out rather than duplicating.
- Comment code for the non-obvious *why*, not the *what*. If the reason is
  already in a README, link to it instead of restating it.

## Gotchas

- The OCB version is pinned in the `Makefile` (`OCB_VERSION`). Building with a
  different builder rewrites the generated files under `cmd/otelcol-gateway/`,
  including absolute paths in `go.mod` replaces. Run `make install-builder`
  first.
- `cmd/otelcol-gateway/` is generated. Change `builder-config.yaml` and rebuild
  instead of editing it by hand.
- Both custom processors keep state in Redis under distinct key prefixes
  (`otelcol:filter:*` and `otelcol:ratelimit:*`), so they can share one
  instance.

[contrib]: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/CONTRIBUTING.md
