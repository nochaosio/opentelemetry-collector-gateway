# Contributing

## Before you start

Read [AGENTS.md](AGENTS.md). It is written for AI agents but the rules apply to
everyone: English everywhere, contrib's commit convention, one logical change
per commit.

## Development

```bash
make install-builder     # OCB, pinned in the Makefile
make install-mdatagen    # mdatagen, same version as the collector
make test                # both processors, with -race
make build               # regenerates the collector binary via OCB
make validate            # validates config/otelcol-gateway.yaml against it
make generate            # regenerates internal/metadata from metadata.yaml
```

A single processor:

```bash
cd processor/ratelimitprocessor && go test -race -count=1 ./...
```

`cmd/otelcol-gateway/` is generated. Change `builder-config.yaml` and rebuild
rather than editing it.

## What a change needs

- **Tests.** New behavior needs a test that fails without the change. The
  processors sit at over 80% coverage; keep it there.
- **A changelog entry** under `.chloggen/`, unless the change is `[chore]`.
  See [.chloggen/README.md](.chloggen/README.md).
- **Generated files regenerated** if you touched `metadata.yaml`. CI checks
  that `make generate` produces no diff.
- **Docs updated** in the component's `README.md` if operator-visible behavior
  changed.

## Commit and PR titles

Contrib's convention, a bracketed scope then a capitalised sentence:

```
[processor/ratelimit] Cap in-memory buckets to bound cardinality
[chore] Bump the collector to v0.159.0
```

Drop the redundant suffix: `processor/ratelimitprocessor` is written
`[processor/ratelimit]`. Use `[chore]` for anything that needs no changelog
entry.

## CI

Every PR runs unit tests with `-race`, golangci-lint, `govulncheck` on all
three modules, an OCB build, and a Trivy scan of the image. All of it must be
green.
