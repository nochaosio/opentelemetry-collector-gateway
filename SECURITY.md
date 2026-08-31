# Security Policy

## Reporting a vulnerability

Report vulnerabilities through
[GitHub private vulnerability reporting](https://github.com/nochaosio/opentelemetry-collector-gateway/security/advisories/new).
Do not open a public issue for a security problem.

Expect an acknowledgement within 5 business days and a fix or a mitigation
plan within 30 days for anything we can reproduce.

## Scope

This repository ships two custom processors and a collector distribution
assembled with the OpenTelemetry Collector Builder.

In scope:

- `processor/ratelimitprocessor` and `processor/statefulfilterprocessor`.
- The build manifest (`builder-config.yaml`) and the Dockerfile.
- Anything that lets telemetry bypass a configured rate limit or filter rule,
  or that lets one tenant affect another's budget.

Out of scope, report upstream instead:

- Components from [opentelemetry-collector][core] and
  [opentelemetry-collector-contrib][contrib]. They are consumed as Go module
  dependencies and their source is not redistributed here.
- Findings in a dependency that we do not reach. `scripts/govulncheck.sh`
  gates on symbol-reachable findings for exactly this reason.

## Supported versions

Only the latest release on `main` receives fixes. This distribution tracks
upstream releases closely, so the remedy for a dependency advisory is usually
a version bump rather than a backport.

## What we already enforce in CI

- `govulncheck` on all three modules, failing on any symbol-reachable finding.
- Trivy on the container image, failing on HIGH/CRITICAL with a fix available.
- An SBOM (SPDX) published as a build artifact for every image.

[core]: https://github.com/open-telemetry/opentelemetry-collector/security
[contrib]: https://github.com/open-telemetry/opentelemetry-collector-contrib/security
