<!--
Thanks for contributing! A few reminders from CONTRIBUTING.md:
  - Small PRs are easier to review.
  - Explain the WHY, not just the WHAT.
  - If this is a new feature, there should be an issue first.
-->

## What this changes

<!-- One or two paragraphs. What is the problem, what is the fix. -->

## Why

<!-- The motivation. A user-visible outcome is stronger than "refactor"
     or "cleanup". If this adds cost (binary size, RSS, CPU), say what
     it buys. -->

## How it was tested

- [ ] `make test` (both processors, with `-race`)
- [ ] `make build` and `make validate`
- [ ] `make test-e2e` (required after a collector or dependency bump)
- [ ] `.chloggen/` entry added, or the change is `[chore]`
- [ ] `./benchmarks/run.sh` unchanged within noise (if you touched a hot path)

## Breaking changes

<!-- If yes, describe the migration path. If no, write "None". -->

None.

## Related issues

Closes #
