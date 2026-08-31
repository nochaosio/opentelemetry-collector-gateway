# Changelog entries

Every user-visible change carries a YAML file in this directory. They are
collected into `CHANGELOG.md` at release time, which keeps release notes
accurate without anyone having to reconstruct them from git log.

Copy `TEMPLATE.yaml`, name the file after your branch
(`ratelimit-bound-cardinality.yaml`), and fill it in:

```yaml
change_type: enhancement
component: processor/ratelimit
note: Cap in-memory buckets so a hostile producer cannot grow the map without bound
issues: [42]
subtext: |
  Optional. Longer explanation, indented, for anything that needs more than
  the one-line note.
```

`change_type` is one of `breaking`, `deprecation`, `new_component`,
`enhancement`, `bug_fix`. `component` uses the same short scope as the commit
subject. Omit the file entirely for `[chore]` changes: CI, tooling, dependency
bumps and formatting do not belong in release notes.
