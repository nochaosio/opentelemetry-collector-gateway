"""Gate on govulncheck JSON output, suppressing allowlisted OSV IDs.

Reads `govulncheck -format json` from stdin. Fails (exit 1) on any
symbol-reachable finding whose OSV ID is not listed in the ignore file
passed as argv[1]. Module/package-level findings are advisory and never
block, matching plain govulncheck behavior.
"""

import json
import sys

ignore = set()
try:
    with open(sys.argv[1]) as f:
        ignore = {l.strip() for l in f if l.strip() and not l.startswith("#")}
except FileNotFoundError:
    pass

findings = set()
decoder = json.JSONDecoder()
data = sys.stdin.read()
idx = 0
while idx < len(data):
    while idx < len(data) and data[idx] in " \n\t\r":
        idx += 1
    if idx >= len(data):
        break
    obj, idx = decoder.raw_decode(data, idx)
    f = obj.get("finding")
    if f and f.get("trace") and f["trace"][0].get("function"):
        findings.add(f["osv"])

suppressed = sorted(findings & ignore)
blocking = sorted(findings - ignore)

for osv in suppressed:
    print(f"suppressed (allowlisted in .govulncheck-ignore): {osv}")
for osv in blocking:
    print(f"BLOCKING: {osv} - https://pkg.go.dev/vuln/{osv}")

if blocking:
    sys.exit(1)
print("govulncheck gate: OK")
