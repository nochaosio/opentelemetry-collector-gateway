#!/usr/bin/env bash
set -euo pipefail

MODULE_DIR="${1:?usage: govulncheck.sh <module-dir>}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

GOVULNCHECK="${GOVULNCHECK:-govulncheck}"
GOVULNCHECK_ATTEMPTS="${GOVULNCHECK_ATTEMPTS:-2}"

RAW="$(mktemp)"
trap 'rm -f "$RAW"' EXIT

cd "$MODULE_DIR"

status=0
for attempt in $(seq 1 "$GOVULNCHECK_ATTEMPTS"); do
  status=0
  "$GOVULNCHECK" -format json ./... >"$RAW" || status=$?
  if [ "$status" -eq 0 ]; then
    break
  fi
  echo "govulncheck exited with status $status on $MODULE_DIR (attempt $attempt/$GOVULNCHECK_ATTEMPTS)" >&2
done

if [ "$status" -ne 0 ]; then
  echo "ERROR: govulncheck did not complete the analysis of $MODULE_DIR (exit $status)." >&2
  echo "       The result is inconclusive. This does NOT mean there are no vulnerabilities." >&2
  exit "$status"
fi

python3 "$SCRIPT_DIR/govulncheck_filter.py" "$REPO_ROOT/.govulncheck-ignore" <"$RAW"
