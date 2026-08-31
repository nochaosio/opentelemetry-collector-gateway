#!/usr/bin/env bash
# Manage the statefulfilter processor's drop rules.
#
# The rules live in Redis, so one write reaches every collector replica within
# `refresh_interval` — no config edit, no rollout. This script is the thin
# ergonomic layer over the two keys the processor reads:
#
#   <prefix>:rules          HASH    field = rule id, value = rule JSON
#   <prefix>:rules:version  STRING  counter; bumped on every write so replicas
#                                   can poll with a single cheap GET
#
# Usage:
#   ./scripts/filter-rules.sh list
#   ./scripts/filter-rules.sh get      <id>
#   ./scripts/filter-rules.sh add      <id> '<json>'
#   ./scripts/filter-rules.sh rm       <id>
#   ./scripts/filter-rules.sh enable   <id>
#   ./scripts/filter-rules.sh disable  <id>
#   ./scripts/filter-rules.sh version
#   ./scripts/filter-rules.sh flush
#
# Environment:
#   REDIS_CLI    redis-cli invocation (default "redis-cli").
#                e.g. REDIS_CLI="docker exec -i my-redis redis-cli"
#                     REDIS_CLI="redis-cli -h redis.internal -a $PASS"
#   KEY_PREFIX   must match the processor's redis.key_prefix (default otelcol:filter)
set -euo pipefail

REDIS_CLI="${REDIS_CLI:-redis-cli}"
KEY_PREFIX="${KEY_PREFIX:-otelcol:filter}"
RULES_KEY="${KEY_PREFIX}:rules"
VERSION_KEY="${KEY_PREFIX}:rules:version"

redis() { $REDIS_CLI "$@"; }

# Every mutation bumps the version. Replicas compare it on each poll and only
# re-read the full hash when it moved; forgetting the bump means the change is
# invisible until the next periodic full resync.
bump() {
  local v
  v=$(redis INCR "$VERSION_KEY")
  echo "rules version -> $v (replicas converge within refresh_interval)"
}

usage() { sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }

cmd="${1:-}"; shift || true

case "$cmd" in
  list)
    if command -v jq >/dev/null 2>&1; then
      redis HGETALL "$RULES_KEY" | paste - - | while IFS=$'\t' read -r id doc; do
        printf '%s\t%s\n' "$id" "$(echo "$doc" | jq -c .)"
      done
    else
      redis HGETALL "$RULES_KEY"
    fi
    echo "--- version: $(redis GET "$VERSION_KEY" || echo 0)"
    ;;

  get)
    [ $# -eq 1 ] || usage
    redis HGET "$RULES_KEY" "$1"
    ;;

  add)
    [ $# -eq 2 ] || usage
    # Fail before writing rather than let the collector reject it later: an
    # invalid rule is silently inert apart from the rules_invalid metric.
    if command -v jq >/dev/null 2>&1; then
      echo "$2" | jq . >/dev/null || { echo "invalid JSON" >&2; exit 1; }
    fi
    redis HSET "$RULES_KEY" "$1" "$2" >/dev/null
    echo "rule '$1' stored"
    bump
    ;;

  rm|del|delete)
    [ $# -eq 1 ] || usage
    redis HDEL "$RULES_KEY" "$1" >/dev/null
    echo "rule '$1' removed"
    bump
    ;;

  enable|disable)
    [ $# -eq 1 ] || usage
    command -v jq >/dev/null 2>&1 || { echo "jq is required for $cmd" >&2; exit 1; }
    doc=$(redis HGET "$RULES_KEY" "$1")
    [ -n "$doc" ] || { echo "no such rule: $1" >&2; exit 1; }
    want=$([ "$cmd" = enable ] && echo true || echo false)
    redis HSET "$RULES_KEY" "$1" "$(echo "$doc" | jq -c --argjson e "$want" '.enabled = $e')" >/dev/null
    echo "rule '$1' ${cmd}d"
    bump
    ;;

  version)
    redis GET "$VERSION_KEY" || echo 0
    ;;

  flush)
    redis DEL "$RULES_KEY" >/dev/null
    echo "all rules removed"
    bump
    ;;

  *) usage ;;
esac
