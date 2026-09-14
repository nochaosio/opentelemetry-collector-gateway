#!/usr/bin/env bash
# End-to-end check: pushes real telemetry through the built binary and asserts
# the counters add up.
#
# Why this exists: `go test` exercises the processors as libraries and
# `otelcol-gateway validate` only reads the config schema. Neither one proves
# that a span survives the binary OCB regenerates on every collector bump,
# which is the artifact that actually changes. This script closes that gap.
#
# Scenarios:
#   1. ratelimit, in memory    allow path and drop path on config/otelcol-gateway.yaml
#   2. ratelimit, shared redis two instances share one token bucket
#   3. statefulfilter          rule driven drop, plus hot reload from Redis
#
# Scenarios 2 and 3 need Docker for Redis and are skipped when it is missing.
#
# Usage:
#   ./scripts/e2e.sh                 # every scenario Docker allows
#   REDIS_SCENARIOS=off ./scripts/e2e.sh
#
# Environment:
#   BINARY           collector binary (default cmd/otelcol-gateway/otelcol-gateway)
#   TELEMETRYGEN     telemetrygen binary (default "telemetrygen" on PATH)
#   REDIS_SCENARIOS  auto (default), on to fail when Docker is absent, off to skip
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

BINARY="${BINARY:-cmd/otelcol-gateway/otelcol-gateway}"
TELEMETRYGEN="${TELEMETRYGEN:-telemetrygen}"
REDIS_SCENARIOS="${REDIS_SCENARIOS:-auto}"
REDIS_CONTAINER="otelcol-e2e-redis"

WORK="$(mktemp -d)"
PIDS=()
FAILURES=0

# Every scenario leaves a collector listening and possibly a Redis container.
# Clean both up on any exit path, including a failed assertion.
cleanup() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
  done
  docker rm -f "$REDIS_CONTAINER" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
info() { printf '   %s\n' "$*"; }
pass() { printf '   \033[32mPASS\033[0m %s\n' "$*"; }
fail() { printf '   \033[31mFAIL\033[0m %s\n' "$*"; FAILURES=$((FAILURES + 1)); }

# Reads one Prometheus sample from a collector's self telemetry endpoint.
# Returns 0 when the series is absent, which is what "nothing happened yet"
# looks like and keeps the arithmetic below total.
metric() {
  local port="$1" pattern="$2" value
  value="$(curl -sf "http://127.0.0.1:${port}/metrics" 2>/dev/null \
    | grep -E "$pattern" | grep -v '^#' | head -1 | awk '{print $NF}')" || true
  printf '%s' "${value:-0}"
}

# The debug exporter prints one line per flush, so a batch split across two
# flushes has to be summed. Reading a single line undercounts.
exporter_spans() {
  local log="$1"
  grep -oE '"spans": [0-9]+' "$log" 2>/dev/null \
    | awk '{sum += $2} END {print sum + 0}'
}

wait_for_health() {
  local port="$1" i
  for i in $(seq 1 40); do
    curl -sf "http://127.0.0.1:${port}/" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  return 1
}

# Starts a collector in the background and blocks until it reports healthy.
# Extra arguments are passed through, which is how callers relocate ports and
# point Redis at localhost.
start_collector() {
  local name="$1" config="$2" health="$3"; shift 3
  "$BINARY" --config "$config" "$@" >"$WORK/$name.log" 2>&1 &
  PIDS+=($!)
  if ! wait_for_health "$health"; then
    fail "$name did not become healthy"
    sed -n '1,25p' "$WORK/$name.log"
    return 1
  fi
  info "$name up (health :$health)"
}

gen_traces() {
  local endpoint="$1" service="$2" traces="$3"; shift 3
  "$TELEMETRYGEN" traces --otlp-insecure --otlp-endpoint "$endpoint" \
    --service "$service" --traces "$traces" "$@" >/dev/null 2>&1
}

require_tools() {
  local missing=0
  [ -x "$BINARY" ] || { echo "ERROR: $BINARY not found. Run 'make build' first." >&2; missing=1; }
  command -v "$TELEMETRYGEN" >/dev/null 2>&1 || {
    echo "ERROR: telemetrygen not on PATH. Install it with:" >&2
    echo "  go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest" >&2
    missing=1
  }
  command -v curl >/dev/null 2>&1 || { echo "ERROR: curl is required." >&2; missing=1; }
  [ "$missing" -eq 0 ] || exit 1
}

start_redis() {
  docker rm -f "$REDIS_CONTAINER" >/dev/null 2>&1 || true
  docker run -d --rm --name "$REDIS_CONTAINER" -p 6379:6379 redis:7-alpine >/dev/null
  local i
  for i in $(seq 1 30); do
    docker exec -i "$REDIS_CONTAINER" redis-cli PING >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  return 1
}

# ---------------------------------------------------------------------------
# Scenario 1: ratelimit in memory, on the config we actually ship.
# ---------------------------------------------------------------------------
scenario_ratelimit_memory() {
  say "Scenario 1: ratelimit (in memory) on config/otelcol-gateway.yaml"

  # A one second batch keeps the run short and the flush deterministic; the
  # shipped default of 10s would only make the script slower to fail.
  start_collector s1 config/otelcol-gateway.yaml 13133 \
    --set=processors.batch.timeout=1s || return

  # Under the limit: the default service cap is 5 requests per second and
  # telemetrygen defaults to one export per second, so nothing should drop.
  gen_traces 127.0.0.1:4317 checkout-service 3 --child-spans 2
  sleep 3

  local recv allowed incoming outgoing spans
  recv="$(metric 8888 'ratelimit_received_items')"
  allowed="$(metric 8888 'ratelimit_allowed_items')"
  incoming="$(metric 8888 'otelcol_processor_incoming_items.*ratelimit')"
  outgoing="$(metric 8888 'otelcol_processor_outgoing_items.*ratelimit')"
  spans="$(exporter_spans "$WORK/s1.log")"

  info "allow path: received=$recv allowed=$allowed exporter_spans=$spans"
  if [ "$recv" -gt 0 ] && [ "$allowed" = "$recv" ]; then
    pass "everything under the limit was allowed"
  else
    fail "expected allowed == received > 0, got allowed=$allowed received=$recv"
  fi
  if [ "$spans" = "$outgoing" ] && [ "$outgoing" = "$incoming" ]; then
    pass "exporter emitted every span the processor passed ($spans)"
  else
    fail "span accounting mismatch: incoming=$incoming outgoing=$outgoing exporter=$spans"
  fi

  # Over the limit: legacy-service is capped at 1 request per second by
  # specific_limits, so a burst with no throttling must shed most of it.
  gen_traces 127.0.0.1:4317 legacy-service 20 --child-spans 2 --rate 0 --workers 4
  sleep 3

  local recv2 allowed2 dropped2
  recv2="$(metric 8888 'ratelimit_received_items')"
  allowed2="$(metric 8888 'ratelimit_allowed_items')"
  dropped2=$((recv2 - allowed2))

  info "drop path: received=$recv2 allowed=$allowed2 dropped=$dropped2"
  if [ "$dropped2" -gt 0 ] && [ "$((dropped2 * 2))" -gt "$recv2" ]; then
    pass "drop_on_limit shed the burst ($dropped2 of $recv2)"
  else
    fail "expected most of the burst to drop, got dropped=$dropped2 of $recv2"
  fi

  local errors
  errors="$(grep -cE $'\terror\t|panic' "$WORK/s1.log" || true)"
  [ "$errors" = "0" ] && pass "no errors or panics" || fail "$errors error lines in the log"

  kill "${PIDS[-1]}" 2>/dev/null || true
  sleep 1
}

# ---------------------------------------------------------------------------
# Scenario 2: two instances, one shared token bucket in Redis.
# The point is that the cap is global, not per replica.
# ---------------------------------------------------------------------------
scenario_ratelimit_redis() {
  say "Scenario 2: ratelimit (shared Redis bucket) across two instances"

  # The shipped config addresses Redis by its compose service name and binds
  # fixed ports, neither of which works for two local processes. Ports are
  # rewritten here rather than with --set because --set cannot index into the
  # telemetry readers array.
  sed -e 's|addr: redis:6379|addr: 127.0.0.1:6379|' \
      config/otelcol-gateway-redis.yaml >"$WORK/redis-a.yaml"
  sed -e 's|:4317|:4327|; s|:4318|:4328|; s|:13133|:13134|; s|port: 8888|port: 8889|' \
      "$WORK/redis-a.yaml" >"$WORK/redis-b.yaml"

  start_collector s2a "$WORK/redis-a.yaml" 13133 \
    --set=processors.batch.timeout=1s || return
  start_collector s2b "$WORK/redis-b.yaml" 13134 \
    --set=processors.batch.timeout=1s || return

  # Both bursts at once, so the two instances really contend for the bucket.
  gen_traces 127.0.0.1:4317 svc-a 100 --child-spans 0 --rate 0 --workers 2 &
  local pa=$!
  gen_traces 127.0.0.1:4327 svc-a 100 --child-spans 0 --rate 0 --workers 2 &
  local pb=$!
  wait $pa $pb
  sleep 3

  local recv_a recv_b allow_a allow_b total_recv total_allowed limit
  recv_a="$(metric 8888 'ratelimit_received_items')"
  recv_b="$(metric 8889 'ratelimit_received_items')"
  allow_a="$(metric 8888 'ratelimit_allowed_items')"
  allow_b="$(metric 8889 'ratelimit_allowed_items')"
  total_recv=$((recv_a + recv_b))
  total_allowed=$((allow_a + allow_b))
  limit=10   # requests_per_second in config/otelcol-gateway-redis.yaml

  info "instance A: received=$recv_a allowed=$allow_a"
  info "instance B: received=$recv_b allowed=$allow_b"
  info "combined:   received=$total_recv allowed=$total_allowed (cap ${limit}/s)"

  if [ "$total_recv" -le 0 ]; then
    fail "no telemetry reached either instance"
  elif [ "$total_allowed" -lt "$total_recv" ]; then
    pass "the shared bucket shed traffic across both instances"
  else
    fail "nothing was limited: allowed=$total_allowed of $total_recv"
  fi

  # The real assertion: had each replica kept its own bucket, a burst this size
  # would let roughly twice the cap through.
  if [ "$total_allowed" -le "$((limit * 4))" ]; then
    pass "combined throughput stayed near the global cap, not a per replica one"
  else
    fail "allowed=$total_allowed suggests per instance buckets, not a shared one"
  fi

  local keys
  keys="$(docker exec -i "$REDIS_CONTAINER" redis-cli KEYS 'otelcol:ratelimit:*' | tr -d '\r')"
  if [ -n "$keys" ]; then
    pass "bucket state reached Redis ($keys)"
  else
    # on_error: open means a broken Redis silently allows everything, so an
    # empty keyspace would invalidate the assertions above.
    fail "no ratelimit keys in Redis, the limiter may have failed open"
  fi

  kill "${PIDS[-1]}" "${PIDS[-2]}" 2>/dev/null || true
  sleep 1
}

# ---------------------------------------------------------------------------
# Scenario 3: statefulfilter drops by rule, and picks up a rule change from
# Redis without a restart.
# ---------------------------------------------------------------------------
scenario_statefulfilter() {
  say "Scenario 3: statefulfilter rule drop and hot reload"

  export REDIS_CLI="docker exec -i $REDIS_CONTAINER redis-cli"
  export KEY_PREFIX=otelcol:filter

  "$SCRIPT_DIR/filter-rules.sh" flush >/dev/null 2>&1 || true
  "$SCRIPT_DIR/filter-rules.sh" add drop-noisy \
    '{"id":"drop-noisy","enabled":true,"action":"drop","signals":["traces"],"conditions":[{"source":"resource","key":"service.name","op":"equals","value":"noisy-service"}]}' \
    >/dev/null

  sed -e 's|addr: localhost:6379|addr: 127.0.0.1:6379|' \
      config/otelcol-gateway-statefulfilter.yaml >"$WORK/sf.yaml"

  # A short refresh keeps the hot reload leg quick; the shipped default is 10s.
  start_collector s3 "$WORK/sf.yaml" 13133 \
    --set=processors.batch.timeout=1s \
    --set=processors.statefulfilter.refresh_interval=2s || return

  gen_traces 127.0.0.1:4317 noisy-service 8 --child-spans 0 --rate 0
  gen_traces 127.0.0.1:4317 quiet-service 5 --child-spans 0 --rate 0
  sleep 3

  local evaluated dropped outgoing spans
  evaluated="$(metric 8888 'statefulfilter_evaluated_items')"
  dropped="$(metric 8888 'statefulfilter_dropped_items.*drop-noisy')"
  outgoing="$(metric 8888 'otelcol_processor_outgoing_items.*statefulfilter')"
  spans="$(exporter_spans "$WORK/s3.log")"

  info "rule active: evaluated=$evaluated dropped=$dropped passed=$outgoing exporter=$spans"
  if [ "$dropped" -gt 0 ] && [ "$outgoing" -gt 0 ]; then
    pass "the rule dropped the matching service and spared the other"
  else
    fail "expected both a drop and a pass, got dropped=$dropped passed=$outgoing"
  fi
  if [ "$spans" = "$outgoing" ]; then
    pass "exporter emitted exactly what the filter passed ($spans)"
  else
    fail "span accounting mismatch: passed=$outgoing exporter=$spans"
  fi

  # Hot reload: flip the rule off in Redis and let the processor poll.
  local before_version after_version
  before_version="$(metric 8888 'statefulfilter_rules_version')"
  "$SCRIPT_DIR/filter-rules.sh" disable drop-noisy >/dev/null
  sleep 6
  after_version="$(metric 8888 'statefulfilter_rules_version')"

  local loaded
  loaded="$(metric 8888 'statefulfilter_rules_loaded')"
  info "hot reload: version $before_version -> $after_version, active rules=$loaded"
  if [ "$after_version" -gt "$before_version" ] && [ "$loaded" = "0" ]; then
    pass "the rule change landed without a restart"
  else
    fail "rule change not picked up: version=$after_version loaded=$loaded"
  fi

  local passed_before
  passed_before="$outgoing"
  gen_traces 127.0.0.1:4317 noisy-service 4 --child-spans 0 --rate 0
  sleep 3
  outgoing="$(metric 8888 'otelcol_processor_outgoing_items.*statefulfilter')"

  if [ "$outgoing" -gt "$passed_before" ]; then
    pass "the once blocked service flows again ($passed_before -> $outgoing)"
  else
    fail "traffic still blocked after disabling the rule (passed=$outgoing)"
  fi

  "$SCRIPT_DIR/filter-rules.sh" flush >/dev/null 2>&1 || true
  kill "${PIDS[-1]}" 2>/dev/null || true
  sleep 1
}

main() {
  require_tools
  info "binary:       $("$BINARY" --version 2>&1 | head -1)"
  info "telemetrygen: $(command -v "$TELEMETRYGEN")"

  scenario_ratelimit_memory

  local want_redis=1
  case "$REDIS_SCENARIOS" in
    off) want_redis=0 ;;
    auto) command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 || want_redis=0 ;;
    on) ;;
    *) echo "ERROR: REDIS_SCENARIOS must be auto, on or off" >&2; exit 1 ;;
  esac

  if [ "$want_redis" -eq 1 ]; then
    if start_redis; then
      scenario_ratelimit_redis
      scenario_statefulfilter
    else
      fail "could not start Redis for the shared state scenarios"
    fi
  elif [ "$REDIS_SCENARIOS" = "off" ]; then
    say "Scenarios 2 and 3 skipped (REDIS_SCENARIOS=off)"
  else
    say "Scenarios 2 and 3 skipped (no usable Docker; set REDIS_SCENARIOS=on to require them)"
  fi

  say "Result"
  if [ "$FAILURES" -eq 0 ]; then
    printf '   \033[32mall assertions passed\033[0m\n\n'
    return 0
  fi
  printf '   \033[31m%d assertion(s) failed\033[0m\n\n' "$FAILURES"
  return 1
}

main "$@"
