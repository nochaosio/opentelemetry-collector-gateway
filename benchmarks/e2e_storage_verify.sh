#!/usr/bin/env bash
#
# End-to-end verification of the pluggable storage backend and the
# default+override rate-limit semantics.
#
# What it exercises:
#   - Default limit (20 rps) applied to a service with no override.
#   - Override for premium-svc (200 rps): very little drop.
#   - Override for legacy-svc (5 rps): heavy drop.
#   - Counters visible at the Prometheus endpoint, per-key.
#
# Run with:
#   ./benchmarks/e2e_storage_verify.sh [memory|redis]
#
# The default backend is "memory"; pass "redis" to target a running
# redis at localhost:6379 (the script does NOT start Redis for you).

set -euo pipefail

BACKEND="${1:-memory}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BINARY="${REPO_ROOT}/cmd/otelcol-gateway/otelcol-gateway"
OTLP_ENDPOINT="localhost:7317"
PROM_ENDPOINT="localhost:9888"
DURATION="5s"

if [[ "${BACKEND}" == "memory" ]]; then
    CONFIG="${REPO_ROOT}/benchmarks/config-storage-memory.yaml"
elif [[ "${BACKEND}" == "redis" ]]; then
    CONFIG="${REPO_ROOT}/benchmarks/config-storage-redis.yaml"
else
    echo "unknown backend: ${BACKEND}" >&2
    exit 2
fi

if [[ ! -x "${BINARY}" ]]; then
    echo "binary not found: ${BINARY}  (run: make build)" >&2
    exit 2
fi

echo "==> Starting collector (backend=${BACKEND})"
"${BINARY}" --config "${CONFIG}" >"/tmp/otelcol-gateway-e2e.log" 2>&1 &
COLLECTOR_PID=$!
trap 'kill "${COLLECTOR_PID}" 2>/dev/null || true' EXIT

# Wait for the OTLP port to be ready.
for _ in $(seq 1 40); do
    if (echo > "/dev/tcp/localhost/7317") 2>/dev/null; then
        break
    fi
    sleep 0.25
done

echo "==> Flooding three services in parallel for ${DURATION}"

# rate=0 means "as fast as possible" — each worker floods the receiver.
# Running concurrently: all three compete for their own buckets.
telemetrygen traces \
    --otlp-endpoint "${OTLP_ENDPOINT}" --otlp-insecure \
    --service default-svc --workers 4 --rate 0 --duration "${DURATION}" \
    >/tmp/tgen_default.log 2>&1 &
PID1=$!

telemetrygen traces \
    --otlp-endpoint "${OTLP_ENDPOINT}" --otlp-insecure \
    --service premium-svc --workers 4 --rate 0 --duration "${DURATION}" \
    >/tmp/tgen_premium.log 2>&1 &
PID2=$!

telemetrygen traces \
    --otlp-endpoint "${OTLP_ENDPOINT}" --otlp-insecure \
    --service legacy-svc  --workers 4 --rate 0 --duration "${DURATION}" \
    >/tmp/tgen_legacy.log  2>&1 &
PID3=$!

wait "${PID1}" "${PID2}" "${PID3}"

# Give the exporter a beat to flush before scraping.
sleep 1

echo "==> Scraping Prometheus counters"
METRICS=$(curl -fsS "http://${PROM_ENDPOINT}/metrics")

extract() {
    local metric="$1"
    local key="$2"
    echo "${METRICS}" | awk -v metric="${metric}" -v key="${key}" '
        $0 ~ "^" metric "{" {
            if (index($0, "key=\"" key "\"") > 0) {
                n = split($0, parts, " ")
                # last field is the value (possibly scientific notation).
                printf "%.0f\n", parts[n]
            }
        }
    ' | head -1
}

printf "\n%-18s %12s %12s %12s\n" "service" "received" "allowed" "denied"
printf "%-18s %12s %12s %12s\n" "------" "--------" "-------" "------"
for svc in default-svc premium-svc legacy-svc; do
    r=$(extract otelcol_processor_ratelimit_received_items_total "${svc}")
    a=$(extract otelcol_processor_ratelimit_allowed_items_total  "${svc}")
    d=$(extract otelcol_processor_ratelimit_denied_items_total   "${svc}")
    printf "%-18s %12s %12s %12s\n" "${svc}" "${r:-0}" "${a:-0}" "${d:-0}"
done
echo

# Sanity bounds (burst+rate*duration). Expected "allowed" ~= burst + rps*duration.
# duration=5s, so:
#   default-svc:   ~20 + 20*5   = 120 allowed
#   premium-svc:   ~200 + 200*5 = 1200 allowed
#   legacy-svc:    ~5 + 5*5     = 30 allowed
# Assertions leave a 25% margin because Lua timing + batching + test noise
# make exact equality flaky.

fail=0
check_allowed() {
    local svc="$1" expected="$2" margin="$3"
    local a
    a=$(extract otelcol_processor_ratelimit_allowed_items_total "${svc}")
    a=${a:-0}
    local lo=$(( expected * (100 - margin) / 100 ))
    local hi=$(( expected * (100 + margin) / 100 ))
    if (( a < lo || a > hi )); then
        echo "FAIL: ${svc} allowed=${a}, expected ~${expected} (±${margin}%)"
        fail=1
    else
        echo "OK:   ${svc} allowed=${a} within ~${expected} (±${margin}%)"
    fi
}

check_denied_nonzero() {
    local svc="$1"
    local d
    d=$(extract otelcol_processor_ratelimit_denied_items_total "${svc}")
    d=${d:-0}
    if (( d == 0 )); then
        echo "FAIL: ${svc} denied=0 (expected overflow drops under flood)"
        fail=1
    else
        echo "OK:   ${svc} denied=${d} (drops happened as expected)"
    fi
}

echo
echo "==> Assertions"
check_allowed default-svc 120 40
check_allowed premium-svc 1200 40
check_allowed legacy-svc 30 40
check_denied_nonzero default-svc
check_denied_nonzero legacy-svc

exit "${fail}"
