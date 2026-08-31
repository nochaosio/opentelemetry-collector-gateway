#!/usr/bin/env bash
#
# otelcol-gateway vs otelcol-contrib, measured as IN vs OUT. See README.md.
#
# Needs: make build, otelcol-contrib v0.159.0, telemetrygen v0.159.0,
# and a Redis on 127.0.0.1:6399.
set -uo pipefail
export LC_ALL=C   # awk compares these numbers; a pt_BR decimal comma breaks that

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CFG_DIR="$SCRIPT_DIR/comparison"
OUT_DIR="$SCRIPT_DIR/results/comparison"
mkdir -p "$OUT_DIR"

GATEWAY="${GATEWAY_BIN:-$REPO_ROOT/cmd/otelcol-gateway/otelcol-gateway}"
CONTRIB="${CONTRIB_BIN:-/tmp/otelcol-contrib}"
TG="${TELEMETRYGEN:-$(command -v telemetrygen || echo "$(go env GOBIN 2>/dev/null)/telemetrygen")}"
REDIS="${REDIS_CLI:-docker exec -i bench-redis redis-cli}"

SUT_CPUS="${SUT_CPUS:-0-3,12-15}"
SINK_CPUS="${SINK_CPUS:-4-5,16-17}"
GEN_CPUS="${GEN_CPUS:-6-11,18-23}"

OTLP_PORT=34317
SINK_OTLP_PORT=34417
SUT_PROM=38888
SINK_PROM=38889
SINK_COUNT_PROM=39090

export GRPC_GO_LOG_SEVERITY_LEVEL=ERROR

for b in "$GATEWAY" "$CONTRIB" "$TG"; do
    [ -x "$b" ] || { echo "missing binary: $b" >&2; exit 2; }
done
$REDIS ping >/dev/null 2>&1 || { echo "no Redis on 6399" >&2; exit 2; }

# A collector that cannot bind leaves the port answering anyway, so a busy port
# silently becomes "measured someone else's process".
for p in $OTLP_PORT $SINK_OTLP_PORT $SUT_PROM $SINK_PROM $SINK_COUNT_PROM; do
    if ss -ltn 2>/dev/null | grep -qE ":$p " || docker ps --format '{{.Ports}}' 2>/dev/null | grep -q ":$p->"; then
        echo "port $p is already in use; set the port vars or free it" >&2
        exit 2
    fi
done

RESULTS="$OUT_DIR/results.tsv"
: > "$RESULTS"
printf 'scenario\tarm\tin_spans\tout_spans\tkept_pct\tcheckout_out\tprobe_out\tlegacy_out\tmax_rss_mb\tavg_cpu_pct\twall_s\n' >> "$RESULTS"

# --- helpers ---------------------------------------------------------------

prom_sum() {
    local port=$1 metric=$2 filter=${3:-}
    curl -s --max-time 5 "127.0.0.1:$port/metrics" 2>/dev/null \
        | grep "^${metric}" \
        | { [ -n "$filter" ] && grep -- "$filter" || cat; } \
        | awk '{s+=$NF} END{printf "%.0f", s+0}'
}

wait_port() {
    local port=$1
    for _ in $(seq 1 60); do
        (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null && return 0
        sleep 0.25
    done
    return 1
}

SINK_PID=""
start_sink() {
    taskset -c "$SINK_CPUS" "$CONTRIB" --config "$CFG_DIR/sink.yaml" \
        > "$OUT_DIR/sink.log" 2>&1 &
    SINK_PID=$!
    wait_port $SINK_OTLP_PORT || { echo "sink did not start" >&2; exit 1; }
}

SUT_PID=""
SAMPLER_PID=""
start_sut() {
    local bin=$1 cfg=$2 tag=$3
    taskset -c "$SUT_CPUS" "$bin" --config "$cfg" > "$OUT_DIR/$tag.log" 2>&1 &
    SUT_PID=$!
    if ! wait_port $OTLP_PORT || ! kill -0 "$SUT_PID" 2>/dev/null; then
        echo "FAIL: $tag did not start; see $OUT_DIR/$tag.log" >&2
        tail -5 "$OUT_DIR/$tag.log" >&2
        return 1
    fi
    ( while kill -0 "$SUT_PID" 2>/dev/null; do
          ps -o rss=,%cpu= -p "$SUT_PID" 2>/dev/null
          sleep 0.25
      done ) > "$OUT_DIR/$tag.samples" &
    SAMPLER_PID=$!
}

stop_sut() {
    [ -n "$SUT_PID" ] && kill -TERM "$SUT_PID" 2>/dev/null
    [ -n "$SUT_PID" ] && wait "$SUT_PID" 2>/dev/null
    [ -n "$SAMPLER_PID" ] && kill "$SAMPLER_PID" 2>/dev/null
    SUT_PID=""; SAMPLER_PID=""
    for _ in $(seq 1 40); do
        ss -ltn 2>/dev/null | grep -qE ":$OTLP_PORT " || break
        sleep 0.25
    done
}

# svc is a span attribute: it is what the sink counts by.
gen() {
    local svc=$1 route=$2 dur=$3 workers=$4 rate=$5 tag=${6:-$1}
    taskset -c "$GEN_CPUS" "$TG" traces \
        --otlp-endpoint "127.0.0.1:$OTLP_PORT" --otlp-insecure \
        --duration "$dur" --workers "$workers" --rate "$rate" --child-spans 2 \
        --service "$svc" \
        --telemetry-attributes "svc=\"$svc\"" \
        --telemetry-attributes "http.route=\"$route\"" \
        > "$OUT_DIR/gen-$tag.log" 2>&1
}

sink_out() { prom_sum $SINK_COUNT_PROM bench_sink_spans_total "${1:-}"; }

record() {
    local scenario=$1 arm=$2 in_s=$3 out_s=$4 co=$5 pr=$6 lg=$7 tag=$8 wall=$9
    local rss cpu kept
    rss=$(awk '{if($1+0>m) m=$1+0} END{printf "%.0f", m/1024}' "$OUT_DIR/$tag.samples" 2>/dev/null)
    cpu=$(awk '{s+=$2; n++} END{if(n>0) printf "%.0f", s/n; else print 0}' "$OUT_DIR/$tag.samples" 2>/dev/null)
    kept=$(awk -v o="$out_s" -v i="$in_s" 'BEGIN{if(i>0) printf "%.1f", 100*o/i; else print 0}')
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$scenario" "$arm" "$in_s" "$out_s" "$kept" "$co" "$pr" "$lg" \
        "${rss:-0}" "${cpu:-0}" "$wall" >> "$RESULTS"
    echo "  $arm: IN=$in_s OUT=$out_s (${kept}% kept)  rss=${rss}MB cpu=${cpu}%"
}

cleanup() {
    stop_sut
    [ -n "$SINK_PID" ] && kill -TERM "$SINK_PID" 2>/dev/null
    jobs -p | xargs -r kill 2>/dev/null
}
trap cleanup EXIT

# --- workload --------------------------------------------------------------

SCENARIOS="${SCENARIOS:-ABCD}"
want() { [[ "$SCENARIOS" == *"$1"* ]]; }

LOAD_SECS="${LOAD_SECS:-20}"
SURGE_SECS="${SURGE_SECS:-30}"
LIVE_SECS="${LIVE_SECS:-40}"

run_mixed_load() {
    local dur=$1
    local pids=()
    gen checkout   /api/checkout "${dur}s" 2 0 & pids+=($!)
    gen kube-probe /healthz      "${dur}s" 4 0 & pids+=($!)
    gen legacy-svc /api/legacy   "${dur}s" 2 0 & pids+=($!)
    wait "${pids[@]}" 2>/dev/null
}

# telemetrygen's --rate is spans/s per worker but saturates near 2k/worker, so
# volume comes from worker count at a rate that is actually honoured.
# legacy-svc: 12k spans/s, tripled to 36k at the halfway mark.
SURGE_RATE="${SURGE_RATE:-1000}"
SURGE_WORKERS="${SURGE_WORKERS:-12}"
run_surge_load() {
    local dur=$1 half=$(( $1 / 2 ))
    local pids=()
    gen checkout   /api/checkout "${dur}s" "$SURGE_WORKERS" "$SURGE_RATE" & pids+=($!)
    gen legacy-svc /api/legacy   "${dur}s" "$SURGE_WORKERS" "$SURGE_RATE" & pids+=($!)
    ( sleep "$half"
      gen legacy-svc /api/legacy "${half}s" $(( SURGE_WORKERS * 2 )) "$SURGE_RATE" legacy-svc-surge ) & pids+=($!)
    wait "${pids[@]}" 2>/dev/null
}

# The sink keeps running between arms, so every arm is measured as a delta.
snapshot() {
    S_IN=$(prom_sum $SUT_PROM otelcol_receiver_accepted_spans)
    S_OUT=$(sink_out)
    S_CO=$(sink_out 'svc="checkout"')
    S_PR=$(sink_out 'svc="kube-probe"')
    S_LG=$(sink_out 'svc="legacy-svc"')
}

measure_arm() {
    local scenario=$1 arm=$2 bin=$3 cfg=$4 dur=$5 load=${6:-run_mixed_load}
    local tag="${scenario}-${arm}"
    start_sut "$bin" "$cfg" "$tag" || { echo "  $arm: SKIPPED (failed to start)"; return 1; }
    snapshot
    local b_out=$S_OUT b_co=$S_CO b_pr=$S_PR b_lg=$S_LG
    local t0 t1
    t0=$(date +%s.%N)
    "$load" "$dur"
    t1=$(date +%s.%N)
    sleep 4   # drain batch + sending queue before the final read
    local in_s out_s
    in_s=$(prom_sum $SUT_PROM otelcol_receiver_accepted_spans)
    out_s=$(sink_out)
    record "$scenario" "$arm" \
        "$in_s" "$((out_s - b_out))" \
        "$(( $(sink_out 'svc="checkout"') - b_co ))" \
        "$(( $(sink_out 'svc="kube-probe"') - b_pr ))" \
        "$(( $(sink_out 'svc="legacy-svc"') - b_lg ))" \
        "$tag" "$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')"
    stop_sut
}


# Scenario C: halfway through the probe flood, an operator decides it must
# stop. Measures the latency of that decision and what making it costs.
DROP_RULE='{"signals":["traces"],"conditions":[{"source":"attribute","key":"http.route","op":"prefix","value":"/healthz"}]}'

live_change() {
    local arm=$1 bin=$2 cfg_before=$3 cfg_after=$4
    local tag="C-live-$arm"

    $REDIS DEL otelcol:filter:rules >/dev/null 2>&1
    $REDIS INCR otelcol:filter:rules:version >/dev/null 2>&1

    start_sut "$bin" "$cfg_before" "$tag" || return 1
    local base_probe base_checkout
    base_probe=$(sink_out 'svc="kube-probe"')
    base_checkout=$(sink_out 'svc="checkout"')

    local t0; t0=$(date +%s.%N)
    local pids=()
    gen checkout   /api/checkout "${LIVE_SECS}s" "$SURGE_WORKERS"          "$SURGE_RATE" & pids+=($!)
    gen kube-probe /healthz      "${LIVE_SECS}s" $(( SURGE_WORKERS * 2 )) "$SURGE_RATE" & pids+=($!)

    ( while :; do
          printf '%s\t%s\n' \
              "$(awk -v a="$t0" -v b="$(date +%s.%N)" 'BEGIN{printf "%.2f", b-a}')" \
              "$(sink_out 'svc="kube-probe"')"
          sleep 0.5
      done ) > "$OUT_DIR/$tag.timeline" &
    local poll=$!

    sleep $(( LIVE_SECS / 2 ))
    local decided; decided=$(awk -v a="$t0" -v b="$(date +%s.%N)" 'BEGIN{printf "%.2f", b-a}')
    local downtime=0

    # Empty cfg_after means "changes the rule without touching the process".
    # Keyed on mechanism, not arm name, so a new arm cannot land in the wrong
    # branch by being named badly.
    if [ -z "$cfg_after" ]; then
        $REDIS HSET otelcol:filter:rules drop-healthz "$DROP_RULE" >/dev/null
        $REDIS INCR otelcol:filter:rules:version >/dev/null
    else
        # The floor for a config change: one process, one machine, no rollout.
        local d0 d1
        d0=$(date +%s.%N)
        kill -TERM "$SUT_PID" 2>/dev/null; wait "$SUT_PID" 2>/dev/null
        kill "$SAMPLER_PID" 2>/dev/null
        taskset -c "$SUT_CPUS" "$bin" --config "$cfg_after" >> "$OUT_DIR/$tag.log" 2>&1 &
        SUT_PID=$!
        wait_port $OTLP_PORT
        d1=$(date +%s.%N)
        downtime=$(awk -v a="$d0" -v b="$d1" 'BEGIN{printf "%.1f", b-a}')
    fi

    wait "${pids[@]}" 2>/dev/null
    sleep 4
    kill "$poll" 2>/dev/null

    local settled leaked total
    settled=$(awk -F'\t' -v d="$decided" '
        $1 >= d && $2 > prev { last = $1 } { prev = $2 }
        END { if (last == "") print 0; else print last }' "$OUT_DIR/$tag.timeline")
    leaked=$(awk -F'\t' -v d="$decided" '
        $1 <= d { at = $2 } END { print $2 - at }' "$OUT_DIR/$tag.timeline")
    total=$(( $(sink_out 'svc="kube-probe"') - base_probe ))

    local tte; tte=$(awk -v s="$settled" -v d="$decided" 'BEGIN{printf "%.1f", (s>d)? s-d : 0}')

    # What the change cost the traffic that must never be touched.
    local co_sent co_got co_loss
    co_sent=$(grep -oE '"traces": [0-9]+' "$OUT_DIR/gen-checkout.log" | grep -oE '[0-9]+' | awk '{s+=$1*3} END{print s+0}')
    co_got=$(( $(sink_out 'svc="checkout"') - base_checkout ))
    co_loss=$(awk -v s="$co_sent" -v g="$co_got" 'BEGIN{if(s>0) printf "%.1f", 100*(s-g)/s; else print 0}')
    printf 'C-live\t%s\ttime_to_effect_s=%s\tingest_downtime_s=%s\tprobe_after_decision=%s\tcheckout_sent=%s\tcheckout_delivered=%s\tcheckout_lost_pct=%s\n' \
        "$arm" "$tte" "$downtime" "$leaked" "$co_sent" "$co_got" "$co_loss" >> "$OUT_DIR/live.tsv"
    echo "  $arm: effect in ${tte}s | downtime ${downtime}s | noise after decision ${leaked} spans | checkout lost ${co_loss}%"

    stop_sut
}

# --- scenarios -------------------------------------------------------------

start_sink
echo "sink up (pid $SINK_PID)"

if want A; then
echo
echo "[A] passthrough: identical config, no filtering. Overhead check."
measure_arm A-passthrough contrib "$CONTRIB" "$CFG_DIR/passthrough.yaml" "$LOAD_SECS"
measure_arm A-passthrough gateway "$GATEWAY" "$CFG_DIR/passthrough.yaml" "$LOAD_SECS"
fi

if want B; then
echo
echo "[B] static noise drop: contrib filter/OTTL vs gateway statefulfilter."
$REDIS DEL otelcol:filter:rules >/dev/null 2>&1
$REDIS HSET otelcol:filter:rules drop-healthz \
    '{"signals":["traces"],"conditions":[{"source":"attribute","key":"http.route","op":"prefix","value":"/healthz"}]}' >/dev/null
$REDIS INCR otelcol:filter:rules:version >/dev/null
measure_arm B-noise contrib "$CONTRIB" "$CFG_DIR/noise-contrib.yaml" "$LOAD_SECS"
measure_arm B-noise gateway "$GATEWAY" "$CFG_DIR/noise-gateway.yaml" "$LOAD_SECS"
fi

if want C; then
echo
echo "[C] live rule change: how fast can the drop decision reach the backend?"
: > "$OUT_DIR/live.tsv"
live_change gateway      "$GATEWAY" "$CFG_DIR/noise-gateway.yaml"      ""
live_change gateway-1s   "$GATEWAY" "$CFG_DIR/noise-gateway-fast.yaml" ""
live_change contrib      "$CONTRIB" "$CFG_DIR/passthrough.yaml"        "$CFG_DIR/noise-contrib.yaml"
fi

if want D; then
echo
echo "[D] noisy neighbour: legacy-svc triples mid-run."
measure_arm D-surge contrib-filter  "$CONTRIB" "$CFG_DIR/surge-contrib-filter.yaml"  "$SURGE_SECS" run_surge_load
measure_arm D-surge contrib-sampler "$CONTRIB" "$CFG_DIR/surge-contrib-sampler.yaml" "$SURGE_SECS" run_surge_load
measure_arm D-surge gateway         "$GATEWAY" "$CFG_DIR/surge-gateway.yaml"         "$SURGE_SECS" run_surge_load
fi

echo
echo "=== results ($RESULTS) ==="
column -t -s$'\t' "$RESULTS"
echo
echo "=== live rule change ==="
column -t -s$'\t' "$OUT_DIR/live.tsv"
