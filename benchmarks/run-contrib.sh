#!/usr/bin/env bash
#
# Fair benchmark: gateway vs otelcol-contrib, same config, same workload.
# Uses port 17317 so it can run next to the existing `run.sh` (which uses
# 14317 for gateway-vs-upstream).
#
# Requirements:
#   - ./cmd/otelcol-gateway/otelcol-gateway   (run `make build` first)
#   - /tmp/otelcol-contrib                     (download the official release)
#   - telemetrygen in $PATH or $HOME/go/bin
#
# To grab the contrib binary:
#   curl -fsSL -o /tmp/otelcol-contrib.tar.gz \
#     https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v0.143.0/otelcol-contrib_0.143.0_linux_amd64.tar.gz
#   tar -xzf /tmp/otelcol-contrib.tar.gz -C /tmp otelcol-contrib
set -u
export GRPC_GO_LOG_SEVERITY_LEVEL=ERROR
export PATH="$HOME/go/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CFG="$SCRIPT_DIR/config-baseline-contrib.yaml"
GATEWAY="${GATEWAY_BIN:-$REPO_ROOT/cmd/otelcol-gateway/otelcol-gateway}"
CONTRIB="${CONTRIB_BIN:-/tmp/otelcol-contrib}"
TG="${TELEMETRYGEN:-$HOME/go/bin/telemetrygen}"
[ -x "$TG" ] || TG="$(command -v telemetrygen)"
PORT=17317

OUT="$SCRIPT_DIR/results"
mkdir -p "$OUT"
RESULTS="$OUT/summary-contrib.txt"
: > "$RESULTS"

for bin in "$GATEWAY" "$CONTRIB" "$TG"; do
    [ -x "$bin" ] || { echo "missing binary: $bin" >&2; exit 2; }
done

bench() {
    local name=$1 bin=$2

    "$bin" --config "$CFG" > /dev/null 2>&1 &
    local pid=$!
    for _ in $(seq 1 25); do
        (echo > /dev/tcp/127.0.0.1/$PORT) 2>/dev/null && break
        sleep 0.2
    done
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "FAIL $name did not start" | tee -a "$RESULTS"; return 1
    fi

    (while kill -0 "$pid" 2>/dev/null; do
         ps -o rss=,%cpu= -p "$pid" 2>/dev/null
         sleep 0.2
     done) > "$OUT/${name}-contrib.samples" &
    local spid=$!

    "$TG" traces --otlp-endpoint 127.0.0.1:$PORT --otlp-insecure \
        --duration 1s --workers 4 --rate 0 >/dev/null 2>&1

    local start end
    start=$(date +%s.%N)
    "$TG" traces --otlp-endpoint 127.0.0.1:$PORT --otlp-insecure \
        --duration 8s --workers 20 --rate 0 --child-spans 2 \
        > "$OUT/${name}-contrib.tg" 2>&1
    end=$(date +%s.%N)

    sleep 1
    kill -TERM "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    kill "$spid" 2>/dev/null
    wait "$spid" 2>/dev/null

    local sent dur max_rss avg_cpu max_cpu throughput
    sent=$(grep -oP 'traces generated:\s*\K[0-9]+' "$OUT/${name}-contrib.tg" | tail -1)
    [ -z "$sent" ] && sent=$(grep -oP '"traces":\s*\K[0-9]+' "$OUT/${name}-contrib.tg" | tail -1)
    dur=$(awk -v s="$start" -v e="$end" 'BEGIN{printf "%.2f", e-s}')
    max_rss=$(awk '{if($1+0>m) m=$1+0} END{print m+0}' "$OUT/${name}-contrib.samples")
    avg_cpu=$(awk '{s+=$2; n++} END{if(n>0) printf "%.1f", s/n; else print "0"}' "$OUT/${name}-contrib.samples")
    max_cpu=$(awk '{if($2+0>m) m=$2+0} END{printf "%.1f", m+0}' "$OUT/${name}-contrib.samples")
    throughput=$(awk -v s="${sent:-0}" -v d="$dur" 'BEGIN{if(d>0) printf "%.0f", (s*3)/d; else print 0}')

    {
        echo "=== $name ==="
        echo "duration_sec=$dur"
        echo "traces_generated=$sent   (spans=$((${sent:-0}*3)))"
        echo "throughput_spans_per_sec=$throughput"
        echo "max_rss_kb=$max_rss   avg_cpu_pct=$avg_cpu   max_cpu_pct=$max_cpu"
    } | tee -a "$RESULTS"
}

bench gateway "$GATEWAY"
sleep 2
bench contrib "$CONTRIB"
echo "=== done ==="
cat "$RESULTS"
