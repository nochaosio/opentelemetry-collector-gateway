#!/usr/bin/env bash
# Fair benchmark: same config, same workload, same host.
#
# Requirements:
#   - ./cmd/otelcol-gateway/otelcol-gateway  (run `make build` first)
#   - /tmp/otelcol                           (upstream binary for comparison)
#   - telemetrygen in $PATH or $HOME/go/bin  (go install go.opentelemetry.io/collector-contrib/cmd/telemetrygen@latest)
set -u
export GRPC_GO_LOG_SEVERITY_LEVEL=ERROR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CFG="$SCRIPT_DIR/config-baseline.yaml"
TG="${TELEMETRYGEN:-$HOME/go/bin/telemetrygen}"
[ -x "$TG" ] || TG="$(command -v telemetrygen)"

OUT="$SCRIPT_DIR/results"
mkdir -p "$OUT"
RESULTS="$OUT/summary.txt"
: > "$RESULTS"

bench() {
  local name=$1 bin=$2
  pkill -9 -f "$bin" 2>/dev/null || true
  sleep 0.5

  "$bin" --config "$CFG" > /dev/null 2>&1 &
  local pid=$!
  for i in $(seq 1 25); do ss -ltn 2>/dev/null | grep -q ':14317 ' && break; sleep 0.2; done
  if ! kill -0 $pid 2>/dev/null; then
    echo "FAIL $name did not start" | tee -a "$RESULTS"
    return 1
  fi

  (while kill -0 $pid 2>/dev/null; do
     ps -o rss=,%cpu= -p $pid 2>/dev/null
     sleep 0.2
   done) > "$OUT/${name}.samples" &
  local spid=$!

  # Warm-up
  "$TG" traces --otlp-endpoint 127.0.0.1:14317 --otlp-insecure \
    --duration 1s --workers 4 --rate 0 >/dev/null 2>&1

  # Main workload
  local start=$(date +%s.%N)
  "$TG" traces --otlp-endpoint 127.0.0.1:14317 --otlp-insecure \
    --duration 8s --workers 20 --rate 0 --child-spans 2 \
    > "$OUT/${name}.tg" 2>&1
  local end=$(date +%s.%N)

  sleep 1
  kill -TERM $pid 2>/dev/null
  wait $pid 2>/dev/null
  kill $spid 2>/dev/null; wait $spid 2>/dev/null

  local sent=$(grep -oP 'traces generated:\s*\K[0-9]+' "$OUT/${name}.tg" | tail -1)
  [ -z "$sent" ] && sent=$(grep -oP '"traces":\s*\K[0-9]+' "$OUT/${name}.tg" | tail -1)
  local dur=$(awk -v s=$start -v e=$end 'BEGIN{printf "%.2f", e-s}')
  local max_rss=$(awk '{if($1+0>m) m=$1+0} END{print m+0}' "$OUT/${name}.samples")
  local avg_cpu=$(awk '{s+=$2; n++} END{if(n>0) printf "%.1f", s/n; else print "0"}' "$OUT/${name}.samples")
  local max_cpu=$(awk '{if($2+0>m) m=$2+0} END{printf "%.1f", m+0}' "$OUT/${name}.samples")
  # child_spans=2 => 3 spans per trace
  local throughput=$(awk -v s="${sent:-0}" -v d=$dur 'BEGIN{if(d>0) printf "%.0f", (s*3)/d; else print 0}')

  {
    echo "=== $name ==="
    echo "duration_sec=$dur"
    echo "traces_generated=$sent   (spans=$((${sent:-0}*3)))"
    echo "throughput_spans_per_sec=$throughput"
    echo "max_rss_kb=$max_rss   avg_cpu_pct=$avg_cpu   max_cpu_pct=$max_cpu"
  } | tee -a "$RESULTS"
}

GATEWAY_BIN="${GATEWAY_BIN:-$REPO_ROOT/cmd/otelcol-gateway/otelcol-gateway}"
UPSTREAM_BIN="${UPSTREAM_BIN:-/tmp/otelcol}"

bench gateway  "$GATEWAY_BIN"
sleep 2
bench upstream "$UPSTREAM_BIN"
echo "=== done ==="
cat "$RESULTS"
