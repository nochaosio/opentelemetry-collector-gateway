#!/usr/bin/env bash
#
# Sends a single OTLP/HTTP request and prints the HTTP status the collector
# answered with (200 = accepted, 429 = the whole batch was rate limited).
#
#   ./send.sh PORT SIGNAL COUNT [resource.key=value ...]
#
#   PORT   collector port (see README.md, one per scenario)
#   SIGNAL traces | metrics | logs
#   COUNT  items in the request: spans, data points or log records
#
# Env overrides:
#   STATUS=error    mark the spans with status=Error (critical items)
#   SEVERITY=ERROR  log severity text (default INFO)
#   ATTR=k=v,k=v    item-level attributes (span / data point / log record)
#   NAME=...        span or metric name
#   BODY=...        log body
set -euo pipefail

PORT=${1:?usage: ./send.sh PORT SIGNAL COUNT [resource.key=value ...]}
SIGNAL=${2:?signal must be traces, metrics or logs}
COUNT=${3:-1}
shift 3 2>/dev/null || shift $#
HOSTNAME_=${HOST:-localhost}

now_ns()   { echo "$(date +%s)000000000"; }
rand_hex() { openssl rand -hex "$1" 2>/dev/null || head -c "$1" /dev/urandom | od -An -tx1 | tr -d ' \n'; }

# "k=v k=v" -> OTLP attribute array
attrs_json() {
  local out="" pair
  for pair in "$@"; do
    [ -z "$pair" ] && continue
    [ -n "$out" ] && out="$out,"
    out="$out{\"key\":\"${pair%%=*}\",\"value\":{\"stringValue\":\"${pair#*=}\"}}"
  done
  printf '[%s]' "$out"
}

RES=$(attrs_json "$@")
IFS=',' read -ra ITEM_ATTRS <<< "${ATTR:-}"
ITEM=$(attrs_json "${ITEM_ATTRS[@]+"${ITEM_ATTRS[@]}"}")

case "${SEVERITY:-INFO}" in
  TRACE) SEV_NUM=1 ;; DEBUG) SEV_NUM=5 ;; INFO) SEV_NUM=9 ;;
  WARN)  SEV_NUM=13 ;; ERROR) SEV_NUM=17 ;; FATAL) SEV_NUM=21 ;; *) SEV_NUM=9 ;;
esac
# status code 2 = Error, 0 = Unset
[ "${STATUS:-}" = "error" ] && SPAN_STATUS='{"code":2}' || SPAN_STATUS='{"code":0}'

items=""
for i in $(seq 1 "$COUNT"); do
  [ -n "$items" ] && items="$items,"
  case "$SIGNAL" in
    traces)
      items="$items{\"traceId\":\"$(rand_hex 16)\",\"spanId\":\"$(rand_hex 8)\",\"name\":\"${NAME:-test-span}\",\"kind\":1,\"startTimeUnixNano\":\"$(now_ns)\",\"endTimeUnixNano\":\"$(now_ns)\",\"attributes\":$ITEM,\"status\":$SPAN_STATUS}" ;;
    metrics)
      items="$items{\"timeUnixNano\":\"$(now_ns)\",\"asDouble\":$i,\"attributes\":$ITEM}" ;;
    logs)
      items="$items{\"timeUnixNano\":\"$(now_ns)\",\"severityText\":\"${SEVERITY:-INFO}\",\"severityNumber\":$SEV_NUM,\"body\":{\"stringValue\":\"${BODY:-hello}\"},\"attributes\":$ITEM}" ;;
    *) echo "unknown signal: $SIGNAL (use traces, metrics or logs)" >&2; exit 2 ;;
  esac
done

case "$SIGNAL" in
  traces)  payload="{\"resourceSpans\":[{\"resource\":{\"attributes\":$RES},\"scopeSpans\":[{\"scope\":{\"name\":\"playground\"},\"spans\":[$items]}]}]}" ;;
  metrics) payload="{\"resourceMetrics\":[{\"resource\":{\"attributes\":$RES},\"scopeMetrics\":[{\"scope\":{\"name\":\"playground\"},\"metrics\":[{\"name\":\"${NAME:-test_gauge}\",\"gauge\":{\"dataPoints\":[$items]}}]}]}]}" ;;
  logs)    payload="{\"resourceLogs\":[{\"resource\":{\"attributes\":$RES},\"scopeLogs\":[{\"scope\":{\"name\":\"playground\"},\"logRecords\":[$items]}]}]}" ;;
esac

code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://$HOSTNAME_:$PORT/v1/$SIGNAL" \
  -H 'Content-Type: application/json' -d "$payload")

printf '%s x%-3s %-40s -> HTTP %s\n' "$SIGNAL" "$COUNT" "$*" "$code"
