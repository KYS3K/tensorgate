#!/usr/bin/env bash
# How much does the retry budget change what clients see when backends fail?
#
# Three mockupstreams behind one gateway, 40 workers sending short streams as
# fast as they can (about 500 requests per second on a laptop). For every failure
# scenario the gateway is started with each retry setting, and the gateway's own
# metrics say what happened:
#
#   errors  share of requests that ended as 5xx, aborted or no backend, as the client saw them
#   retries second tries made;  denied  second tries the budget refused
#   load    upstream attempts per client request: (requests + retries) / requests
#
# Needs free ports 8080, 9090, 19001-19003. Takes about 3 minutes.
#
#   scripts/lb-retry-experiment.sh            # all scenarios
#   DURATION=20 scripts/lb-retry-experiment.sh
#   EXTRA="--lb-passive-threshold 1000000" scripts/lb-retry-experiment.sh   # passive ejection off: only retries decide
#   SCENARIOS="all-flaky" scripts/lb-retry-experiment.sh
set -u
cd "$(dirname "$0")/.."
DURATION=${DURATION:-12}
EXTRA=${EXTRA:-}
SCENARIOS=${SCENARIOS:-one-dead one-flaky all-flaky}
work=$(mktemp -d)
pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null; wait 2>/dev/null; rm -rf "$work"; }
trap cleanup EXIT

go build -o "$work/mu" ./cmd/mockupstream && go build -o "$work/gw" ./cmd/tensorgate &&
  go build -o "$work/lg" ./cmd/loadgen || exit 1

UPS=http://127.0.0.1:19001,http://127.0.0.1:19002,http://127.0.0.1:19003

metric() { # metric NAME [CLASS_REGEX]: sum of the series, optionally only those whose class matches
  curl -s 127.0.0.1:9090/metrics | awk -v n="$1" -v c="${2:-}" '
    index($0, n) == 1 && substr($0, length(n) + 1, 1) ~ /[{ ]/ { if (c == "" || $0 ~ ("class=\"(" c ")\"")) s += $NF }
    END { printf "%d", s }'
}

run() { # run SCENARIO CONFIG_NAME GATEWAY_FLAGS...
  local scenario=$1 name=$2; shift 2
  pids=()
  for p in 19001 19002 19003; do "$work/mu" --addr 127.0.0.1:$p >/dev/null 2>&1 & pids+=($!); done
  "$work/gw" --pprof "" --upstreams "$UPS" --lb-check-interval 500ms --lb-check-timeout 200ms $EXTRA "$@" >"$work/gw.log" 2>&1 &
  pids+=($!)
  sleep 1
  case $scenario in
    one-dead)   kill "${pids[1]}" ;;
    one-flaky)  curl -s -X POST '127.0.0.1:19002/admin/fail?p=0.5' ;;
    all-flaky)  for p in 19001 19002 19003; do curl -s -X POST "127.0.0.1:$p/admin/fail?p=0.3"; done ;;
  esac
  "$work/lg" -url http://127.0.0.1:8080/v1/chat/completions -streams 40 -rate 400 -tokens 3 -tpot 5ms -ttft 10ms \
    -backoff 1ms -honor-retry-after=false -duration "${DURATION}s" >"$work/lg.log" 2>&1

  local req err ret den
  req=$(metric tensorgate_lb_requests_total)
  err=$(metric tensorgate_lb_requests_total '5xx|aborted|no_backend')
  ret=$(metric tensorgate_lb_retries_total)
  den=$(metric tensorgate_lb_retry_budget_exhausted_total)
  awk -v s="$scenario" -v n="$name" -v req="$req" -v err="$err" -v ret="$ret" -v den="$den" 'BEGIN {
    printf "%-10s %-14s %8d  %6.2f%%  %7d  %7d  %5.2f\n", s, n, req, req ? 100*err/req : 0, ret, den, req ? (req+ret)/req : 0 }'

  kill "${pids[@]}" 2>/dev/null; wait 2>/dev/null; sleep 0.5
}

printf "%-10s %-14s %8s  %7s  %7s  %7s  %5s\n" scenario config requests errors retries denied load
for scenario in $SCENARIOS; do
  run $scenario "no-retry"     --lb-max-attempts 1
  run $scenario "budget-10%"   --lb-retry-budget-percent 10
  run $scenario "budget-20%"   --lb-retry-budget-percent 20
  run $scenario "unlimited"    --lb-retry-budget-percent 100000
done
