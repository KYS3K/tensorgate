#!/usr/bin/env bash
# Load balancer runs on the three bench VMs (bench/scripts/up.sh with UPS=1):
#   direct  the gateway answers with its built-in mock (the baseline from BENCHMARKS.md)
#   lb      the gateway balances over three mockupstream processes on the UPS VM
# Each run holds STREAMS concurrent SSE streams for DURATION and saves the loadgen log, mpstat of the
# gateway and upstream VMs and the gateway's /metrics taken near the end.
#
#   source bench/env.sh
#   bench/scripts/lb.sh                # 2000 and 4000 streams, direct and lb
#   STREAMS="3000" bench/scripts/lb.sh
set -u
SRV=${SRV:?run: source bench/env.sh}
CLI=${CLI:?} UPS=${UPS:?run up.sh with UPS=1} SRV_PRIV=${SRV_PRIV:?} UPS_PRIV=${UPS_PRIV:?}
STREAMS=${STREAMS:-"2000 4000"}
MODES=${MODES:-"direct lb"}
DURATION=${DURATION:-90}
OUT=${OUT:-bench/$(date +%F)-lb}
SSH="timeout 300 ssh -n -o BatchMode=yes"
mkdir -p "$OUT"

UPSTREAMS=http://$UPS_PRIV:9001,http://$UPS_PRIV:9002,http://$UPS_PRIV:9003

start_upstreams() {
  $SSH $UPS "pkill -f '^./mockupstream-linux'; sleep 0.5; for p in 9001 9002 9003; do setsid nohup ./mockupstream-linux --addr $UPS_PRIV:\$p > ups-\$p.log 2>&1 < /dev/null & done; sleep 1; pgrep -fc '^./mockupstream-linux'"
}

start_gateway() { # extra flags...
  $SSH $SRV "pkill -f '^./tensorgate-linux'; sleep 1; setsid nohup ./tensorgate-linux $* > srv.log 2>&1 < /dev/null & sleep 1; pgrep -f '^./tensorgate-linux' >/dev/null && echo gateway up"
}

cpu_avg() { # file: average busy % of the 'all' rows between seconds 30 and 80 (mpstat prints "08:02:48 PM all ...")
  awk '$3 == "all" && ++n >= 30 && n <= 80 { busy += 100 - $NF; c++ } END { if (c) printf "%.0f%%", busy / c; else printf "n/a" }' "$1"
}

run_case() { # name streams mode
  local name=$1 streams=$2 mode=$3 flags=""
  [ "$mode" = lb ] && flags="--upstreams $UPSTREAMS"
  echo "=== $name: $streams streams, $mode $(date +%T)"
  start_gateway "--pprof '' $flags"
  $SSH $SRV "setsid nohup mpstat -P ALL 1 $((DURATION + 5)) > mpstat-srv-$name.log 2>&1 < /dev/null &"
  $SSH $UPS "setsid nohup mpstat -P ALL 1 $((DURATION + 5)) > mpstat-ups-$name.log 2>&1 < /dev/null &"
  $SSH $CLI "cd /root; setsid nohup ./loadgen-linux -url http://$SRV_PRIV:8080/v1/chat/completions -streams $streams -rate 200 -tokens 2000 -tpot 30ms -duration ${DURATION}s > run-$name.log 2>&1 < /dev/null &"
  sleep $((DURATION - 20))
  $SSH $SRV "curl -s localhost:9090/metrics > metrics-$name.txt"
  $SSH $CLI "while pgrep -x loadgen-linux >/dev/null; do sleep 2; done"
  $SSH $SRV "pkill -TERM -f '^./tensorgate-linux'; sleep 2; pkill -x mpstat; true"
  $SSH $UPS "pkill -x mpstat; true"
  scp -q $CLI:/root/run-$name.log "$OUT/"
  scp -q $SRV:/root/mpstat-srv-$name.log $SRV:/root/metrics-$name.txt "$OUT/"
  scp -q $UPS:/root/mpstat-ups-$name.log "$OUT/"
  echo "    gateway CPU $(cpu_avg "$OUT/mpstat-srv-$name.log")  upstream VM CPU $(cpu_avg "$OUT/mpstat-ups-$name.log")"
  sed -n '/=== summary/,$p' "$OUT/run-$name.log" | grep -E "streams started|errors:|^ttft|^late|^gap" | sed 's/^/    /'
  sleep 8
}

echo "upstreams: $(start_upstreams)"
for s in $STREAMS; do
  for m in $MODES; do run_case "$m-$s" "$s" "$m"; done
done
ls -la "$OUT"
