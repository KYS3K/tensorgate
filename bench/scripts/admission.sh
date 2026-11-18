#!/usr/bin/env bash
# Admission control A/B: the same overload (more streams than the server holds) against the same binary,
# once with the gateway bare and once with -admission. mpstat on both VMs, loadgen CSV for the p99 chart,
# and the admission counters scraped from the server's /metrics.
#
#   source bench/env.sh && bench/scripts/admission.sh
#
# Defaults: 8000 streams against a 5000-stream limit (BENCHMARKS.md: ~5000 fit the SLO on 2 vCPU).
# KEYS spreads the clients over many API keys so the per-key rate limit stays out of the way and the
# stream limit (503) is what is measured. Use KEYS=1 to see the 429 path instead.
set -u
SRV=${SRV:?run: source bench/env.sh}
CLI=${CLI:?run: source bench/env.sh}
SRV_PRIV=${SRV_PRIV:?run: source bench/env.sh}
STREAMS=${STREAMS:-8000}
KEYS=${KEYS:-1000}
DUR_S=${DUR_S:-180}
ADM_FLAGS=${ADM_FLAGS:--admission -max-streams 5000}
MODES=${MODES:-off on}          # MODES=on re-runs only the admission side
OUT=${OUT:-bench/$(date +%F)-admission}
SSH="timeout 300 ssh -n -o BatchMode=yes"
SSH_WAIT="timeout $((DUR_S + 120)) ssh -n -o BatchMode=yes" # waits for the whole loadgen run
mkdir -p "$OUT"

cleanup() {
  $SSH $SRV "pkill -TERM -f '^./tensorgate-linux'; pkill -x mpstat; true"
  $SSH $CLI "pkill -x loadgen-linux; pkill -x mpstat; true"
}
trap cleanup EXIT

for mode in $MODES; do
  flags=""
  [ "$mode" = on ] && flags=$ADM_FLAGS
  echo "=== admission $mode: start $(date +%T)  (server flags: '$flags')"
  # the flags are the only record of what this run was; keep them next to its data
  echo "server flags: $flags" > "$OUT/flags-$mode.txt"
  echo "loadgen: -streams $STREAMS -keys $KEYS -duration ${DUR_S}s" >> "$OUT/flags-$mode.txt"
  # "cd; cmd &", not "cd && cmd &": backgrounding the whole && list leaves a shell holding the ssh channel
  $SSH $SRV "cd /root; setsid nohup ./tensorgate-linux $flags > srv-$mode.log 2>&1 < /dev/null & sleep 1; pgrep -f '^./tensorgate-linux' >/dev/null && echo server up"
  $SSH $SRV "setsid nohup mpstat -P ALL 1 $((DUR_S + 5)) > /root/mpstat-server-$mode.log 2>&1 < /dev/null &"
  $SSH $CLI "setsid nohup mpstat -P ALL 1 $((DUR_S + 5)) > /root/mpstat-client-$mode.log 2>&1 < /dev/null &"
  $SSH $CLI "cd /root; setsid nohup ./loadgen-linux -url http://$SRV_PRIV:8080/v1/chat/completions -streams $STREAMS -rate 200 -keys $KEYS -tokens 2000 -tpot 30ms -ttft 200ms -duration ${DUR_S}s -csv run-$mode.csv > run-$mode.log 2>&1 < /dev/null &"

  sleep 5
  $SSH_WAIT $CLI "while pgrep -x loadgen-linux >/dev/null; do sleep 2; done; echo loadgen done"
  # before the server stops: the counters die with the process (all zero in the "off" run, kept for symmetry)
  $SSH $SRV "curl -s localhost:9090/metrics | grep -E '^tensorgate_admission' > /root/metrics-$mode.txt"
  $SSH $SRV "pkill -TERM -f '^./tensorgate-linux'; sleep 3; pgrep -f '^./tensorgate-linux' || echo server stopped; pkill -x mpstat; true"
  $SSH $CLI "pkill -x mpstat; true"

  scp -q $SRV:/root/mpstat-server-$mode.log $SRV:/root/srv-$mode.log $SRV:/root/metrics-$mode.txt "$OUT/"
  scp -q $CLI:/root/run-$mode.log $CLI:/root/run-$mode.csv $CLI:/root/mpstat-client-$mode.log "$OUT/"
  echo "=== admission $mode: done $(date +%T)"
  sleep 10 # let TIME_WAIT and the box settle
done

echo
echo "=== loadgen summaries"
for mode in $MODES; do echo "--- admission $mode"; sed -n '/=== summary/,$p' "$OUT/run-$mode.log"; done
ls -la "$OUT"
