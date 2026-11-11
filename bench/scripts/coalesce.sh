#!/usr/bin/env bash
# Flush coalescing on the bench VMs: flush-interval 0 vs 100ms at 4000/5000/6000 streams, 3m each, qdisc fq.
set -u
SRV=${SRV:?set SRV=root@<server public IP>}
CLI=${CLI:?set CLI=root@<client public IP>}
SRV_PRIV=${SRV_PRIV:?set SRV_PRIV=<server private IP>}
IF=${IF:-enp7s0}                       # server interface on the private network
OUT=${OUT:-bench/$(date +%F)-coalesce}
SSH="timeout 300 ssh -n -o BatchMode=yes"
NSTAT_RE='TcpOutSegs|TcpInSegs|TcpRetransSegs|TcpExtTCPTimeouts|TcpExtTCPDSACKRecv'
mkdir -p "$OUT"

cleanup() {
  $SSH $SRV "pkill -TERM -f '^./tensorgate-'; pkill -x mpstat; true"
  $SSH $CLI "pkill -x loadgen-linux; pkill -x mpstat; true"
}
trap cleanup EXIT

for n in 4000 5000 6000; do
  for fi in 0 100ms; do
    run="$n-$fi"
    echo "=== $run: start $(date +%T)"
    $SSH $SRV "tc qdisc replace dev $IF root fq; cd /root; setsid nohup ./tensorgate-coalesce -flush-interval $fi > srv-$run.log 2>&1 < /dev/null & sleep 1; pgrep -f '^./tensorgate-coalesce' >/dev/null && echo server up"
    $SSH $SRV "nstat -n; setsid nohup mpstat -P ALL 1 185 > /root/mpstat-server-$run.log 2>&1 < /dev/null &"
    $SSH $CLI "nstat -n; setsid nohup mpstat -P ALL 1 185 > /root/mpstat-client-$run.log 2>&1 < /dev/null &"
    $SSH $CLI "cd /root; setsid nohup ./loadgen-linux -url http://$SRV_PRIV:8080/v1/chat/completions -streams $n -rate 200 -tokens 2000 -tpot 30ms -duration 3m > run-$run.log 2>&1 < /dev/null &"

    sleep 5
    $SSH $CLI "while pgrep -x loadgen-linux >/dev/null; do sleep 2; done; echo loadgen done"
    $SSH $SRV "nstat -z | grep -E '$NSTAT_RE' > /root/nstat-server-$run.txt; curl -s localhost:9090/metrics | grep -E '^tensorgate_stream_flush_events_(sum|count)' > /root/flush-$run.txt"
    $SSH $SRV "pkill -TERM -f '^./tensorgate-coalesce'; sleep 3; pkill -x mpstat; true"
    $SSH $CLI "pkill -x mpstat; true"

    for f in mpstat-server-$run.log nstat-server-$run.txt flush-$run.txt srv-$run.log; do scp -q "$SRV:/root/$f" "$OUT/"; done
    for f in run-$run.log mpstat-client-$run.log; do scp -q "$CLI:/root/$f" "$OUT/"; done
    echo "=== $run: done $(date +%T)"
    sleep 10
  done
done
ls "$OUT"
