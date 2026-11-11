#!/usr/bin/env bash
# A/B load run: for each server build, 3m of 5000 streams, CPU profile at 45-75s, mpstat on both VMs.
set -u
SRV=${SRV:?set SRV=root@<server public IP>}
CLI=${CLI:?set CLI=root@<client public IP>}
SRV_PRIV=${SRV_PRIV:?set SRV_PRIV=<server private IP>}
IF=${IF:-enp7s0}                       # server interface on the private network
OUT=${OUT:-bench/$(date +%F)-ab}
SSH="timeout 300 ssh -n -o BatchMode=yes"
mkdir -p "$OUT"

for v in baseline zeroalloc; do
  echo "=== $v: start $(date +%T)"
  # "cd; cmd &", not "cd && cmd &": backgrounding the whole && list leaves a shell holding the ssh channel
  $SSH $SRV "cd /root; setsid nohup ./tensorgate-$v > srv-$v.log 2>&1 < /dev/null & sleep 1; pgrep -f '^./tensorgate-$v' >/dev/null && echo server up"
  $SSH $SRV "setsid nohup mpstat -P ALL 1 185 > /root/mpstat-server-$v.log 2>&1 < /dev/null &"
  $SSH $CLI "setsid nohup mpstat -P ALL 1 185 > /root/mpstat-client-$v.log 2>&1 < /dev/null &"
  $SSH $CLI "cd /root; setsid nohup ./loadgen-linux -url http://$SRV_PRIV:8080/v1/chat/completions -streams 5000 -rate 200 -tokens 2000 -tpot 30ms -duration 3m > run-$v.log 2>&1 < /dev/null &"

  # steady state: past the 25s ramp-up
  $SSH $SRV "sleep 45; curl -s -o /root/cpu-$v.pb 'http://127.0.0.1:6060/debug/pprof/profile?seconds=30' && echo profile taken"
  $SSH $CLI "while pgrep -x loadgen-linux >/dev/null; do sleep 2; done; echo loadgen done"
  $SSH $SRV "pkill -TERM -f '^./tensorgate-$v'; sleep 3; pgrep -f '^./tensorgate-$v' || echo server stopped; pkill -x mpstat; true"
  $SSH $CLI "pkill -x mpstat; true"

  scp -q $SRV:/root/cpu-$v.pb $SRV:/root/mpstat-server-$v.log $SRV:/root/srv-$v.log "$OUT/"
  scp -q $CLI:/root/run-$v.log $CLI:/root/mpstat-client-$v.log "$OUT/"
  echo "=== $v: done $(date +%T)"
  sleep 10 # let TIME_WAIT and the box settle
done
ls -la "$OUT"
