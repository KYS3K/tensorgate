#!/usr/bin/env bash
# zeroalloc under 5000 streams with different egress qdiscs on the server; per-run TCP and qdisc counters.
set -u
SRV=${SRV:?set SRV=root@<server public IP>}
CLI=${CLI:?set CLI=root@<client public IP>}
SRV_PRIV=${SRV_PRIV:?set SRV_PRIV=<server private IP>}
IF=${IF:-enp7s0}                       # server interface on the private network
OUT=${OUT:-bench/$(date +%F)-qdisc}
SSH="timeout 300 ssh -n -o BatchMode=yes"
NSTAT_RE='TcpOutSegs|TcpRetransSegs|TcpExtTCPTimeouts|TcpExtTCPLossProbes|TcpExtTCPFastRetrans|TcpExtTCPSpuriousRTOs|TcpExtTCPLostRetransmit'
mkdir -p "$OUT"

restore() {
  $SSH $SRV "pkill -TERM -f '^./tensorgate-'; pkill -x mpstat; tc qdisc replace dev $IF root fq_codel; ip link set $IF txqueuelen 1000; tc qdisc show dev $IF; ip link show $IF | head -1"
  $SSH $CLI "pkill -x loadgen-linux; pkill -x mpstat; true"
}
trap restore EXIT

for spec in "fq_codel|fq_codel|1000" "fq|fq|1000" "pfifo|pfifo limit 10000|10000"; do
  IFS='|' read -r name qd qlen <<<"$spec"
  echo "=== $name: start $(date +%T)"
  # replacing the qdisc also zeroes its counters
  $SSH $SRV "ip link set $IF txqueuelen $qlen; tc qdisc replace dev $IF root $qd; tc qdisc show dev $IF"
  $SSH $SRV "cd /root; setsid nohup ./tensorgate-zeroalloc > srv-$name.log 2>&1 < /dev/null & sleep 1; pgrep -f '^./tensorgate-zeroalloc' >/dev/null && echo server up"
  $SSH $SRV "nstat -n; setsid nohup mpstat -P ALL 1 185 > /root/mpstat-server-$name.log 2>&1 < /dev/null &"
  $SSH $CLI "nstat -n; setsid nohup mpstat -P ALL 1 185 > /root/mpstat-client-$name.log 2>&1 < /dev/null &"
  $SSH $CLI "cd /root; setsid nohup ./loadgen-linux -url http://$SRV_PRIV:8080/v1/chat/completions -streams 5000 -rate 200 -tokens 2000 -tpot 30ms -duration 3m > run-$name.log 2>&1 < /dev/null &"

  sleep 5
  $SSH $CLI "while pgrep -x loadgen-linux >/dev/null; do sleep 2; done; echo loadgen done"
  $SSH $SRV "nstat -z | grep -E '$NSTAT_RE' > /root/nstat-server-$name.txt; tc -s qdisc show dev $IF > /root/qdisc-$name.txt"
  $SSH $CLI "nstat -z | grep -E '$NSTAT_RE' > /root/nstat-client-$name.txt"
  $SSH $SRV "pkill -TERM -f '^./tensorgate-zeroalloc'; sleep 3; pkill -x mpstat; true"
  $SSH $CLI "pkill -x mpstat; true"

  # one remote path per argument: modern scp (SFTP) doesn't expand {a,b} on the remote side
  for f in mpstat-server-$name.log nstat-server-$name.txt qdisc-$name.txt srv-$name.log; do scp -q "$SRV:/root/$f" "$OUT/"; done
  for f in run-$name.log mpstat-client-$name.log nstat-client-$name.txt; do scp -q "$CLI:/root/$f" "$OUT/"; done
  echo "=== $name: done $(date +%T)"
  sleep 10
done
ls "$OUT"
