#!/usr/bin/env bash
# Capacity search: zeroalloc on fq, stepped stream counts, 3m each; per-run TCP counters incl. reordering/DSACK.
set -u
SRV=${SRV:?set SRV=root@<server public IP>}
CLI=${CLI:?set CLI=root@<client public IP>}
SRV_PRIV=${SRV_PRIV:?set SRV_PRIV=<server private IP>}
IF=${IF:-enp7s0}                       # server interface on the private network
OUT=${OUT:-bench/$(date +%F)-steps}
SSH="timeout 300 ssh -n -o BatchMode=yes"
NSTAT_RE='TcpOutSegs|TcpRetransSegs|TcpExtTCPTimeouts|TcpExtTCPFastRetrans|TcpExtTCPLostRetransmit|TcpExtTCPDSACKRecv|TcpExtTCPDSACKOldSent|TcpExtTCPSACKReorder|TcpExtTCPTSReorder|TcpExtTCPRenoReorder|TcpExtTCPSpuriousRTOs|TcpExtTCPDSACKIgnoredNoUndo|TcpExtTCPDSACKUndo|TcpExtTCPLossUndo'
mkdir -p "$OUT"

cleanup() {
  $SSH $SRV "pkill -TERM -f '^./tensorgate-'; pkill -x mpstat; true"
  $SSH $CLI "pkill -x loadgen-linux; pkill -x mpstat; true"
}
trap cleanup EXIT

for n in 3000 3500 4000 4500; do
  echo "=== $n streams: start $(date +%T)"
  $SSH $SRV "tc qdisc replace dev $IF root fq; cd /root; setsid nohup ./tensorgate-zeroalloc > srv-$n.log 2>&1 < /dev/null & sleep 1; pgrep -f '^./tensorgate-zeroalloc' >/dev/null && echo server up"
  $SSH $SRV "nstat -n; setsid nohup mpstat -P ALL 1 185 > /root/mpstat-server-$n.log 2>&1 < /dev/null &"
  $SSH $CLI "nstat -n; setsid nohup mpstat -P ALL 1 185 > /root/mpstat-client-$n.log 2>&1 < /dev/null &"
  $SSH $CLI "cd /root; setsid nohup ./loadgen-linux -url http://$SRV_PRIV:8080/v1/chat/completions -streams $n -rate 200 -tokens 2000 -tpot 30ms -duration 3m > run-$n.log 2>&1 < /dev/null &"

  sleep 5
  $SSH $CLI "while pgrep -x loadgen-linux >/dev/null; do sleep 2; done; echo loadgen done"
  $SSH $SRV "nstat -z | grep -E '$NSTAT_RE' > /root/nstat-server-$n.txt; tc -s qdisc show dev $IF > /root/qdisc-$n.txt"
  $SSH $CLI "nstat -z | grep -E '$NSTAT_RE' > /root/nstat-client-$n.txt"
  $SSH $SRV "pkill -TERM -f '^./tensorgate-zeroalloc'; sleep 3; pkill -x mpstat; true"
  $SSH $CLI "pkill -x mpstat; true"

  # one remote path per argument: modern scp (SFTP) doesn't expand {a,b} on the remote side
  for f in mpstat-server-$n.log nstat-server-$n.txt qdisc-$n.txt srv-$n.log; do scp -q "$SRV:/root/$f" "$OUT/"; done
  for f in run-$n.log mpstat-client-$n.log nstat-client-$n.txt; do scp -q "$CLI:/root/$f" "$OUT/"; done
  echo "=== $n streams: done $(date +%T)"
  sleep 10
done
ls "$OUT"
