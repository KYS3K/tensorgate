#!/usr/bin/env bash
# Game day: hold steady load through the load balancer and break the upstreams one after another,
# while Prometheus (on the UPS VM) watches the gateway and evaluates the alert rules in ops/alerts.yml.
# Everything the script does to the system is written with a timestamp to $OUT/events.log, and the alert
# states seen by Prometheus to $OUT/alerts.log, so the POSTMORTEM timeline comes from data, not from memory.
#
# Needs: bench/scripts/up.sh with UPS=1, `apt-get install prometheus` on the UPS VM (this script does it).
#
#   source bench/env.sh
#   bench/scripts/lb-gameday.sh
set -u
SRV=${SRV:?run: source bench/env.sh}
CLI=${CLI:?} UPS=${UPS:?run up.sh with UPS=1} SRV_PRIV=${SRV_PRIV:?} UPS_PRIV=${UPS_PRIV:?}
STREAMS=${STREAMS:-1500}
OUT=${OUT:-bench/$(date +%F)-gameday}
SSH="timeout 120 ssh -n -o BatchMode=yes"
mkdir -p "$OUT"
UPSTREAMS=http://$UPS_PRIV:9001,http://$UPS_PRIV:9002,http://$UPS_PRIV:9003

# the timeline of the incident, seconds after the load is steady
T_KILL=${T_KILL:-120}      # upstream B is killed (SIGKILL)
T_REVIVE=${T_REVIVE:-300}    # upstream B is started again
T_SLOW=${T_SLOW:-420}      # upstream C answers 700 ms late: slower, but still passes the 1 s health probe
T_FAST=${T_FAST:-600}      # C is fast again
T_FLAKY=${T_FLAKY:-720}     # upstream A fails half of its requests with 502, /healthz stays fine
T_FIXED=${T_FIXED:-840}     # A is fixed
T_END=${T_END:-960}

now() { date +%s; }
event() { echo "$(now) $(date +%T) $*" | tee -a "$OUT/events.log"; }

echo "=== prometheus on the UPS VM"
$SSH $UPS "DEBIAN_FRONTEND=noninteractive apt-get install -y -qq prometheus >/dev/null 2>&1; systemctl stop prometheus; pkill -x prometheus; sleep 1; which prometheus promtool"
scp -q ops/alerts.yml "$UPS:/root/alerts.yml"
$SSH $UPS "cat > /root/prometheus.yml <<YML
global:
  scrape_interval: 5s
  evaluation_interval: 5s
rule_files:
  - /root/alerts.yml
scrape_configs:
  - job_name: tensorgate
    static_configs:
      - targets: ['$SRV_PRIV:9090']
YML
promtool check config /root/prometheus.yml"
$SSH $UPS "rm -rf /root/promdata; setsid nohup prometheus --config.file=/root/prometheus.yml --storage.tsdb.path=/root/promdata --web.listen-address=127.0.0.1:9099 > prom.log 2>&1 < /dev/null & sleep 3; curl -s localhost:9099/-/ready"

echo "=== start the system"
$SSH $UPS "pkill -f '^./mockupstream-linux'; sleep 0.5; for p in 9001 9002 9003; do setsid nohup ./mockupstream-linux --addr $UPS_PRIV:\$p > ups-\$p.log 2>&1 < /dev/null & done; sleep 1; pgrep -fc '^./mockupstream-linux'"
$SSH $SRV "pkill -f '^./tensorgate-linux'; sleep 1; setsid nohup ./tensorgate-linux --pprof '' --upstreams $UPSTREAMS > srv.log 2>&1 < /dev/null & sleep 1; pgrep -f '^./tensorgate-linux' >/dev/null && echo gateway up"
$SSH $CLI "cd /root; setsid nohup ./loadgen-linux -url http://$SRV_PRIV:8080/v1/chat/completions -streams $STREAMS -rate 200 -tokens 2000 -tpot 30ms -duration $((T_END + 60))s > gameday-load.log 2>&1 < /dev/null &"

# poll the alerts every 2 seconds in the background and write every change of state
cat > "$OUT/.fmt_alerts.py" <<'PYEND2'
import json, sys
try:
    alerts = json.load(sys.stdin)["data"]["alerts"]
except Exception:
    alerts = []
names = sorted(a["labels"]["alertname"] + ("/" + a["labels"]["backend"] if "backend" in a["labels"] else "") + "=" + a["state"] for a in alerts)
print(",".join(names))
PYEND2
(
  prev=""
  while sleep 2; do
    cur=$($SSH $UPS "curl -s localhost:9099/api/v1/alerts" | python3 "$OUT/.fmt_alerts.py")
    if [ "$cur" != "$prev" ]; then echo "$(now) $(date +%T) alerts: ${cur:-none}" >> "$OUT/alerts.log"; prev=$cur; fi
  done
) &
POLL=$!
trap 'kill $POLL 2>/dev/null' EXIT

START=$(( $(now) + 30 ))   # the ramp-up takes about 8 s, the rest is margin
sleep 30
event "load is steady: $STREAMS streams, 3 upstreams, all healthy"
at() { local t=$(( START + $1 - $(now) )); [ $t -gt 0 ] && sleep $t; }

at $T_KILL;   event "INJECT kill -9 upstream B (:9002)";               $SSH $UPS "pkill -9 -f 'addr $UPS_PRIV:9002'"
at $T_REVIVE; event "FIX start upstream B again";                       $SSH $UPS "setsid nohup ./mockupstream-linux --addr $UPS_PRIV:9002 > ups-9002.log 2>&1 < /dev/null &"
at $T_SLOW;   event "INJECT upstream C slow: +700ms on every request";  $SSH $UPS "curl -s -X POST 'http://$UPS_PRIV:9003/admin/delay?d=700ms'"
at $T_FAST;   event "FIX upstream C fast again";                        $SSH $UPS "curl -s -X POST 'http://$UPS_PRIV:9003/admin/delay?d=0s'"
at $T_FLAKY;  event "INJECT upstream A answers 502 to half of the completions"; $SSH $UPS "curl -s -X POST 'http://$UPS_PRIV:9001/admin/fail?p=0.5'"
at $T_FIXED;  event "FIX upstream A healthy again";                     $SSH $UPS "curl -s -X POST 'http://$UPS_PRIV:9001/admin/fail?p=0'"
at $T_END;    event "end of the game day"
sleep 20

echo "=== collect"
END_TS=$(now); BEGIN_TS=$((START - 60))
q() { # name promql: the range of a query over the whole game day, as JSON
  $SSH $UPS "curl -s -G localhost:9099/api/v1/query_range --data-urlencode 'query=$2' --data-urlencode start=$BEGIN_TS --data-urlencode end=$END_TS --data-urlencode step=5" > "$OUT/q-$1.json"
}
q error_ratio 'sum(rate(tensorgate_lb_requests_total{class=~"5xx|aborted|no_backend"}[30s])) / sum(rate(tensorgate_lb_requests_total{class!="canceled"}[30s]))'
q rps 'sum(rate(tensorgate_lb_requests_total[30s]))'
q ttft_p99 'histogram_quantile(0.99, sum by (le) (rate(tensorgate_lb_ttft_seconds_bucket[30s])))'
q ttft_p99_by_backend 'histogram_quantile(0.99, sum by (le, backend) (rate(tensorgate_lb_ttft_seconds_bucket[30s])))'
q ttft_p50 'histogram_quantile(0.50, sum by (le) (rate(tensorgate_lb_ttft_seconds_bucket[30s])))'
q alive 'tensorgate_lb_backend_alive'
q alerts 'ALERTS{alertname=~"Tensorgate.*"}'
q inflight 'tensorgate_lb_backend_inflight'
q retries 'rate(tensorgate_lb_retries_total[30s])'
q denied 'rate(tensorgate_lb_retry_budget_exhausted_total[30s])'
q by_backend 'sum by (backend) (rate(tensorgate_lb_requests_total[30s]))'
q errors_by_class 'sum by (class) (rate(tensorgate_lb_requests_total{class=~"5xx|aborted|no_backend"}[30s]))'
# stop the load BEFORE the gateway: otherwise the clients log hundreds of errors that belong to the shutdown, not to the incident
$SSH $CLI "pkill -x loadgen-linux; sleep 2; true"
scp -q $CLI:/root/gameday-load.log "$OUT/"
$SSH $SRV "curl -s localhost:9090/metrics > metrics-end.txt; pkill -TERM -f '^./tensorgate-linux'; sleep 1; true"
scp -q $SRV:/root/metrics-end.txt $SRV:/root/srv.log "$OUT/"
$SSH $UPS "pkill -x prometheus; pkill -f '^./mockupstream-linux'; true"
echo "$START" > "$OUT/start.txt"
echo "done: $OUT"; echo "--- events"; cat "$OUT/events.log"; echo "--- alerts"; cat "$OUT/alerts.log" 2>/dev/null
