#!/usr/bin/env bash
# Manual end-to-end scenario for the load balancer: three mock upstreams behind
# one gateway, then kill / slow down / make sick / revive them and watch where
# the requests go. Needs free ports 8080, 9090 and 19001-19003.
#
#   scripts/lb-scenario.sh
set -u
cd "$(dirname "$0")/.."
work=$(mktemp -d)
pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null; wait 2>/dev/null; rm -rf "$work"; }
trap cleanup EXIT

go build -o "$work/mu" ./cmd/mockupstream && go build -o "$work/gw" ./cmd/tensorgate || exit 1

start_mu() { "$work/mu" --addr 127.0.0.1:"$1" >"$work/mu$1.log" 2>&1 & echo $!; }

# tally N: send N requests, print how many went to which upstream with which status
tally() {
  for _ in $(seq "$1"); do
    curl -s -o /dev/null -D - -X POST 127.0.0.1:8080/v1/chat/completions \
      -H 'content-type: application/json' --max-time 5 \
      -d '{"model":"m","messages":[{"role":"user","content":"hi"}],"stream":false}' |
      awk 'BEGIN{u="-";s="?"} /^HTTP/{s=$2} /^X-Upstream/{u=$2} END{print s, u}'
  done | sort | uniq -c
}

PA=$(start_mu 19001); PB=$(start_mu 19002); PC=$(start_mu 19003); pids+=("$PA" "$PC")
"$work/gw" --pprof "" \
  --upstreams http://127.0.0.1:19001,http://127.0.0.1:19002,http://127.0.0.1:19003 \
  --lb-check-interval 200ms --lb-check-timeout 100ms >"$work/gw.log" 2>&1 &
pids+=($!)
sleep 1

echo "== 1. all healthy: requests spread over A, B, C"
tally 30
echo "== 2. B is killed: after ~3 failed probes nothing goes to B"
kill "$PB"; sleep 1.2; tally 30
echo "== 3. B is back: it takes traffic again"
PB=$(start_mu 19002); pids+=("$PB"); sleep 1.2; tally 30
echo "== 4. C becomes slow (1s, probe timeout is 100ms): C is dropped"
curl -s -X POST '127.0.0.1:19003/admin/delay?d=1s'; sleep 1.5; tally 30
echo "== 5. C is fast again: C is back"
curl -s -X POST '127.0.0.1:19003/admin/delay?d=0s'; sleep 1.2; tally 30
echo "== 6. A answers /healthz with 500: A is dropped"
curl -s -X POST '127.0.0.1:19001/admin/health?status=500'; sleep 1.2; tally 30
echo "== 7. everything dead: 503 with Retry-After"
kill "$PB" "$PC"; sleep 1.2
curl -s -i -X POST 127.0.0.1:8080/v1/chat/completions -H 'content-type: application/json' \
  -d '{"model":"m","messages":[{"role":"user","content":"hi"}]}' | head -4
