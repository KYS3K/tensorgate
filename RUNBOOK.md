# Runbook: what to do when an alert fires

Written for someone who has never seen this system. Every alert below has the same five parts: **what it means** in plain words, **how bad it is**, **look here first** (copy-paste queries), **usual causes and the fix**, and **when to call someone**. The promises behind the alerts are in [SLO.md](SLO.md), the rules in [ops/alerts.yml](ops/alerts.yml).

## The picture in 30 seconds

```
clients --> tensorgate (the gateway, :8080) --> upstream A, B, C (the model servers)
                 |
                 +-- metrics on :9090 (Prometheus reads them)
```

- The gateway picks the upstream with the **fewest requests in flight** right now.
- An upstream that fails is **taken out of rotation** ("dead"). Two things can do it:
  - *real requests*: 3 failed requests in a row (network error, 502, 504 or a plain 503). It never takes out more than half of the upstreams this way;
  - *health probes*: every 2 seconds the gateway asks `GET /healthz`; 3 failures in a row mean dead, 2 successes in a row bring it back. Probes can take out any upstream, even the last one.
- A request that fails **before anything is sent to the client** is tried once more on another upstream, if the retry budget allows (20% of the requests of the last 10 seconds).
- The gateway writes a log line when it takes an upstream out of rotation or brings it back (`lb: upstream 10.0.0.2:9002 is dead: ...` and `... is back: ...`). A stream cut in the middle is logged too (`ReverseProxy read error during body copy: unexpected EOF`).

## Five queries you will use every time

Run them in Prometheus (or `curl -s localhost:9090/metrics | grep <name>` on the gateway host).

| Question | Query |
|---|---|
| Who is alive? (1 = yes) | `tensorgate_lb_backend_alive` |
| How many streams does each upstream hold right now? | `tensorgate_lb_backend_inflight` |
| Where do the errors come from? | `sum by (backend, class) (rate(tensorgate_lb_requests_total{class=~"5xx\|aborted\|no_backend"}[5m]))` |
| Are retries happening, and are they being refused? | `rate(tensorgate_lb_retries_total[5m])` and `rate(tensorgate_lb_retry_budget_exhausted_total[5m])` |
| How slow is it, overall? | `tensorgate:lb_ttft_seconds:p99_2m` (the 2-minute p99, in seconds) |
| Which upstream is the slow one? | `histogram_quantile(0.99, sum by (le, backend) (rate(tensorgate_lb_ttft_seconds_bucket[2m])))` |

Also useful: ask an upstream directly, bypassing the gateway: `curl -s -o /dev/null -w '%{http_code} %{time_total}s\n' http://<upstream>/healthz`. A healthy one answers `200` in a few milliseconds.

---

## TensorgateNoBackends (page)

**What it means.** Every upstream is out of rotation. The gateway has nobody to send requests to and answers `503` with a `Retry-After` header to everyone.

**How bad.** Total outage for all users. Act immediately.

**Look here first.**
1. `tensorgate_lb_backend_alive`: all zeros. Note the time they went to zero.
2. From the gateway host, check each upstream directly with the `curl` above. Two cases:
   - the upstreams **do not answer** (connection refused, timeout): they are really down;
   - the upstreams **answer fine**: the gateway thinks they are dead but they are not. Probes run every 2 seconds, so within a few seconds of the upstreams answering again the gateway brings them back by itself. If it does not, restart the gateway.

**Usual causes and the fix.**
- The model servers crashed or were restarted together (a bad deploy, out of GPU memory): start them again, then watch `tensorgate_lb_backend_alive` go back to 1 on its own.
- A network or firewall problem between the gateway and the upstreams: fix the network; the gateway needs no change.
- `/healthz` itself broke (it returns an error while real requests would work): fix `/healthz`. Do not edit the gateway during the incident.

**Call someone** if the upstreams are up and the gateway still refuses to bring them back after a restart.

---

## TensorgateBackendDown (ticket)

**What it means.** One upstream has been out of rotation for more than a minute. The alert names it (`backend` label).

**How bad.** Users probably notice nothing: the other upstreams take its traffic, and failed requests are retried. But you have lost capacity and a safety margin. If a second one dies, it gets worse. Look at it within hours, not minutes.

**Look here first.**
1. Which one, and since when: `tensorgate_lb_backend_alive == 0`.
2. Is it really broken? `curl` its `/healthz` directly.
3. Did users feel it? `rate(tensorgate_lb_retries_total[5m])` above 0 means some requests needed a second try; `tensorgate:lb_error_ratio:rate5m` shows the share that failed anyway.
4. Do the others cope? `tensorgate_lb_backend_inflight` on the remaining ones, and the CPU of the gateway.

**Usual causes and the fix.**
- Process crashed or host down: restart it. The gateway brings it back after 2 good probes (about 4 seconds), nothing to do on the gateway.
- Process is up but `/healthz` fails or is slow (more than 1 second): look at that server's own logs and load.
- It flaps (dead, alive, dead...): the server is half-broken. Take it out on purpose (stop it) instead of letting it bounce, and fix it calmly.

**Call someone** if two or more upstreams are down at once.

---

## TensorgateTTFTHigh (page)

**What it means.** The slowest 1% of requests wait more than 500 ms for their first byte, for over a minute. (Healthy on the bench stand: about 260 ms.)

**How bad.** Users feel the service as slow. Nothing is failing, so the error alerts stay quiet. This is the alert that catches a **slow but not dead** upstream, which the health probes cannot: a probe only fails after 1 second, so an upstream that answers in 700 ms looks perfectly healthy to the gateway while users wait.

**Look here first.**
1. Which upstream is slow? Run the "which upstream is the slow one" query above. One `backend` far above the others is your suspect. (Do not use `tensorgate_lb_backend_inflight` for this: a stream lives for a minute, so an upstream that is 0.7 seconds slower holds almost the same number of streams as a healthy one. The game day showed exactly that: 266 against 267.)
2. `tensorgate_lb_backend_alive`: if everyone is alive, the problem is slowness, not death. The gateway cannot see a slow upstream by itself while it still answers `/healthz` in under 1 second.
3. Is it everyone or one? If all upstreams show a similar high p99, the model servers are overloaded or the gateway itself is out of CPU (check the gateway host CPU: above ~90% means the gateway is the limit).
4. `curl -w '%{time_total}'` each upstream's `/healthz` directly and compare.

**Usual causes and the fix.**
- One slow upstream (a hot GPU, another job on the same machine, a stuck process): **stop it** so the gateway sends everything to the others. Stopping a slow upstream is better than leaving it, because the probes will not do it for you. Fix it, then start it again.
- All slow because there are too many streams for the capacity: add upstreams or reduce the load (admission control, see BENCHMARKS.md).
- The gateway out of CPU: it holds at most about 1000 comfortable streams per 2 vCPU when balancing; above that, TTFT climbs (see SLO.md).

**Call someone** if every upstream is slow at the same time and none of the causes above fits.

---

## TensorgateBackendFlapping (ticket)

**What it means.** One upstream went dead and came back at least twice in 10 minutes. Each time it was dead for less than a minute, so `TensorgateBackendDown` never fired.

**How bad.** Users usually notice nothing, because the retries hide the failures, but the upstream is sick: in the game day one that failed half of its requests flapped like this for two minutes and nobody was paged. Treat it as a half-broken server, not as a blip.

**Look here first.**
1. Which upstream (the `backend` label) and how often: `changes(tensorgate_lb_backend_alive[10m])`.
2. How much it costs: `rate(tensorgate_lb_retries_total[5m])` above 0 means requests are being redone on another upstream; `tensorgate_lb_retry_budget_exhausted_total` growing means the retries are running out and users will start to see errors.
3. The gateway log says why it was taken out: `... is dead: 3 requests in a row failed` (real requests fail, `/healthz` is fine) or `... is dead: health probes keep failing`.

**Usual causes and the fix.** The upstream answers `/healthz` but fails a part of the real requests (a bad model load, out of memory, a broken dependency). The gateway keeps trying it again every few seconds. **Stop the upstream on purpose**, fix it, start it again.

**Call someone** if it flaps on more than one upstream.

---

## TensorgateErrorBudgetFastBurn (page)

**What it means.** More than 7.2% of requests have failed, both over the last 5 minutes and over the last hour. At this pace the monthly error budget (see SLO.md) is gone in about two days.

**How bad.** A real, sustained failure for users. If every upstream is dead, `TensorgateNoBackends` fires first; this alert is the slower, broader net for partial failures.

**Look here first.**
1. What kind of failure: `sum by (class) (rate(tensorgate_lb_requests_total{class=~"5xx|aborted|no_backend"}[5m]))` (every series exists from the start at 0, so a first failure of a kind is counted too)
   - mostly `5xx`: upstreams answer with errors, or every try failed;
   - mostly `aborted`: streams die half way (an upstream crashes during generation);
   - mostly `no_backend`: nobody to send to (should have fired NoBackends too).
2. From where: the same query `by (backend)`. All on one backend means one bad upstream. Spread across all means something shared (a bad model version, overload, the network).
3. Are the retries helping or running dry? `rate(tensorgate_lb_retries_total[5m])` and `rate(tensorgate_lb_retry_budget_exhausted_total[5m])`. If the budget is exhausted, retries are being refused on purpose (to avoid doubling the load on sick servers), and users see the errors the retries would have hidden.

**Usual causes and the fix.**
- One upstream fails part of its requests but still answers `/healthz` (a "gray" failure): the gateway keeps sending it traffic. Stop that upstream.
- *All* upstreams fail a share of requests (a bad model deploy, overload): the gateway keeps at least half of them in rotation on purpose, otherwise a partly working service would turn into a total outage. Roll back the deploy or reduce the load.
- Streams `aborted`: look at the crashed upstream's logs (out of memory is the usual reason).

**Call someone** if the cause is not an upstream you can stop or roll back within 15 minutes.

---

## After any incident

Write down: when it started, how it was noticed (which alert, how many minutes after the start), what you looked at, what fixed it, what you would change. Use [POSTMORTEM.md](POSTMORTEM.md) as the model.
