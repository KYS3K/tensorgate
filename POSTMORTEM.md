# Postmortem: game day, 2026-11-25

This is a **planned exercise**, not a real outage: we held steady load through the load balancer and broke the upstreams on purpose, to find out what the alerts and the gateway do. Nobody was harmed, but the bugs it found are real. The note is written so that someone who was not there can follow it. Terms are explained where they first appear.

- **Raw data:** [bench/2026-11-25-gameday](bench/2026-11-25-gameday) (the full run) and [bench/2026-11-25-gameday-verify](bench/2026-11-25-gameday-verify) (a shorter second run on the fixed build).
- **Script:** [bench/scripts/lb-gameday.sh](bench/scripts/lb-gameday.sh). Alert rules: [ops/alerts.yml](ops/alerts.yml). What the numbers mean: [SLO.md](SLO.md). What to do when an alert fires: [RUNBOOK.md](RUNBOOK.md).

## In five lines

1. We ran 800 SSE streams (each streams tokens for about a minute) through the gateway to three mock upstreams A, B and C, and broke them one at a time.
2. **Killing B cut 267 streams (a third of all open ones), and the error-rate chart showed 0%.** The chart was blind to a kind of failure it was supposed to count.
3. **A slow but alive upstream (C, +700 ms) was invisible** to the health checks and to the balancer. Only the TTFT alert caught it, after 135 seconds, and no metric said *which* upstream was slow.
4. **An upstream failing half of its requests (A) flapped between dead and alive for two minutes and no alert fired.** Users did not notice, because retries covered for it.
5. We fixed the blind chart, made TTFT visible per upstream, added a flapping alert, and added log lines for upstream deaths. A second run confirmed all four.

## Setup

Three Hetzner Cloud VMs on a private network: the gateway on 2 vCPU, `loadgen` on a third, and three `mockupstream` processes (A `:9001`, B `:9002`, C `:9003`) on the upstream VM. Prometheus ran next to the upstreams, scraped the gateway every 5 seconds and evaluated the rules in [ops/alerts.yml](ops/alerts.yml) every 5 seconds. Load: 800 streams of 2000 tokens at 30 ms per token. Normal state before the exercise: p99 TTFT 0.25 s, no errors, each upstream holds about 267 streams.

Words used below:

- **Upstream**: a model server the gateway forwards to. **Dead / out of rotation**: the gateway stopped sending it requests.
- **TTFT**: time to first token (here: first byte of the answer).
- **Health probe**: every 2 seconds the gateway calls `/healthz` on each upstream. It counts 3 failures in a row as dead, 2 successes in a row as alive again. The probe times out after 1 second.
- **Passive ejection**: the gateway also marks an upstream dead when 3 real requests in a row fail.
- **Retry**: a request that failed before anything reached the client is sent once to another upstream.

## Timeline

Times are seconds after the load became steady (`t = 0` is 20:17:11 on the first run). "Injected" lines are what the script did; the rest is what the system did, taken from the metrics and logs.

| t | What happened |
|---|---|
| 0 | Steady: 800 streams, all three upstreams alive, no errors. |
| **120** | **Injected:** `kill -9` upstream B. |
| 121 | Every stream B was holding is cut: **267 clients** get `unexpected EOF` (loadgen) and the gateway logs 267 lines `ReverseProxy read error during body copy: unexpected EOF`. |
| 125-130 | The gateway takes B out of rotation (first Prometheus sample with `alive = 0` is at 130). **Detection by the gateway: under 10 seconds.** New requests that were sent to B before this are retried on A or C: **34 retries in total, 0 failures for clients.** |
| 135 | `TensorgateBackendDown` for B is *pending*; at 195 it fires (it waits one minute on purpose). |
| 300 | B is started again. At 310 it is back (two good probes) and the alert clears. |
| **420** | **Injected:** upstream C answers 700 ms late (still under the probe's 1 second limit). |
| 420-600 | C stays alive the whole time: its health probes pass. **Traffic to C does not drop:** 4.3 requests/s against 4.4 and 4.6 for the others, and 266 open streams against 267. |
| 490 | The 30-second p99 TTFT first goes above 0.5 s (peak 0.99 s). `TensorgateTTFTHigh` pending at 495, **fires at 555: 135 seconds after the injection.** |
| 600 | C is fast again. The p99 is back under 0.5 s at 640, the alert clears at 760 (its 2-minute window has to empty first). |
| **720** | **Injected:** upstream A answers 502 to half of the completions, `/healthz` stays fine. |
| 730-825 | A goes dead and comes back four times (5 seconds dead, 25 seconds alive). The gateway ejects it after 3 failed requests in a row, the probes bring it back after 2 good probes, and it fails again. **Clients see no errors** (retries: up to 0.4 per second). `TensorgateBackendDown` is pending each time but never fires, because no death lasts a minute. **No alert fires in these two minutes.** |
| 840 | A is fixed. |

Client side, over the whole run: 267 broken streams (the kill) and nothing else. (The first run also logged ~950 errors at the very end; those came from our script stopping the gateway before the load generator and are not part of the incident. The script is fixed.)

## What we found

### 1. The error chart read 0% while 267 streams were cut (fixed)

**What we saw.** The error ratio (`tensorgate:lb_error_ratio`) stayed at 0% for the whole run, including the moment B died. The gateway's own counter said otherwise: `tensorgate_lb_requests_total{class="aborted"} = 267`.

**Why.** A Prometheus counter is created the first time it is incremented. The series `{backend="B", class="aborted"}` did not exist before the kill, so its first sample already had the value 267. `rate()` and `increase()` compare a sample with the one *before* it; there is no earlier sample, so they report 0. We checked it directly: `increase(...{class="aborted"}[10m])` returned `0` while the raw counter was `267`.

**Why it matters.** The first failure of every kind, on every upstream, is invisible. That is exactly the failure the alert exists for.

**Fix.** Every `(backend, class)` series is created at 0 when the gateway starts (`TestMetrics_SeriesExistAtZeroFromTheStart`). **Confirmed** on the second run: the `aborted` rate peaked at 9.3 per second at the kill, and the error-budget alert fired.

*A caveat about that alert:* it fired about two minutes after the kill, but only because Prometheus had just been started and had three minutes of history, so its "last hour" window was almost the same as its "last 5 minutes". With an hour of healthy traffic behind it, 267 cut streams out of tens of thousands of requests is far below the 7.2% threshold and would not page, which is what [SLO.md](SLO.md) intends. Also, requests are counted **when they finish**, and a stream lasts a minute, so in the seconds after the kill the only requests that finished were the cut ones, and the ratio briefly reads 100%. The ratio is noisy with long streams and should be read over minutes.

### 2. A slow but alive upstream is invisible (partly fixed)

**What we saw.** C answered 700 ms late for three minutes. The health probes stayed green (700 ms is under the 1 second limit), and the balancer kept sending C a full third of the new requests.

**Why the balancer did not help.** It picks the upstream with the fewest requests in flight. That works when requests are short. Here a stream lives for a minute, so an upstream that is 0.7 seconds slower holds almost the same number of streams: 266 against 267. Slowness in the *first byte* is too small a part of a minute-long stream for the count to change.

**Why detection was slow.** The only signal was the TTFT alert: 70 seconds until the p99 crossed the line (new requests arrive in waves with this load generator, and the 30 s to 2 min windows average them), plus its one-minute wait. **135 seconds** with a third of the new requests slow.

**The diagnosis gap.** The TTFT metric was one histogram for the whole gateway, so nothing in the metrics said *which* upstream was slow. In this exercise we knew it was C only because the script had injected it. A person on call would have had to call each upstream by hand.

**Fix done.** TTFT is now a histogram **per upstream** (`backend` label). On the second run the per-upstream p99 pointed at once at C (0.99 s against 0.25 s for A and B). The runbook says to use it.

**Still open.** The gateway still does not *act* on a slow upstream. It needs outlier ejection by latency (take out an upstream whose TTFT is several times worse than its siblings'). See the action items.

### 3. A half-broken upstream flapped for two minutes and nobody was told (fixed)

**What we saw.** A failed half of its completions while `/healthz` was fine. Sequence, repeated four times: 3 failed requests in a row eject A, two good probes (about 4 seconds) bring it back, 3 more failures eject it again. The second-run log shows it:

```
20:45:30 lb: upstream 10.77.1.12:9001 is dead: 3 requests in a row failed
20:45:34 lb: upstream 10.77.1.12:9001 is back: health probes pass again
20:45:34 lb: upstream 10.77.1.12:9001 is dead: 3 requests in a row failed
20:45:36 lb: upstream 10.77.1.12:9001 is back: health probes pass again
```

**Why no alert.** `TensorgateBackendDown` waits a full minute of `alive == 0`, and no single death lasted a minute (about 5 seconds each). Retries hid the failures from users (0 client errors, never out of budget), so the error alert stayed quiet too.

**Why it is bad anyway.** The upstream was failing half of its real requests. We were spending retries (extra load) and one-third of the pool's attention on a sick server, and a second sick server would have turned this into user-visible errors.

**Fix.** New alert `TensorgateBackendFlapping`: four state changes in ten minutes. On the second run it fired 45 seconds after the injection. It has a unit test built from this exact pattern.

### 4. Killing an upstream cuts its streams (known limit, not fixed)

When B was killed, every stream it held (267 of 800, one third) was cut. Retries cannot help a stream that has already started: the client already received tokens. This is the real cost of an upstream going away, and the design accepts it. What *can* be improved is a **planned** restart: tell the gateway to stop sending new requests to an upstream, wait for its streams to finish, then stop it (draining). The gateway has no way to do that yet.

### 5. Nothing in the gateway log said an upstream died (fixed)

While investigating we had metrics but no log line for "B was taken out" or "B is back". Now `kill`, `resurrect` and passive ejection each log a line with the reason (see the log above).

### 6. Problems with the exercise itself

Worth writing down, because they cost us time and would have misled us:

- `pkill -x` silently does nothing for process names longer than 15 characters, so the first benchmark attempt compared "the gateway with the built-in mock" with itself four times and called two of them "through the load balancer". Found because the upstream VM showed 0 processes. The scripts now match on the full command line.
- The alert poller had a quoting bug and wrote nothing. The alert history was recovered from Prometheus's own `ALERTS` series, which it keeps on disk. Lesson: when a log fails, the monitoring system itself is the second source.
- Stopping the gateway before the load generator produced ~950 fake "errors". Fixed in the script.

### 7. Side result: balancing is CPU-hungry

Not an incident, but the numbers collected for the SLO say so: through the load balancer the 2 vCPU gateway is at 79% CPU for 1000 streams and at 99% for 2000 (p99 TTFT 0.5 s), while the built-in mock gateway carries 2000 streams at 60%. Probable reason: the balancer forwards every token with its own write, while the built-in path batches them (the coalescing from BENCHMARKS.md). Not verified; it is an action item. Details are in [SLO.md](SLO.md).

## What went well

- **Retries worked.** 34 retries, 0 client errors from new requests, the retry budget never ran out. A fleet of three covered for a dead upstream and for a failing one.
- **The gateway noticed a dead upstream in under 10 seconds** and balanced evenly the rest of the time (667 / 666 / 667 open streams at 2000).
- **The alert pipeline works end to end**: BackendDown, TTFTHigh, Flapping and the error-budget alert all fired when expected, and `promtool test rules` proves each rule on made-up data.
- **The exercise found real bugs in a system whose unit tests were all green.** Unit tests said every function was right; only a run with real load, real processes and real Prometheus showed that the chart was blind.

## Action items

| | What | Status |
|---|---|---|
| 1 | Create every metric series at 0 at startup | **Done**, test, confirmed on the second run |
| 2 | TTFT histogram per upstream | **Done**, test, confirmed |
| 3 | Alert `TensorgateBackendFlapping` | **Done**, promtool test, fired in 45 s |
| 4 | Log when an upstream dies or comes back | **Done** |
| 5 | Fix the game day script (process names, alert poller, stop order) | **Done** |
| 6 | Outlier ejection by latency: take out an upstream whose TTFT is far worse than its siblings' | Open |
| 7 | Draining: stop new requests to an upstream before a planned restart | Open |
| 8 | A revived upstream is killed again at once by its next failures: slow start / back-off on revival | Open |
| 9 | Coalesce token writes in the proxy path (CPU per stream) | Open |
| 10 | Re-measure TTFT and the 500 ms threshold with a real model | Open |

## Lessons

1. **A chart that says 0 is not the same as nothing went wrong.** Check that the thing you count can show a failure the first time it happens.
2. **Health checks only see what they ask.** "Does `/healthz` answer in a second?" says nothing about "is the model fast". Measure the thing users feel (TTFT) and make it visible per component.
3. **Retries hide problems.** That is their job, and the danger: a sick server can stay in rotation for a long time with nobody being told. Count retries and alert on them.
4. **Try to break it before it breaks.** Three of the five fixes here came from one 17-minute exercise.
