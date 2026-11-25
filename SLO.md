# SLO: what Tensorgate promises and how we know

Tensorgate sits between clients and the model servers (the "upstreams") and passes their streamed answers through. This page says, in plain numbers, what counts as "working", what we promise, and when a human gets woken up. The alert rules that enforce it are in [ops/alerts.yml](ops/alerts.yml), the steps to follow when one fires are in [RUNBOOK.md](RUNBOOK.md).

## The two promises

| | What the user feels | Promise | Window |
|---|---|---|---|
| **Availability** | "my request worked" | 99.5% of requests do not fail | 30 days |
| **Time to first byte (TTFT)** | "the answer started quickly" | 99% of requests get their first byte within **500 ms** | 30 days |

"Fail" and "first byte" are defined below. Both are measured on the gateway, so they include what the load balancer adds, but not the network between the client and the gateway.

### Availability, step by step

A request **fails** when it ends in one of these (the `class` label of `tensorgate_lb_requests_total`):

- `5xx`: the gateway answered with an error (an upstream said 502/504/500, or every try failed);
- `aborted`: the answer started and then broke in the middle (the upstream died during the stream);
- `no_backend`: every upstream was dead and the gateway answered 503.

These do **not** count as failures:

- `canceled`: the client left. That is not our fault.
- `4xx`: the client sent something wrong.
- **Admission control** answers (`429` too many requests from one key, `503` no free stream slot): those requests are turned away *before* they reach the load balancer, on purpose, to protect everyone else (see BENCHMARKS.md, admission control). They are counted by the `tensorgate_admission_*` metrics, and the promise above is about the requests we agreed to serve.

```promql
# share of failed requests over 5 minutes
sum(rate(tensorgate_lb_requests_total{class=~"5xx|aborted|no_backend"}[5m]))
/ sum(rate(tensorgate_lb_requests_total{class!="canceled"}[5m]))
```

### TTFT, step by step

TTFT is the time from the moment the gateway receives the request to the moment the **first byte of the answer body** is written to the client. Only successful (2xx) answers are measured, because a quick error has no "first token". `tensorgate_lb_ttft_seconds` is a histogram with one series per upstream (`backend` label); the overall value sums them with `sum by (le)`.

```promql
# p99 TTFT over 2 minutes
histogram_quantile(0.99, sum by (le) (rate(tensorgate_lb_ttft_seconds_bucket[2m])))
```

## The error budget, with numbers

A 99.5% promise means **0.5% of requests are allowed to fail**. That allowance is the *error budget*. Over 30 days it equals 3 hours 36 minutes of total outage (30 days x 24 h x 0.5%). You can spend it on deploys, bad luck and experiments. When it is gone, we stop shipping risky changes and fix reliability first.

**Burn rate** says how fast the budget is being spent: 1x means "at exactly the pace that empties it in 30 days". 14.4x means it would be empty in 30 / 14.4 = about 2 days. One hour at 14.4x spends 2% of the whole month's budget.

The page alert fires when the failure share is above **7.2%** (14.4 x 0.5%) **both** over the last 5 minutes and over the last hour:

- the 1-hour window says "this is not a blip" (a 3 minute total outage is only 5% of an hour, no page; ten minutes is 16%, page);
- the 5-minute window says "it is still happening" (so the alert stops soon after the problem is fixed).

Proof that these rules do what this page says: `promtool test rules ops/alerts_test.yml` (9 cases, including the 3-minute blip that must not page and the 10-minute outage that must).

## Where the numbers come from

Measured on Hetzner Cloud shared vCPU, 2026-11-25: the gateway on 2 vCPU, three `mockupstream` processes on another VM (private network), `loadgen` on a third. Streams of 2000 tokens at 30 ms per token, mock TTFT 200 ms (see [bench/scripts/lb.sh](bench/scripts/lb.sh)). One 90-second run per row, so treat them as a rough picture, not a precise limit.

| Streams | Path | Gateway CPU | TTFT p50 | TTFT p99 | Token lateness p99 | Errors |
|---|---|---|---|---|---|---|
| 2000 | gateway answers itself (no balancing) | 60% | 201.7 ms | 206.6 ms | 106 ms | 0 |
| 1000 | through the load balancer | 79% | 203.7 ms | **261.2 ms** | 61 ms | 0 |
| 2000 | through the load balancer | 99% | 282.1 ms | 497.9 ms | 440 ms | 0 |
| 4000 | through the load balancer | 97% | 418.4 ms | 1.53 s | 3.31 s | 1 |

Reading the table:

- **500 ms for TTFT** is about twice the healthy value (261 ms at a load the gateway carries comfortably) and just below where the gateway runs out of CPU (498 ms at 2000 streams). A p99 above it means something is wrong, not just busy.
- **The mock adds a fixed 200 ms to TTFT**, so a real GPU will have a different healthy value. Re-measure and move the 500 ms before relying on it with a real model. What this table does show is how much the *gateway* adds on top of the upstream: about 60 ms at p99 at 1000 streams.
- **The load balancer is much more expensive than the built-in mock** (79% CPU at 1000 streams versus 60% at 2000 without it). The likely reason is that it forwards every token with its own write to the client, while the built-in mock batches them (the coalescing from BENCHMARKS.md). That is a follow-up, not part of this SLO.
- Balancing itself works: with 2000 streams the three upstreams held 667, 666 and 667 of them.

## What is not covered

- Quality of the model's answers (wrong or empty text). We only measure whether the answer arrived and how fast.
- Slowness *after* the first token (the gaps between tokens). The gateway records it for the built-in mock, not yet for proxied streams.
- The client's own network.
