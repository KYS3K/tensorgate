# Benchmarks

**v0 · 2026-11-11 · gateway overhead with a mock backend**

How many concurrent SSE completion streams one tensorgate process holds on 2 vCPU, what limits it, and what each fix bought. The backend is the built-in mock, so every number here is the cost of the gateway, the Linux network stack and the virtual network, not of a model.

## Summary

| Stage | Change | Delivered at 5000 streams (demand 166.7k tok/s) | CPU per token at 5000 streams | Streams within SLO | Limited by |
|---|---|---|---|---|---|
| Baseline | `json.Marshal` + `fmt` per token | 112.9k tok/s (68%) | 17.7 µs | ≤ ~3400 (throughput ceiling) | CPU |
| Zero-alloc token path | [`005681d`](#builds) | 165.2k tok/s (99%) | 11.8 µs | n/a: streams stalled for seconds | egress qdisc dropping packets |
| + `fq` qdisc | kernel setting | 165.7k tok/s | 11.6 µs | 3500, 4000 borderline | packet loss in the virtual network above ~125k packets/s |
| + flush coalescing, 100 ms | [`9d2887a`](#builds) | 166.2k tok/s | 10.3 µs | **6000** (highest tested) | CPU (96% busy at 6000) |

TPOT 30 ms throughout. "Streams within SLO" uses the SLOs defined [below](#slos).

What mattered most: the kernel and the virtual network pay **per packet, not per byte**, and every token was its own `write`, TCP segment and ACK. Halving allocations helped; sending a third of the packets helped more.

## What is and isn't measured

Measured: client-observed throughput and latency, and server CPU, for long-lived streaming completions through the gateway, with a backend that emits tokens on an exact schedule (TTFT 200 ms, TPOT 30 ms, 2000 tokens per stream).

Not measured: a real model (no batching jitter, no variable token sizes), request admission and routing (not implemented yet), TLS, HTTP/2, non-streaming requests.

## Setup

### Hardware and OS

| | Server | Client (load generator) |
|---|---|---|
| VM | Hetzner Cloud, shared vCPU | Hetzner Cloud, shared vCPU |
| CPU | 2 vCPU, AMD EPYC (Genoa) | 4 vCPU, AMD EPYC (Genoa) |
| RAM | 4 GB | 8 GB |
| OS | Ubuntu 26.04.1, kernel 7.0.0 | same |
| NIC | virtio-net on a private network, MTU 1450, **1 queue** (`ethtool -l`: `Combined: 1`) | same |

Traffic goes over the private network only. The client stayed at 50–58% idle in every run (`mpstat`), so it was never the bottleneck.

### Kernel settings

Both VMs, `/etc/sysctl.d/99-bench.conf`:

```ini
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
fs.file-max = 2097152
net.ipv4.tcp_rmem = 4096 87380 4194304
net.ipv4.tcp_wmem = 4096 65536 4194304
```

and `nofile 65535` in `/etc/security/limits.conf`.

Server, from [result 3](#3-egress-qdisc) on: `net.core.default_qdisc = fq` plus `tc qdisc replace dev enp7s0 root fq`. Ubuntu's default is `fq_codel`.

### Builds

`make bench-build`: static linux/amd64 binaries with `-trimpath`, symbols kept for `perf`. Go 1.27.

| Commit | What it adds |
|---|---|
| `4a26300` | baseline |
| `d468b3f` | pprof on `127.0.0.1:6060` (`-pprof`) |
| `d456087` | `cmd/loadgen` |
| `005681d` | zero-alloc token path |
| `07f5716` | token lateness in loadgen |
| `9d2887a` | flush coalescing, `-flush-interval` (default 100 ms) |

The baseline binary was `4a26300` plus the pprof listener.

### Load generator

`cmd/loadgen` is a Go client that holds a fixed pool of streams. It opens new streams at `-rate` per second (200 here, so 5000 streams take 25 s, to keep SYNs from overflowing the server's accept queues), and when a stream ends with `[DONE]` it starts the next one over the same keep-alive connection. Every run: `-tokens 2000 -tpot 30ms -ttft 200ms -duration 3m`.

Metrics, all measured on the client:

| Metric | Definition |
|---|---|
| tok/s | content chunks received per second; compare with demand = streams / TPOT |
| TTFT | request sent → first content chunk |
| gap | time between consecutive content chunks of a stream |
| lateness | arrival of the i-th content chunk minus its ideal time `start + TTFT + (i−1)·TPOT` |
| CPU per token | server CPU busy (`mpstat`, both vCPU) / tok/s |
| packets/s, retransmits, RTO | server `nstat` deltas over the run: `TcpOutSegs`, `TcpRetransSegs`, `TcpExtTCPTimeouts` |

Gap stops meaning anything once tokens are batched: it becomes ≈0 inside a batch and ≈interval between batches. Lateness is the delay a reader actually sees, with or without batching, so it is the SLO metric for the coalescing runs.

### Method

- Each configuration is one 3-minute run. Reported values are the mean of the per-second values over the steady-state window, seconds 40–175, past the 25 s ramp-up. "Worst" is the largest per-second maximum in that window.
- A/B comparisons run back to back on the same VMs: start server, start `mpstat` on both VMs, reset `nstat`, run loadgen, collect counters, stop server, wait 10 s.
- CPU profiles: `/debug/pprof/profile?seconds=30` taken at seconds 45–75. `perf record -e cpu-clock -F 999 -g` (the default `cycles` event misattributes samples to `__irqentry_text_start` inside the VM).

### SLOs

A stream count is "within SLO" if all of these hold:

- delivered tok/s ≥ 95% of demand;
- without coalescing: gap p99 < 60 ms (2 × TPOT);
- with coalescing interval W: lateness p99 ≤ W + 50 ms;
- no errors.

## Results

### 1. Baseline: where the CPU goes

5000 streams. The server is CPU-bound (99.9% busy) and delivers 68% of demand: 112.9k tok/s, gap p99 292 ms, worst gap 1.7 s, no errors.

pprof, 30 s at steady state (59.7 s of samples = both vCPU):

| | CPU |
|---|---|
| `write` syscall (`linux.Syscall6`, flat) | **45.8%** |
| `sse.Writer.Send` (cum), of which `Flush` 53.5% | 60.8% |
| JSON encoding of each chunk (`json/v2` cum) | ~10% |
| mock ticker: `timers.run` + `selectgo` | 8.4% + 5.1% |
| `mockToken` (`fmt.Sprintf`) | 4.4% |
| `mallocgc` | 4.9% |
| GC background worker | 1.2% |
| Prometheus histogram `Observe` | 1.8% |

perf over the same workload: 60% of the process's CPU time is in the kernel, 39% in Go. The largest kernel items are per-packet:

- `iowrite16` 8.1%: the virtio doorbell on every transmitted packet, which exits to the hypervisor;
- ACK processing ~7.5%: `tcp_ack`, `tcp_clean_rtx_queue`, `__inet_lookup_established`, softirq;
- skb alloc/free ~4%;
- the `write` path itself ~4%;
- virtqueue enqueue ~3%;
- AppArmor `sendmsg` hooks 1.5%;
- a TX queue spinlock 1%.

### 2. Zero-alloc token path

Every token used to cost `fmt.Sprintf`, a `json.Marshal` of the whole chunk, `string(payload)` and a `strings.Builder` + `fmt.Fprintf` in the SSE writer: 8 allocations and 664 B.

Now:

- `sse.Writer` appends each event into a buffer it reuses;
- `SendData([]byte)` takes an encoded payload without a string conversion;
- a per-stream `chunkEncoder` encodes everything before `"delta"` once and appends only the escaped content per token.

`FuzzChunkEncoder` checks the output is byte-for-byte what `json.Marshal` produces (8M inputs). It caught one difference on the first run: Go 1.27 replaces invalid UTF-8 with the raw U+FFFD bytes, not a `\ufffd` escape.

Microbenchmark, per token (Ryzen 7 9800X3D): **740 ns, 664 B, 8 allocs → 85 ns, 0 B, 0 allocs.**

Under load, 5000 streams, back to back:

| | Baseline | Zero-alloc |
|---|---|---|
| Delivered tok/s | 112.9k (68%) | **165.2k (99%)** |
| Server CPU busy | 99.9% | 97.8% |
| CPU per token | 17.7 µs | **11.8 µs** |
| gap p99 | 292 ms | 177 ms |
| Worst gap | 1.7 s | **> 10 s** |

The CPU saving under load (−33%) is much larger than the microbenchmark suggests (~0.7 µs of ~17 µs). The baseline was overloaded, and overload makes every token more expensive: late timer wakeups, extra scheduling, less batching in the kernel. Leaving overload removed that overhead too. One run can't separate the two effects.

The worst gap got worse: see the next section.

pprof after the change: `write` syscall 45.5%, no `json` or `Sprintf` in the profile, `chunkEncoder.content` 3.1%. The `fmt.Fprintf` still visible (2.9%) is `net/http` writing chunked-encoding headers.

### 3. Egress qdisc

With the gateway no longer CPU-bound, the single TX queue of the virtio NIC became the choke point. Packets queued in the qdisc, `fq_codel` dropped the ones that waited longer than its 5 ms target (`drop_overlimit 0`, so these were delay-based drops, not overflow), and TCP resent many of them only after a retransmission timeout. That timeout doubles on every repeated loss, which is where the multi-second stalls came from. The client saw no drops of its own.

5000 streams, zero-alloc build, counters reset per run:

| Server qdisc | qdisc drops | Retransmits | RTO timeouts | gap p99 | Worst gap | Seconds with a ≥1 s gap |
|---|---|---|---|---|---|---|
| `fq_codel` (default) | 854k (3.4% of packets) | 1.31M (5.3%) | 137k | 167 ms | > 10 s | 135 / 135 |
| **`fq`** | **0** | **386k (1.5%)** | **23k** | **97 ms** | 5.7 s | **18 / 135** |
| `pfifo limit 10000` | 0 | 591k (2.4%) | 17k | 106 ms | 4.6 s | 40 / 135 |

`fq` also keeps a queue per flow but never drops on delay, which fits a host that originates thousands of TCP flows. `pfifo` had no drops either, but more retransmits and more softirq time (39% vs 31%). All following runs use `fq`.

The 1.5% retransmits left with `fq` happen with zero drops anywhere inside either VM (qdisc, softnet, interface counters), so packets are lost between the VMs: hypervisor, vhost or the provider network.

### 4. Capacity without coalescing

Zero-alloc build, `fq`:

| Streams | Delivered / demand | gap p99 | Worst gap | Server packets/s | Retransmits | RTO | CPU busy | CPU per token | SLO |
|---|---|---|---|---|---|---|---|---|---|
| 3000 | 99.7k / 100k | 35 ms | 67 ms | 96k | 0.00% | 0 | 68% | 13.6 µs | ✅ |
| 3500 | 116.4k / 116.7k | 39 ms | 98 ms | 111k | 0.01% | 0 | 77% | 13.3 µs | ✅ |
| 4000 | 132.8k / 133.3k | 58 ms | 1.6 s | 125k | 0.55% | 9k | 88% | 13.2 µs | borderline |
| 4500 | 149.3k / 150k | 102 ms | 3.9 s | 135k | 3.34% | 66k | 92% | 12.4 µs | ❌ |

The tail breaks down before the CPU runs out. Retransmits grow nonlinearly past ~125k packets/s from the server.

These are real losses, not reordering. At 4500 streams only 11k of the 813k retransmits were reported back as spurious (DSACK).

### 5. Flush coalescing

`sse.Coalescer` buffers a stream's events and flushes them:

- never while the producer already has the next event at hand, so a burst leaves as one write;
- immediately if the previous flush was at least W ago, so the first token and a token after a pause are never held back;
- otherwise when W since the previous flush is up.

The end of a stream always flushes immediately. A token waits at most W, and a stream flushes at most once per W.

The mock now emits tokens on a fixed clock schedule and sends everything due after a late wakeup as one burst, the way a real backend keeps generating while the gateway falls behind. Without overload it behaves as before.

`fq`, W = 0 (one write per token) vs W = 100 ms:

| Streams | W | Delivered tok/s | Events per flush | Server packets/s | Retransmits | Lateness p50 | Lateness p99 | Worst lateness | CPU busy | CPU per token | SLO |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 4000 | 0 | 133.0k | 1.01 | 124k | 0.26% | 8 ms | 63 ms | 1.2 s | 93% | 14.0 µs | ❌ (bound 50 ms) |
| 4000 | 100 ms | 133.0k | 3.35 | **38k** | **0.00%** | 53 ms | 103 ms | 207 ms | 69% | **10.4 µs** | ✅ |
| 5000 | 0 | 166.0k | 1.04 | 137k | 2.44% | 58 ms | 291 ms | 3.9 s | 98% | 11.8 µs | ❌ |
| 5000 | 100 ms | 166.2k | 3.36 | **46k** | **0.00%** | 54 ms | 107 ms | 216 ms | 85% | **10.3 µs** | ✅ |
| 6000 | 0 | 198.7k | 1.43 | 127k | 2.47% | 188 ms | 887 ms | 4.4 s | 100% | 10.1 µs | ❌ |
| 6000 | 100 ms | 199.5k | 3.40 | **54k** | **0.00%** | 59 ms | 136 ms | 322 ms | 96% | **9.6 µs** | ✅ |

- **Packets per second drop 2.3–3.3×** (about TPOT/W = 3.3 tokens per packet). Retransmits go to zero at every load, and the multi-second stalls disappear.
- **The tail becomes bounded by W:** worst lateness ≈ W plus a few ms of queueing, instead of seconds.
- **The price is the median at low load:** at 4000 streams lateness p50 goes from 8 to 53 ms, because a token waits half a window on average. TTFT is unchanged (first token is never held back).
- **6000 streams pass the SLO at 96% server CPU,** so this is close to the CPU ceiling: 2 vCPU / 9.6 µs ≈ 208k tok/s ≈ 6200 streams at TPOT 30 ms. Higher counts were not tested.
- **Burst coalescing needs no interval:** at 6000 streams with W = 0 the overloaded gateway still sent 1.43 events per flush, because tokens that piled up behind a late wakeup left together.

CPU per token falls with load even without coalescing (14.0 → 11.8 → 10.1 µs from 4000 to 6000 streams), as the kernel batches more ACKs and interrupts per pass. Compare CPU per token only at equal stream counts.

The server exports `tensorgate_stream_flush_events` (events per flush) to check how much batching happens in production.

## Limitations

- **Mock backend:** perfectly regular tokens of ~10 bytes. A real model has bursty output, larger tokens and its own latency, and the coalescing trade-off may land differently.
- **Shared vCPU and a shared provider network:** the same configuration gave 1.5% and 3.3% retransmits in different runs (5000 streams in [3](#3-egress-qdisc), 4500 in [4](#4-capacity-without-coalescing)). Every number here comes from a single 3-minute run; capacity boundaries need 2–3 repeats before they can be quoted with confidence.
- **Step resolution:** capacity was searched in steps of 500–1000 streams.
- **Client-side measurement:** latency includes the network round trip (~0.5 ms) and the client's own scheduling. The client had ≥ 50% idle CPU, but its receive softirq ran on a single core.
- **The baseline capacity of ≤ ~3400 streams** is its throughput ceiling (112.9k tok/s ÷ 33.3 tok/s per stream), not a measured SLO boundary.
- **HTTP/1.1 only, one connection per stream,** as OpenAI SDK clients do by default. HTTP/2 would multiplex streams onto fewer connections and change the packet math.

## Reproducing

1. **VMs:** two VMs on one private network, kernel settings as [above](#kernel-settings). Install `sysstat` and `linux-tools-$(uname -r)`.
2. **Build and copy:**
   ```bash
   make bench-build
   scp bin/tensorgate-linux root@<server>:/root/
   scp bin/loadgen-linux   root@<client>:/root/
   ```
3. **Run:**
   ```bash
   # server
   ./tensorgate-linux -flush-interval 100ms
   # client
   ./loadgen-linux -url http://<server private IP>:8080/v1/chat/completions \
     -streams 5000 -rate 200 -tokens 2000 -tpot 30ms -duration 3m
   ```
4. **Profiles:** pprof listens on the server's loopback only:
   ```bash
   ssh -L 6060:localhost:6060 root@<server>
   go tool pprof -http=:8000 'http://localhost:6060/debug/pprof/profile?seconds=30'
   ```

Scripts used for the tables, in [`bench/scripts`](bench/scripts). They take `SRV`, `CLI`, `SRV_PRIV` and optionally `IF`, `OUT` from the environment:

| Script | Runs |
|---|---|
| `ab.sh` | two server builds back to back, with CPU profiles (`tensorgate-baseline`, `tensorgate-zeroalloc`) |
| `qdisc.sh` | `fq_codel`, `fq` and `pfifo` |
| `steps.sh` | stepped stream counts |
| `coalesce.sh` | flush interval × stream count |
| `steady.awk` | steady-state means for loadgen logs before `07f5716` |
| `late.awk` | steady-state means for loadgen logs from `07f5716` on, with lateness |

## Raw data

Per run: loadgen log, server and client `mpstat`, server `nstat` deltas, qdisc stats or flush metrics, CPU profiles where taken.

| Directory | Section |
|---|---|
| [`bench/2026-11-10-zeroalloc`](bench/2026-11-10-zeroalloc) | 1, 2 |
| [`bench/2026-11-11-qdisc`](bench/2026-11-11-qdisc) | 3 |
| [`bench/2026-11-11-capacity-fq`](bench/2026-11-11-capacity-fq) | 4 |
| [`bench/2026-11-11-coalesce`](bench/2026-11-11-coalesce) | 5 |
