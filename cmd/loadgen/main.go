// Command loadgen holds a fixed pool of concurrent SSE streams against /v1/chat/completions
// and reports client-side TTFT, token lateness and inter-token gaps once per second.
//
// Lateness is how much later than the ideal schedule start+TTFT+(i-1)*TPOT the i-th token
// arrived. Unlike gaps it stays meaningful when the server batches tokens into one write:
// a batch gives gaps near 0 and near the batch interval, but its lateness is the real delay.
//
// Connections are opened at a fixed rate (-rate) instead of all at once, so the ramp-up
// doesn't overflow the server's SYN/accept backlog. When a stream finishes, its worker
// reconnects right away over the same keep-alive connection, so the pool stays at -streams.
//
//	loadgen -url http://10.0.0.2:8080/v1/chat/completions -streams 5000 -rate 200 -duration 10m
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	target    = flag.String("url", "http://localhost:8080/v1/chat/completions", "completions endpoint")
	streams   = flag.Int("streams", 5000, "concurrent streams to hold")
	rate      = flag.Float64("rate", 200, "new streams opened per second during ramp-up")
	duration  = flag.Duration("duration", 0, "total run time including ramp-up, 0 runs until Ctrl+C")
	tokens    = flag.Int("tokens", 2000, "X-Mock-Tokens: tokens per stream")
	tpot      = flag.Duration("tpot", 30*time.Millisecond, "X-Mock-TPOT: gap between tokens")
	ttft      = flag.Duration("ttft", 200*time.Millisecond, "X-Mock-TTFT: delay before the first token")
	backoff   = flag.Duration("backoff", time.Second, "wait after a failed stream before retrying (jittered)")
	keepAlive = flag.Bool("keepalive", true, "reuse the connection for the next stream; false dials a new one every time")

	apiKey = flag.String("api-key", "",
		`API key sent in X-Api-Key, "" sends none; with -keys > 1 it is the prefix of key-0, key-1, ... ("loadgen" if empty)`)
	keys = flag.Int("keys", 1,
		"spread the workers over this many API keys, round-robin: the server limits every key on its own, so few keys provoke 429 and many keys leave only the stream limit (503)")
	honorRetryAfter = flag.Bool("honor-retry-after", true,
		"after a 429/503 wait the server's Retry-After instead of -backoff, like a well-behaved client")
	csvPath = flag.String("csv", "", "also write the per-second report to this CSV file, numbers only, for plotting")
)

var body = []byte(`{"model":"tensorgate-mock-v0","stream":true,"messages":[{"role":"user","content":"hi"}]}`)

var (
	dataPrefix    = []byte("data: ")
	doneMarker    = []byte("[DONE]")
	contentMarker = []byte(`"content":`)
)

func main() {
	flag.Parse()
	if *streams < 1 || *rate <= 0 {
		log.Fatal("-streams and -rate must be positive")
	}
	if *keys < 1 {
		log.Fatal("-keys must be at least 1")
	}
	apiKeys := makeAPIKeys()

	var csvOut io.Writer // stays a nil interface when -csv is off
	if *csvPath != "" {
		f, err := os.Create(*csvPath)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		csvOut = f
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	g := newGen()
	start := time.Now()

	var wg sync.WaitGroup
	reporterDone := make(chan struct{})
	go func() {
		g.report(ctx, start, csvOut)
		close(reporterDone)
	}()

	// ramp-up: one new worker per tick, each tied to one API key for its whole life
	tick := time.NewTicker(time.Duration(float64(time.Second) / *rate))
ramp:
	for i := range *streams {
		select {
		case <-ctx.Done():
			break ramp
		case <-tick.C:
		}
		key := apiKeys[i%len(apiKeys)]
		wg.Go(func() { g.worker(ctx, key) })
	}
	tick.Stop()
	if ctx.Err() == nil {
		log.Printf("ramp-up done: %d workers in %v", *streams, time.Since(start).Round(time.Millisecond))
	}

	<-ctx.Done()
	wg.Wait()
	<-reporterDone
	g.summary(time.Since(start))
}

// makeAPIKeys returns the keys the workers are spread over; a single empty key means "send no header"
func makeAPIKeys() []string {
	if *apiKey == "" && *keys == 1 {
		return []string{""}
	}
	base := *apiKey
	if base == "" {
		base = "loadgen"
	}
	if *keys == 1 {
		return []string{base}
	}
	out := make([]string, *keys)
	for i := range out {
		out[i] = base + "-" + strconv.Itoa(i)
	}
	return out
}

type gen struct {
	client *http.Client

	inflight  atomic.Int64 // requests holding a connection: waiting for TTFT or streaming
	started   atomic.Int64
	completed atomic.Int64 // reached [DONE]
	connErrs  atomic.Int64 // dial / request failed before a response
	httpErrs  atomic.Int64 // non-200 response other than the two admission rejections below
	streamErr atomic.Int64 // stream broke before [DONE]
	tokensRx  atomic.Int64 // content chunks received

	// what admission control did: a rejection is the server working as designed, not an error
	accepted atomic.Int64 // 200: a stream was opened
	rej429   atomic.Int64 // rate limit of the API key
	rej503   atomic.Int64 // no free stream slot within the queue wait

	// interval histograms are swapped out by the reporter every second
	ttft atomic.Pointer[hist]
	late atomic.Pointer[hist]
	gap  atomic.Pointer[hist]
	// whole-run histograms, written only by the reporter
	ttftTotal, lateTotal, gapTotal *hist

	errMu sync.Mutex
	errs  map[string]int64
}

func newGen() *gen {
	tr := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        *streams,
		MaxIdleConnsPerHost: *streams,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		DisableKeepAlives:   !*keepAlive,
		// the server flushes headers together with the first chunk, i.e. after TTFT
		ResponseHeaderTimeout: *ttft + 10*time.Second,
	}
	g := &gen{
		client:    &http.Client{Transport: tr}, // no Client.Timeout: streams are long-lived
		errs:      make(map[string]int64),
		ttftTotal: new(hist),
		lateTotal: new(hist),
		gapTotal:  new(hist),
	}
	g.ttft.Store(new(hist))
	g.late.Store(new(hist))
	g.gap.Store(new(hist))
	return g
}

func (g *gen) worker(ctx context.Context, key string) {
	for ctx.Err() == nil {
		err := g.stream(ctx, key)
		if err == nil || ctx.Err() != nil {
			continue
		}
		// jitter so workers that failed together don't retry together
		d := *backoff/2 + rand.N(*backoff)
		if rej, ok := errors.AsType[*rejectedError](err); ok {
			// not an error, so it stays out of "top errors"; the server's Retry-After already carries jitter
			if *honorRetryAfter && rej.retryAfter > 0 {
				d = rej.retryAfter
			}
		} else {
			g.recordErr(err)
		}
		select {
		case <-ctx.Done():
		case <-time.After(d):
		}
	}
}

type httpStatusError int

func (e httpStatusError) Error() string { return "http status " + strconv.Itoa(int(e)) }

// rejectedError is a 429 or 503 answer from admission control
type rejectedError struct {
	status     int
	retryAfter time.Duration // 0 if the header is missing or not a number of seconds
}

func (e *rejectedError) Error() string { return "rejected with status " + strconv.Itoa(e.status) }

func parseRetryAfter(h http.Header) time.Duration {
	if s, err := strconv.Atoi(h.Get("Retry-After")); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return 0
}

var errPrematureEnd = errors.New("stream ended before [DONE]")

func (g *gen) stream(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, *target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if key != "" {
		req.Header.Set("X-Api-Key", key)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-Mock-Tokens", strconv.Itoa(*tokens))
	req.Header.Set("X-Mock-TPOT", tpot.String())
	req.Header.Set("X-Mock-TTFT", ttft.String())

	g.started.Add(1)
	g.inflight.Add(1)
	defer g.inflight.Add(-1)

	start := time.Now()
	resp, err := g.client.Do(req)
	if err != nil {
		// a cancel at shutdown isn't a server error
		if ctx.Err() == nil {
			g.connErrs.Add(1)
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		switch resp.StatusCode {
		case http.StatusTooManyRequests:
			g.rej429.Add(1)
			return &rejectedError{resp.StatusCode, parseRetryAfter(resp.Header)}
		case http.StatusServiceUnavailable:
			g.rej503.Add(1)
			return &rejectedError{resp.StatusCode, parseRetryAfter(resp.Header)}
		}
		g.httpErrs.Add(1)
		return httpStatusError(resp.StatusCode)
	}
	g.accepted.Add(1)

	br := bufio.NewReaderSize(resp.Body, 4096)
	var last time.Time
	n := 0 // content tokens received so far
	for {
		// ReadSlice doesn't allocate; chunks are far below the buffer size
		line, err := br.ReadSlice('\n')
		if err != nil {
			if ctx.Err() == nil {
				g.streamErr.Add(1)
			}
			if errors.Is(err, io.EOF) {
				return errPrematureEnd
			}
			return err
		}
		payload, ok := bytes.CutPrefix(line, dataPrefix)
		if !ok {
			continue
		}
		payload = bytes.TrimRight(payload, "\r\n")

		if bytes.Equal(payload, doneMarker) {
			g.completed.Add(1)
			// read up to the terminating chunk, otherwise the transport can't reuse the connection
			_, _ = io.Copy(io.Discard, br)
			return nil
		}
		// role and finish chunks carry no content; TTFT and TPOT are defined on content chunks,
		// the same way the server measures them
		if !bytes.Contains(payload, contentMarker) {
			continue
		}

		now := time.Now()
		if last.IsZero() {
			g.ttft.Load().record(now.Sub(start))
		} else {
			g.gap.Load().record(now.Sub(last))
		}
		due := start.Add(*ttft + time.Duration(n)*(*tpot))
		g.late.Load().record(now.Sub(due)) // early can't happen beyond clock noise; record clamps it to 0
		n++
		last = now
		g.tokensRx.Add(1)
	}
}

func (g *gen) recordErr(err error) {
	// drop addresses so "read tcp 10.0.0.3:41234->10.0.0.2:8080: ..." doesn't make a key per port
	msg := err.Error()
	if opErr, ok := errors.AsType[*net.OpError](err); ok {
		msg = opErr.Op + ": " + opErr.Err.Error()
	}
	g.errMu.Lock()
	if _, ok := g.errs[msg]; ok || len(g.errs) < 50 {
		g.errs[msg]++
	}
	g.errMu.Unlock()
}

// csvMs formats a histogram quantile for the CSV: milliseconds, empty when there was no sample
func csvMs(d time.Duration) string {
	if d < 0 {
		return ""
	}
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 1, 64)
}

// report prints one row per second; csvOut, if not nil, gets the same rows as plain numbers
func (g *gen) report(ctx context.Context, start time.Time, csvOut io.Writer) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	if csvOut != nil {
		fmt.Fprintln(csvOut, "elapsed_s,inflight,tok_s,accepted_s,rej_429_s,rej_503_s,err_s,"+
			"ttft_p50_ms,ttft_p99_ms,late_p50_ms,late_p99_ms,late_max_ms,gap_p99_ms")
	}

	totalTTFT, totalLate, totalGap := g.ttftTotal, g.lateTotal, g.gapTotal

	var prevTokens, prevErrs, prevAcc, prev429, prev503 int64
	prev := time.Now()
	for line := 0; ; line++ {
		select {
		case <-ctx.Done():
			// fold in what the last partial interval recorded
			totalTTFT.merge(g.ttft.Load())
			totalLate.merge(g.late.Load())
			totalGap.merge(g.gap.Load())
			return
		case <-t.C:
		}
		now := time.Now()
		ttft := g.ttft.Swap(new(hist))
		late := g.late.Swap(new(hist))
		gap := g.gap.Swap(new(hist))
		totalTTFT.merge(ttft)
		totalLate.merge(late)
		totalGap.merge(gap)

		tok := g.tokensRx.Load()
		errs := g.connErrs.Load() + g.httpErrs.Load() + g.streamErr.Load()
		acc, r429, r503 := g.accepted.Load(), g.rej429.Load(), g.rej503.Load()
		secs := now.Sub(prev).Seconds()

		// the admission columns go last: bench/scripts/*.awk read the columns above by position
		if line%20 == 0 {
			fmt.Printf("%7s %8s %9s %9s %7s %9s | %8s %8s | %8s %8s %8s | %8s %8s | %7s %7s %7s\n",
				"elapsed", "inflight", "started", "done", "err/s", "tok/s",
				"ttft p50", "ttft p99", "late p50", "late p99", "late max", "gap p99", "gap max",
				"acc/s", "429/s", "503/s")
		}
		fmt.Printf("%7s %8d %9d %9d %7.0f %9.0f | %8s %8s | %8s %8s %8s | %8s %8s | %7.0f %7.0f %7.0f\n",
			now.Sub(start).Round(time.Second),
			g.inflight.Load(), g.started.Load(), g.completed.Load(),
			float64(errs-prevErrs)/secs, float64(tok-prevTokens)/secs,
			fmtDur(ttft.quantile(0.5)), fmtDur(ttft.quantile(0.99)),
			fmtDur(late.quantile(0.5)), fmtDur(late.quantile(0.99)), fmtDur(late.max()),
			fmtDur(gap.quantile(0.99)), fmtDur(gap.max()),
			float64(acc-prevAcc)/secs, float64(r429-prev429)/secs, float64(r503-prev503)/secs)

		if csvOut != nil {
			fmt.Fprintf(csvOut, "%.0f,%d,%.0f,%.1f,%.1f,%.1f,%.1f,%s,%s,%s,%s,%s,%s\n",
				now.Sub(start).Seconds(), g.inflight.Load(), float64(tok-prevTokens)/secs,
				float64(acc-prevAcc)/secs, float64(r429-prev429)/secs, float64(r503-prev503)/secs,
				float64(errs-prevErrs)/secs,
				csvMs(ttft.quantile(0.5)), csvMs(ttft.quantile(0.99)),
				csvMs(late.quantile(0.5)), csvMs(late.quantile(0.99)), csvMs(late.max()),
				csvMs(gap.quantile(0.99)))
		}

		prevTokens, prevErrs, prev = tok, errs, now
		prevAcc, prev429, prev503 = acc, r429, r503
	}
}

func (g *gen) summary(elapsed time.Duration) {
	fmt.Printf("\n=== summary after %v ===\n", elapsed.Round(time.Second))
	fmt.Printf("streams started %d, completed %d\n", g.started.Load(), g.completed.Load())
	if r429, r503 := g.rej429.Load(), g.rej503.Load(); r429+r503 > 0 {
		// share of all requests the server answered, so it is the share of demand that was turned away
		total := g.accepted.Load() + r429 + r503
		fmt.Printf("admission: accepted %d (%.1f%%), rejected 429 %d (%.1f%%), rejected 503 %d (%.1f%%)\n",
			g.accepted.Load(), 100*float64(g.accepted.Load())/float64(total),
			r429, 100*float64(r429)/float64(total), r503, 100*float64(r503)/float64(total))
	}
	fmt.Printf("errors: connect %d, http %d, mid-stream %d\n", g.connErrs.Load(), g.httpErrs.Load(), g.streamErr.Load())
	fmt.Printf("tokens received %d (%.0f/s avg)\n", g.tokensRx.Load(), float64(g.tokensRx.Load())/elapsed.Seconds())
	for _, h := range []struct {
		name string
		h    *hist
	}{{"ttft", g.ttftTotal}, {"late", g.lateTotal}, {"gap ", g.gapTotal}} {
		fmt.Printf("%s  p50 %s  p90 %s  p99 %s  p99.9 %s  max %s  (n=%d)\n", h.name,
			fmtDur(h.h.quantile(0.5)), fmtDur(h.h.quantile(0.9)), fmtDur(h.h.quantile(0.99)),
			fmtDur(h.h.quantile(0.999)), fmtDur(h.h.max()), h.h.count())
	}

	if len(g.errs) == 0 {
		return
	}
	type kv struct {
		msg string
		n   int64
	}
	var list []kv
	for m, n := range g.errs {
		list = append(list, kv{m, n})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].n > list[j].n })
	fmt.Println("top errors:")
	for _, e := range list[:min(len(list), 10)] {
		fmt.Printf("  %8d  %s\n", e.n, e.msg)
	}
}

// hist is a lock-free fixed-resolution histogram: 100µs buckets up to 10s, plus an overflow bucket
const (
	histRes     = 100 * time.Microsecond
	histBuckets = int(10*time.Second/histRes) + 1
)

type hist struct {
	b [histBuckets]atomic.Int64
}

func (h *hist) record(d time.Duration) {
	i := int(d / histRes)
	if i < 0 {
		i = 0
	}
	if i >= histBuckets {
		i = histBuckets - 1
	}
	h.b[i].Add(1)
}

func (h *hist) merge(o *hist) {
	for i := range h.b {
		if n := o.b[i].Load(); n != 0 {
			h.b[i].Add(n)
		}
	}
}

func (h *hist) count() int64 {
	var n int64
	for i := range h.b {
		n += h.b[i].Load()
	}
	return n
}

// quantile returns the upper edge of the bucket holding quantile q, or -1 if empty
func (h *hist) quantile(q float64) time.Duration {
	total := h.count()
	if total == 0 {
		return -1
	}
	rank := int64(float64(total)*q + 0.5)
	rank = max(rank, 1)
	var seen int64
	for i := range h.b {
		seen += h.b[i].Load()
		if seen >= rank {
			return time.Duration(i+1) * histRes
		}
	}
	return time.Duration(histBuckets) * histRes
}

func (h *hist) max() time.Duration {
	for i := histBuckets - 1; i >= 0; i-- {
		if h.b[i].Load() != 0 {
			return time.Duration(i+1) * histRes
		}
	}
	return -1
}

func fmtDur(d time.Duration) string {
	switch {
	case d < 0:
		return "-"
	case d >= time.Duration(histBuckets)*histRes:
		return ">10s"
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	default:
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	}
}
