package lb

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KYS3K/tensorgate/internal/metrics"
)

const routeMetric = "tensorgate_lb_prefix_route_total"

// namedUpstream answers with its own name in X-Served-By, optionally after waiting on gate.
func namedUpstream(t *testing.T, name string, gate <-chan struct{}) *httptest.Server {
	t.Helper()
	return newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if gate != nil {
			select {
			case <-gate:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("X-Served-By", name)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
}

func postChat(t *testing.T, gw *httptest.Server, body []byte) string {
	t.Helper()
	resp, err := http.Post(gw.URL, "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	return resp.Header.Get("X-Served-By")
}

func prefixCfg() Config {
	return Config{Balancing: BalancingPrefix, PrefixBlockBytes: testBlock, PrefixBlocks: testBlocks}
}

func TestPrefix_RequestsWithOneSystemPromptMeetOnOneUpstream(t *testing.T) {
	ups := []*httptest.Server{namedUpstream(t, "a", nil), namedUpstream(t, "b", nil), namedUpstream(t, "c", nil)}
	m := startMetered(t, prefixCfg(), ups...)

	served := map[string]int{}
	for i := range 30 {
		body := chatBody("m", [2]string{"system", longSystem}, [2]string{"user", strings.Repeat("q", i+1)})
		served[postChat(t, m.gw, body)]++
	}
	if len(served) != 1 {
		t.Fatalf("one system prompt was served by %v", served)
	}
	if got := m.value(t, routeMetric, map[string]string{"result": metrics.LBRouteHome}); got != 30 {
		t.Errorf("home routes = %v, want 30", got)
	}
}

func TestPrefix_DifferentPromptsSpreadOverUpstreams(t *testing.T) {
	ups := []*httptest.Server{namedUpstream(t, "a", nil), namedUpstream(t, "b", nil), namedUpstream(t, "c", nil)}
	m := startMetered(t, prefixCfg(), ups...)

	served := map[string]int{}
	for i := range 90 {
		body := chatBody("m", [2]string{"system", strings.Repeat(string(rune('a'+i%26)), 100) + strings.Repeat("z", i)})
		served[postChat(t, m.gw, body)]++
	}
	if len(served) != 3 {
		t.Fatalf("90 different prompts reached %v", served)
	}
}

func TestPrefix_ShortPromptsUseLeastOutstanding(t *testing.T) {
	ups := []*httptest.Server{namedUpstream(t, "a", nil), namedUpstream(t, "b", nil)}
	m := startMetered(t, prefixCfg(), ups...)
	postChat(t, m.gw, chatBody("m", [2]string{"user", "hi"}))
	if got := m.value(t, routeMetric, map[string]string{"result": metrics.LBRouteNoKey}); got != 1 {
		t.Errorf("no_key routes = %v, want 1", got)
	}
}

// a burst of one hot prefix does not pile up on one upstream
func TestPrefix_HotPrefixSpillsWhenItsUpstreamIsBusy(t *testing.T) {
	gate := make(chan struct{})
	ups := []*httptest.Server{namedUpstream(t, "a", gate), namedUpstream(t, "b", gate), namedUpstream(t, "c", gate)}
	m := startMetered(t, prefixCfg(), ups...)

	const burst = 12
	body := chatBody("m", [2]string{"system", longSystem}, [2]string{"user", "hello"})
	var wg sync.WaitGroup
	for range burst {
		wg.Add(1)
		go func() {
			defer wg.Done()
			postChat(t, m.gw, body)
		}()
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		var total int64
		busy := 0
		for _, b := range m.p.Backends() {
			total += b.InFlight()
			if b.InFlight() > 0 {
				busy++
			}
		}
		if total == burst {
			if busy < 2 {
				t.Errorf("all %d requests of one prefix sit on %d upstream", burst, busy)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d requests reached the upstreams", total, burst)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(gate)
	wg.Wait()

	if got := m.value(t, routeMetric, map[string]string{"result": metrics.LBRouteSpill}); got == 0 {
		t.Error("no spill was counted")
	}
	if got := m.value(t, routeMetric, map[string]string{"result": metrics.LBRouteHome}); got == 0 {
		t.Error("no home route was counted")
	}
}

func TestPrefix_RetryGoesToAnotherUpstream(t *testing.T) {
	bad := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	})
	ups := []*httptest.Server{bad, namedUpstream(t, "good", nil)}
	m := startMetered(t, prefixCfg(), ups...)

	// find a prompt whose home is the bad upstream
	for i := range 200 {
		body := chatBody("m", [2]string{"system", strconv.Itoa(i) + " " + longSystem})
		k, _ := m.p.routeKey(body)
		if home, _ := m.p.ring.Get(k); home != backendFor(t, m.p, bad).Name {
			continue
		}
		if who := postChat(t, m.gw, body); who != "good" {
			t.Fatalf("served by %q, want the retry on the good upstream", who)
		}
		return
	}
	t.Fatal("no prompt found that belongs to the bad upstream")
}

func TestPrefix_SeriesExistAtZeroFromTheStart(t *testing.T) {
	m := startMetered(t, prefixCfg(), namedUpstream(t, "a", nil))
	for _, result := range metrics.LBRouteResults {
		labels := map[string]string{"result": result}
		if !m.has(t, routeMetric, labels) {
			t.Errorf("no series for %v before any request", labels)
		}
	}
}

func TestPrefix_LORModeCountsNoRoutes(t *testing.T) {
	m := startMetered(t, Config{}, namedUpstream(t, "a", nil), namedUpstream(t, "b", nil))
	postChat(t, m.gw, chatBody("m", [2]string{"system", longSystem}))
	for _, result := range metrics.LBRouteResults {
		if got := m.value(t, routeMetric, map[string]string{"result": result}); got != 0 {
			t.Errorf("%s = %v in least-outstanding mode", result, got)
		}
	}
}
