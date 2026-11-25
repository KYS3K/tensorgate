package lb

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestOnFail_KillsExactlyAtThreshold(t *testing.T) {
	b := newBackend(testURL(t), Config{PassiveThreshold: 3})

	for i := 1; i <= 2; i++ {
		b.onFail()
		if !b.Alive() {
			t.Fatalf("backend died after %d failures, threshold is 3", i)
		}
	}
	b.onFail()
	if b.Alive() {
		t.Error("backend still alive after 3 failures, want dead at the threshold")
	}
}

func TestOnSuccess_ResetsTheStreak(t *testing.T) {
	b := newBackend(testURL(t), Config{PassiveThreshold: 3})

	for range 10 {
		b.onFail()
		b.onFail()
		b.onSuccess() // failures must be consecutive: this breaks the streak
	}
	if !b.Alive() {
		t.Error("backend died from non-consecutive failures")
	}
}

// A zero Config must not give a zero threshold, or the very first error kills the node.
func TestNewBackend_ZeroConfigHasPassiveThreshold(t *testing.T) {
	b := newBackend(testURL(t), Config{})

	b.onFail()
	if !b.Alive() {
		t.Error("one failure killed a backend built from a zero Config: failThreshold is not taken from the defaults")
	}
}

func TestPassive_DeadUpstreamCountsAndAnswers502(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {})
	spare := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {})
	// the last working backend is never ejected, so there must be a spare one; it is made busy to never be picked
	gw, p := startGatewayCfg(t, Config{PassiveThreshold: 3, MaxAttempts: 1}, time.Second, 0, up, spare)
	busy(t, p, spare)
	up.Close()
	b := backendFor(t, p, up)

	for i := 1; i <= 3; i++ {
		resp, err := http.Get(gw.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("request %d: status = %d, want 502", i, resp.StatusCode)
		}
		if i < 3 && !b.Alive() {
			t.Fatalf("backend died after %d failed requests, threshold is 3 (is a failure counted twice?)", i)
		}
	}
	waitInFlight(t, b, 0)
	if b.Alive() {
		t.Error("backend still alive after 3 network failures")
	}
}

// An upstream 502/503/504 is one failure, and the client must get the upstream's own answer.
func TestPassive_UpstreamStatusCountsOnceAndPassesThrough(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-From", "upstream")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "upstream says no")
	})
	gw, p := startGatewayCfg(t, Config{PassiveThreshold: 3}, time.Second, 0, up)
	b := p.Backends()[0]

	for i := 1; i <= 2; i++ {
		resp, err := http.Get(gw.URL)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", resp.StatusCode)
		}
		if resp.Header.Get("X-From") != "upstream" || string(body) != "upstream says no" {
			t.Errorf("client got %q / X-From=%q: the upstream response was replaced, ModifyResponse must return nil",
				body, resp.Header.Get("X-From"))
		}
		if !b.Alive() {
			t.Fatalf("backend died after %d failures, threshold is 3: one upstream error is counted more than once", i)
		}
	}
}

func TestPassive_ClientErrorsAndSuccessDoNotCount(t *testing.T) {
	for _, code := range []int{200, 201, 204, 400, 401, 404, 429, 500} {
		up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
		})
		gw, p := startGatewayCfg(t, Config{PassiveThreshold: 1}, time.Second, 0, up)

		resp, err := http.Get(gw.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if !p.Backends()[0].Alive() {
			t.Errorf("status %d killed the backend, only 502/503/504 are backend failures", code)
		}
	}
}

func TestPassive_SuccessBetweenFailuresKeepsBackendAlive(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Fail") == "1" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	gw, p := startGatewayCfg(t, Config{PassiveThreshold: 3}, time.Second, 0, up)

	do := func(failing bool) {
		req, _ := http.NewRequest(http.MethodGet, gw.URL, nil)
		if failing {
			req.Header.Set("X-Fail", "1")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	for range 5 {
		do(true)
		do(true)
		do(false)
	}
	if !p.Backends()[0].Alive() {
		t.Error("backend died although every two failures were followed by a success")
	}
}

// The client hanging up is not the backend's fault.
func TestPassive_ClientCancelDoesNotCount(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	})
	gw, p := startGatewayCfg(t, Config{PassiveThreshold: 1}, time.Second, 0, up)
	b := p.Backends()[0]

	for range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, gw.URL, nil)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		cancel()
	}
	waitInFlight(t, b, 0)
	if !b.Alive() {
		t.Error("clients that gave up killed a healthy backend: context.Canceled must not count")
	}
}

func TestErrorHandler_CanceledIsNotAFailure(t *testing.T) {
	b := newBackend(testURL(t), Config{PassiveThreshold: 1})
	rec := httptest.NewRecorder()

	b.errorHandler(rec, httptest.NewRequest(http.MethodGet, "/", nil), context.Canceled)
	if !b.Alive() {
		t.Error("context.Canceled killed the backend")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	b.errorHandler(rec, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx), context.DeadlineExceeded)
	if !b.Alive() {
		t.Error("our own request deadline killed the backend")
	}

	b.errorHandler(rec, httptest.NewRequest(http.MethodGet, "/", nil), errors.New("connection refused"))
	if b.Alive() {
		t.Error("a network error did not count")
	}
}

// An upstream that sheds load on purpose (503 + Retry-After) is alive: it must
// not be killed, and the answer must reach the client untouched.
func TestPassive_ShedLoad503WithRetryAfterDoesNotCount(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	gw, p := startGatewayCfg(t, Config{PassiveThreshold: 1}, time.Second, 0, up)

	for range 5 {
		resp, err := http.Get(gw.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "7" {
			t.Fatalf("got %d / Retry-After=%q, want the upstream's own 503 / 7",
				resp.StatusCode, resp.Header.Get("Retry-After"))
		}
	}
	if !p.Backends()[0].Alive() {
		t.Error("a 503 with Retry-After killed the backend, it is load shedding, not a failure")
	}
}

// ...but a bare 503 (a crashed app behind a proxy, a starting server) is a failure.
func TestPassive_Bare503StillCounts(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	spare := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {})
	gw, p := startGatewayCfg(t, Config{PassiveThreshold: 2, MaxAttempts: 1}, time.Second, 0, up, spare)
	busy(t, p, spare)

	for range 2 {
		resp, err := http.Get(gw.URL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if backendFor(t, p, up).Alive() {
		t.Error("two bare 503 did not kill the backend")
	}
}

// Load shedding in the middle of real failures must not break or extend the streak.
func TestPassive_ShedLoadDoesNotTouchTheStreak(t *testing.T) {
	b := newBackend(testURL(t), Config{PassiveThreshold: 3})
	b.onFail()
	b.onFail()

	shed := &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{"Retry-After": {"1"}}}
	if err := b.modifyResponse(shed); err != nil {
		t.Fatal(err)
	}
	if !b.Alive() {
		t.Fatal("load shedding counted as a failure")
	}
	b.onFail()
	if b.Alive() {
		t.Error("load shedding reset the streak: the third real failure must still kill")
	}
}
