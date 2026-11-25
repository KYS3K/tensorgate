// Command mockupstream is a fake LLM backend for trying out the load balancer.
//
// It serves the same mock completions as the gateway itself, plus /healthz,
// and can be made sick or slow at runtime:
//
//	mockupstream --addr :9001                       # healthy
//	mockupstream --addr :9002 --delay 3s            # slow on every request
//	curl -X POST 'localhost:9001/admin/health?status=500'   # /healthz starts failing
//	curl -X POST 'localhost:9001/admin/health?status=200'   # ...and recovers
//	curl -X POST 'localhost:9001/admin/delay?d=2s'          # change the delay live
//	kill <pid>                                              # a dead backend
//
// Every response carries X-Upstream: <addr>, so you can see who served it.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/KYS3K/tensorgate/internal/gateway"
	"github.com/KYS3K/tensorgate/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type upstream struct {
	addr         string
	healthStatus atomic.Int32 // what /healthz answers with
	delay        atomic.Int64 // time.Duration added before every non-admin request
}

func main() {
	addr := flag.String("addr", ":9001", "listen address")
	delay := flag.Duration("delay", 0, "latency added to every request (health checks included)")
	healthStatus := flag.Int("health-status", http.StatusOK, "initial status of /healthz")
	flag.Parse()

	u := &upstream{addr: *addr}
	u.healthStatus.Store(int32(*healthStatus))
	u.delay.Store(int64(*delay))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{Addr: *addr, Handler: u.routes()}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("mockupstream listening on %s (delay=%v, healthz=%d)", *addr, *delay, *healthStatus)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func (u *upstream) routes() http.Handler {
	// a private registry: the mock handler wants metrics, nobody scrapes them here
	completions := gateway.NewCompletionsHandler(metrics.New(prometheus.NewRegistry()))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		status := int(u.healthStatus.Load())
		w.WriteHeader(status)
		_, _ = w.Write([]byte(http.StatusText(status)))
	})
	mux.Handle("POST /v1/chat/completions", completions)
	mux.HandleFunc("POST /admin/health", u.setHealth)
	mux.HandleFunc("POST /admin/delay", u.setDelay)

	return u.tag(u.slow(mux))
}

// tag marks every response with the address of the instance that produced it.
func (u *upstream) tag(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", u.addr)
		next.ServeHTTP(w, r)
	})
}

// slow sleeps before the request is handled, so a "slow server" is slow for
// health checks too. The sleep ends early if the client goes away.
func (u *upstream) slow(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if d := time.Duration(u.delay.Load()); d > 0 && !strings.HasPrefix(r.URL.Path, "/admin/") {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-t.C:
			case <-r.Context().Done():
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (u *upstream) setHealth(w http.ResponseWriter, r *http.Request) {
	status, err := strconv.Atoi(r.URL.Query().Get("status"))
	if err != nil || status < 200 || status > 599 {
		http.Error(w, "status must be an HTTP status code, e.g. ?status=500", http.StatusBadRequest)
		return
	}
	u.healthStatus.Store(int32(status))
	log.Printf("healthz now answers %d", status)
	w.WriteHeader(http.StatusNoContent)
}

func (u *upstream) setDelay(w http.ResponseWriter, r *http.Request) {
	d, err := time.ParseDuration(r.URL.Query().Get("d"))
	if err != nil || d < 0 {
		http.Error(w, "d must be a duration, e.g. ?d=2s (0s turns it off)", http.StatusBadRequest)
		return
	}
	u.delay.Store(int64(d))
	log.Printf("delay is now %v", d)
	w.WriteHeader(http.StatusNoContent)
}
