package lb

import (
	"net/http"
	"time"
)

type recorder struct {
	http.ResponseWriter
	start  time.Time
	status int
	ttft   time.Duration
	wrote  bool
}

func newRecorder(w http.ResponseWriter, start time.Time) *recorder {
	return &recorder{ResponseWriter: w, start: start}
}

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 && code >= 200 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if !r.wrote && len(p) > 0 {
		r.wrote = true
		r.ttft = time.Since(r.start)
	}
	return r.ResponseWriter.Write(p)
}

func (r *recorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func class(status int, finished bool) string {
	if !finished {
		return "aborted"
	}
	switch status / 100 {
	case 0:
		return "canceled"
	case 2:
		return "2xx"
	case 3:
		return "3xx"
	case 4:
		return "4xx"
	default:
		return "5xx"
	}
}
