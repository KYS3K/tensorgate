package main

import (
	"fmt"
	"net/http"
	"tensorgate/internal/core/sse"
	"time"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /events", sse.Handler)

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	fmt.Println("listening on :8080")
	if err := srv.ListenAndServe(); err != nil {
		fmt.Println(err)
	}
}
