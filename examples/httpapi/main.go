// Command httpapi serves a small API protected by per-client limits:
//
//   - every client IP: 10 requests/second with bursts of 20 (GCRA);
//   - POST /login: 5 attempts per minute per IP (sliding window);
//   - /healthz is never limited.
//
// Run it behind a reverse proxy by setting TRUSTED_PROXIES to the proxy's
// CIDRs (comma separated); without it, X-Forwarded-For is ignored.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/httplimit"
	"github.com/mjahidhasand10/rategen/memory"
)

func main() {
	store, err := memory.New(memory.Config{MaxKeys: 100_000})
	if err != nil {
		log.Fatal(err)
	}
	// Expired keys are also removed lazily; the janitor just returns
	// memory sooner. It stops when ctx is canceled.
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go store.RunJanitor(ctx, time.Minute)

	perIP, err := store.GCRA("per-ip", rategen.PerSecond(10), 20)
	if err != nil {
		log.Fatal(err)
	}
	logins, err := store.SlidingWindow("login", rategen.PerMinute(5))
	if err != nil {
		log.Fatal(err)
	}

	var trusted []string
	if v := os.Getenv("TRUSTED_PROXIES"); v != "" {
		trusted = strings.Split(v, ",")
	}
	prefixes, err := httplimit.ParsePrefixes(trusted...)
	if err != nil {
		log.Fatal(err)
	}
	ips, err := httplimit.NewIPExtractor(httplimit.IPConfig{TrustedProxies: prefixes})
	if err != nil {
		log.Fatal(err)
	}

	limit, err := httplimit.New(httplimit.Config{
		Rules: []httplimit.Rule{
			{Limiter: perIP, Key: ips.Key()},
			{
				Limiter: logins,
				Key:     ips.Key(),
				Match:   func(r *http.Request) bool { return r.Method == http.MethodPost && r.URL.Path == "/login" },
			},
		},
		Skip:     func(r *http.Request) bool { return r.URL.Path == "/healthz" },
		Headers:  httplimit.HeadersDraft11,
		ErrorLog: func(_ *http.Request, err error) { log.Printf("rate limit: %v", err) },
	})
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /items", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("[]\n")) })
	mux.HandleFunc("POST /login", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
	})

	srv := &http.Server{Addr: "127.0.0.1:8080", Handler: limit.Handler(mux), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("listening on http://%s", srv.Addr)
	log.Fatal(srv.ListenAndServe())
}
