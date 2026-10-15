// Command peruser limits authenticated users individually, after an
// authentication middleware has put the user ID in the request context.
// Anonymous requests fall back to a stricter per-IP limit. User IDs are
// hashed with HMAC before they are used as keys.
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/httplimit"
	"github.com/mjahidhasand10/rategen/memory"
)

type userKey struct{}

// authenticate is a stand-in for real authentication: it trusts a bearer
// token of the form "Bearer user-<name>".
func authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tok, ok := bearer(r); ok {
			r = r.WithContext(context.WithValue(r.Context(), userKey{}, tok))
		}
		next.ServeHTTP(w, r)
	})
}

func bearer(r *http.Request) (string, bool) {
	const p = "Bearer user-"
	h := r.Header.Get("Authorization")
	if len(h) > len(p) && h[:len(p)] == p {
		return h[len(p):], true
	}
	return "", false
}

func userID(r *http.Request) (string, bool) {
	id, ok := r.Context().Value(userKey{}).(string)
	return id, ok
}

func main() {
	store, err := memory.New(memory.Config{})
	if err != nil {
		log.Fatal(err)
	}
	perUser, err := store.TokenBucket("per-user", rategen.PerMinute(60), 3)
	if err != nil {
		log.Fatal(err)
	}
	anon, err := store.FixedWindow("anonymous", rategen.PerMinute(1))
	if err != nil {
		log.Fatal(err)
	}

	// In production, load a persistent secret from your secret manager so
	// that keys are stable across restarts and instances.
	secret := make([]byte, 32)
	rand.Read(secret)
	hash, err := httplimit.HMACSHA256(secret)
	if err != nil {
		log.Fatal(err)
	}

	isAnonymous := func(r *http.Request) bool { _, ok := userID(r); return !ok }
	limit, err := httplimit.New(httplimit.Config{
		Rules: []httplimit.Rule{
			{Limiter: perUser, Key: httplimit.ByFunc(userID, hash), Optional: true},
			{Limiter: anon, Key: httplimit.ByIP(), Match: isAnonymous},
		},
		Headers: httplimit.HeadersLegacy,
	})
	if err != nil {
		log.Fatal(err)
	}
	handler := authenticate(limit.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "hello")
	})))

	// Exercise the handler in-process.
	send := func(auth string) int {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "198.51.100.7:4000"
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}
	for i := range 4 {
		fmt.Printf("alice #%d: %d\n", i+1, send("Bearer user-alice"))
	}
	fmt.Printf("bob   #1: %d\n", send("Bearer user-bob"))
	for i := range 2 {
		fmt.Printf("anon  #%d: %d\n", i+1, send(""))
	}
}
