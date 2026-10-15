package httplimit_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/httplimit"
	"github.com/mjahidhasand10/rategen/memory"
	"github.com/mjahidhasand10/rategen/ratetest"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func newStore(t testing.TB, cfg memory.Config) (*memory.Store, *ratetest.FakeClock) {
	clk := ratetest.NewFakeClock(epoch)
	cfg.Clock = clk
	return must(memory.New(cfg)), clk
}

var ok = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "ok")
})

func do(h http.Handler, remote string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = remote
	for k, v := range hdr {
		req.Header.Add(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAllowRejectAndRetryAfter(t *testing.T) {
	s, clk := newStore(t, memory.Config{})
	l := must(s.GCRA("per-ip", rategen.PerSecond(1), 2))
	mw := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}}}))
	h := mw.Handler(ok)
	for i := range 2 {
		if rec := do(h, "192.0.2.1:1234", nil); rec.Code != 200 || rec.Body.String() != "ok" {
			t.Fatalf("request %d: %d %q", i, rec.Code, rec.Body)
		}
	}
	rec := do(h, "192.0.2.1:1234", nil)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
	if rec.Header().Get("RateLimit") != "" || rec.Header().Get("X-RateLimit-Limit") != "" {
		t.Fatal("rate-limit headers sent without being configured")
	}
	if do(h, "192.0.2.2:1", nil).Code != 200 {
		t.Fatal("other client rejected")
	}
	clk.Advance(time.Second)
	if do(h, "192.0.2.1:1234", nil).Code != 200 {
		t.Fatal("not admitted after retry interval")
	}
}

func TestHeaderStyles(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	burst := must(s.TokenBucket("burst", rategen.PerMinute(60), 10))
	daily := must(s.FixedWindow("daily", rategen.PerDay(1000)))
	rules := []httplimit.Rule{
		{Limiter: burst, Key: httplimit.ByIP()},
		{Limiter: daily, Key: httplimit.ByIP()},
	}
	draft := must(httplimit.New(httplimit.Config{Rules: rules, Headers: httplimit.HeadersDraft11})).Handler(ok)
	rec := do(draft, "192.0.2.1:1", nil)
	// The fake clock starts at midnight UTC, so the daily window has a full
	// day left.
	if got, want := rec.Header().Get("RateLimit-Policy"), `"burst";q=10;w=10, "daily";q=1000;w=86400`; got != want {
		t.Errorf("RateLimit-Policy = %q, want %q", got, want)
	}
	if got, want := rec.Header().Get("RateLimit"), `"burst";r=9;t=1, "daily";r=999;t=86400`; got != want {
		t.Errorf("RateLimit = %q, want %q", got, want)
	}

	s2, _ := newStore(t, memory.Config{})
	l := must(s2.FixedWindow("min", rategen.PerMinute(5)))
	legacy := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}}, Headers: httplimit.HeadersLegacy})).Handler(ok)
	rec = do(legacy, "192.0.2.1:1", nil)
	for k, want := range map[string]string{"X-RateLimit-Limit": "5", "X-RateLimit-Remaining": "4", "X-RateLimit-Reset": "60"} {
		if got := rec.Header().Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
}

func TestProxyTrust(t *testing.T) {
	trusted := must(httplimit.ParsePrefixes("10.0.0.0/8", "2001:db8:ffff::/48"))
	type tc struct {
		name   string
		cfg    httplimit.IPConfig
		remote string
		hdr    map[string][]string
		want   string
		err    bool
	}
	cases := []tc{
		{name: "no trust ignores XFF", remote: "192.0.2.9:1", hdr: map[string][]string{"X-Forwarded-For": {"203.0.113.1"}}, want: "192.0.2.9"},
		{name: "untrusted peer ignored", cfg: httplimit.IPConfig{TrustedProxies: trusted}, remote: "192.0.2.9:1", hdr: map[string][]string{"X-Forwarded-For": {"203.0.113.1"}}, want: "192.0.2.9"},
		{name: "trusted peer", cfg: httplimit.IPConfig{TrustedProxies: trusted}, remote: "10.0.0.1:1", hdr: map[string][]string{"X-Forwarded-For": {"203.0.113.1"}}, want: "203.0.113.1"},
		{name: "spoofed left entries ignored", cfg: httplimit.IPConfig{TrustedProxies: trusted}, remote: "10.0.0.1:1", hdr: map[string][]string{"X-Forwarded-For": {"1.1.1.1, 2.2.2.2, 203.0.113.1, 10.0.0.2"}}, want: "203.0.113.1"},
		{name: "multiple header lines", cfg: httplimit.IPConfig{TrustedProxies: trusted}, remote: "10.0.0.1:1", hdr: map[string][]string{"X-Forwarded-For": {"9.9.9.9", "203.0.113.5, 10.1.1.1"}}, want: "203.0.113.5"},
		{name: "all trusted uses leftmost", cfg: httplimit.IPConfig{TrustedProxies: trusted}, remote: "10.0.0.1:1", hdr: map[string][]string{"X-Forwarded-For": {"10.0.0.3, 10.0.0.2"}}, want: "10.0.0.3"},
		{name: "no header uses peer", cfg: httplimit.IPConfig{TrustedProxies: trusted}, remote: "10.0.0.1:1", want: "10.0.0.1"},
		{name: "garbage from trusted proxy", cfg: httplimit.IPConfig{TrustedProxies: trusted}, remote: "10.0.0.1:1", hdr: map[string][]string{"X-Forwarded-For": {"not-an-ip"}}, err: true},
		{name: "garbage left of client ignored", cfg: httplimit.IPConfig{TrustedProxies: trusted}, remote: "10.0.0.1:1", hdr: map[string][]string{"X-Forwarded-For": {"<script>, 203.0.113.1"}}, want: "203.0.113.1"},
		{name: "forwarded rfc7239", cfg: httplimit.IPConfig{TrustedProxies: trusted, Header: "Forwarded"}, remote: "10.0.0.1:1", hdr: map[string][]string{"Forwarded": {`for=1.1.1.1, for="[2001:db8:1:2::7]:4711";proto=https`}}, want: "2001:db8:1:2::/64"},
		{name: "forwarded unknown", cfg: httplimit.IPConfig{TrustedProxies: trusted, Header: "Forwarded"}, remote: "10.0.0.1:1", hdr: map[string][]string{"Forwarded": {"for=unknown"}}, err: true},
		{name: "single header trusted", cfg: httplimit.IPConfig{TrustedProxies: trusted, Header: "CF-Connecting-IP"}, remote: "10.0.0.1:1", hdr: map[string][]string{"Cf-Connecting-Ip": {"203.0.113.8"}}, want: "203.0.113.8"},
		{name: "single header untrusted", cfg: httplimit.IPConfig{TrustedProxies: trusted, Header: "X-Real-IP"}, remote: "192.0.2.1:1", hdr: map[string][]string{"X-Real-Ip": {"203.0.113.8"}}, want: "192.0.2.1"},
		{name: "single header duplicated", cfg: httplimit.IPConfig{TrustedProxies: trusted, Header: "X-Real-IP"}, remote: "10.0.0.1:1", hdr: map[string][]string{"X-Real-Ip": {"203.0.113.8", "203.0.113.9"}}, err: true},
		{name: "ipv6 aggregated to /64", remote: "[2001:db8:1:2:3:4:5:6]:443", want: "2001:db8:1:2::/64"},
		{name: "ipv6 full", cfg: httplimit.IPConfig{IPv6Prefix: 128}, remote: "[2001:db8::1]:443", want: "2001:db8::1"},
		{name: "ipv4 /24", cfg: httplimit.IPConfig{IPv4Prefix: 24}, remote: "192.0.2.77:1", want: "192.0.2.0/24"},
		{name: "ipv4-mapped", remote: "[::ffff:192.0.2.1]:80", want: "192.0.2.1"},
		{name: "trusted ipv6 proxy", cfg: httplimit.IPConfig{TrustedProxies: trusted}, remote: "[2001:db8:ffff::1]:1", hdr: map[string][]string{"X-Forwarded-For": {"198.51.100.4"}}, want: "198.51.100.4"},
		// More trusted hops than MaxHops: refused, not collapsed onto a
		// proxy's key.
		{name: "max hops", cfg: httplimit.IPConfig{TrustedProxies: trusted, MaxHops: 2}, remote: "10.0.0.1:1", hdr: map[string][]string{"X-Forwarded-For": {"203.0.113.1, 10.0.0.4, 10.0.0.3, 10.0.0.2"}}, err: true},
		{name: "empty lines count as hops", cfg: httplimit.IPConfig{TrustedProxies: trusted, MaxHops: 2}, remote: "10.0.0.1:1", hdr: map[string][]string{"X-Forwarded-For": {"203.0.113.9", "", "", ""}}, err: true},
		{name: "ipv4-mapped trusted prefix", cfg: httplimit.IPConfig{TrustedProxies: must(httplimit.ParsePrefixes("::ffff:10.0.0.0/104"))}, remote: "10.1.2.3:5", hdr: map[string][]string{"X-Forwarded-For": {"203.0.113.9"}}, want: "203.0.113.9"},
		{name: "bad remote addr", remote: "garbage", err: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			x := must(httplimit.NewIPExtractor(c.cfg))
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = c.remote
			for k, vs := range c.hdr {
				for _, v := range vs {
					req.Header.Add(k, v)
				}
			}
			got, err := x.Key()(req)
			if c.err {
				if !errors.Is(err, httplimit.ErrInvalidClientIP) {
					t.Fatalf("got %q, %v; want ErrInvalidClientIP", got, err)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("got %q, %v; want %q", got, err, c.want)
			}
		})
	}
}

func TestIPConfigValidation(t *testing.T) {
	for _, cfg := range []httplimit.IPConfig{
		{IPv4Prefix: 33}, {IPv6Prefix: 129}, {MaxHops: -1}, {TrustedProxies: []netip.Prefix{{}}},
	} {
		if _, err := httplimit.NewIPExtractor(cfg); !errors.Is(err, rategen.ErrInvalidConfig) {
			t.Errorf("%+v accepted", cfg)
		}
	}
	if _, err := httplimit.ParsePrefixes("10.0.0.0/33"); err == nil {
		t.Error("bad prefix accepted")
	}
	if _, err := httplimit.ParsePrefixes("example.com"); err == nil {
		t.Error("hostname accepted")
	}
}

func TestSpoofedHeaderDoesNotBypass(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	l := must(s.FixedWindow("ip", rategen.PerMinute(1)))
	mw := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}}}))
	h := mw.Handler(ok)
	do(h, "192.0.2.1:1", nil)
	for i := range 20 {
		hdr := map[string]string{"X-Forwarded-For": "203.0.113." + strconv.Itoa(i), "X-Real-IP": "198.51.100.1"}
		if do(h, "192.0.2.1:1", hdr).Code != http.StatusTooManyRequests {
			t.Fatal("forged forwarding header bypassed the limit")
		}
	}
}

func TestAPIKeyRulesAndMissingKeys(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	perIP := must(s.FixedWindow("ip", rategen.PerMinute(100)))
	perKey := must(s.FixedWindow("key", rategen.PerMinute(2)))
	hasher := must(httplimit.HMACSHA256([]byte("0123456789abcdef0123456789abcdef")))
	var logged []error
	mw := must(httplimit.New(httplimit.Config{
		Rules: []httplimit.Rule{
			{Limiter: perIP, Key: httplimit.ByIP()},
			{Limiter: perKey, Key: httplimit.ByHeader("X-API-Key", hasher), Optional: true},
		},
		ErrorLog: func(_ *http.Request, err error) { logged = append(logged, err) },
	}))
	h := mw.Handler(ok)
	key := map[string]string{"X-API-Key": "sk_live_secret"}
	for range 2 {
		if do(h, "192.0.2.1:1", key).Code != 200 {
			t.Fatal("keyed request rejected")
		}
	}
	if do(h, "192.0.2.2:1", key).Code != http.StatusTooManyRequests {
		t.Fatal("API-key limit not shared across IPs")
	}
	if do(h, "192.0.2.1:1", nil).Code != 200 {
		t.Fatal("anonymous request not covered by the optional rule")
	}

	required := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{
		{Limiter: perKey, Key: httplimit.ByHeader("X-API-Key", hasher)},
	}})).Handler(ok)
	rec := do(required, "192.0.2.1:1", nil)
	if rec.Code != http.StatusBadRequest || strings.Contains(rec.Body.String(), "X-API-Key") {
		t.Fatalf("missing required key: %d %q", rec.Code, rec.Body)
	}
	if do(required, "192.0.2.1:1", map[string]string{"X-API-Key": strings.Repeat("a", httplimit.MaxIdentityLen+1)}).Code != http.StatusBadRequest {
		t.Fatal("oversized identity accepted")
	}
	for _, err := range logged {
		if strings.Contains(err.Error(), "sk_live_secret") {
			t.Fatal("API key leaked into an error")
		}
	}
	if _, err := httplimit.HMACSHA256([]byte("short")); !errors.Is(err, rategen.ErrInvalidConfig) {
		t.Fatal("short HMAC secret accepted")
	}
}

func TestHashedKeysAreOpaque(t *testing.T) {
	var seen []string
	spy := rategen.LimiterFunc(func(_ context.Context, key string, _ int64) (rategen.Decision, error) {
		seen = append(seen, key)
		return rategen.Decision{Allowed: true}, nil
	})
	user := func(r *http.Request) (string, bool) { return r.Header.Get("X-User"), r.Header.Get("X-User") != "" }
	mw := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{
		{Limiter: spy, Key: httplimit.ByHeader("Authorization", nil)},
		{Limiter: spy, Key: httplimit.ByFunc(user, httplimit.SHA256())},
		{Limiter: spy, Key: httplimit.Join(httplimit.Static("global"), httplimit.ByFunc(user, nil))},
		{Limiter: spy, Key: httplimit.ByFunc(func(*http.Request) (string, bool) { return strings.Repeat("x", 1000), true }, nil)},
	}}))
	do(mw.Handler(ok), "192.0.2.1:1", map[string]string{"Authorization": "Bearer tok", "X-User": "alice"})
	if len(seen) != 4 {
		t.Fatalf("keys: %q", seen)
	}
	if strings.Contains(seen[0], "tok") || strings.Contains(seen[1], "alice") {
		t.Fatalf("raw identity used as key: %q", seen)
	}
	if seen[2] != "6:global5:alice" {
		t.Fatalf("Join = %q", seen[2])
	}
	if len(seen[3]) > 128 {
		t.Fatalf("long key not bounded: %d bytes", len(seen[3]))
	}
}

func TestMultipleRulesAtomic(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	perSec := must(s.TokenBucket("per-sec", rategen.PerSecond(10), 10))
	global := must(s.FixedWindow("global", rategen.PerMinute(3)))
	mw := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{
		{Limiter: perSec, Key: httplimit.ByIP()},
		{Limiter: global, Key: httplimit.Static("all")},
	}}))
	h := mw.Handler(ok)
	for i := range 3 {
		do(h, "192.0.2."+strconv.Itoa(i)+":1", nil)
	}
	if do(h, "192.0.2.9:1", nil).Code != http.StatusTooManyRequests {
		t.Fatal("global limit not enforced")
	}
	// The rejected request did not consume the client's per-second budget.
	if d, _ := perSec.AllowN(context.Background(), "192.0.2.9", 10); !d.Allowed {
		t.Fatal("rejected request consumed per-IP capacity")
	}
}

func TestSkipMatchCostAndPattern(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	l := must(s.FixedWindow("route", rategen.PerMinute(10)))
	var key atomic.Value
	spy := rategen.LimiterFunc(func(ctx context.Context, k string, n int64) (rategen.Decision, error) {
		key.Store(k)
		return l.AllowN(ctx, k, n)
	})
	mw := must(httplimit.New(httplimit.Config{
		Rules: []httplimit.Rule{{
			Limiter: spy,
			Key:     httplimit.ByPattern(),
			Match:   func(r *http.Request) bool { return r.Method == http.MethodPost },
			Cost:    func(r *http.Request) int64 { return 5 },
		}},
		Skip: func(r *http.Request) bool { return r.URL.Path == "/healthz" },
	}))
	mux := http.NewServeMux()
	mux.Handle("POST /items/{id}", mw.Handler(ok))
	mux.Handle("GET /items/{id}", mw.Handler(ok))
	mux.Handle("/healthz", mw.Handler(ok))
	post := func(path string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, path, nil)
		mux.ServeHTTP(rec, req)
		return rec.Code
	}
	if post("/items/1") != 200 || post("/items/2") != 200 {
		t.Fatal("first two posts rejected")
	}
	if post("/items/3") != http.StatusTooManyRequests {
		t.Fatal("cost 5 x 3 > 10 admitted")
	}
	if key.Load() != "POST /items/{id}" {
		t.Fatalf("pattern key = %v", key.Load())
	}
	for range 5 {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/items/1", nil))
		if rec.Code != 200 {
			t.Fatal("non-matching method limited")
		}
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/healthz", nil))
		if rec.Code != 200 {
			t.Fatal("skipped route limited")
		}
	}
}

func TestBackendFailure(t *testing.T) {
	boom := errors.New("redis: i/o timeout to 10.1.2.3:6379 password=hunter2")
	closed := rategen.LimiterFunc(func(context.Context, string, int64) (rategen.Decision, error) {
		return rategen.Decision{}, errors.Join(rategen.ErrStoreUnavailable, boom)
	})
	open := rategen.LimiterFunc(func(context.Context, string, int64) (rategen.Decision, error) {
		return rategen.Decision{Allowed: true, Degraded: true}, errors.Join(rategen.ErrStoreUnavailable, boom)
	})
	var logged int
	logf := func(*http.Request, error) { logged++ }

	h := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: closed, Key: httplimit.ByIP()}}, ErrorLog: logf, Headers: httplimit.HeadersDraft11})).Handler(ok)
	rec := do(h, "192.0.2.1:1", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("fail-closed status %d", rec.Code)
	}
	if body := rec.Body.String(); strings.Contains(body, "hunter2") || strings.Contains(body, "10.1.2.3") {
		t.Fatalf("internal error leaked: %q", body)
	}
	h = must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: open, Key: httplimit.ByIP()}}, ErrorLog: logf, Headers: httplimit.HeadersDraft11})).Handler(ok)
	rec = do(h, "192.0.2.1:1", nil)
	if rec.Code != 200 || rec.Header().Get("RateLimit") != "" {
		t.Fatalf("fail-open: %d, headers %v", rec.Code, rec.Header())
	}
	if logged != 2 {
		t.Fatalf("ErrorLog called %d times, want 2", logged)
	}
}

func TestCanceledRequest(t *testing.T) {
	var called atomic.Bool
	l := rategen.LimiterFunc(func(ctx context.Context, _ string, _ int64) (rategen.Decision, error) {
		return rategen.Decision{}, ctx.Err()
	})
	h := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}}})).Handler(
		http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called.Store(true) }))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if called.Load() || rec.Body.Len() != 0 {
		t.Fatal("canceled request reached the handler or got a body")
	}
}

func TestKeyFloodIsBounded(t *testing.T) {
	s, _ := newStore(t, memory.Config{MaxKeys: 1000, Shards: 8})
	l := must(s.GCRA("ip", rategen.PerSecond(1), 5))
	h := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}}})).Handler(ok)
	for i := range 20_000 {
		do(h, "10."+strconv.Itoa(i>>16&255)+"."+strconv.Itoa(i>>8&255)+"."+strconv.Itoa(i&255)+":1", nil)
	}
	// IPv6 rotation inside one /64 maps to one key.
	for i := range 100 {
		do(h, "[2001:db8::"+strconv.FormatInt(int64(i), 16)+"]:1", nil)
	}
	if k := s.Stats().Keys; k > 1000 {
		t.Fatalf("store grew to %d keys", k)
	}
	if do(h, "[2001:db8::ffff]:1", nil).Code != http.StatusTooManyRequests {
		t.Fatal("IPv6 rotation within a /64 bypassed the limit")
	}
}

func TestStreamingBodyAndComposition(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	inner := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: must(s.FixedWindow("inner", rategen.PerMinute(10))), Key: httplimit.ByIP()}}}))
	outer := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: must(s.FixedWindow("outer", rategen.PerMinute(10))), Key: httplimit.ByIP()}}, Headers: httplimit.HeadersLegacy}))
	echo := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", 500)
			return
		}
		sc := bufio.NewScanner(r.Body)
		for sc.Scan() {
			io.WriteString(w, strings.ToUpper(sc.Text())+"\n")
			f.Flush()
		}
	})
	srv := httptest.NewServer(outer.Handler(inner.Handler(echo)))
	defer srv.Close()
	resp, err := http.Post(srv.URL, "text/plain", strings.NewReader("a\nb\n"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "A\nB\n" {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if resp.Header.Get("X-RateLimit-Remaining") != "9" {
		t.Fatalf("outer headers lost: %v", resp.Header)
	}
}

func TestCustomRejectAndStatus(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	l := must(s.FixedWindow("x", rategen.PerMinute(1)))
	h := must(httplimit.New(httplimit.Config{
		Rules:      []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}},
		StatusCode: http.StatusServiceUnavailable,
		OnReject: func(w http.ResponseWriter, _ *http.Request, res rategen.Result) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, `{"error":"slow down","policy":"`+res.Decision().Policy+`"}`)
		},
	})).Handler(ok)
	do(h, "192.0.2.1:1", nil)
	rec := do(h, "192.0.2.1:1", nil)
	if rec.Code != 503 || rec.Body.String() != `{"error":"slow down","policy":"x"}` || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("%d %q %v", rec.Code, rec.Body, rec.Header())
	}
}

func TestConfigValidation(t *testing.T) {
	l := rategen.LimiterFunc(func(context.Context, string, int64) (rategen.Decision, error) {
		return rategen.Decision{Allowed: true}, nil
	})
	for _, cfg := range []httplimit.Config{
		{},
		{Rules: []httplimit.Rule{{Limiter: l}}},
		{Rules: []httplimit.Rule{{Key: httplimit.ByIP()}}},
		{Rules: []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}}, StatusCode: 200},
		{Rules: []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}}, Headers: 7},
		{Rules: []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}}, MaxKeyLen: 4},
	} {
		if _, err := httplimit.New(cfg); !errors.Is(err, rategen.ErrInvalidConfig) {
			t.Errorf("%+v accepted", cfg)
		}
	}
}

func BenchmarkMiddleware(b *testing.B) {
	noop := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	s := must(memory.New(memory.Config{}))
	l := must(s.GCRA("ip", rategen.PerSecond(1<<40), 1<<40))
	limited := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}}})).Handler(noop)
	draft := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}}, Headers: httplimit.HeadersDraft11})).Handler(noop)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	for _, bc := range []struct {
		name string
		h    http.Handler
	}{{"baseline", noop}, {"limited", limited}, {"limited+draft-headers", draft}} {
		b.Run(bc.name, func(b *testing.B) {
			w := httptest.NewRecorder()
			for b.Loop() {
				clear(w.Header())
				bc.h.ServeHTTP(w, req)
			}
		})
	}
}

func TestNoRateLimitHeadersOnStoreFailure(t *testing.T) {
	// A realistic fail-closed decision carries the policy's limit.
	down := rategen.LimiterFunc(func(context.Context, string, int64) (rategen.Decision, error) {
		return rategen.Decision{Policy: "p", Limit: 10, Window: time.Minute}, rategen.ErrStoreUnavailable
	})
	for _, style := range []httplimit.HeaderStyle{httplimit.HeadersLegacy, httplimit.HeadersDraft11} {
		h := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: down, Key: httplimit.ByIP()}}, Headers: style})).Handler(ok)
		rec := do(h, "192.0.2.1:1", nil)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status %d", rec.Code)
		}
		for _, k := range []string{"X-RateLimit-Limit", "X-RateLimit-Remaining", "RateLimit", "RateLimit-Policy", "Retry-After"} {
			if v := rec.Header().Get(k); v != "" {
				t.Errorf("style %d: %s = %q on a store failure", style, k, v)
			}
		}
	}
}

func TestLongKeysRespectMaxKeyLen(t *testing.T) {
	var got string
	spy := rategen.LimiterFunc(func(_ context.Context, key string, _ int64) (rategen.Decision, error) {
		got = key
		return rategen.Decision{Allowed: true}, nil
	})
	long := httplimit.ByFunc(func(*http.Request) (string, bool) { return strings.Repeat("k", 100), true }, nil)
	h := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: spy, Key: long}}, MaxKeyLen: 32})).Handler(ok)
	do(h, "192.0.2.1:1", nil)
	if len(got) > 32 {
		t.Fatalf("hashed key is %d bytes, above MaxKeyLen 32", len(got))
	}
}

func TestHugeForwardingHeaderIsCheap(t *testing.T) {
	trusted := must(httplimit.ParsePrefixes("10.0.0.0/8"))
	x := must(httplimit.NewIPExtractor(httplimit.IPConfig{TrustedProxies: trusted}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "10.0.0.1:1"
	req.Header.Set("X-Forwarded-For", "203.0.113.1"+strings.Repeat(",", 1<<20))
	allocs := testing.AllocsPerRun(10, func() { x.ClientIP(req) })
	if allocs > 2 {
		t.Fatalf("%v allocations parsing a padded header", allocs)
	}
	// Only MaxHops entries are examined; past them the request is refused
	// rather than scanned further.
	if _, err := x.ClientIP(req); !errors.Is(err, httplimit.ErrInvalidClientIP) {
		t.Fatalf("got %v, want ErrInvalidClientIP", err)
	}
	// An attacker padding the left of the header (where client-supplied
	// entries sit) costs nothing: the client is found at the right.
	req.Header.Set("X-Forwarded-For", strings.Repeat(",", 1<<20)+"203.0.113.1")
	if a, err := x.ClientIP(req); err != nil || a.String() != "203.0.113.1" {
		t.Fatalf("got %v, %v", a, err)
	}
}

func TestDryRun(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	l := must(s.FixedWindow("pilot", rategen.PerMinute(2)))
	down := rategen.LimiterFunc(func(context.Context, string, int64) (rategen.Decision, error) {
		return rategen.Decision{Policy: "down", Limit: 1}, rategen.ErrStoreUnavailable
	})
	var would, errs int
	var lastPolicy string
	cfg := httplimit.Config{
		Rules:   []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}},
		DryRun:  true,
		Headers: httplimit.HeadersDraft11,
		OnWouldReject: func(_ *http.Request, res rategen.Result) {
			would++
			lastPolicy = res.Decision().Policy
		},
		ErrorLog: func(*http.Request, error) { errs++ },
	}
	h := must(httplimit.New(cfg)).Handler(ok)
	for i := range 5 {
		rec := do(h, "192.0.2.1:1", nil)
		if rec.Code != 200 || rec.Body.String() != "ok" {
			t.Fatalf("request %d: dry run blocked the request (%d)", i, rec.Code)
		}
		for _, k := range []string{"RateLimit", "RateLimit-Policy", "Retry-After"} {
			if rec.Header().Get(k) != "" {
				t.Fatalf("dry run sent %s", k)
			}
		}
	}
	if would != 3 || lastPolicy != "pilot" {
		t.Fatalf("OnWouldReject called %d times (policy %q), want 3", would, lastPolicy)
	}
	// Capacity is consumed exactly as when enforcing.
	if d, _ := l.AllowN(context.Background(), "192.0.2.1", 1); d.Allowed {
		t.Fatal("dry run did not consume capacity")
	}

	// Backend failures and missing keys are logged, not enforced.
	cfg.Rules = []httplimit.Rule{
		{Limiter: down, Key: httplimit.ByIP()},
		{Limiter: l, Key: httplimit.ByHeader("X-API-Key", nil)},
	}
	h = must(httplimit.New(cfg)).Handler(ok)
	if rec := do(h, "192.0.2.9:1", nil); rec.Code != 200 {
		t.Fatalf("missing key blocked in dry run: %d", rec.Code)
	}
	cfg.Rules = cfg.Rules[:1]
	h = must(httplimit.New(cfg)).Handler(ok)
	if rec := do(h, "192.0.2.9:1", nil); rec.Code != 200 {
		t.Fatalf("store failure blocked in dry run: %d", rec.Code)
	}
	if errs != 2 {
		t.Fatalf("ErrorLog called %d times, want 2", errs)
	}
}

// failOpenLimiter admits with a degraded decision, as a Redis limiter does
// under FailOpen during an outage, and cannot refund.
var failOpenLimiter = rategen.LimiterFunc(func(context.Context, string, int64) (rategen.Decision, error) {
	return rategen.Decision{Allowed: true, Degraded: true, Policy: "remote"}, errors.Join(rategen.ErrStoreUnavailable, errors.New("connection refused"))
})

func TestRealRejectionIsNot503WhenAnotherRuleDegrades(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	local := must(s.GCRA("local", rategen.PerHour(1), 1))
	var logged int
	h := must(httplimit.New(httplimit.Config{
		Rules: []httplimit.Rule{
			{Limiter: failOpenLimiter, Key: httplimit.ByIP()},
			{Limiter: local, Key: httplimit.ByIP()},
		},
		ErrorLog: func(*http.Request, error) { logged++ },
	})).Handler(ok)
	do(h, "192.0.2.1:1", nil)
	rec := do(h, "192.0.2.1:1", nil)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("real limit rejection answered %d (Retry-After %q), want 429", rec.Code, rec.Header().Get("Retry-After"))
	}
	if logged == 0 {
		t.Fatal("degraded rule's error not logged")
	}
}

func TestUpstreamDeadlineIsAnswered(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	l := must(s.GCRA("x", rategen.PerHour(10), 10))
	mw := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: l, Key: httplimit.ByIP()}}}))
	h := mw.Handler(ok)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("deadline passed upstream: status %d, want 503 (not an empty 200)", rec.Code)
	}
}

func TestNestedMiddlewareHeaders(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	strict := must(s.GCRA("outer", rategen.PerHour(3), 3))
	loose := must(s.GCRA("inner", rategen.PerHour(100), 100))
	for _, style := range []httplimit.HeaderStyle{httplimit.HeadersLegacy, httplimit.HeadersDraft11} {
		outer := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: strict, Key: httplimit.Static("o" + strconv.Itoa(int(style)))}}, Headers: style}))
		inner := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{{Limiter: loose, Key: httplimit.Static("i" + strconv.Itoa(int(style)))}}, Headers: style}))
		rec := do(outer.Handler(inner.Handler(ok)), "192.0.2.1:1", nil)
		switch style {
		case httplimit.HeadersLegacy:
			if got := rec.Header().Get("X-RateLimit-Remaining"); got != "2" {
				t.Errorf("legacy: inner overwrote the stricter outer limit: Remaining=%q", got)
			}
		case httplimit.HeadersDraft11:
			if got := rec.Header().Get("RateLimit"); !strings.Contains(got, `"outer"`) || !strings.Contains(got, `"inner"`) {
				t.Errorf("draft-11: RateLimit = %q, want both policies", got)
			}
		}
	}
}

func TestRepeatedIdentityHeaderCannotSkipOptionalRule(t *testing.T) {
	s, _ := newStore(t, memory.Config{})
	perKey := must(s.FixedWindow("key", rategen.PerHour(1)))
	h := must(httplimit.New(httplimit.Config{Rules: []httplimit.Rule{
		{Limiter: perKey, Key: httplimit.ByHeader("X-API-Key", nil), Optional: true},
	}})).Handler(ok)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "192.0.2.1:1"
	req.Header.Add("X-API-Key", "random-1")
	req.Header.Add("X-API-Key", "real-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("repeated identity header: status %d, want 400", rec.Code)
	}
}
