// Package httplimit provides net/http middleware that applies rategen
// limits to incoming requests.
//
// A Middleware holds a list of Rules. Each rule pairs a limiter with a
// KeyFunc that derives the key from the request (client IP, API key, user,
// route, or a combination). All matching rules are evaluated together for
// each request with rategen.Evaluate, atomically when the limiters share a
// backend; the request proceeds only if every rule admits it.
//
// Client IPs come from RemoteAddr unless an IPExtractor is configured with
// trusted proxies. Forwarding headers from untrusted peers are never used.
package httplimit

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/mjahidhasand10/rategen"
)

// Rule is one limit applied to requests.
type Rule struct {
	// Limiter enforces the limit. Required.
	Limiter rategen.Limiter
	// Key derives the key. Required.
	Key KeyFunc
	// Cost returns the number of units a request consumes; nil means 1.
	// Costs below 1 are treated as 1.
	Cost func(r *http.Request) int64
	// Match restricts the rule to some requests; nil means all.
	Match func(r *http.Request) bool
	// Optional skips the rule when Key reports rategen.ErrMissingKey,
	// instead of failing the request. Use it for rules that apply only to
	// identified callers (per-API-key), alongside a rule that always
	// applies (per-IP). A missing key never means "unlimited" unless a
	// rule is explicitly Optional.
	Optional bool
}

// HeaderStyle selects which rate-limit response headers are sent.
// Retry-After is always sent on rejections that can be retried.
type HeaderStyle int

const (
	// HeadersNone sends only Retry-After on rejection.
	HeadersNone HeaderStyle = iota
	// HeadersLegacy sends X-RateLimit-Limit, X-RateLimit-Remaining and
	// X-RateLimit-Reset (seconds until reset) for the most restrictive
	// rule.
	HeadersLegacy
	// HeadersDraft11 sends RateLimit-Policy and RateLimit fields as defined
	// by draft-ietf-httpapi-ratelimit-headers-11, one item per rule. The
	// draft is not yet an RFC; its syntax may change in later drafts.
	HeadersDraft11
)

// Config configures a Middleware.
type Config struct {
	// Rules are the limits to apply. At least one is required.
	Rules []Rule

	// Skip exempts requests from all rules, for example health checks.
	Skip func(r *http.Request) bool

	// StatusCode is sent when a request is rejected; default 429.
	StatusCode int

	// Headers selects rate-limit response headers; default HeadersNone.
	Headers HeaderStyle

	// OnReject writes the response for a rejected request. The default
	// writes StatusCode with a short plain-text body. Headers selected by
	// Headers and Retry-After are already set when it is called.
	OnReject func(w http.ResponseWriter, r *http.Request, res rategen.Result)

	// OnError writes the response when a request cannot be evaluated: a
	// required key is missing or invalid, or a backend failed and its
	// failure policy rejected the request. The default sends 400 for key
	// errors and 503 for backend errors, with generic bodies that never
	// include error details.
	OnError func(w http.ResponseWriter, r *http.Request, err error)

	// ErrorLog, if set, is called for errors that did not stop the
	// request, such as a backend failure under a fail-open policy, and
	// for errors passed to OnError. Errors never contain keys.
	ErrorLog func(r *http.Request, err error)

	// MaxKeyLen bounds key length; longer keys are replaced by a 24-byte
	// form of their SHA-256 hash. Default 128, minimum 32.
	MaxKeyLen int

	// DryRun evaluates every rule and consumes capacity exactly as in
	// normal operation, but never rejects: requests that would have been
	// rejected, and requests whose keys or backends failed, are passed to
	// the handler. No rate-limit headers are sent. Use it to pilot limits on
	// live traffic before enforcing them; report would-be rejections with
	// OnWouldReject and errors with ErrorLog.
	DryRun bool

	// OnWouldReject is called in DryRun mode for each request that a limit
	// would have rejected. It must not write to the response.
	OnWouldReject func(r *http.Request, res rategen.Result)
}

// Middleware applies rate limits to HTTP requests.
type Middleware struct {
	cfg Config
}

// New validates cfg and returns a Middleware.
func New(cfg Config) (*Middleware, error) {
	if len(cfg.Rules) == 0 {
		return nil, fmt.Errorf("%w: at least one rule is required", rategen.ErrInvalidConfig)
	}
	cfg.Rules = append([]Rule(nil), cfg.Rules...)
	for i, r := range cfg.Rules {
		if r.Limiter == nil || r.Key == nil {
			return nil, fmt.Errorf("%w: rule %d needs a Limiter and a Key", rategen.ErrInvalidConfig, i)
		}
	}
	if cfg.StatusCode == 0 {
		cfg.StatusCode = http.StatusTooManyRequests
	}
	if cfg.StatusCode < 400 || cfg.StatusCode > 599 {
		return nil, fmt.Errorf("%w: StatusCode %d is not an error status", rategen.ErrInvalidConfig, cfg.StatusCode)
	}
	if cfg.Headers < HeadersNone || cfg.Headers > HeadersDraft11 {
		return nil, fmt.Errorf("%w: unknown HeaderStyle %d", rategen.ErrInvalidConfig, cfg.Headers)
	}
	if cfg.MaxKeyLen == 0 {
		cfg.MaxKeyLen = 128
	}
	if cfg.MaxKeyLen < 32 {
		return nil, fmt.Errorf("%w: MaxKeyLen must be at least 32", rategen.ErrInvalidConfig)
	}
	if cfg.OnReject == nil {
		status := cfg.StatusCode
		cfg.OnReject = func(w http.ResponseWriter, _ *http.Request, _ rategen.Result) {
			http.Error(w, "rate limit exceeded", status)
		}
	}
	if cfg.OnError == nil {
		cfg.OnError = defaultOnError
	}
	return &Middleware{cfg: cfg}, nil
}

// Handler returns next wrapped with rate limiting.
func (m *Middleware) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		m.serve(w, r, next)
	})
}

// HandlerFunc is Handler for a function.
func (m *Middleware) HandlerFunc(next http.HandlerFunc) http.Handler {
	return m.Handler(next)
}

func defaultOnError(w http.ResponseWriter, _ *http.Request, err error) {
	if isKeyError(err) {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	http.Error(w, "service unavailable", http.StatusServiceUnavailable)
}

func isKeyError(err error) bool {
	return errors.Is(err, rategen.ErrMissingKey) || errors.Is(err, ErrInvalidClientIP) || errors.Is(err, ErrInvalidKey)
}

func (m *Middleware) serve(w http.ResponseWriter, r *http.Request, next http.Handler) {
	if m.cfg.Skip != nil && m.cfg.Skip(r) {
		next.ServeHTTP(w, r)
		return
	}
	checks := make([]rategen.Check, 0, len(m.cfg.Rules))
	for _, rule := range m.cfg.Rules {
		if rule.Match != nil && !rule.Match(r) {
			continue
		}
		key, err := rule.Key(r)
		if err != nil {
			if rule.Optional && errors.Is(err, rategen.ErrMissingKey) {
				continue
			}
			if m.dryRunError(r, err) {
				next.ServeHTTP(w, r)
				return
			}
			m.fail(w, r, err)
			return
		}
		if len(key) > m.cfg.MaxKeyLen {
			sum := sha256.Sum256([]byte(key))
			key = "h:" + base64.RawURLEncoding.EncodeToString(sum[:16])
		}
		n := int64(1)
		if rule.Cost != nil {
			n = max(1, rule.Cost(r))
		}
		checks = append(checks, rategen.Check{Limiter: rule.Limiter, Key: key, N: n})
	}
	if len(checks) == 0 {
		next.ServeHTTP(w, r)
		return
	}

	res, err := rategen.Evaluate(r.Context(), checks...)
	if errors.Is(r.Context().Err(), context.Canceled) {
		// The client went away; there is no one to answer. A deadline
		// set upstream, by contrast, still has a client waiting and is
		// answered through OnError below.
		return
	}
	// A rejection without any limit verdict (backend failed under
	// FailClosed, deadline passed) is an error, not a 429.
	failed := !res.Allowed && !res.RejectedByLimit()
	if m.cfg.DryRun {
		switch {
		case failed:
			m.dryRunError(r, err)
		default:
			if err != nil && m.cfg.ErrorLog != nil {
				m.cfg.ErrorLog(r, err)
			}
			if !res.Allowed && m.cfg.OnWouldReject != nil {
				m.cfg.OnWouldReject(r, res)
			}
		}
		next.ServeHTTP(w, r)
		return
	}
	if res.Allowed {
		m.setHeaders(w.Header(), res)
		if err != nil && m.cfg.ErrorLog != nil {
			m.cfg.ErrorLog(r, err)
		}
		next.ServeHTTP(w, r)
		return
	}
	if failed {
		// The decision is not a real limit verdict, so no rate-limit
		// headers describe it.
		if err == nil {
			err = rategen.ErrStoreUnavailable
		}
		m.fail(w, r, err)
		return
	}
	if err != nil && m.cfg.ErrorLog != nil {
		// For example an allowed check that degraded, or a refund that
		// failed on the non-atomic path.
		m.cfg.ErrorLog(r, err)
	}
	m.setHeaders(w.Header(), res)
	if d := res.Decision(); d.RetryAfter > 0 {
		w.Header().Set("Retry-After", strconv.FormatInt(ceilSeconds(d.RetryAfter), 10))
	}
	m.cfg.OnReject(w, r, res)
}

// dryRunError reports err and returns true if the middleware is in DryRun
// mode, in which errors never stop a request.
func (m *Middleware) dryRunError(r *http.Request, err error) bool {
	if !m.cfg.DryRun {
		return false
	}
	if m.cfg.ErrorLog != nil {
		m.cfg.ErrorLog(r, err)
	}
	return true
}

func (m *Middleware) fail(w http.ResponseWriter, r *http.Request, err error) {
	if m.cfg.ErrorLog != nil {
		m.cfg.ErrorLog(r, err)
	}
	if errors.Is(err, context.Canceled) && r.Context().Err() != nil {
		return
	}
	m.cfg.OnError(w, r, err)
}

func (m *Middleware) setHeaders(h http.Header, res rategen.Result) {
	switch m.cfg.Headers {
	case HeadersLegacy:
		d := res.Decision()
		if d.Degraded || d.Limit == 0 {
			return
		}
		// Nested middlewares: keep whichever limit is more restrictive.
		if prev := h.Get("X-RateLimit-Remaining"); prev != "" {
			if n, err := strconv.ParseInt(prev, 10, 64); err == nil && n <= d.Remaining {
				return
			}
		}
		h.Set("X-RateLimit-Limit", strconv.FormatInt(d.Limit, 10))
		h.Set("X-RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
		h.Set("X-RateLimit-Reset", strconv.FormatInt(ceilSeconds(d.ResetAfter), 10))
	case HeadersDraft11:
		var policy, limit []byte
		for _, d := range res.Decisions {
			if d.Degraded || d.Limit == 0 {
				continue
			}
			if len(policy) > 0 {
				policy = append(policy, ", "...)
				limit = append(limit, ", "...)
			}
			policy = appendSFString(policy, d.Policy)
			policy = append(policy, ";q="...)
			policy = strconv.AppendInt(policy, d.Limit, 10)
			policy = append(policy, ";w="...)
			policy = strconv.AppendInt(policy, max(1, ceilSeconds(d.Window)), 10)
			limit = appendSFString(limit, d.Policy)
			limit = append(limit, ";r="...)
			limit = strconv.AppendInt(limit, d.Remaining, 10)
			limit = append(limit, ";t="...)
			limit = strconv.AppendInt(limit, ceilSeconds(d.ResetAfter), 10)
		}
		if len(policy) > 0 {
			// Append to items set by an outer middleware rather than
			// replacing them; both are Structured Field Lists.
			h["Ratelimit-Policy"] = []string{joinList(h["Ratelimit-Policy"], policy)}
			h["Ratelimit"] = []string{joinList(h["Ratelimit"], limit)}
		}
	}
}

func joinList(prev []string, items []byte) string {
	if len(prev) == 0 || prev[0] == "" {
		return string(items)
	}
	return prev[0] + ", " + string(items)
}

// appendSFString appends s encoded as a Structured Field String (RFC 9651).
// Policy names are validated to a safe character set, but encode
// defensively anyway.
func appendSFString(b []byte, s string) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\\':
			b = append(b, '\\', c)
		case c < 0x20 || c > 0x7e:
			b = append(b, '_')
		default:
			b = append(b, c)
		}
	}
	return append(b, '"')
}

func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64(math.Ceil(d.Seconds()))
}
