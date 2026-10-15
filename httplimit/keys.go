package httplimit

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/mjahidhasand10/rategen"
)

// KeyFunc derives a rate-limit key from a request. It returns an error
// wrapping rategen.ErrMissingKey when the request carries no identity of
// the expected kind. KeyFuncs run on every request before any limit is
// checked, so they must be cheap.
type KeyFunc func(r *http.Request) (string, error)

// Hasher maps a sensitive identity (API key, token, user ID) to an opaque
// key so that the raw value never reaches storage, logs or metrics.
type Hasher func(value string) string

// HMACSHA256 returns a Hasher computing a keyed HMAC-SHA256, truncated to
// 128 bits and base64url-encoded. With a secret kept out of the store, a
// leaked store does not allow offline guessing of low-entropy identities.
// The secret should be at least 32 random bytes.
func HMACSHA256(secret []byte) (Hasher, error) {
	if len(secret) < 16 {
		return nil, fmt.Errorf("%w: HMAC secret must be at least 16 bytes", rategen.ErrInvalidConfig)
	}
	key := append([]byte(nil), secret...)
	return func(v string) string {
		m := hmac.New(sha256.New, key)
		m.Write([]byte(v))
		return base64.RawURLEncoding.EncodeToString(m.Sum(nil)[:16])
	}, nil
}

// SHA256 returns an unkeyed Hasher (SHA-256, truncated to 128 bits,
// base64url). It hides identities from casual inspection, but low-entropy
// values (user IDs, emails) can be recovered by guessing; prefer HMACSHA256
// for those.
func SHA256() Hasher {
	return func(v string) string {
		sum := sha256.Sum256([]byte(v))
		return base64.RawURLEncoding.EncodeToString(sum[:16])
	}
}

// ErrInvalidKey reports an identity that is present but unusable, such as a
// repeated identity header. Unlike rategen.ErrMissingKey, it never causes
// an Optional rule to be skipped.
var ErrInvalidKey = errors.New("httplimit: invalid rate-limit key")

// MaxIdentityLen is the longest identity value ByHeader and ByFunc accept.
// Longer values are rejected as missing, bounding the cost of hashing
// attacker-supplied input.
const MaxIdentityLen = 4096

// ByHeader returns a KeyFunc keyed by the value of the named request header
// (for example "X-API-Key"), passed through h. A nil h means SHA256. A
// missing, empty or oversized header yields ErrMissingKey; a repeated
// header yields ErrInvalidKey.
//
// ByHeader only identifies the caller; it does not authenticate. Unless the
// header is verified before rate limiting, clients can choose arbitrary
// values to obtain fresh limits, so combine it with a per-IP rule or place
// the middleware after authentication.
func ByHeader(name string, h Hasher) KeyFunc {
	if h == nil {
		h = SHA256()
	}
	name = http.CanonicalHeaderKey(name)
	return func(r *http.Request) (string, error) {
		vs := r.Header.Values(name)
		if len(vs) > 1 {
			// An authentication layer may read a different line than
			// we do; a client could then rotate one line for fresh
			// limits while authenticating with the other. This is not
			// ErrMissingKey, so Optional rules cannot be skipped this way.
			return "", fmt.Errorf("%w: multiple %s headers", ErrInvalidKey, name)
		}
		if len(vs) == 0 || vs[0] == "" || len(vs[0]) > MaxIdentityLen {
			return "", fmt.Errorf("%w: header %s", rategen.ErrMissingKey, name)
		}
		return h(vs[0]), nil
	}
}

// ByFunc returns a KeyFunc keyed by an identity obtained from the request,
// typically an authenticated user or tenant ID placed in the context by
// earlier middleware. If ok is false, or the value is empty or oversized,
// the KeyFunc yields ErrMissingKey. A nil h uses the identity unhashed,
// which is appropriate only for non-sensitive identifiers.
func ByFunc(identity func(r *http.Request) (id string, ok bool), h Hasher) KeyFunc {
	return func(r *http.Request) (string, error) {
		id, ok := identity(r)
		if !ok || id == "" || len(id) > MaxIdentityLen {
			return "", rategen.ErrMissingKey
		}
		if h != nil {
			id = h(id)
		}
		return id, nil
	}
}

// ByPattern returns a KeyFunc keyed by the http.ServeMux pattern that matched
// the request (http.Request.Pattern, for example "GET /items/{id}"). The
// pattern is set only when the middleware runs inside a ServeMux handler;
// otherwise the KeyFunc yields ErrMissingKey. Keying by pattern rather than
// by URL path keeps the number of keys bounded by the number of routes.
func ByPattern() KeyFunc {
	return func(r *http.Request) (string, error) {
		if r.Pattern == "" {
			return "", fmt.Errorf("%w: no ServeMux pattern", rategen.ErrMissingKey)
		}
		return r.Pattern, nil
	}
}

// Static returns a KeyFunc that always yields key, for limits shared by all
// requests (a global service limit).
func Static(key string) KeyFunc {
	return func(*http.Request) (string, error) { return key, nil }
}

// Join combines KeyFuncs into one, for limits per (user, route) or similar.
// Parts are length-prefixed, so different combinations can never produce
// the same key. Any part's error is returned.
func Join(parts ...KeyFunc) KeyFunc {
	return func(r *http.Request) (string, error) {
		var b strings.Builder
		for _, p := range parts {
			k, err := p(r)
			if err != nil {
				return "", err
			}
			b.WriteString(strconv.Itoa(len(k)))
			b.WriteByte(':')
			b.WriteString(k)
		}
		return b.String(), nil
	}
}
