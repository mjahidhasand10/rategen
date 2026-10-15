package httplimit

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"

	"github.com/mjahidhasand10/rategen"
)

// ErrInvalidClientIP reports that a client address could not be determined:
// RemoteAddr is unparsable, or a trusted proxy supplied an unparsable or
// obfuscated ("unknown", "_hidden") address.
var ErrInvalidClientIP = errors.New("httplimit: invalid client IP")

// IPConfig configures client-IP extraction.
type IPConfig struct {
	// TrustedProxies lists the networks of reverse proxies whose forwarding
	// headers are believed. If empty, forwarding headers are ignored and
	// the client IP is the connection's RemoteAddr. Never list networks
	// that untrusted clients can send from.
	TrustedProxies []netip.Prefix

	// Header is the forwarding header set by the trusted proxies:
	// "X-Forwarded-For" (the default), "Forwarded" (RFC 7239), or a
	// single-address header such as "X-Real-IP" or "CF-Connecting-IP".
	Header string

	// IPv4Prefix and IPv6Prefix aggregate addresses into networks before
	// they are used as keys. The defaults are 32 (one key per IPv4
	// address) and 64 (one key per IPv6 /64, the usual allocation to a
	// single subscriber; per-address IPv6 keys let one client rotate
	// through 2^64 addresses).
	IPv4Prefix int
	IPv6Prefix int

	// MaxHops bounds how many forwarding-header entries (including empty
	// ones) are examined, right to left. The default is 16. A chain of more
	// than MaxHops trusted proxies is rejected with ErrInvalidClientIP.
	MaxHops int
}

// IPExtractor determines the client IP of requests. It is safe for
// concurrent use.
type IPExtractor struct {
	trusted []netip.Prefix
	header  string
	kind    headerKind
	v4, v6  int
	maxHops int
}

type headerKind int

const (
	headerXFF headerKind = iota
	headerForwarded
	headerSingle
)

// NewIPExtractor validates cfg and returns an extractor.
func NewIPExtractor(cfg IPConfig) (*IPExtractor, error) {
	x := &IPExtractor{
		trusted: append([]netip.Prefix(nil), cfg.TrustedProxies...),
		header:  http.CanonicalHeaderKey(cfg.Header),
		v4:      cfg.IPv4Prefix,
		v6:      cfg.IPv6Prefix,
		maxHops: cfg.MaxHops,
	}
	if x.header == "" {
		x.header = "X-Forwarded-For"
	}
	switch x.header {
	case "X-Forwarded-For":
		x.kind = headerXFF
	case "Forwarded":
		x.kind = headerForwarded
	default:
		x.kind = headerSingle
	}
	if x.v4 == 0 {
		x.v4 = 32
	}
	if x.v6 == 0 {
		x.v6 = 64
	}
	if x.v4 < 1 || x.v4 > 32 || x.v6 < 1 || x.v6 > 128 {
		return nil, fmt.Errorf("%w: IP prefix lengths out of range (v4 %d, v6 %d)", rategen.ErrInvalidConfig, x.v4, x.v6)
	}
	if x.maxHops == 0 {
		x.maxHops = 16
	}
	if x.maxHops < 1 {
		return nil, fmt.Errorf("%w: MaxHops must be positive", rategen.ErrInvalidConfig)
	}
	for i, p := range x.trusted {
		if !p.IsValid() {
			return nil, fmt.Errorf("%w: invalid trusted proxy prefix at index %d", rategen.ErrInvalidConfig, i)
		}
		u, err := unmapPrefix(p)
		if err != nil {
			return nil, err
		}
		x.trusted[i] = u
	}
	return x, nil
}

// ParsePrefixes parses IP addresses and CIDR prefixes, for building
// IPConfig.TrustedProxies from configuration. A bare address is treated as
// a single-host prefix.
func ParsePrefixes(values ...string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if strings.Contains(v, "/") {
			p, err := netip.ParsePrefix(v)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", rategen.ErrInvalidConfig, err)
			}
			u, err := unmapPrefix(p)
			if err != nil {
				return nil, err
			}
			out = append(out, u)
			continue
		}
		a, err := netip.ParseAddr(v)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", rategen.ErrInvalidConfig, err)
		}
		a = a.Unmap()
		out = append(out, netip.PrefixFrom(a, a.BitLen()))
	}
	return out, nil
}

// unmapPrefix masks p and rewrites IPv4-mapped IPv6 prefixes
// (::ffff:10.0.0.0/104) as IPv4 prefixes (10.0.0.0/8), because client and
// proxy addresses are always compared in unmapped form.
func unmapPrefix(p netip.Prefix) (netip.Prefix, error) {
	p = p.Masked()
	if !p.Addr().Is4In6() {
		return p, nil
	}
	if p.Bits() < 96 {
		return netip.Prefix{}, fmt.Errorf("%w: prefix %v mixes IPv4-mapped and other IPv6 addresses", rategen.ErrInvalidConfig, p)
	}
	return netip.PrefixFrom(p.Addr().Unmap(), p.Bits()-96).Masked(), nil
}

func (x *IPExtractor) isTrusted(a netip.Addr) bool {
	for _, p := range x.trusted {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// ClientIP returns the client address for r. Forwarding headers are
// consulted only when the direct peer is a trusted proxy; the header is then
// read right to left, skipping trusted proxies, and the first untrusted
// address is the client. Entries left of it, which the client may have
// forged, are never read.
func (x *IPExtractor) ClientIP(r *http.Request) (netip.Addr, error) {
	peer, err := remoteAddr(r.RemoteAddr)
	if err != nil {
		return netip.Addr{}, err
	}
	if len(x.trusted) == 0 || !x.isTrusted(peer) {
		return peer, nil
	}
	values := r.Header.Values(x.header)
	if len(values) == 0 {
		return peer, nil
	}
	if x.kind == headerSingle {
		// The trusted proxy is responsible for overwriting this header;
		// several values mean it did not.
		if len(values) != 1 {
			return netip.Addr{}, fmt.Errorf("%w: multiple %s headers", ErrInvalidClientIP, x.header)
		}
		return parseAddr(strings.TrimSpace(values[0]))
	}
	// Walk entries from the rightmost header line to the leftmost.
	last := peer
	hops := 0
	for i := len(values) - 1; i >= 0; i-- {
		v := values[i]
		if v == "" {
			// Empty header lines count as entries too, keeping the
			// work bounded by MaxHops.
			if hops++; hops > x.maxHops {
				return netip.Addr{}, errTooManyHops
			}
			continue
		}
		for v != "" {
			var item string
			if j := strings.LastIndexByte(v, ','); j >= 0 {
				item, v = v[j+1:], v[:j]
			} else {
				item, v = v, ""
			}
			// Every entry, empty or not, counts towards MaxHops, so a
			// header of a million commas costs no more than MaxHops
			// entries.
			if hops++; hops > x.maxHops {
				return netip.Addr{}, errTooManyHops
			}
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			if x.kind == headerForwarded {
				item = forwardedFor(item)
			}
			a, err := parseAddr(item)
			if err != nil {
				return netip.Addr{}, err
			}
			if !x.isTrusted(a) {
				return a, nil
			}
			last = a
		}
	}
	// Every hop was a trusted proxy: the request originated inside the
	// trusted network. Use the leftmost address seen.
	return last, nil
}

// Key returns a KeyFunc yielding the client's (aggregated) network, such as
// "203.0.113.7" or "2001:db8:1:2::/64".
func (x *IPExtractor) Key() KeyFunc {
	return func(r *http.Request) (string, error) {
		a, err := x.ClientIP(r)
		if err != nil {
			return "", err
		}
		return x.network(a), nil
	}
}

func (x *IPExtractor) network(a netip.Addr) string {
	bits := x.v6
	if a.Is4() {
		bits = x.v4
	}
	if bits == a.BitLen() {
		return a.String()
	}
	p, _ := a.Prefix(bits)
	return p.String()
}

// errTooManyHops reports a forwarding chain longer than MaxHops in which
// every examined hop was a trusted proxy. Falling back to the last proxy's
// address would put every client behind it under one shared key, so the
// request is refused instead.
var errTooManyHops = fmt.Errorf("%w: more than MaxHops trusted proxies", ErrInvalidClientIP)

var directIP = &IPExtractor{v4: 32, v6: 64, maxHops: 16}

// ByIP returns a KeyFunc keyed by the connection's RemoteAddr, ignoring all
// forwarding headers, with IPv6 aggregated to /64. Use an IPExtractor with
// TrustedProxies when running behind a reverse proxy.
func ByIP() KeyFunc { return directIP.Key() }

func remoteAddr(s string) (netip.Addr, error) {
	if ap, err := netip.ParseAddrPort(s); err == nil {
		return ap.Addr().Unmap().WithZone(""), nil
	}
	// RemoteAddr without a port (some test servers and transports).
	host := s
	if h, _, err := net.SplitHostPort(s); err == nil {
		host = h
	}
	return parseAddr(host)
}

func parseAddr(s string) (netip.Addr, error) {
	if len(s) > 64 {
		return netip.Addr{}, fmt.Errorf("%w: address too long", ErrInvalidClientIP)
	}
	// Strip brackets and an optional port: "[2001:db8::1]:443", "1.2.3.4:80".
	if strings.HasPrefix(s, "[") {
		if end := strings.IndexByte(s, ']'); end > 0 {
			s = s[1:end]
		}
	} else if strings.Count(s, ":") == 1 {
		s = s[:strings.IndexByte(s, ':')]
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Addr{}, ErrInvalidClientIP
	}
	return a.Unmap().WithZone(""), nil
}

// forwardedFor extracts the for= parameter of one RFC 7239 element.
func forwardedFor(elem string) string {
	for pair := range strings.SplitSeq(elem, ";") {
		k, v, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if ok && strings.EqualFold(k, "for") {
			return strings.Trim(v, `"`)
		}
	}
	return ""
}
