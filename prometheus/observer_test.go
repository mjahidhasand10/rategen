package prometheus_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/mjahidhasand10/rategen"
	"github.com/mjahidhasand10/rategen/memory"
	rgprom "github.com/mjahidhasand10/rategen/prometheus"
)

func TestObserverMetrics(t *testing.T) {
	reg := prom.NewRegistry()
	obs, err := rgprom.NewObserver(reg, rgprom.Options{})
	if err != nil {
		t.Fatal(err)
	}
	s, _ := memory.New(memory.Config{Observer: obs, MaxKeys: 1, Shards: 1, OnFull: memory.RejectNew})
	l, _ := s.FixedWindow("login", rategen.PerMinute(1))
	ctx := context.Background()
	l.AllowN(ctx, "user-123", 1)
	l.AllowN(ctx, "user-123", 1)
	l.AllowN(ctx, "user-456", 1) // store full
	l.AllowN(ctx, "user-123", 5) // exceeds capacity

	want := `
# HELP rategen_decisions_total Rate-limit decisions by policy and result.
# TYPE rategen_decisions_total counter
rategen_decisions_total{algorithm="fixed_window",backend="memory",degraded="false",policy="login",result="allowed"} 1
rategen_decisions_total{algorithm="fixed_window",backend="memory",degraded="false",policy="login",result="rejected"} 3
# HELP rategen_errors_total Errors that accompanied rate-limit decisions.
# TYPE rategen_errors_total counter
rategen_errors_total{algorithm="fixed_window",backend="memory",kind="invalid_request",policy="login"} 1
rategen_errors_total{algorithm="fixed_window",backend="memory",kind="store_full",policy="login"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "rategen_decisions_total", "rategen_errors_total"); err != nil {
		t.Fatal(err)
	}
	if n := testutil.CollectAndCount(reg, "rategen_decision_duration_seconds"); n != 1 {
		t.Fatalf("latency series = %d, want 1", n)
	}
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(fmt.Sprint(mfs), "user-") {
		t.Fatal("key leaked into metrics")
	}
	if _, err := rgprom.NewObserver(reg, rgprom.Options{}); err == nil {
		t.Fatal("duplicate registration not reported")
	}
}
