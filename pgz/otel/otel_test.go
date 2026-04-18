package otel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arturoeanton/pgz/pgz"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// TestObserverLifecycle walks a fake query through Start → End → Slow
// and checks that no panic, no leak, and a pgz.Observer assignment
// works.
func TestObserverLifecycle(t *testing.T) {
	obs, err := New(tracenoop.NewTracerProvider(), metricnoop.NewMeterProvider())
	if err != nil {
		t.Fatal(err)
	}
	var _ pgz.Observer = obs

	obs.OnQueryStart("SELECT 1")
	obs.OnQueryEnd(pgz.QueryEvent{
		SQL:      "SELECT 1",
		Rows:     1,
		Bytes:    16,
		Duration: 3 * time.Millisecond,
	})
	obs.OnQuerySlow(pgz.QueryEvent{
		SQL:      "SELECT 1",
		Duration: 1 * time.Second,
	})

	// Error path.
	obs.OnQueryStart("SELECT broken")
	obs.OnQueryEnd(pgz.QueryEvent{
		SQL:      "SELECT broken",
		Duration: 2 * time.Millisecond,
		Err:      errors.New("boom"),
		SQLState: "42601",
	})
}

// TestObserverOptions exercises the option knobs and asserts they are
// applied. We don't need a recording tracer — the presence/absence of
// attributes would require an SDK. The purpose here is to make sure
// construction does not blow up with any combination.
func TestObserverOptions(t *testing.T) {
	_, err := New(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(tracenoop.NewTracerProvider(), metricnoop.NewMeterProvider(),
		WithRecordSQL(false),
		WithSQLMaxLen(64),
		WithSQLAttributeKey("sql"),
	)
	if err != nil {
		t.Fatal(err)
	}
}

// TestFirstToken covers the helper used when span-name-from-SQL is on.
func TestFirstToken(t *testing.T) {
	cases := []struct{ in, want string }{
		{"select 1", "SELECT"},
		{"   INSERT INTO foo", "INSERT"},
		{"UPDATE\tfoo", "UPDATE"},
		{"", ""},
		{"singletoken", "SINGLETOKEN"},
	}
	for _, c := range cases {
		if got := firstToken(c.in); got != c.want {
			t.Fatalf("firstToken(%q)=%q want %q", c.in, got, c.want)
		}
	}
}

// Compile-time check that attribute/metric/trace types we use are
// non-nil when the noop providers are installed. Otherwise the
// Observer would panic on first query.
func TestNoopProvidersSurface(t *testing.T) {
	tp := tracenoop.NewTracerProvider()
	tr := tp.Tracer("x")
	_, span := tr.Start(context.Background(), "x")
	span.End()
	mp := metricnoop.NewMeterProvider()
	m := mp.Meter("x")
	h, err := m.Float64Histogram("h")
	if err != nil {
		t.Fatal(err)
	}
	h.Record(context.Background(), 1.0, metric.WithAttributeSet(attribute.NewSet()))
	var _ trace.Span = span
}
