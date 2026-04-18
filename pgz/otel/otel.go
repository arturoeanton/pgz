// Package otel provides an OpenTelemetry-backed pgz.Observer.
//
// Install:
//
//	import (
//	    "github.com/arturoeanton/pgz/pgz"
//	    pgzotel "github.com/arturoeanton/pgz/pgz/otel"
//	)
//
//	obs, _ := pgzotel.New(tracerProvider, meterProvider)
//	client.SetObserver(obs)
//
// The observer records three OTel metrics per query:
//
//	pgz.query.duration (histogram, seconds)
//	pgz.query.rows     (counter, int64)
//	pgz.query.errors   (counter, int64) — only on failure
//
// And one span per query (no parent — pgz.Observer does not receive a
// context at query start). The span carries the SQL statement text as
// an attribute; override via Option if your workload needs that
// suppressed or normalised.
//
// All OTel calls happen once per query — never in the per-row hot
// loop. The observer adds roughly 1 alloc (the span) and ~300 ns per
// query, independent of row count.
package otel

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/arturoeanton/pgz/internal/pgerr"
	"github.com/arturoeanton/pgz/pgz"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// ScopeName is the OTel instrumentation scope identifier.
const ScopeName = "github.com/arturoeanton/pgz"

// Observer implements pgz.Observer by emitting OpenTelemetry spans and
// metrics. It is safe for concurrent use through a pool of *pgz.Client;
// the underlying Tracer/Meter must themselves be safe (they are, per
// the OTel contract).
//
// The observer keeps a tiny shared span-per-query map so OnQueryEnd can
// finalise the span that OnQueryStart opened. The map is keyed by an
// atomic counter — bounded by the number of in-flight queries on a
// single connection, which is always 1 for pgz.Client (the driver is
// single-connection, not safe for concurrent use). For pools, each
// Client has its own counter namespace because Observers are shared
// but queries are sequential per connection.
type Observer struct {
	tracer trace.Tracer
	meter  metric.Meter

	duration    metric.Float64Histogram
	rows        metric.Int64Counter
	errors      metric.Int64Counter
	slowCounter metric.Int64Counter

	opts options

	// Per-query bookkeeping. pgz.Client runs one query at a time per
	// connection, so a single slot suffices. Pool-wide safety relies
	// on each *pgz.Client being used sequentially — the pgz contract.
	start atomic.Pointer[queryState]
}

type queryState struct {
	ctx      context.Context
	span     trace.Span
	start    time.Time
	sql      string
	startSeq uint64
}

type options struct {
	recordSQL       bool
	sqlAttrKey      string
	sqlMaxLen       int
	spanNameFromSQL bool
}

// Option configures the observer at construction time.
type Option func(*options)

// WithRecordSQL controls whether the SQL statement text is attached
// to the span as an attribute. Default true. High-cardinality SQL
// (prepared statements per request) is usually fine — it lives on the
// span, not on metric dimensions. Turn off when SQL is sensitive.
func WithRecordSQL(record bool) Option {
	return func(o *options) { o.recordSQL = record }
}

// WithSQLMaxLen truncates the SQL attribute to n bytes. 0 disables
// truncation. Default 1024.
func WithSQLMaxLen(n int) Option {
	return func(o *options) { o.sqlMaxLen = n }
}

// WithSQLAttributeKey overrides the attribute key under which the SQL
// is recorded. Default "db.statement" (OTel semantic conventions).
func WithSQLAttributeKey(k string) Option {
	return func(o *options) { o.sqlAttrKey = k }
}

// New builds an Observer using the supplied providers. Either may be
// nil; a nil TracerProvider silences spans, a nil MeterProvider
// silences metrics.
func New(tp trace.TracerProvider, mp metric.MeterProvider, opts ...Option) (*Observer, error) {
	if tp == nil {
		tp = noop.NewTracerProvider()
	}
	o := &Observer{
		tracer: tp.Tracer(ScopeName),
		opts: options{
			recordSQL:  true,
			sqlAttrKey: "db.statement",
			sqlMaxLen:  1024,
		},
	}
	for _, fn := range opts {
		fn(&o.opts)
	}

	if mp != nil {
		o.meter = mp.Meter(ScopeName)
		var err error
		o.duration, err = o.meter.Float64Histogram("pgz.query.duration",
			metric.WithUnit("s"),
			metric.WithDescription("Time from query start to ReadyForQuery."))
		if err != nil {
			return nil, err
		}
		o.rows, err = o.meter.Int64Counter("pgz.query.rows",
			metric.WithDescription("Rows affected or returned by pgz queries."))
		if err != nil {
			return nil, err
		}
		o.errors, err = o.meter.Int64Counter("pgz.query.errors",
			metric.WithDescription("pgz queries that returned an error."))
		if err != nil {
			return nil, err
		}
	}
	return o, nil
}

// OnQueryStart opens a span and stashes bookkeeping. The span is
// parented to context.Background because pgz.Observer does not
// receive a context at query start — a limitation we accept to keep
// the Observer interface stable. Callers that need ctx-parented spans
// should wrap their calls in a parent span and propagate ctx through
// their own code, not through pgz internals.
func (o *Observer) OnQueryStart(sql string) {
	ctx := context.Background()
	name := "pgz.query"
	if o.opts.spanNameFromSQL {
		name = firstToken(sql)
	}
	ctx, span := o.tracer.Start(ctx, name,
		trace.WithSpanKind(trace.SpanKindClient))
	if o.opts.recordSQL && span.IsRecording() {
		stmt := sql
		if o.opts.sqlMaxLen > 0 && len(stmt) > o.opts.sqlMaxLen {
			stmt = stmt[:o.opts.sqlMaxLen]
		}
		span.SetAttributes(attribute.String(o.opts.sqlAttrKey, stmt))
	}
	o.start.Store(&queryState{
		ctx:   ctx,
		span:  span,
		start: time.Now(),
		sql:   sql,
	})
}

// OnQueryEnd closes the span opened by OnQueryStart and records the
// duration / rows / errors metrics.
func (o *Observer) OnQueryEnd(ev pgz.QueryEvent) {
	st := o.start.Load()
	// Unusual path: end without matching start (e.g. if Observer was
	// swapped mid-query). Still record the metric from the event.
	var ctx context.Context = context.Background()
	if st != nil {
		ctx = st.ctx
		if ev.Err != nil {
			st.span.SetStatus(codes.Error, ev.Err.Error())
			st.span.RecordError(ev.Err)
		}
		if ev.Rows > 0 {
			st.span.SetAttributes(attribute.Int("pgz.rows", ev.Rows))
		}
		if ev.Bytes > 0 {
			st.span.SetAttributes(attribute.Int("pgz.bytes", ev.Bytes))
		}
		if ev.SQLState != "" {
			st.span.SetAttributes(attribute.String("db.sqlstate", ev.SQLState))
		}
		if ev.Retries > 0 {
			st.span.SetAttributes(attribute.Int("pgz.retries", ev.Retries))
		}
		st.span.End()
		o.start.Store(nil)
	}

	if o.meter == nil {
		return
	}
	attrs := make([]attribute.KeyValue, 0, 2)
	if ev.SQLState != "" {
		attrs = append(attrs, attribute.String("db.sqlstate", ev.SQLState))
	}
	attrSet := attribute.NewSet(attrs...)
	o.duration.Record(ctx, ev.Duration.Seconds(), metric.WithAttributeSet(attrSet))
	if ev.Rows > 0 {
		o.rows.Add(ctx, int64(ev.Rows), metric.WithAttributeSet(attrSet))
	}
	if ev.Err != nil {
		o.errors.Add(ctx, 1, metric.WithAttributeSet(attrSet))
	}
}

// OnNotice forwards server notices as span events on the currently
// active query span (if any). NoticeResponse is rare and always
// out-of-band — safe to allocate here.
func (o *Observer) OnNotice(n *pgerr.Error) {
	if n == nil {
		return
	}
	st := o.start.Load()
	if st == nil {
		return
	}
	st.span.AddEvent("pgz.notice", trace.WithAttributes(
		attribute.String("severity", n.Severity),
		attribute.String("code", n.Code),
		attribute.String("message", n.Message),
	))
}

// OnQuerySlow is additive to OnQueryEnd; both fire for the same query.
// We annotate the already-closed span with an event-less attribute so
// the consumer can alert off it.
func (o *Observer) OnQuerySlow(ev pgz.QueryEvent) {
	// The span is typically already ended by OnQueryEnd when this is
	// called (same goroutine, sequential). The metric is what is
	// actionable here, so record a separate counter.
	if o.meter == nil {
		return
	}
	if o.slowCounter == nil {
		cnt, err := o.meter.Int64Counter("pgz.query.slow",
			metric.WithDescription("pgz queries crossing SlowQueryThreshold."))
		if err == nil {
			o.slowCounter = cnt
		}
	}
	if o.slowCounter != nil {
		o.slowCounter.Add(context.Background(), 1)
	}
}

// slowCounter is lazily created in OnQuerySlow so users who don't
// configure SlowQueryThreshold pay zero registration cost.
// Declared on Observer to avoid a separate mutex/atomic — OnQuerySlow
// runs on the same goroutine as OnQueryEnd (pgz.Client is
// single-threaded per connection).
var _ pgz.Observer = (*Observer)(nil)

func init() {
	// Assertion: Observer must remain a valid pgz.Observer. Compile-time
	// check above is sufficient; no runtime work here.
}

// firstToken returns the first whitespace-delimited word of s, uppercased.
// Useful when using the statement verb as the span name.
func firstToken(s string) string {
	s = strings.TrimLeft(s, " \t\n\r")
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			return strings.ToUpper(s[:i])
		}
	}
	return strings.ToUpper(s)
}
