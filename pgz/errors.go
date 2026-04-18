package pgz

import (
	"errors"
	"fmt"

	"github.com/arturoeanton/pgz/internal/pgerr"
)

// PGError is the structured error returned by the server as an
// ErrorResponse. Use errors.As to extract it from the error returned by
// any pgz call:
//
//	var pgErr *pgz.PGError
//	if errors.As(err, &pgErr) && pgErr.IsUniqueViolation() { ... }
//
// The Fields map holds every raw field the server sent, keyed by the
// single-byte field code from the wire format ('C'=Code, 'M'=Message,
// 'D'=Detail, 'H'=Hint, 'P'=Position, 's'=Schema, 't'=Table,
// 'n'=Constraint, etc.). The convenience accessors (SQLState, helper
// predicates) cover the common cases without touching Fields.
type PGError = pgerr.Error

// ErrResponseTooLarge is the sentinel returned (wrapped in
// *ResponseTooLargeError) when MaxResponseBytes or MaxResponseRows is hit.
// Use errors.Is to detect it.
var ErrResponseTooLarge = errors.New("pgz: response exceeded configured cap")

// ResponseTooLargeError is the typed error returned when a query is aborted
// because Config.MaxResponseBytes or Config.MaxResponseRows was crossed.
//
// Committed reports whether any bytes had already been flushed to the user's
// downstream io.Writer when the cap tripped. If true, the partial JSON
// downstream is malformed (truncated mid-array / mid-NDJSON line) and the
// caller is responsible for terminating its own output channel — the same
// contract that applies to mid-stream Citus worker errors.
type ResponseTooLargeError struct {
	Limit     string // "bytes" or "rows"
	LimitVal  int64
	Observed  int64
	Committed bool
}

func (e *ResponseTooLargeError) Error() string {
	return fmt.Sprintf("pgz: response exceeded %s cap (limit=%d observed=%d committed=%v)",
		e.Limit, e.LimitVal, e.Observed, e.Committed)
}

func (e *ResponseTooLargeError) Unwrap() error { return ErrResponseTooLarge }
