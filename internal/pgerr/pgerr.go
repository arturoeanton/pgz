// Package pgerr models PostgreSQL ErrorResponse / NoticeResponse.
package pgerr

import (
	"bytes"
	"fmt"
)

// Error is a structured PostgreSQL error. We only surface the most useful
// fields; the rest are kept in Fields for callers that want them.
type Error struct {
	Severity string
	Code     string
	Message  string
	Detail   string
	Hint     string
	Where    string
	Fields   map[byte]string
}

func (e *Error) Error() string {
	return fmt.Sprintf("postgres: %s %s: %s", e.Severity, e.Code, e.Message)
}

// SQLState returns the 5-character SQLSTATE code ('' if unknown).
// Convenience accessor that does not allocate.
func (e *Error) SQLState() string { return e.Code }

// SQLStateClass returns the first two characters of SQLSTATE, which
// identify the error class (e.g. "23" for integrity violations, "40"
// for transaction rollbacks). Empty string if Code is not a full
// 5-character SQLSTATE.
func (e *Error) SQLStateClass() string {
	if len(e.Code) < 2 {
		return ""
	}
	return e.Code[:2]
}

// --- Integrity constraint violations (class 23) ---

// IsUniqueViolation reports SQLSTATE 23505.
func (e *Error) IsUniqueViolation() bool { return e.Code == "23505" }

// IsForeignKeyViolation reports SQLSTATE 23503.
func (e *Error) IsForeignKeyViolation() bool { return e.Code == "23503" }

// IsCheckViolation reports SQLSTATE 23514.
func (e *Error) IsCheckViolation() bool { return e.Code == "23514" }

// IsNotNullViolation reports SQLSTATE 23502.
func (e *Error) IsNotNullViolation() bool { return e.Code == "23502" }

// IsExclusionViolation reports SQLSTATE 23P01.
func (e *Error) IsExclusionViolation() bool { return e.Code == "23P01" }

// IsIntegrityViolation reports any class-23 error.
func (e *Error) IsIntegrityViolation() bool { return e.SQLStateClass() == "23" }

// --- Concurrency (class 40) ---

// IsSerializationFailure reports SQLSTATE 40001 — conflict under
// SERIALIZABLE / REPEATABLE READ. Safe to retry the whole transaction.
func (e *Error) IsSerializationFailure() bool { return e.Code == "40001" }

// IsDeadlock reports SQLSTATE 40P01.
func (e *Error) IsDeadlock() bool { return e.Code == "40P01" }

// --- Operator intervention / cancellation ---

// IsQueryCanceled reports SQLSTATE 57014 (statement_timeout or
// server-side CancelRequest). Expected when ctx.Done fires.
func (e *Error) IsQueryCanceled() bool { return e.Code == "57014" }

// IsAdminShutdown reports SQLSTATE 57P01 (server shutting down).
func (e *Error) IsAdminShutdown() bool { return e.Code == "57P01" }

// --- PgBouncer / prepared-statement lifecycle ---

// IsInvalidSQLStatementName reports SQLSTATE 26000 — raised when a
// prepared statement name is unknown to the server. In PgBouncer
// transaction pooling this fires when a pooled connection rotates and
// the client's stmt-cache goes stale. pgz recovers transparently.
func (e *Error) IsInvalidSQLStatementName() bool { return e.Code == "26000" }

// Parse decodes the body of an ErrorResponse / NoticeResponse message
// (everything after the type byte and length prefix).
func Parse(body []byte) *Error {
	e := &Error{Fields: make(map[byte]string, 8)}
	for len(body) > 0 {
		code := body[0]
		if code == 0 {
			break
		}
		body = body[1:]
		i := bytes.IndexByte(body, 0)
		if i < 0 {
			break
		}
		val := string(body[:i])
		body = body[i+1:]
		e.Fields[code] = val
		switch code {
		case 'S':
			e.Severity = val
		case 'C':
			e.Code = val
		case 'M':
			e.Message = val
		case 'D':
			e.Detail = val
		case 'H':
			e.Hint = val
		case 'W':
			e.Where = val
		}
	}
	return e
}
