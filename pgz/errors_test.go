package pgz

import (
	"errors"
	"fmt"
	"testing"
)

// TestPGErrorAlias verifies that errors.As unwraps into *PGError and the
// predicate helpers reflect SQLSTATE correctly.
func TestPGErrorAlias(t *testing.T) {
	cases := []struct {
		code string
		want func(*PGError) bool
		name string
	}{
		{"23505", (*PGError).IsUniqueViolation, "unique"},
		{"23503", (*PGError).IsForeignKeyViolation, "fk"},
		{"23514", (*PGError).IsCheckViolation, "check"},
		{"23502", (*PGError).IsNotNullViolation, "notnull"},
		{"23P01", (*PGError).IsExclusionViolation, "exclusion"},
		{"40001", (*PGError).IsSerializationFailure, "serialization"},
		{"40P01", (*PGError).IsDeadlock, "deadlock"},
		{"57014", (*PGError).IsQueryCanceled, "cancel"},
		{"57P01", (*PGError).IsAdminShutdown, "shutdown"},
		{"26000", (*PGError).IsInvalidSQLStatementName, "stale-stmt"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var err error = &PGError{Code: c.code, Severity: "ERROR", Message: "x"}
			// errors.As must succeed via the alias — compile-time guarantee.
			var pe *PGError
			if !errors.As(err, &pe) {
				t.Fatalf("errors.As failed for %s", c.code)
			}
			if !c.want(pe) {
				t.Fatalf("predicate returned false for code %s", c.code)
			}
			if pe.SQLState() != c.code {
				t.Fatalf("SQLState=%q want %q", pe.SQLState(), c.code)
			}
		})
	}
}

// TestPGErrorClass confirms the 2-char class split and the
// IsIntegrityViolation umbrella.
func TestPGErrorClass(t *testing.T) {
	e := &PGError{Code: "23505"}
	if e.SQLStateClass() != "23" {
		t.Fatalf("class=%q", e.SQLStateClass())
	}
	if !e.IsIntegrityViolation() {
		t.Fatal("23505 should be integrity violation")
	}
	e2 := &PGError{Code: "40001"}
	if e2.IsIntegrityViolation() {
		t.Fatal("40001 is not an integrity violation")
	}
	// Short codes don't panic.
	e3 := &PGError{Code: ""}
	if e3.SQLStateClass() != "" {
		t.Fatal("empty class expected for empty code")
	}
	if e3.IsIntegrityViolation() {
		t.Fatal("empty code should not be integrity violation")
	}
}

// TestPGErrorWrapping checks interop with fmt.Errorf %w and errors.Is.
func TestPGErrorWrapping(t *testing.T) {
	orig := &PGError{Code: "23505", Message: "duplicate key"}
	wrapped := fmt.Errorf("insert user: %w", orig)
	var pe *PGError
	if !errors.As(wrapped, &pe) {
		t.Fatal("errors.As through fmt.Errorf %w failed")
	}
	if !pe.IsUniqueViolation() {
		t.Fatal("wrapped error lost its identity")
	}
}
