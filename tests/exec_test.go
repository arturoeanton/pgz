package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestExecInsertUpdateDelete(t *testing.T) {
	c := openClient(t)
	defer c.Close()
	ctx := context.Background()

	// Setup: create a temp table (drop first for idempotency).
	c.Exec(ctx, "DROP TABLE IF EXISTS exec_test")
	_, err := c.Exec(ctx, "CREATE TABLE exec_test (id int4 PRIMARY KEY, name text)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.Exec(ctx, "DROP TABLE IF EXISTS exec_test")
	})

	// INSERT
	res, err := c.Exec(ctx, "INSERT INTO exec_test (id, name) VALUES ($1, $2), ($3, $4)", 1, "alice", 2, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsAffected != 2 {
		t.Fatalf("INSERT: want 2 rows affected, got %d (tag=%q)", res.RowsAffected, res.Tag)
	}

	// UPDATE
	res, err = c.Exec(ctx, "UPDATE exec_test SET name = 'ALICE' WHERE id = $1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("UPDATE: want 1 row affected, got %d (tag=%q)", res.RowsAffected, res.Tag)
	}

	// DELETE
	res, err = c.Exec(ctx, "DELETE FROM exec_test WHERE id = $1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsAffected != 1 {
		t.Fatalf("DELETE: want 1 row affected, got %d (tag=%q)", res.RowsAffected, res.Tag)
	}

	// Verify via SELECT
	got, err := c.QueryJSON(ctx, "SELECT id, name FROM exec_test ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"id":1,"name":"ALICE"}]`
	if string(bytes.TrimSpace(got)) != want {
		t.Fatalf("SELECT after DML:\n got: %s\nwant: %s", got, want)
	}
}

func TestExecReturning(t *testing.T) {
	c := openClient(t)
	defer c.Close()
	ctx := context.Background()

	// Setup
	c.Exec(ctx, "DROP TABLE IF EXISTS exec_ret_test")
	_, err := c.Exec(ctx, "CREATE TABLE exec_ret_test (id serial PRIMARY KEY, name text)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.Exec(ctx, "DROP TABLE IF EXISTS exec_ret_test")
	})

	// INSERT ... RETURNING via streaming
	var buf bytes.Buffer
	err = c.ExecReturning(ctx, &buf, "INSERT INTO exec_ret_test (name) VALUES ($1), ($2) RETURNING id, name", "alice", "bob")
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("ExecReturning: want 2 NDJSON lines, got %d: %s", len(lines), buf.String())
	}
	for i, line := range lines {
		var row map[string]any
		if err := json.Unmarshal(line, &row); err != nil {
			t.Fatalf("line %d: invalid JSON: %v\n%s", i, err, line)
		}
		if _, ok := row["id"]; !ok {
			t.Fatalf("line %d: missing 'id' field", i)
		}
		if _, ok := row["name"]; !ok {
			t.Fatalf("line %d: missing 'name' field", i)
		}
	}

	// INSERT ... RETURNING via buffered
	got, err := c.ExecReturningJSON(ctx, "INSERT INTO exec_ret_test (name) VALUES ($1) RETURNING id, name", "charlie")
	if err != nil {
		t.Fatal(err)
	}
	var row map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(got), &row); err != nil {
		t.Fatalf("ExecReturningJSON: invalid JSON: %v\n%s", err, got)
	}
	if row["name"] != "charlie" {
		t.Fatalf("ExecReturningJSON: want name=charlie, got %v", row["name"])
	}

	// UPDATE ... RETURNING
	buf.Reset()
	err = c.ExecReturning(ctx, &buf, "UPDATE exec_ret_test SET name = upper(name) WHERE name = $1 RETURNING id, name", "alice")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &row); err != nil {
		t.Fatalf("UPDATE RETURNING: invalid JSON: %v\n%s", err, buf.String())
	}
	if row["name"] != "ALICE" {
		t.Fatalf("UPDATE RETURNING: want name=ALICE, got %v", row["name"])
	}

	// DELETE ... RETURNING
	buf.Reset()
	err = c.ExecReturning(ctx, &buf, "DELETE FROM exec_ret_test WHERE name = $1 RETURNING id, name", "bob")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &row); err != nil {
		t.Fatalf("DELETE RETURNING: invalid JSON: %v\n%s", err, buf.String())
	}
	if row["name"] != "bob" {
		t.Fatalf("DELETE RETURNING: want name=bob, got %v", row["name"])
	}
}

func TestExecCall(t *testing.T) {
	c := openClient(t)
	defer c.Close()
	ctx := context.Background()

	// Create a simple procedure.
	_, err := c.Exec(ctx, `CREATE OR REPLACE PROCEDURE test_noop() LANGUAGE plpgsql AS $$ BEGIN NULL; END; $$`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.Exec(ctx, "DROP PROCEDURE IF EXISTS test_noop()")
	})

	res, err := c.Exec(ctx, "CALL test_noop()")
	if err != nil {
		t.Fatal(err)
	}
	if res.RowsAffected != 0 {
		t.Fatalf("CALL: want 0 rows affected, got %d (tag=%q)", res.RowsAffected, res.Tag)
	}
}

func TestExecSelectRejected(t *testing.T) {
	c := openClient(t)
	defer c.Close()
	ctx := context.Background()

	_, err := c.QueryJSON(ctx, "INSERT INTO nonexistent VALUES (1)")
	if err == nil {
		t.Fatal("QueryJSON should reject INSERT")
	}
}
