package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/arturoeanton/pgz/pgz"
)

func TestBatchExecHappy(t *testing.T) {
	c := openClient(t)
	defer c.Close()

	_, _ = c.Exec(context.Background(), "DROP TABLE IF EXISTS pgz_batch_u")
	if _, err := c.Exec(context.Background(),
		"CREATE UNLOGGED TABLE pgz_batch_u (id int4, name text)"); err != nil {
		t.Fatal(err)
	}

	b := pgz.NewBatch()
	b.Queue("INSERT INTO pgz_batch_u VALUES ($1, $2)", 1, "alice")
	b.Queue("INSERT INTO pgz_batch_u VALUES ($1, $2)", 2, "bob")
	b.Queue("UPDATE pgz_batch_u SET name = $1 WHERE id = $2", "bobby", 2)

	br := c.SendBatch(context.Background(), b)
	for i := 0; i < b.Len(); i++ {
		res, err := br.Exec()
		if err != nil {
			t.Fatalf("item %d: %v", i, err)
		}
		if res.RowsAffected != 1 {
			t.Fatalf("item %d RowsAffected=%d", i, res.RowsAffected)
		}
	}
	if err := br.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify post-batch state.
	var name string
	if err := c.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	buf, err := c.QueryJSON(context.Background(),
		"SELECT name FROM pgz_batch_u WHERE id = 2")
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != `[{"name":"bobby"}]` {
		t.Fatalf("update lost: %s", buf)
	}
	_, _ = c.Exec(context.Background(), "DROP TABLE pgz_batch_u")
	_ = name
}

func TestBatchErrorAbortsSubsequent(t *testing.T) {
	c := openClient(t)
	defer c.Close()

	_, _ = c.Exec(context.Background(), "DROP TABLE IF EXISTS pgz_batch_ab")
	if _, err := c.Exec(context.Background(),
		"CREATE UNLOGGED TABLE pgz_batch_ab (id int4 PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	b := pgz.NewBatch()
	b.Queue("INSERT INTO pgz_batch_ab VALUES ($1)", 1)
	b.Queue("INSERT INTO pgz_batch_ab VALUES ($1)", 1) // PK violation
	b.Queue("INSERT INTO pgz_batch_ab VALUES ($1)", 2) // should be aborted

	br := c.SendBatch(context.Background(), b)
	defer br.Close()

	// First Exec succeeds.
	if _, err := br.Exec(); err != nil {
		t.Fatalf("item 0: %v", err)
	}
	// Second fails with unique violation.
	_, err := br.Exec()
	if err == nil {
		t.Fatal("expected PK violation on item 1")
	}
	var pgErr *pgz.PGError
	if !errors.As(err, &pgErr) || !pgErr.IsUniqueViolation() {
		t.Fatalf("want unique violation, got %v", err)
	}
	// Third returns aborted.
	_, err = br.Exec()
	if !errors.Is(err, pgz.ErrBatchAborted) {
		t.Fatalf("item 2 should be aborted, got %v", err)
	}

	// After Close, conn is usable again (and only row 1 was inserted
	// because the implicit tx rolled back... actually PG commits each
	// Exec under simple-query pipeline mode per-statement, so row 1
	// remains).
	_ = br.Close()
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("conn unusable: %v", err)
	}
	_, _ = c.Exec(context.Background(), "DROP TABLE pgz_batch_ab")
}

func TestBatchEmpty(t *testing.T) {
	c := openClient(t)
	defer c.Close()
	b := pgz.NewBatch()
	br := c.SendBatch(context.Background(), b)
	_ = br.Close()
	if err := c.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}
