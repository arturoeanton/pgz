// Integration + correctness tests for CopyFrom / CopyFromBinary.
// Skipped without PGZ_TEST_DSN.
package tests

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/arturoeanton/pgz/pgz"
)

// prepCopyTable (re)creates a disposable target table for each test.
func prepCopyTable(t testing.TB, c *pgz.Client, table, cols string) {
	t.Helper()
	_, err := c.Exec(context.Background(),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Exec(context.Background(),
		fmt.Sprintf("CREATE TABLE %s (%s)", table, cols))
	if err != nil {
		t.Fatal(err)
	}
}

func TestCopyFromCSV(t *testing.T) {
	c := openClient(t)
	defer c.Close()
	prepCopyTable(t, c, "pgz_copy_csv", "id int4, name text, score float8")

	csv := "1,alice,1.5\n2,bob,2.5\n3,charlie,3.5\n"
	rows, err := c.CopyFrom(context.Background(),
		"COPY pgz_copy_csv FROM STDIN (FORMAT csv)",
		strings.NewReader(csv))
	if err != nil {
		t.Fatal(err)
	}
	if rows != 3 {
		t.Fatalf("rows=%d want 3", rows)
	}

	buf, err := c.QueryJSON(context.Background(),
		"SELECT count(*)::int AS n, sum(id)::int AS s FROM pgz_copy_csv")
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"n":3,"s":6}]`
	if string(buf) != want {
		t.Fatalf("got %s want %s", buf, want)
	}
	_, _ = c.Exec(context.Background(), "DROP TABLE pgz_copy_csv")
}

func TestCopyFromBinary(t *testing.T) {
	c := openClient(t)
	defer c.Close()
	prepCopyTable(t, c, "pgz_copy_bin",
		"id int4, name text, score float8, flag bool, note text")

	rows := []struct {
		id    int32
		name  string
		score float64
		flag  bool
		note  *string
	}{
		{1, "alice", 1.5, true, strPtr("hello")},
		{2, "bob", 2.5, false, nil},
		{3, "charlie", 3.5, true, strPtr("world")},
	}

	var idx int
	n, err := c.CopyFromBinary(context.Background(),
		"COPY pgz_copy_bin FROM STDIN (FORMAT binary)", 5,
		func(w *pgz.CopyWriter) error {
			if idx >= len(rows) {
				return io.EOF
			}
			r := rows[idx]
			idx++
			w.Int4(r.id)
			w.Text(r.name)
			w.Float8(r.score)
			w.Bool(r.flag)
			if r.note == nil {
				w.Null()
			} else {
				w.Text(*r.note)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(rows)) {
		t.Fatalf("n=%d want %d", n, len(rows))
	}

	buf, err := c.QueryJSON(context.Background(),
		"SELECT id, name, score, flag, note FROM pgz_copy_bin ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	const want = `[{"id":1,"name":"alice","score":1.5,"flag":true,"note":"hello"},` +
		`{"id":2,"name":"bob","score":2.5,"flag":false,"note":null},` +
		`{"id":3,"name":"charlie","score":3.5,"flag":true,"note":"world"}]`
	if string(buf) != want {
		t.Fatalf("\n got:  %s\nwant: %s", buf, want)
	}
	_, _ = c.Exec(context.Background(), "DROP TABLE pgz_copy_bin")
}

func TestCopyFromBinaryWrongFieldCount(t *testing.T) {
	c := openClient(t)
	defer c.Close()
	prepCopyTable(t, c, "pgz_copy_wrong", "id int4, name text")

	_, err := c.CopyFromBinary(context.Background(),
		"COPY pgz_copy_wrong FROM STDIN (FORMAT binary)", 2,
		func(w *pgz.CopyWriter) error {
			w.Int4(1) // only one field — expected 2
			return nil
		})
	if err == nil {
		t.Fatal("expected error on field-count mismatch")
	}
	// Connection must stay usable after the client-side abort.
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("conn unusable after CopyFail: %v", err)
	}
	_, _ = c.Exec(context.Background(), "DROP TABLE pgz_copy_wrong")
}

func TestCopyFromBinaryTimestamp(t *testing.T) {
	c := openClient(t)
	defer c.Close()
	prepCopyTable(t, c, "pgz_copy_ts", "id int4, ts timestamptz")

	tt := time.Date(2025, 1, 15, 12, 30, 45, 123456000, time.UTC)
	var sent bool
	_, err := c.CopyFromBinary(context.Background(),
		"COPY pgz_copy_ts FROM STDIN (FORMAT binary)", 2,
		func(w *pgz.CopyWriter) error {
			if sent {
				return io.EOF
			}
			w.Int4(1)
			w.TimestampTZ(tt)
			sent = true
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	buf, err := c.QueryJSON(context.Background(),
		"SELECT ts = '2025-01-15 12:30:45.123456+00'::timestamptz AS ok FROM pgz_copy_ts")
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != `[{"ok":true}]` {
		t.Fatalf("ts round-trip failed: %s", buf)
	}
	_, _ = c.Exec(context.Background(), "DROP TABLE pgz_copy_ts")
}

func strPtr(s string) *string { return &s }
