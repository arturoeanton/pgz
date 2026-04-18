package tests

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/arturoeanton/pgz/pgz"
)

func TestCopyToCSV(t *testing.T) {
	c := openClient(t)
	defer c.Close()

	var buf bytes.Buffer
	rows, err := c.CopyTo(context.Background(),
		"COPY (SELECT id, name FROM bench_mixed_5col WHERE id BETWEEN 1 AND 3) TO STDOUT (FORMAT csv)",
		&buf)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 3 {
		t.Fatalf("rows=%d", rows)
	}
	out := buf.String()
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines=%d got=%q", len(lines), out)
	}
	if lines[0] != "1,name-1" || lines[2] != "3,name-3" {
		t.Fatalf("unexpected output:\n%s", out)
	}
}

func TestCopyToBinaryRoundtrip(t *testing.T) {
	c := openClient(t)
	defer c.Close()

	// Export the first 100 rows of bench_mixed_5col and verify each
	// tuple decodes identically to what SELECT would return.
	sql := "COPY (SELECT id, name, score, flag FROM bench_mixed_5col ORDER BY id LIMIT 100) " +
		"TO STDOUT (FORMAT binary)"

	type row struct {
		id    int32
		name  string
		score float64
		flag  bool
	}
	var got []row
	n, err := c.CopyToBinary(context.Background(), sql, 4, func(r *pgz.CopyReader) error {
		id, _ := r.Int4()
		nameRaw, _ := r.Text()
		score, _ := r.Float8()
		flag, _ := r.Bool()
		got = append(got, row{
			id:    id,
			name:  string(nameRaw),
			score: score,
			flag:  flag,
		})
		return r.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 || len(got) != 100 {
		t.Fatalf("n=%d len=%d", n, len(got))
	}
	if got[0].id != 1 || got[0].name != "name-1" {
		t.Fatalf("row0: %+v", got[0])
	}
	if got[99].id != 100 {
		t.Fatalf("row99: %+v", got[99])
	}
	// Parity with SELECT for score and flag.
	for i, r := range got {
		wantScore := float64(r.id) / 3.0
		if r.score != wantScore {
			t.Fatalf("row %d score=%v want %v", i, r.score, wantScore)
		}
		wantFlag := r.id%2 == 0
		if r.flag != wantFlag {
			t.Fatalf("row %d flag=%v want %v", i, r.flag, wantFlag)
		}
	}
}

func TestCopyToHandlerEarlyStop(t *testing.T) {
	c := openClient(t)
	defer c.Close()
	sql := "COPY (SELECT id FROM bench_narrow_int) TO STDOUT (FORMAT binary)"
	seen := 0
	_, err := c.CopyToBinary(context.Background(), sql, 1, func(r *pgz.CopyReader) error {
		_, _ = r.Int4()
		seen++
		if seen == 10 {
			return io.EOF
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if seen != 10 {
		t.Fatalf("seen=%d", seen)
	}
	// Conn must remain usable after early termination.
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("conn unusable after early EOF: %v", err)
	}
}
