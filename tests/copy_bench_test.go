// Bulk-load benchmarks: pgz.CopyFromBinary vs pgx.Conn.CopyFrom vs
// lib/pq CopyIn (via database/sql). All load the same 100 000 rows of
// (id int4, name text, score float8) into a fresh table per run. The
// server and table structure are identical across drivers so any delta
// is the driver's copy pipeline.
//
// Skipped without PGZ_TEST_DSN.
package tests

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/arturoeanton/pgz/pgz"
	"github.com/jackc/pgx/v5"
	_ "github.com/lib/pq"
)

const copyBenchRows = 100_000

func resetCopyTarget(tb testing.TB, c *pgz.Client) {
	tb.Helper()
	_, err := c.Exec(context.Background(), "DROP TABLE IF EXISTS bench_copy_target")
	if err != nil {
		tb.Fatal(err)
	}
	_, err = c.Exec(context.Background(),
		"CREATE UNLOGGED TABLE bench_copy_target (id int4, name text, score float8)")
	if err != nil {
		tb.Fatal(err)
	}
}

// BenchmarkCopyBinary measures the binary path only. This is the one
// that matters for numeric-heavy bulk-load — text has to stringify
// every float.
func BenchmarkCopyBinary(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	b.Run("PgzBinary", func(b *testing.B) {
		c := openClient(b)
		defer c.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			resetCopyTarget(b, c)
			b.StartTimer()
			var idx int32
			_, err := c.CopyFromBinary(context.Background(),
				"COPY bench_copy_target FROM STDIN (FORMAT binary)", 3,
				func(w *pgz.CopyWriter) error {
					if idx >= copyBenchRows {
						return io.EOF
					}
					w.Int4(idx)
					w.Text(copyRowName(idx))
					w.Float8(float64(idx) / 3.0)
					idx++
					return nil
				})
			if err != nil {
				b.Fatal(err)
			}
		}
		b.SetBytes(int64(copyBenchRows) * copyRowBytesBinary)
		b.ReportMetric(float64(copyBenchRows*b.N)/b.Elapsed().Seconds(), "rows/s")
	})

	// pgx CopyFrom uses its own binary encoder under the hood.
	b.Run("PgxCopyFrom", func(b *testing.B) {
		c := pgxConnect(b, dsn)
		defer c.Close(context.Background())
		setupPgxTarget(b, dsn)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			resetPgxTarget(b, dsn)
			b.StartTimer()
			src := pgx.CopyFromSlice(copyBenchRows, func(i int) ([]any, error) {
				return []any{int32(i), copyRowName(int32(i)), float64(i) / 3.0}, nil
			})
			_, err := c.CopyFrom(context.Background(),
				pgx.Identifier{"bench_copy_target"},
				[]string{"id", "name", "score"}, src)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.SetBytes(int64(copyBenchRows) * copyRowBytesBinary)
		b.ReportMetric(float64(copyBenchRows*b.N)/b.Elapsed().Seconds(), "rows/s")
	})
}

// BenchmarkCopyText compares text-protocol paths: pgz CopyFrom with a
// strings.Reader against lib/pq CopyIn. pq speaks only text, so this is
// its best case.
func BenchmarkCopyText(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	body := buildCSVBody(copyBenchRows)

	b.Run("PgzText", func(b *testing.B) {
		c := openClient(b)
		defer c.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			resetCopyTarget(b, c)
			b.StartTimer()
			_, err := c.CopyFrom(context.Background(),
				"COPY bench_copy_target FROM STDIN (FORMAT csv)",
				strings.NewReader(body))
			if err != nil {
				b.Fatal(err)
			}
		}
		b.SetBytes(int64(len(body)))
		b.ReportMetric(float64(copyBenchRows*b.N)/b.Elapsed().Seconds(), "rows/s")
	})

	b.Run("PqCopyIn", func(b *testing.B) {
		db := pqConnect(b, dsn)
		defer db.Close()
		setupPqTarget(b, db)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			resetPqTarget(b, db)
			b.StartTimer()
			tx, err := db.Begin()
			if err != nil {
				b.Fatal(err)
			}
			stmt, err := tx.Prepare(`COPY bench_copy_target (id, name, score) FROM STDIN`)
			if err != nil {
				b.Fatal(err)
			}
			for j := int32(0); j < copyBenchRows; j++ {
				if _, err := stmt.Exec(j, copyRowName(j), float64(j)/3.0); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := stmt.Exec(); err != nil {
				b.Fatal(err)
			}
			if err := stmt.Close(); err != nil {
				b.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
		b.SetBytes(int64(len(body)))
		b.ReportMetric(float64(copyBenchRows*b.N)/b.Elapsed().Seconds(), "rows/s")
	})
}

// Rough binary-wire byte count per row: int4 (4+4) + text "name-NNNNNN"
// avg ≈ 9 bytes + int4 len = 13 + float8 (8+4) = 33. Close enough for
// MB/s comparisons against text, which is larger per row.
const copyRowBytesBinary = 33

func copyRowName(i int32) string {
	// Deterministic stringification without fmt.Sprintf per row.
	var buf [16]byte
	n := 0
	buf[n] = 'n'
	n++
	buf[n] = 'a'
	n++
	buf[n] = 'm'
	n++
	buf[n] = 'e'
	n++
	buf[n] = '-'
	n++
	v := i
	if v < 0 {
		buf[n] = '-'
		n++
		v = -v
	}
	start := n
	if v == 0 {
		buf[n] = '0'
		n++
	} else {
		for v > 0 {
			buf[n] = byte('0' + v%10)
			v /= 10
			n++
		}
		// reverse digits in place from start..n.
		for a, b := start, n-1; a < b; a, b = a+1, b-1 {
			buf[a], buf[b] = buf[b], buf[a]
		}
	}
	return string(buf[:n])
}

func buildCSVBody(n int) string {
	var b strings.Builder
	b.Grow(n * 20)
	for i := int32(0); i < int32(n); i++ {
		b.WriteString(fmt.Sprintf("%d,%s,%g\n", i, copyRowName(i), float64(i)/3.0))
	}
	return b.String()
}

func setupPgxTarget(b *testing.B, dsn string) {
	b.Helper()
	c := pgxConnect(b, dsn)
	defer c.Close(context.Background())
	_, _ = c.Exec(context.Background(), "DROP TABLE IF EXISTS bench_copy_target")
	_, err := c.Exec(context.Background(),
		"CREATE UNLOGGED TABLE bench_copy_target (id int4, name text, score float8)")
	if err != nil {
		b.Fatal(err)
	}
}

func resetPgxTarget(b *testing.B, dsn string) {
	b.Helper()
	c := pgxConnect(b, dsn)
	defer c.Close(context.Background())
	if _, err := c.Exec(context.Background(), "TRUNCATE bench_copy_target"); err != nil {
		b.Fatal(err)
	}
}

func setupPqTarget(b *testing.B, db *sql.DB) {
	b.Helper()
	_, _ = db.Exec("DROP TABLE IF EXISTS bench_copy_target")
	if _, err := db.Exec(
		"CREATE UNLOGGED TABLE bench_copy_target (id int4, name text, score float8)"); err != nil {
		b.Fatal(err)
	}
}

func resetPqTarget(b *testing.B, db *sql.DB) {
	b.Helper()
	if _, err := db.Exec("TRUNCATE bench_copy_target"); err != nil {
		b.Fatal(err)
	}
}
