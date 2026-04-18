package tests

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/arturoeanton/pgz/pgz"
	"github.com/jackc/pgx/v5"
)

// BenchmarkCopyToBinary — pgz CopyToBinary vs pgx CopyTo.
// pq has no binary COPY TO and falls out of this comparison.
func BenchmarkCopyToBinary(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	const sql = "COPY (SELECT id, name, score FROM bench_mixed_5col) TO STDOUT (FORMAT binary)"

	b.Run("Pgz", func(b *testing.B) {
		c := openClient(b)
		defer c.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			n, err := c.CopyToBinary(context.Background(), sql, 3, func(r *pgz.CopyReader) error {
				_, _ = r.Int4()
				_, _ = r.Text()
				_, _ = r.Float8()
				return r.Err()
			})
			if err != nil {
				b.Fatal(err)
			}
			if n != 100_000 {
				b.Fatalf("got %d rows", n)
			}
		}
		b.ReportMetric(float64(100_000*b.N)/b.Elapsed().Seconds(), "rows/s")
	})

	b.Run("Pgx", func(b *testing.B) {
		c := pgxConnect(b, dsn)
		defer c.Close(context.Background())
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := c.PgConn().CopyTo(context.Background(), io.Discard, sql)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(100_000*b.N)/b.Elapsed().Seconds(), "rows/s")
	})
}

// BenchmarkCopyToText — same query in CSV; compares the byte-pump path.
func BenchmarkCopyToText(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	const sql = "COPY (SELECT id, name, score FROM bench_mixed_5col) TO STDOUT (FORMAT csv)"

	b.Run("Pgz", func(b *testing.B) {
		c := openClient(b)
		defer c.Close()
		b.ReportAllocs()
		b.ResetTimer()
		var bytesSum int64
		for i := 0; i < b.N; i++ {
			cw := &countingWriterBytes{}
			n, err := c.CopyTo(context.Background(), sql, cw)
			if err != nil {
				b.Fatal(err)
			}
			if n != 100_000 {
				b.Fatalf("got %d rows", n)
			}
			bytesSum += cw.n
		}
		b.SetBytes(bytesSum / int64(b.N))
		b.ReportMetric(float64(100_000*b.N)/b.Elapsed().Seconds(), "rows/s")
	})

	b.Run("Pgx", func(b *testing.B) {
		c := pgxConnect(b, dsn)
		defer c.Close(context.Background())
		b.ReportAllocs()
		b.ResetTimer()
		var bytesSum int64
		for i := 0; i < b.N; i++ {
			cw := &countingWriterBytes{}
			_, err := c.PgConn().CopyTo(context.Background(), cw, sql)
			if err != nil {
				b.Fatal(err)
			}
			bytesSum += cw.n
		}
		b.SetBytes(bytesSum / int64(b.N))
		b.ReportMetric(float64(100_000*b.N)/b.Elapsed().Seconds(), "rows/s")
	})
}

var _ = pgx.Conn{} // ensure import
