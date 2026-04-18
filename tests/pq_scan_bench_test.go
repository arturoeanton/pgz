// Struct-scan comparison: pgz.ScanStruct vs lib/pq over database/sql.
// Reads 100 000 rows from the seeded docker tables. The pq path uses
// the text protocol — there is no binary format to reach for — so this
// is as fast as pq can go when filling a typed struct.
package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	"github.com/arturoeanton/pgz/pgz"
	_ "github.com/lib/pq"
)

func BenchmarkScanMixedPq(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}

	const q = "SELECT id, name, score, flag, meta FROM bench_mixed_5col"

	b.Run("Pg2JSON_ScanStruct", func(b *testing.B) {
		c := openClient(b)
		defer c.Close()
		b.ReportAllocs()
		b.ResetTimer()
		var rowsTotal int64
		for i := 0; i < b.N; i++ {
			out, err := pgz.ScanStruct[benchMixedRow](c, context.Background(), q)
			if err != nil {
				b.Fatal(err)
			}
			rowsTotal += int64(len(out))
		}
		b.ReportMetric(float64(rowsTotal)/b.Elapsed().Seconds(), "rows/s")
	})

	b.Run("PqScan", func(b *testing.B) {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			b.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		defer db.Close()
		b.ReportAllocs()
		b.ResetTimer()
		var rowsTotal int64
		for i := 0; i < b.N; i++ {
			rs, err := db.QueryContext(context.Background(), q)
			if err != nil {
				b.Fatal(err)
			}
			var count int64
			for rs.Next() {
				var r benchMixedRow
				var meta []byte
				if err := rs.Scan(&r.Id, &r.Name, &r.Score, &r.Flag, &meta); err != nil {
					b.Fatal(err)
				}
				r.Meta = json.RawMessage(meta)
				count++
			}
			rs.Close()
			if err := rs.Err(); err != nil {
				b.Fatal(err)
			}
			rowsTotal += count
		}
		b.ReportMetric(float64(rowsTotal)/b.Elapsed().Seconds(), "rows/s")
	})
}

func BenchmarkScanNarrowPq(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	const q = "SELECT id FROM bench_narrow_int"

	b.Run("Pg2JSON_ScanStruct", func(b *testing.B) {
		c := openClient(b)
		defer c.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := pgz.ScanStruct[benchNarrowRow](c, context.Background(), q); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("PqScan", func(b *testing.B) {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			b.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		defer db.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rs, err := db.QueryContext(context.Background(), q)
			if err != nil {
				b.Fatal(err)
			}
			for rs.Next() {
				var r benchNarrowRow
				if err := rs.Scan(&r.Id); err != nil {
					b.Fatal(err)
				}
			}
			rs.Close()
			if err := rs.Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkScanWideJSONBPq(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	const q = "SELECT id, j FROM bench_wide_jsonb"

	b.Run("Pg2JSON_ScanStruct", func(b *testing.B) {
		c := openClient(b)
		defer c.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := pgz.ScanStruct[benchWideRow](c, context.Background(), q); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("PqScan", func(b *testing.B) {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			b.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		defer db.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rs, err := db.QueryContext(context.Background(), q)
			if err != nil {
				b.Fatal(err)
			}
			for rs.Next() {
				var r benchWideRow
				var j []byte
				if err := rs.Scan(&r.Id, &j); err != nil {
					b.Fatal(err)
				}
				r.J = json.RawMessage(j)
			}
			rs.Close()
			if err := rs.Err(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
