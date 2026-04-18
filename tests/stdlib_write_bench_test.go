// Write-path benchmarks: pgz/stdlib vs pgx/stdlib vs lib/pq, all
// speaking database/sql. Measures the path sqlc / goose / sqlx use in
// production — Exec of parameterised INSERTs, with and without a
// prepared statement, plus a short transaction burst.
//
// Skipped without PGZ_TEST_DSN.
package tests

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/arturoeanton/pgz/pgz/stdlib"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
)

func openSQL(b *testing.B, driverName, dsn string) *sql.DB {
	b.Helper()
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(context.Background()); err != nil {
		b.Fatal(err)
	}
	return db
}

func resetWriteTarget(b *testing.B, db *sql.DB) {
	b.Helper()
	if _, err := db.Exec("DROP TABLE IF EXISTS bench_write_target"); err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(
		`CREATE UNLOGGED TABLE bench_write_target (id int4 PRIMARY KEY, name text, score float8)`,
	); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkStdlibInsertExec measures the path most ORMs take: repeated
// db.Exec with $1..$N parameters (no Prepare). Each driver caches the
// prepared statement internally by SQL text — this compares cache
// efficiency plus bind/execute overhead.
func BenchmarkStdlibInsertExec(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	const sqlText = `INSERT INTO bench_write_target (id, name, score) VALUES ($1, $2, $3)`

	run := func(b *testing.B, driverName string) {
		db := openSQL(b, driverName, dsn)
		defer db.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			resetWriteTarget(b, db)
			b.StartTimer()
			for j := 0; j < insertBatchRows; j++ {
				if _, err := db.Exec(sqlText, int32(j), "n", float64(j)/3.0); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.ReportMetric(float64(insertBatchRows*b.N)/b.Elapsed().Seconds(), "rows/s")
	}

	b.Run("Pgz", func(b *testing.B) { run(b, "pgz") })
	b.Run("Pgx", func(b *testing.B) { run(b, "pgx") })
	b.Run("Pq", func(b *testing.B) { run(b, "postgres") })
}

// BenchmarkStdlibInsertPrepared is db.Prepare once + stmt.Exec N
// times. This is the path sqlc generates by default: prepare per
// method, call many times.
func BenchmarkStdlibInsertPrepared(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	const sqlText = `INSERT INTO bench_write_target (id, name, score) VALUES ($1, $2, $3)`

	run := func(b *testing.B, driverName string) {
		db := openSQL(b, driverName, dsn)
		defer db.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			resetWriteTarget(b, db)
			stmt, err := db.Prepare(sqlText)
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			for j := 0; j < insertBatchRows; j++ {
				if _, err := stmt.Exec(int32(j), "n", float64(j)/3.0); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			stmt.Close()
			b.StartTimer()
		}
		b.ReportMetric(float64(insertBatchRows*b.N)/b.Elapsed().Seconds(), "rows/s")
	}

	b.Run("Pgz", func(b *testing.B) { run(b, "pgz") })
	b.Run("Pgx", func(b *testing.B) { run(b, "pgx") })
	b.Run("Pq", func(b *testing.B) { run(b, "postgres") })
}

// BenchmarkStdlibTxInsert measures BEGIN / N * INSERT / COMMIT —
// the batch-in-a-tx pattern that goose/golang-migrate use for every
// migration step.
func BenchmarkStdlibTxInsert(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	const sqlText = `INSERT INTO bench_write_target (id, name, score) VALUES ($1, $2, $3)`

	run := func(b *testing.B, driverName string) {
		db := openSQL(b, driverName, dsn)
		defer db.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			resetWriteTarget(b, db)
			b.StartTimer()
			tx, err := db.Begin()
			if err != nil {
				b.Fatal(err)
			}
			for j := 0; j < insertBatchRows; j++ {
				if _, err := tx.Exec(sqlText, int32(j), "n", float64(j)/3.0); err != nil {
					b.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(insertBatchRows*b.N)/b.Elapsed().Seconds(), "rows/s")
	}

	b.Run("Pgz", func(b *testing.B) { run(b, "pgz") })
	b.Run("Pgx", func(b *testing.B) { run(b, "pgx") })
	b.Run("Pq", func(b *testing.B) { run(b, "postgres") })
}

// insertBatchRows is small enough that the per-op overhead is
// dominated by Parse/Bind/Execute cycles rather than table growth —
// which is exactly what we want to measure for database/sql write
// throughput. Larger batches shift the bottleneck to the server's WAL
// path and stop telling us about the driver.
const insertBatchRows = 1_000
