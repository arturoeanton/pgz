package tests

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/arturoeanton/pgz/pgz"
	"github.com/jackc/pgx/v5"
)

// BenchmarkPipelineBatch — pgz.SendBatch vs pgx.SendBatch on a batch
// of 100 INSERTs. pq has no pipeline / batch API and falls out of
// this comparison.
func BenchmarkPipelineBatch(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	const batchRows = 100
	const insertSQL = "INSERT INTO bench_pipe (id, name) VALUES ($1, $2)"

	setup := func(exec func(string) error) {
		_ = exec("DROP TABLE IF EXISTS bench_pipe")
		if err := exec("CREATE UNLOGGED TABLE bench_pipe (id int4, name text)"); err != nil {
			b.Fatal(err)
		}
	}
	truncate := func(exec func(string) error) {
		if err := exec("TRUNCATE bench_pipe"); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("Pgz", func(b *testing.B) {
		c := openClient(b)
		defer c.Close()
		setup(func(s string) error {
			_, err := c.Exec(context.Background(), s)
			return err
		})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			truncate(func(s string) error {
				_, err := c.Exec(context.Background(), s)
				return err
			})
			batch := pgz.NewBatch()
			for j := 0; j < batchRows; j++ {
				batch.Queue(insertSQL, int32(j), "n")
			}
			b.StartTimer()
			br := c.SendBatch(context.Background(), batch)
			for j := 0; j < batchRows; j++ {
				if _, err := br.Exec(); err != nil {
					b.Fatal(err)
				}
			}
			if err := br.Close(); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(batchRows*b.N)/b.Elapsed().Seconds(), "rows/s")
	})

	b.Run("Pgx", func(b *testing.B) {
		c := pgxConnect(b, dsn)
		defer c.Close(context.Background())
		setup(func(s string) error {
			_, err := c.Exec(context.Background(), s)
			return err
		})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			truncate(func(s string) error {
				_, err := c.Exec(context.Background(), s)
				return err
			})
			batch := &pgx.Batch{}
			for j := 0; j < batchRows; j++ {
				batch.Queue(insertSQL, int32(j), "n")
			}
			b.StartTimer()
			br := c.SendBatch(context.Background(), batch)
			for j := 0; j < batchRows; j++ {
				if _, err := br.Exec(); err != nil {
					b.Fatal(err)
				}
			}
			if err := br.Close(); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(batchRows*b.N)/b.Elapsed().Seconds(), "rows/s")
	})
}

var _ = fmt.Sprintf
