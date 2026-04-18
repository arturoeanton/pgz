package tests

import (
	"context"
	"os"
	"testing"

	"github.com/arturoeanton/pgz/pgz"
)

func BenchmarkNumericStream(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	cfg, _ := pgz.ParseDSN(dsn)
	c, err := pgz.Open(context.Background(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	const sql = "SELECT id, price, rate FROM bench_numeric"
	b.ReportAllocs()
	b.ResetTimer()
	var total int64
	for i := 0; i < b.N; i++ {
		cw := &countingWriterBytes{}
		if err := c.StreamNDJSON(context.Background(), cw, sql); err != nil {
			b.Fatal(err)
		}
		total += cw.n
	}
	b.SetBytes(total / int64(b.N))
}
