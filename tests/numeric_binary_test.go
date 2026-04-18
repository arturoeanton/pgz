package tests

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/arturoeanton/pgz/pgz"
)

// openClientBinaryNumeric opens a client with Config.BinaryNumeric = true.
// Separate from openClient so the default bench path stays on the text
// decoder — the documented safer default.
func openClientBinaryNumeric(t testing.TB) *pgz.Client {
	t.Helper()
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		t.Skip("PGZ_TEST_DSN not set")
	}
	cfg, err := pgz.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.BinaryNumeric = true
	c, err := pgz.Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestBinaryNumericCorrectness verifies that enabling Config.BinaryNumeric
// produces identical JSON output to the default text path across a range
// of precision/scale combinations plus the canonical edge cases.
func TestBinaryNumericCorrectness(t *testing.T) {
	text := openClient(t)
	defer text.Close()
	bin := openClientBinaryNumeric(t)
	defer bin.Close()

	// One SELECT per case keeps the expected rendering per value simple.
	cases := []string{
		"0",
		"-0",
		"1",
		"-1",
		"12345.6789",
		"-12345.6789",
		"1234567890.1234567890",
		"0.000001",
		"99999999999999999999.99999999",
		"'NaN'",
		"'Infinity'",
		"'-Infinity'",
	}
	for _, v := range cases {
		t.Run(v, func(t *testing.T) {
			sql := "SELECT " + v + "::numeric AS n"
			a, err := text.QueryJSON(context.Background(), sql)
			if err != nil {
				t.Fatal(err)
			}
			b, err := bin.QueryJSON(context.Background(), sql)
			if err != nil {
				t.Fatal(err)
			}
			// Compare as raw JSON — both paths must produce the same
			// bytes (same numeric canonicalisation, same NaN/Infinity
			// string form).
			if string(a) != string(b) {
				t.Fatalf("text=%s binary=%s", a, b)
			}
			// Also: must parse as valid JSON.
			var v any
			if err := json.Unmarshal(a, &v); err != nil {
				t.Fatalf("invalid JSON from text path: %v", err)
			}
		})
	}
}

// BenchmarkNumericBinary measures streaming NDJSON of the seeded
// bench_numeric table (100k rows × numeric(18,4) + numeric(10,6)) with
// Config.BinaryNumeric on and off. The default (off) remains the
// production recommendation unless a profile shows otherwise.
func BenchmarkNumericBinary(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	const sql = "SELECT id, price, rate FROM bench_numeric"

	b.Run("Text", func(b *testing.B) {
		c := openClient(b)
		defer c.Close()
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
	})

	b.Run("Binary", func(b *testing.B) {
		c := openClientBinaryNumeric(b)
		defer c.Close()
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
	})
}
