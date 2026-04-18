// Apples-to-apples comparison: pgz vs lib/pq on the same wire, same PG,
// same shapes as the pgx bench. All paths write NDJSON to io.Discard.
// Requires PGZ_TEST_DSN.
//
// Three paths:
//
//	Pg2JSON    — native StreamNDJSON. Identical source to the pgx bench.
//	PqMap      — database/sql + lib/pq, rows.Scan into []any →
//	             map[string]any → json.Marshal. The naive-but-common
//	             "I use database/sql" pattern.
//	PqRaw      — database/sql + lib/pq, rows.Scan into []sql.RawBytes
//	             and a hand-written NDJSON encoder that handles the
//	             per-OID text format pq delivers. Best case for pq; no
//	             intermediate Go types.
//
// pq speaks the PostgreSQL text protocol only. There is no binary
// decoder to reach for, so PqRaw is as fast as pq can go by design.
package tests

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

func BenchmarkPq(b *testing.B) {
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		b.Skip("PGZ_TEST_DSN not set")
	}
	for _, sh := range benchShapes {
		for _, n := range benchSizes {
			sql := sh.sql(n)
			b.Run(fmt.Sprintf("%s/%d/Pg2JSON", sh.name, n), func(b *testing.B) {
				c := openClient(b)
				defer c.Close()
				b.ReportAllocs()
				b.ResetTimer()
				var bytesSum int64
				for i := 0; i < b.N; i++ {
					cw := &countingWriterBytes{}
					if err := c.StreamNDJSON(context.Background(), cw, sql); err != nil {
						b.Fatal(err)
					}
					bytesSum += cw.n
				}
				b.SetBytes(bytesSum / int64(b.N))
			})
			b.Run(fmt.Sprintf("%s/%d/PqMap", sh.name, n), func(b *testing.B) {
				db := pqConnect(b, dsn)
				defer db.Close()
				b.ReportAllocs()
				b.ResetTimer()
				var bytesSum int64
				for i := 0; i < b.N; i++ {
					n, err := runPqMap(db, sql, io.Discard)
					if err != nil {
						b.Fatal(err)
					}
					bytesSum += n
				}
				b.SetBytes(bytesSum / int64(b.N))
			})
			b.Run(fmt.Sprintf("%s/%d/PqRaw", sh.name, n), func(b *testing.B) {
				db := pqConnect(b, dsn)
				defer db.Close()
				b.ReportAllocs()
				b.ResetTimer()
				var bytesSum int64
				for i := 0; i < b.N; i++ {
					n, err := runPqRaw(db, sql, io.Discard)
					if err != nil {
						b.Fatal(err)
					}
					bytesSum += n
				}
				b.SetBytes(bytesSum / int64(b.N))
			})
		}
	}
}

func pqConnect(b *testing.B, dsn string) *sql.DB {
	b.Helper()
	db, err := sql.Open("postgres", dsn)
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

// runPqMap: sql.Rows.Scan into []any → map[string]any → json.Marshal.
// Matches the shape of runPgxMap.
func runPqMap(db *sql.DB, q string, w io.Writer) (int64, error) {
	rows, err := db.QueryContext(context.Background(), q)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	vals := make([]any, len(cols))
	dests := make([]any, len(cols))
	for i := range vals {
		dests[i] = &vals[i]
	}
	var total int64
	for rows.Next() {
		if err := rows.Scan(dests...); err != nil {
			return total, err
		}
		m := make(map[string]any, len(cols))
		for i, c := range cols {
			// pq returns []byte for text-y columns; flip to string so
			// json.Marshal produces a string not a base64 blob.
			if bb, ok := vals[i].([]byte); ok {
				m[c] = string(bb)
			} else {
				m[c] = vals[i]
			}
		}
		b, err := json.Marshal(m)
		if err != nil {
			return total, err
		}
		n, err := w.Write(b)
		total += int64(n)
		if err != nil {
			return total, err
		}
		n, err = w.Write([]byte("\n"))
		total += int64(n)
		if err != nil {
			return total, err
		}
	}
	return total, rows.Err()
}

// runPqRaw: sql.Rows.Scan into []sql.RawBytes and a hand-written
// NDJSON encoder that understands the per-column PostgreSQL text
// format. The column types come from rows.ColumnTypes() so we can
// avoid quoting numerics, bools, jsonb, and int arrays.
func runPqRaw(db *sql.DB, q string, w io.Writer) (int64, error) {
	rows, err := db.QueryContext(context.Background(), q)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	cts, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}
	keys := make([][]byte, len(cols))
	kinds := make([]pqKind, len(cols))
	for i, c := range cols {
		keys[i] = []byte(`"` + c + `":`)
		kinds[i] = classifyPq(cts[i].DatabaseTypeName())
	}
	raws := make([]sql.RawBytes, len(cols))
	dests := make([]any, len(cols))
	for i := range raws {
		dests[i] = &raws[i]
	}
	buf := make([]byte, 0, 64*1024)
	var total int64
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		n, err := w.Write(buf)
		total += int64(n)
		buf = buf[:0]
		return err
	}
	for rows.Next() {
		if err := rows.Scan(dests...); err != nil {
			return total, err
		}
		buf = append(buf, '{')
		for i, raw := range raws {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, keys[i]...)
			buf = appendPqCell(buf, kinds[i], raw)
		}
		buf = append(buf, '}', '\n')
		if len(buf) >= 32*1024 {
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	if err := flush(); err != nil {
		return total, err
	}
	return total, rows.Err()
}

type pqKind int

const (
	pqString pqKind = iota
	pqNumber
	pqBool
	pqJSON
	pqIntArray
)

func classifyPq(name string) pqKind {
	switch name {
	case "INT2", "INT4", "INT8", "FLOAT4", "FLOAT8", "NUMERIC":
		return pqNumber
	case "BOOL":
		return pqBool
	case "JSON", "JSONB":
		return pqJSON
	case "_INT4", "_INT2", "_INT8":
		return pqIntArray
	default:
		return pqString
	}
}

// appendPqCell: pq hands us the server's text format as-is. For
// numbers/bools/JSON we copy verbatim; for strings we escape; for
// int arrays we rewrite {1,2,3} → [1,2,3].
func appendPqCell(dst []byte, k pqKind, raw sql.RawBytes) []byte {
	if raw == nil {
		return append(dst, 'n', 'u', 'l', 'l')
	}
	switch k {
	case pqNumber:
		return append(dst, raw...)
	case pqBool:
		if len(raw) == 1 && raw[0] == 't' {
			return append(dst, 't', 'r', 'u', 'e')
		}
		return append(dst, 'f', 'a', 'l', 's', 'e')
	case pqJSON:
		return append(dst, raw...)
	case pqIntArray:
		dst = append(dst, '[')
		for i, c := range raw {
			if i == 0 || i == len(raw)-1 {
				continue // skip '{' and '}'
			}
			if c == ',' {
				dst = append(dst, ',')
			} else {
				dst = append(dst, c)
			}
		}
		return append(dst, ']')
	default:
		dst = append(dst, '"')
		dst = appendEscaped(dst, raw)
		return append(dst, '"')
	}
}
