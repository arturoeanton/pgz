# pgz

Zero-alloc PostgreSQL driver for Go. The fastest way to get JSON out of
PostgreSQL, and now back in too.

SELECT, INSERT, UPDATE, DELETE, RETURNING, CALL -- no ORM, no
reflection, no dependencies.

Available in [English](README.md) | [Español](README.es.md).

---

## Install

```bash
go get github.com/arturoeanton/pgz
```

Requires Go 1.21+. No cgo. Zero non-stdlib runtime dependencies.

---

## Quick start

```go
import (
    "context"
    "github.com/arturoeanton/pgz/pgz"
)

cfg, _ := pgz.ParseDSN("postgres://user:pass@host/db?sslmode=require")
c, err := pgz.Open(context.Background(), cfg)
if err != nil { log.Fatal(err) }
defer c.Close()
```

### Read path -- SELECT to JSON

```go
// Buffered JSON array
buf, _ := c.QueryJSON(ctx,
    "SELECT id, name, meta FROM users WHERE id = $1", 42)
// buf == [{"id":42,"name":"alice","meta":{"k":42}}]

// Stream NDJSON to any io.Writer
c.StreamNDJSON(ctx, w,
    "SELECT id, name FROM users WHERE active = $1", true)

// Typed struct scan
type User struct {
    ID    int32
    Name  string
    Email sql.NullString
    Tags  []string             // text[]
    Meta  json.RawMessage      // jsonb
}
users, _ := pgz.ScanStruct[User](c, ctx,
    "SELECT id, name, email, tags, meta FROM users")

// Memory-bounded batch scan -- O(batchSize) memory for 100M rows
pgz.ScanStructBatched[User](c, ctx, 10_000,
    func(batch []User) error {
        return processBatch(batch)
    },
    "SELECT id, name, email, tags, meta FROM user_log")
```

### Write path -- DML

```go
// INSERT / UPDATE / DELETE
res, _ := c.Exec(ctx,
    "INSERT INTO users (name, email) VALUES ($1, $2)", "alice", "alice@example.com")
fmt.Println(res.RowsAffected) // 1

// DML with RETURNING -- streams the result as NDJSON
c.ExecReturning(ctx, w,
    "INSERT INTO users (name) VALUES ($1), ($2) RETURNING id, name",
    "bob", "charlie")

// Buffered RETURNING
buf, _ := c.ExecReturningJSON(ctx,
    "UPDATE users SET name = upper(name) WHERE id = $1 RETURNING *", 42)

// Stored procedures
c.Exec(ctx, "CALL refresh_materialized_views()")
```

### `database/sql` adapter

```go
import (
    "database/sql"
    _ "github.com/arturoeanton/pgz/pgz/stdlib"
)

db, _ := sql.Open("pgz", "postgres://user:pass@host/db?sslmode=require")
rows, _ := db.Query("SELECT id, name FROM users WHERE active = $1", true)
```

The adapter is read-only: `db.Exec` and `db.Begin` return an explicit
error. Route writes through `Exec`/`ExecReturning` on the native API.

---

## Output modes

| Method | Shape | Typical consumer |
|---|---|---|
| `QueryJSON` | `[{...},{...}]` buffered | REST body < 10 MB |
| `StreamJSON` | `[{...},{...}]` streamed | HTTP response, large result |
| `StreamNDJSON` | `{...}\n{...}\n` | jq, Kafka, S3, line-oriented |
| `StreamColumnar` | `{"columns":[...],"rows":[[...]]}` | ag-grid, spreadsheets |
| `StreamTOON` | `[?]{col,col}\nval,val\n` | LLM / agent pipelines |

All modes flush incrementally by byte threshold and elapsed time.
The header is deferred until the first DataRow, so a failed query
writes zero bytes downstream.

---

## Supported types

Binary format is requested for every OID with a specialized decoder.
Text is the correctness-preserving fallback.

| PostgreSQL | Wire | JSON | Struct target |
|---|---|---|---|
| bool | binary | `true`/`false` | `bool` |
| int2, int4, int8, oid | binary | number | `int16/32/64`, `uint32` |
| float4, float8 | binary | number (NaN -> `"NaN"`) | `float32/64` |
| numeric | text | number | `string`, `sql.Scanner` |
| text, varchar, bpchar | text | escaped string | `string`, `[]byte` |
| uuid | binary | `"xxxxxxxx-..."` | `string`, `[16]byte` |
| json, jsonb | binary | embedded JSON | `json.RawMessage` |
| bytea | binary | `"\\x<hex>"` | `[]byte` |
| date, timestamp, timestamptz | binary | ISO 8601 | `time.Time` |
| interval | binary | ISO 8601 duration | `string` |
| arrays (1-D, 2-D) | binary | nested JSON arrays | `[]T`, `[][]T` |
| ranges | binary | quoted string | `pgz.RangeBytes` |
| composite types | binary | nested object | struct (declare OID) |
| anything else | text | escaped string | `string` |

---

## Performance

### pgz vs pgx -- streaming JSON, 100k rows

Measured on two platforms. Full bench source in `tests/pgx_compare_test.go`.

**Intel Core Ultra 7 155U, Linux, PostgreSQL 17:**

| Shape | pgz | pgx Map | pgx Raw | vs Map |
|---|---|---|---|---|
| mixed_5col | 97 MB/s, 6 allocs | 16 MB/s, 3.3M allocs | 49 MB/s | **6x faster** |
| wide_jsonb | 75 MB/s, 6 allocs | 7 MB/s, 3.4M allocs | 72 MB/s | **10x faster** |
| array_int | 104 MB/s, 6 allocs | 14 MB/s, 4.5M allocs | 90 MB/s | **7.7x faster** |
| null_heavy | 67 MB/s, 6 allocs | 14 MB/s, 1.3M allocs | 87 MB/s | **4.8x faster** |

### pgz vs pgx -- struct scan, 100k rows

| Path | rows/s | allocs |
|---|---|---|
| pgz ScanStruct | **2.1M** | 200k |
| pgx Scan | 1.2M | 600k |
| pgx CollectByName | 559k | 700k |

The advantage is architectural: pgz decodes wire bytes directly into
JSON or struct fields without intermediate Go types, maps, or
`json.Marshal` round-trips. This holds across platforms.

Run the benchmarks yourself:

```bash
docker compose -f docker/docker-compose.yml up -d
export PGZ_TEST_DSN="postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable"
go test ./tests -run '^$' -bench BenchmarkPgx -benchmem -benchtime=3s
```

---

## Production hardening

Built for data gateways fronting Citus + PgBouncer in transaction mode.

```go
p, _ := pool.New(pool.Config{
    Config: pgz.Config{
        Host: "pgbouncer", Port: 6432,
        Database: "app", User: "gateway",
        MaxResponseBytes:     16 << 20,
        MaxResponseRows:      100_000,
        DefaultQueryTimeout:  10 * time.Second,
        FlushInterval:        100 * time.Millisecond,
        SlowQueryThreshold:   250 * time.Millisecond,
        RetryOnSerialization: true,
        Keepalive:            30 * time.Second,
    },
    MaxConns: 32,
})
defer p.Close()
```

- **Hard response caps** -- `CancelRequest` + `*ResponseTooLargeError`
- **PgBouncer-txn safe** -- transparent re-prepare on SQLSTATE 26000
- **Serialization retry** -- opt-in retry on 40001/40P01 (Citus rebalance)
- **Real CancelRequest** on `ctx.Cancel`
- **Deferred header** -- zero bytes downstream on early failure
- **Graceful shutdown** -- `Pool.Drain(ctx)`, `Pool.WaitIdle(ctx)`
- **Observer** -- `OnQueryStart/End/Slow/Notice` + atomic `Stats()`
- **TCP keepalive**, socket buffer knobs, configurable bufio reader

---

## Examples

### REST endpoint streaming NDJSON

```go
func listUsers(p *pool.Pool) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        c, err := p.Acquire(r.Context())
        if err != nil { http.Error(w, err.Error(), 503); return }
        defer c.Release()

        w.Header().Set("Content-Type", "application/x-ndjson")
        c.StreamNDJSON(r.Context(), w,
            "SELECT id, email, created_at FROM users WHERE active = $1", true)
    }
}
```

### INSERT with RETURNING as JSON

```go
func createUser(c *pgz.Client, name, email string) ([]byte, error) {
    return c.ExecReturningJSON(context.Background(),
        "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, created_at",
        name, email)
}
```

### Bulk export with bounded memory

```go
pgz.ScanStructBatched[User](c, ctx, 5_000,
    func(batch []User) error {
        for _, u := range batch {
            b, _ := json.Marshal(u)
            fmt.Fprintln(out, string(b))
        }
        return nil
    },
    "SELECT id, name, email, created_at FROM users")
```

---

## Layout

```
pgz/
├── pgz/                     # public API
│   ├── conn.go              # Open/Close, handshake, auth
│   ├── config.go            # Config, SSLMode, ParseDSN
│   ├── query.go             # SELECT paths (Query/Stream)
│   ├── exec.go              # DML paths (Exec/ExecReturning)
│   ├── scan.go              # ScanStruct[T], ScanStructBatched[T]
│   ├── iter.go              # RawQuery / Iterator
│   ├── stream.go            # outWriter abstraction
│   ├── stmtcache.go         # prepared-statement cache
│   ├── observer.go          # telemetry hook + atomic counters
│   ├── ctx.go               # ctx -> CancelRequest watcher
│   ├── args.go              # Go value -> text-format wire
│   ├── pool/                # connection pool
│   └── stdlib/              # database/sql adapter
├── internal/
│   ├── wire/                # framed reader/writer over net.Conn
│   ├── protocol/            # message codes + OIDs
│   ├── auth/                # MD5 + SCRAM-SHA-256 (stdlib only)
│   ├── rows/                # plan compile + DataRow hot loop
│   ├── types/               # per-OID text + binary encoders
│   ├── jsonwriter/          # direct-to-[]byte JSON appender
│   ├── bufferpool/          # sync.Pool of []byte
│   └── pgerr/               # ErrorResponse decoder
├── docker/                  # docker-compose + benchmark seed data
├── cmd/
│   ├── pgz_demo/            # CLI demo
│   └── pgz_bench/           # end-to-end perf harness
└── tests/                   # integration, comparison, benchmarks
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for design rationale.

---

## Build tags

- `pgz_simd` -- SWAR JSON string escape. Pure Go, no assembly.
  ~4x faster on medium/long ASCII strings.

```bash
go build -tags pgz_simd ./...
```

---

## License

MIT. See [LICENSE](LICENSE).
