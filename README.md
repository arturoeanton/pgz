# pgz

Zero-alloc PostgreSQL driver for Go. The fastest way to move rows
between Go and PostgreSQL — SELECT to JSON, typed struct scans, DML,
COPY both directions, pipelined batches, and a full `database/sql`
adapter.

No ORM. No reflection in the hot path. No non-stdlib runtime
dependencies.

Available in [English](README.md) | [Español](README.es.md).

---

## Install

```bash
go get github.com/arturoeanton/pgz
```

Requires Go 1.21+. No cgo.

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

### Read path — SELECT to JSON

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

// Memory-bounded batch scan — O(batchSize) memory for 100M rows
pgz.ScanStructBatched[User](c, ctx, 10_000,
    func(batch []User) error {
        return processBatch(batch)
    },
    "SELECT id, name, email, tags, meta FROM user_log")
```

### Write path — DML

```go
// INSERT / UPDATE / DELETE
res, _ := c.Exec(ctx,
    "INSERT INTO users (name, email) VALUES ($1, $2)", "alice", "alice@example.com")
fmt.Println(res.RowsAffected) // 1

// DML with RETURNING — streams the result as NDJSON
c.ExecReturning(ctx, w,
    "INSERT INTO users (name) VALUES ($1), ($2) RETURNING id, name",
    "bob", "charlie")

// Buffered RETURNING
buf, _ := c.ExecReturningJSON(ctx,
    "UPDATE users SET name = upper(name) WHERE id = $1 RETURNING *", 42)

// Stored procedures
c.Exec(ctx, "CALL refresh_materialized_views()")
```

### Bulk data — COPY

```go
// Import from any io.Reader (CSV, TEXT)
c.CopyFrom(ctx, "COPY users (id, name) FROM STDIN (FORMAT csv)",
    strings.NewReader(csvBody))

// Binary import — typed field-by-field, faster than pgx.CopyFrom
rows := []row{...}
var i int
c.CopyFromBinary(ctx,
    "COPY users (id, name, score) FROM STDIN (FORMAT binary)", 3,
    func(w *pgz.CopyWriter) error {
        if i >= len(rows) { return io.EOF }
        w.Int4(rows[i].id); w.Text(rows[i].name); w.Float8(rows[i].score)
        i++
        return nil
    })

// Text export — zero allocs per row, pumps bytes to any io.Writer
c.CopyTo(ctx, "COPY (SELECT * FROM users) TO STDOUT (FORMAT csv)", w)

// Binary export with typed CopyReader
c.CopyToBinary(ctx,
    "COPY (SELECT id, name FROM users) TO STDOUT (FORMAT binary)", 2,
    func(r *pgz.CopyReader) error {
        id, _ := r.Int4()
        name, _ := r.Text()
        return r.Err()
    })
```

### Pipelined batches

```go
b := pgz.NewBatch()
b.Queue("UPDATE users SET last_seen = now() WHERE id = $1", 42)
b.Queue("INSERT INTO audit (user_id, action) VALUES ($1, $2)", 42, "login")

br := c.SendBatch(ctx, b)
defer br.Close()
for i := 0; i < b.Len(); i++ {
    if _, err := br.Exec(); err != nil { return err }
}
```

All queued items ship in one TCP write. The first occurrence of a
given SQL is Parsed + Described and cached; subsequent items (and
future batches) skip Parse entirely. Measured +16 % throughput and
10.6× less memory than `pgx.SendBatch`.

### Structured errors

```go
if _, err := c.Exec(ctx, "INSERT ..."); err != nil {
    var pgErr *pgz.PGError
    if errors.As(err, &pgErr) && pgErr.IsUniqueViolation() {
        return ErrDuplicateUser
    }
    return err
}
```

Helpers cover every SQLSTATE a gateway routinely branches on —
`IsUniqueViolation`, `IsForeignKeyViolation`, `IsSerializationFailure`,
`IsDeadlock`, `IsQueryCanceled`, `IsAdminShutdown`,
`IsInvalidSQLStatementName`, and more.

### `database/sql` adapter

```go
import (
    "database/sql"
    _ "github.com/arturoeanton/pgz/pgz/stdlib"
)

db, _ := sql.Open("pgz", "postgres://user:pass@host/db?sslmode=require")

// Read
rows, _ := db.Query("SELECT id, name FROM users WHERE active = $1", true)

// Write
db.Exec("INSERT INTO users (name, email) VALUES ($1, $2)", "alice", "alice@...")

// Transaction
tx, _ := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
tx.Exec("UPDATE ...")
tx.Commit()

// INSERT ... RETURNING
var id int
db.QueryRow("INSERT INTO users (name) VALUES ($1) RETURNING id", "bob").Scan(&id)
```

The adapter is feature-complete: read, write, transactions, prepared
statements, RETURNING via QueryRow. Compatible with `sqlc`, `goose`,
`golang-migrate`, `sqlx`.

### OpenTelemetry

```go
import pgzotel "github.com/arturoeanton/pgz/pgz/otel"

obs, _ := pgzotel.New(tracerProvider, meterProvider)
c.SetObserver(obs)
```

One span per query, four metrics (`pgz.query.duration`,
`pgz.query.rows`, `pgz.query.errors`, `pgz.query.slow`). OTel
dependencies ship only when you import the subpackage.

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
| float4, float8 | binary | number (NaN → `"NaN"`) | `float32/64` |
| numeric | text (binary opt-in) | number | `string`, `sql.Scanner` |
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

Parameters are sent binary for int / float / bool / bytea / timestamp;
text for strings and anything else.

---

## Performance

Apple M4 Max, macOS, PostgreSQL 17.9 on Docker loopback.

### vs pgx — streaming JSON, 100 000 rows

| Shape | pgz `StreamNDJSON` | pgx Map | pgx Raw (hand-tuned) |
|---|---|---|---|
| narrow_int | **129.6 MB/s**, 6 allocs | 40.1 MB/s, 1.0M allocs | 128.3 MB/s, 6 allocs |
| mixed_5col | **168.4 MB/s**, 6 allocs | 70.5 MB/s, 3.3M allocs | 168.9 MB/s, 11 allocs |
| wide_jsonb | **143.2 MB/s**, 6 allocs | 32.4 MB/s, 3.4M allocs | 143.5 MB/s, 8 allocs |
| null_heavy | **174.0 MB/s**, 6 allocs | 59.1 MB/s, 1.3M allocs | 179.4 MB/s, 8 allocs |

pgz matches or beats hand-tuned `RawValues` encoders with a native API
that requires zero custom code. Naive paths (the ones production code
actually ships) are left 2–4× behind.

### vs pgx — COPY and Pipeline

| Path | pgz | pgx |
|---|---|---|
| COPY FROM binary (100k rows) | **190.1 MB/s**, 100k allocs | 132.2 MB/s, 500k allocs |
| COPY TO text (100k rows) | 166.1 MB/s, **1 alloc** | 169.0 MB/s, 3 allocs |
| SendBatch (100 INSERTs) | **280 553 rows/s**, 102 allocs | 242 241 rows/s, 814 allocs |

### vs pgx — `database/sql` write path (1000 INSERTs)

| Pattern | pgz | pgx/stdlib | lib/pq |
|---|---|---|---|
| InsertExec | **8 451 rows/s** | 8 146 rows/s | 4 108 rows/s |
| InsertPrepared | **8 714 rows/s** | 8 170 rows/s | 8 405 rows/s |
| Tx batch | **8 676 rows/s** | 8 289 rows/s | 4 233 rows/s |

See [BENCHMARKS.md](BENCHMARKS.md) for the full matrix — 24
scenarios, podium counts, verdict per feature, and the three places
where pgz does not win.

Run it yourself:

```bash
docker compose -f docker/docker-compose.yml up -d
export PGZ_TEST_DSN="postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable"
go test ./tests -run '^$' -bench . -benchmem -benchtime=2s
```

---

## Production hardening

Built for data gateways fronting Citus + PgBouncer in transaction
mode.

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

- **Hard response caps** — `CancelRequest` + `*ResponseTooLargeError`
- **PgBouncer-txn safe** — transparent re-prepare on SQLSTATE 26000
- **Serialization retry** — opt-in retry on 40001/40P01 (Citus rebalance)
- **Real CancelRequest** on `ctx.Cancel`
- **Deferred header** — zero bytes downstream on early failure
- **Graceful shutdown** — `Pool.Drain(ctx)`, `Pool.WaitIdle(ctx)`
- **Observer** — `OnQueryStart/End/Slow/Notice` + atomic `Stats()`
- **OpenTelemetry** — `pgz/otel` subpackage for spans + metrics
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

### Batched OLTP writes

```go
b := pgz.NewBatch()
for _, evt := range events {
    b.Queue("INSERT INTO events (user_id, kind, payload) VALUES ($1, $2, $3)",
        evt.UserID, evt.Kind, evt.Payload)
}
br := c.SendBatch(ctx, b)
defer br.Close()
for range events {
    if _, err := br.Exec(); err != nil { return err }
}
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
│   ├── copy.go              # CopyFrom (text + binary)
│   ├── copy_to.go           # CopyTo (text + binary)
│   ├── pipeline.go          # Batch / SendBatch
│   ├── iter.go              # RawQuery / RawQueryAny / Iterator
│   ├── stream.go            # outWriter abstraction
│   ├── stmtcache.go         # prepared-statement cache
│   ├── observer.go          # telemetry hook + atomic counters
│   ├── errors.go            # PGError + helpers
│   ├── args.go              # Go value -> wire binary / text
│   ├── pool/                # connection pool
│   ├── stdlib/              # database/sql adapter (read + write)
│   └── otel/                # OpenTelemetry observer
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

See [ARCHITECTURE.md](ARCHITECTURE.md) for design rationale and
[BENCHMARKS.md](BENCHMARKS.md) for the head-to-head matrix.

---

## Build tags

- `pgz_simd` — SWAR JSON string escape. Pure Go, no assembly.
  ~4× faster on medium/long ASCII strings.

```bash
go build -tags pgz_simd ./...
```

---

## License

MIT. See [LICENSE](LICENSE).
