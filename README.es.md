# pgz

Driver PostgreSQL zero-alloc para Go. La forma mas rapida de sacar JSON
de PostgreSQL, y ahora tambien de meter datos.

SELECT, INSERT, UPDATE, DELETE, RETURNING, CALL -- sin ORM, sin
reflexion, sin dependencias.

Disponible en [English](README.md) | [Español](README.es.md).

---

## Instalacion

```bash
go get github.com/arturoeanton/pgz
```

Requiere Go 1.21+. Sin cgo. Cero dependencias runtime fuera de la
stdlib.

---

## Inicio rapido

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

### Lectura -- SELECT a JSON

```go
// JSON array bufferizado
buf, _ := c.QueryJSON(ctx,
    "SELECT id, name, meta FROM users WHERE id = $1", 42)
// buf == [{"id":42,"name":"alice","meta":{"k":42}}]

// Stream NDJSON a cualquier io.Writer
c.StreamNDJSON(ctx, w,
    "SELECT id, name FROM users WHERE active = $1", true)

// Scan tipado a struct
type User struct {
    ID    int32
    Name  string
    Email sql.NullString
    Tags  []string             // text[]
    Meta  json.RawMessage      // jsonb
}
users, _ := pgz.ScanStruct[User](c, ctx,
    "SELECT id, name, email, tags, meta FROM users")

// Scan por batch -- O(batchSize) en memoria para 100M filas
pgz.ScanStructBatched[User](c, ctx, 10_000,
    func(batch []User) error {
        return processBatch(batch)
    },
    "SELECT id, name, email, tags, meta FROM user_log")
```

### Escritura -- DML

```go
// INSERT / UPDATE / DELETE
res, _ := c.Exec(ctx,
    "INSERT INTO users (name, email) VALUES ($1, $2)", "alice", "alice@example.com")
fmt.Println(res.RowsAffected) // 1

// DML con RETURNING -- streamea el resultado como NDJSON
c.ExecReturning(ctx, w,
    "INSERT INTO users (name) VALUES ($1), ($2) RETURNING id, name",
    "bob", "charlie")

// RETURNING bufferizado
buf, _ := c.ExecReturningJSON(ctx,
    "UPDATE users SET name = upper(name) WHERE id = $1 RETURNING *", 42)

// Stored procedures
c.Exec(ctx, "CALL refresh_materialized_views()")
```

### Adapter `database/sql`

```go
import (
    "database/sql"
    _ "github.com/arturoeanton/pgz/pgz/stdlib"
)

db, _ := sql.Open("pgz", "postgres://user:pass@host/db?sslmode=require")
rows, _ := db.Query("SELECT id, name FROM users WHERE active = $1", true)
```

El adapter es read-only para SELECTs: `db.Exec` y `db.Begin` devuelven
error. Usa `Exec`/`ExecReturning` en la API nativa para escrituras.

---

## Modos de salida

| Metodo | Forma | Consumidor tipico |
|---|---|---|
| `QueryJSON` | `[{...},{...}]` bufferizado | body REST < 10 MB |
| `StreamJSON` | `[{...},{...}]` streameado | respuesta HTTP grande |
| `StreamNDJSON` | `{...}\n{...}\n` | jq, Kafka, S3, por linea |
| `StreamColumnar` | `{"columns":[...],"rows":[[...]]}` | ag-grid, planillas |
| `StreamTOON` | `[?]{col,col}\nval,val\n` | pipelines LLM / agentes |

Todos los modos flushean incremental por umbral de bytes y tiempo.
El header se difiere hasta la primera DataRow: una query que falla
antes de una fila produce cero bytes downstream.

---

## Tipos soportados

Se pide formato binario para cada OID con decoder especializado.
Texto es el fallback correcto.

| PostgreSQL | Wire | JSON | Struct target |
|---|---|---|---|
| bool | binary | `true`/`false` | `bool` |
| int2, int4, int8, oid | binary | numero | `int16/32/64`, `uint32` |
| float4, float8 | binary | numero (NaN -> `"NaN"`) | `float32/64` |
| numeric | text | numero | `string`, `sql.Scanner` |
| text, varchar, bpchar | text | string escapado | `string`, `[]byte` |
| uuid | binary | `"xxxxxxxx-..."` | `string`, `[16]byte` |
| json, jsonb | binary | JSON embebido | `json.RawMessage` |
| bytea | binary | `"\\x<hex>"` | `[]byte` |
| date, timestamp, timestamptz | binary | ISO 8601 | `time.Time` |
| interval | binary | duracion ISO 8601 | `string` |
| arrays (1-D, 2-D) | binary | arrays JSON anidados | `[]T`, `[][]T` |
| ranges | binary | string quoteado | `pgz.RangeBytes` |
| composite types | binary | objeto anidado | struct (declarar OID) |
| cualquier otro | text | string escapado | `string` |

---

## Performance

### pgz vs pgx -- streaming JSON, 100k filas

Medido en dos plataformas. Bench completo en `tests/pgx_compare_test.go`.

**Intel Core Ultra 7 155U, Linux, PostgreSQL 17:**

| Forma | pgz | pgx Map | pgx Raw | vs Map |
|---|---|---|---|---|
| mixed_5col | 97 MB/s, 6 allocs | 16 MB/s, 3.3M allocs | 49 MB/s | **6x mas rapido** |
| wide_jsonb | 75 MB/s, 6 allocs | 7 MB/s, 3.4M allocs | 72 MB/s | **10x mas rapido** |
| array_int | 104 MB/s, 6 allocs | 14 MB/s, 4.5M allocs | 90 MB/s | **7.7x mas rapido** |
| null_heavy | 67 MB/s, 6 allocs | 14 MB/s, 1.3M allocs | 87 MB/s | **4.8x mas rapido** |

### pgz vs pgx -- struct scan, 100k filas

| Path | rows/s | allocs |
|---|---|---|
| pgz ScanStruct | **2.1M** | 200k |
| pgx Scan | 1.2M | 600k |
| pgx CollectByName | 559k | 700k |

La ventaja es arquitectural: pgz decodifica bytes del wire directo a
JSON o campos de struct sin tipos Go intermedios, maps, ni round-trips
por `json.Marshal`. Esto se mantiene en todas las plataformas.

Corré los benchmarks:

```bash
docker compose -f docker/docker-compose.yml up -d
export PGZ_TEST_DSN="postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable"
go test ./tests -run '^$' -bench BenchmarkPgx -benchmem -benchtime=3s
```

---

## Hardening para produccion

Pensado para gateways de datos frente a Citus + PgBouncer en modo
transaccion.

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

- **Topes duros por respuesta** -- `CancelRequest` + `*ResponseTooLargeError`
- **PgBouncer-txn safe** -- re-prepare transparente en SQLSTATE 26000
- **Retry en serializacion** -- opt-in en 40001/40P01 (rebalance Citus)
- **CancelRequest real** en `ctx.Cancel`
- **Header diferido** -- cero bytes downstream si falla antes de una fila
- **Shutdown graceful** -- `Pool.Drain(ctx)`, `Pool.WaitIdle(ctx)`
- **Observer** -- `OnQueryStart/End/Slow/Notice` + `Stats()` atomicos
- **TCP keepalive**, perillas de socket buffers, bufio configurable

---

## Ejemplos

### Endpoint REST streameando NDJSON

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

### INSERT con RETURNING como JSON

```go
func createUser(c *pgz.Client, name, email string) ([]byte, error) {
    return c.ExecReturningJSON(context.Background(),
        "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, created_at",
        name, email)
}
```

### Export bulk con memoria acotada

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
├── pgz/                     # API publica
│   ├── conn.go              # Open/Close, handshake, auth
│   ├── config.go            # Config, SSLMode, ParseDSN
│   ├── query.go             # paths SELECT (Query/Stream)
│   ├── exec.go              # paths DML (Exec/ExecReturning)
│   ├── scan.go              # ScanStruct[T], ScanStructBatched[T]
│   ├── iter.go              # RawQuery / Iterator
│   ├── stream.go            # abstraccion outWriter
│   ├── stmtcache.go         # cache de prepared statements
│   ├── observer.go          # hook de telemetria + contadores atomicos
│   ├── ctx.go               # ctx -> CancelRequest watcher
│   ├── args.go              # valor Go -> wire text-format
│   ├── pool/                # pool de conexiones
│   └── stdlib/              # adapter database/sql
├── internal/
│   ├── wire/                # reader/writer framed sobre net.Conn
│   ├── protocol/            # codigos de mensaje + OIDs
│   ├── auth/                # MD5 + SCRAM-SHA-256 (solo stdlib)
│   ├── rows/                # compilacion de plan + hot loop DataRow
│   ├── types/               # encoders text + binary per OID
│   ├── jsonwriter/          # appender JSON directo a []byte
│   ├── bufferpool/          # sync.Pool de []byte
│   └── pgerr/               # decoder ErrorResponse
├── docker/                  # docker-compose + datos de benchmark
├── cmd/
│   ├── pgz_demo/            # CLI demo
│   └── pgz_bench/           # harness de performance
└── tests/                   # integracion, comparacion, benchmarks
```

Ver [ARCHITECTURE.md](ARCHITECTURE.md) para el rationale de diseno.

---

## Build tags

- `pgz_simd` -- escape SWAR de strings JSON. Go puro, sin assembly.
  ~4x mas rapido en strings ASCII medianas/largas.

```bash
go build -tags pgz_simd ./...
```

---

## Licencia

MIT. Ver [LICENSE](LICENSE).
