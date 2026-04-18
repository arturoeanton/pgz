# pgz

Driver PostgreSQL zero-alloc para Go. La forma mas rapida de mover
filas entre Go y PostgreSQL — SELECT a JSON, scans tipados a struct,
DML, COPY en ambas direcciones, batches pipelinadas, y un adaptador
`database/sql` completo.

Sin ORM. Sin reflexion en el hot path. Sin dependencias runtime fuera
de la stdlib.

Disponible en [English](README.md) | [Español](README.es.md).

---

## Instalacion

```bash
go get github.com/arturoeanton/pgz
```

Requiere Go 1.21+. Sin cgo.

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

### Lectura — SELECT a JSON

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

// Scan por batch — O(batchSize) en memoria para 100M filas
pgz.ScanStructBatched[User](c, ctx, 10_000,
    func(batch []User) error {
        return processBatch(batch)
    },
    "SELECT id, name, email, tags, meta FROM user_log")
```

### Escritura — DML

```go
// INSERT / UPDATE / DELETE
res, _ := c.Exec(ctx,
    "INSERT INTO users (name, email) VALUES ($1, $2)", "alice", "alice@example.com")
fmt.Println(res.RowsAffected) // 1

// DML con RETURNING — streamea el resultado como NDJSON
c.ExecReturning(ctx, w,
    "INSERT INTO users (name) VALUES ($1), ($2) RETURNING id, name",
    "bob", "charlie")

// RETURNING bufferizado
buf, _ := c.ExecReturningJSON(ctx,
    "UPDATE users SET name = upper(name) WHERE id = $1 RETURNING *", 42)

// Stored procedures
c.Exec(ctx, "CALL refresh_materialized_views()")
```

### Datos en masa — COPY

```go
// Import desde cualquier io.Reader (CSV, TEXT)
c.CopyFrom(ctx, "COPY users (id, name) FROM STDIN (FORMAT csv)",
    strings.NewReader(csvBody))

// Import binario — campo a campo tipado, mas rapido que pgx.CopyFrom
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

// Export texto — cero allocs por fila, pumpea bytes a cualquier io.Writer
c.CopyTo(ctx, "COPY (SELECT * FROM users) TO STDOUT (FORMAT csv)", w)

// Export binario con CopyReader tipado
c.CopyToBinary(ctx,
    "COPY (SELECT id, name FROM users) TO STDOUT (FORMAT binary)", 2,
    func(r *pgz.CopyReader) error {
        id, _ := r.Int4()
        name, _ := r.Text()
        return r.Err()
    })
```

### Batches pipelinadas

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

Todos los items en cola van en una sola escritura TCP. La primera
ocurrencia de cada SQL unico se Parsea + Describe y se cachea; los
items siguientes (y batches futuros) saltean Parse del todo.
Medido +16 % throughput y 10.6× menos memoria que `pgx.SendBatch`.

### Errores estructurados

```go
if _, err := c.Exec(ctx, "INSERT ..."); err != nil {
    var pgErr *pgz.PGError
    if errors.As(err, &pgErr) && pgErr.IsUniqueViolation() {
        return ErrDuplicateUser
    }
    return err
}
```

Los helpers cubren cada SQLSTATE donde un gateway tipicamente
ramifica — `IsUniqueViolation`, `IsForeignKeyViolation`,
`IsSerializationFailure`, `IsDeadlock`, `IsQueryCanceled`,
`IsAdminShutdown`, `IsInvalidSQLStatementName`, y mas.

### Adaptador `database/sql`

```go
import (
    "database/sql"
    _ "github.com/arturoeanton/pgz/pgz/stdlib"
)

db, _ := sql.Open("pgz", "postgres://user:pass@host/db?sslmode=require")

// Lectura
rows, _ := db.Query("SELECT id, name FROM users WHERE active = $1", true)

// Escritura
db.Exec("INSERT INTO users (name, email) VALUES ($1, $2)", "alice", "alice@...")

// Transaccion
tx, _ := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
tx.Exec("UPDATE ...")
tx.Commit()

// INSERT ... RETURNING
var id int
db.QueryRow("INSERT INTO users (name) VALUES ($1) RETURNING id", "bob").Scan(&id)
```

El adaptador esta completo: lectura, escritura, transacciones,
prepared statements, RETURNING via QueryRow. Compatible con `sqlc`,
`goose`, `golang-migrate`, `sqlx`.

### OpenTelemetry

```go
import pgzotel "github.com/arturoeanton/pgz/pgz/otel"

obs, _ := pgzotel.New(tracerProvider, meterProvider)
c.SetObserver(obs)
```

Un span por query, cuatro metricas (`pgz.query.duration`,
`pgz.query.rows`, `pgz.query.errors`, `pgz.query.slow`). Las
dependencias de OTel solo se cargan cuando importas el subpackage.

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
| float4, float8 | binary | numero (NaN → `"NaN"`) | `float32/64` |
| numeric | text (binario opt-in) | numero | `string`, `sql.Scanner` |
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

Los parametros se envian en binario para int / float / bool / bytea
/ timestamp; texto para strings y lo demas.

---

## Performance

Apple M4 Max, macOS, PostgreSQL 17.9 en Docker loopback.

### vs pgx — streaming JSON, 100 000 filas

| Shape | pgz `StreamNDJSON` | pgx Map | pgx Raw (hand-tuned) |
|---|---|---|---|
| narrow_int | **129.6 MB/s**, 6 allocs | 40.1 MB/s, 1.0M allocs | 128.3 MB/s, 6 allocs |
| mixed_5col | **168.4 MB/s**, 6 allocs | 70.5 MB/s, 3.3M allocs | 168.9 MB/s, 11 allocs |
| wide_jsonb | **143.2 MB/s**, 6 allocs | 32.4 MB/s, 3.4M allocs | 143.5 MB/s, 8 allocs |
| null_heavy | **174.0 MB/s**, 6 allocs | 59.1 MB/s, 1.3M allocs | 179.4 MB/s, 8 allocs |

pgz iguala o gana a los encoders `RawValues` hand-tuned con una API
nativa que requiere cero codigo custom. Los caminos naive (los que
realmente escribe el codigo de produccion) quedan 2–4× atras.

### vs pgx — COPY y Pipeline

| Camino | pgz | pgx |
|---|---|---|
| COPY FROM binario (100k filas) | **190.1 MB/s**, 100k allocs | 132.2 MB/s, 500k allocs |
| COPY TO texto (100k filas) | 166.1 MB/s, **1 alloc** | 169.0 MB/s, 3 allocs |
| SendBatch (100 INSERTs) | **280 553 filas/s**, 102 allocs | 242 241 filas/s, 814 allocs |

### vs pgx — camino `database/sql` de escritura (1000 INSERTs)

| Patron | pgz | pgx/stdlib | lib/pq |
|---|---|---|---|
| InsertExec | **8 451 filas/s** | 8 146 filas/s | 4 108 filas/s |
| InsertPrepared | **8 714 filas/s** | 8 170 filas/s | 8 405 filas/s |
| Tx en lote | **8 676 filas/s** | 8 289 filas/s | 4 233 filas/s |

Ver [BENCHMARKS.md](BENCHMARKS.md) para la matriz completa — 24
escenarios, conteo de podios, veredicto por feature, y los tres
lugares donde pgz no gana.

Corre los numeros vos:

```bash
docker compose -f docker/docker-compose.yml up -d
export PGZ_TEST_DSN="postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable"
go test ./tests -run '^$' -bench . -benchmem -benchtime=2s
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

- **Topes duros por respuesta** — `CancelRequest` + `*ResponseTooLargeError`
- **PgBouncer-txn safe** — re-prepare transparente en SQLSTATE 26000
- **Retry en serializacion** — opt-in en 40001/40P01 (rebalance Citus)
- **CancelRequest real** en `ctx.Cancel`
- **Header diferido** — cero bytes downstream si falla antes de una fila
- **Shutdown graceful** — `Pool.Drain(ctx)`, `Pool.WaitIdle(ctx)`
- **Observer** — `OnQueryStart/End/Slow/Notice` + `Stats()` atomicos
- **OpenTelemetry** — subpackage `pgz/otel` para spans + metricas
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

### Escrituras OLTP en batch

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
├── pgz/                     # API publica
│   ├── conn.go              # Open/Close, handshake, auth
│   ├── config.go            # Config, SSLMode, ParseDSN
│   ├── query.go             # paths SELECT (Query/Stream)
│   ├── exec.go              # paths DML (Exec/ExecReturning)
│   ├── scan.go              # ScanStruct[T], ScanStructBatched[T]
│   ├── copy.go              # CopyFrom (text + binary)
│   ├── copy_to.go           # CopyTo (text + binary)
│   ├── pipeline.go          # Batch / SendBatch
│   ├── iter.go              # RawQuery / RawQueryAny / Iterator
│   ├── stream.go            # abstraccion outWriter
│   ├── stmtcache.go         # cache de prepared statements
│   ├── observer.go          # hook de telemetria + contadores atomicos
│   ├── errors.go            # PGError + helpers
│   ├── args.go              # valor Go -> wire binary / text
│   ├── pool/                # pool de conexiones
│   ├── stdlib/              # adapter database/sql (lectura + escritura)
│   └── otel/                # observer OpenTelemetry
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

Ver [ARCHITECTURE.md](ARCHITECTURE.md) para el rationale de diseno
y [BENCHMARKS.md](BENCHMARKS.md) para la matriz cara a cara.

---

## Build tags

- `pgz_simd` — escape SWAR de strings JSON. Go puro, sin assembly.
  ~4× mas rapido en strings ASCII medianas/largas.

```bash
go build -tags pgz_simd ./...
```

---

## Licencia

MIT. Ver [LICENSE](LICENSE).
