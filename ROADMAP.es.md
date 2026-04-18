# Roadmap

## Releases

- **v0.1.0** (2026-04-18). Primer tag publicado. Ver
  [CHANGELOG.md](CHANGELOG.md).

## Implementado

### Wire + protocolo

- Conexion TCP con TCP keepalive (`Config.Keepalive`, default 30s).
- Negociacion TLS (`SSLRequest`), modos `disable / prefer / require /
  verify-ca / verify-full`.
- Auth: trust, cleartext, MD5, SCRAM-SHA-256 (vector RFC 7677
  validado; PBKDF2 solo stdlib).
- Startup + BackendKeyData + ParameterStatus + ReadyForQuery.
- El handshake respeta `ctx.Deadline()` del caller para todo el
  intercambio TLS + auth.
- Simple Query (`Q`) y Extended Query
  (`Parse` / `Describe` / `Bind` / `Execute` / `Sync`).
- Cache de prepared statements por conexion, acotado, con batching
  de `Close` server-side.
- PgBouncer-txn safe: SQLSTATE 26000 dispara re-`Parse` transparente
  cuando no se flushearon bytes.
- `CancelRequest` real en conexion lateral manejado por
  `context.Cancel`.
- Reader con bufio (32 KiB configurable).

### Decoders de tipo

- Formato binario para: bool, int2/4/8, oid, float4/8, uuid, json,
  jsonb, bytea, date, timestamp, timestamptz.
- Decoders binarios de interval / time / timetz. Interval emite
  duracion ISO-8601.
- Decoder binario de arrays, recursivo, multi-dim, despacha por el
  encoder binario escalar segun OID del elemento.
- Decoder binario opt-in de `numeric` via `Config.BinaryNumeric`
  (default texto; binario gana en links de alta latencia y
  precisiones altas).
- Fallback text-format para todo lo demas.
- Plan de encoder compilado por shape, construido una vez por
  `RowDescription`, reutilizado para cada fila. Hot loop: 0
  allocs/op.

### Output + streaming

- Modos de salida JSON: array de objetos, NDJSON, columnar, TOON.
- Writer JSON directo con escape RFC 8259 y tabla lookup de 256
  entradas.
- API streaming `io.Writer` flusheando por umbral de bytes y tiempo
  transcurrido.
- API bufferizada `[]byte`.
- Header diferido hasta `BindComplete + primera DataRow`.

### Parametros

- Encoding binario de parametros para bool, int2/4/8, oid,
  float4/8, bytea, timestamp, timestamptz. Fallback texto para
  strings, numeric, y OIDs desconocidos.
- Driven por OID: usa la `ParameterDescription` del servidor para
  elegir el ancho binario correcto por parametro. Cero
  allocations por argumento escalar.

### Soporte DML

- `Exec(ctx, sql, args...)` para INSERT / UPDATE / DELETE / CALL /
  DDL. Devuelve `ExecResult` con `RowsAffected` y tag raw de
  `CommandComplete`.
- `ExecReturning(ctx, w, sql, args...)` streamea resultados de
  clausulas RETURNING como NDJSON, reutilizando el path completo
  de streaming JSON.
- `ExecReturningJSON(ctx, sql, args...)` variante bufferizada.
- Cache de statements y re-prepare PgBouncer-txn compartido con el
  path SELECT.

### COPY

- `CopyFrom(ctx, sql, r io.Reader)` — import texto / CSV. Streamea
  desde cualquier `io.Reader` a frames `CopyData`. 3 allocations
  para un import de 100 000 filas.
- `CopyFromBinary(ctx, sql, fields, emit)` — import binario con
  `CopyWriter` tipado. Arma tuplas in-place; un mensaje
  `CopyData` por flush (sin memcpy intermedia). Medido +44 %
  throughput vs `pgx.CopyFrom` en el mismo input.
- `CopyTo(ctx, sql, w io.Writer)` — export texto / CSV. Pumpea
  bytes directo al writer (cero allocation por fila).
- `CopyToBinary(ctx, sql, fields, handler)` — export binario con
  `CopyReader` tipado. Parsea tuplas in-place desde el buffer del
  wire.

### Pipeline / batch

- `NewBatch()` + `Client.SendBatch(ctx, b)` envian cada statement
  en una sola escritura TCP. Caching de Parse por batch promueve
  el primer SQL de cada tipo al cache de statements persistente,
  para que los items siguientes (y batches futuros) salteen
  `Parse + Describe`.
- Medido +16 % throughput, 10.6× menos memoria, 8× menos allocs
  vs `pgx.SendBatch` en un batch de 100 INSERTs.

### Struct scan

- `ScanStruct[T]` — scan tipado a struct desde resultados SELECT.
- `ScanStructBatched[T]` — scan por batch con memoria acotada y
  callback.
- Soporta: escalares, `*T` nullable, `sql.Null*`, `sql.Scanner`,
  arrays 1-D y 2-D, embedded structs, range types
  (`pgz.RangeBytes`), composite types (declarar OID en
  `Config.BinaryOIDs`).

### Errores

- `pgz.PGError` — error estructurado de PostgreSQL con todos los
  campos de wire (`Severity`, `Code`, `Message`, `Detail`, `Hint`,
  `Where`, mapa raw `Fields`).
- Helpers de predicado: `IsUniqueViolation`,
  `IsForeignKeyViolation`, `IsCheckViolation`,
  `IsNotNullViolation`, `IsExclusionViolation`,
  `IsIntegrityViolation`, `IsSerializationFailure`, `IsDeadlock`,
  `IsQueryCanceled`, `IsAdminShutdown`,
  `IsInvalidSQLStatementName`.
- Accesores `SQLState()` y `SQLStateClass()`. `errors.As`
  funciona a traves de wrappers `fmt.Errorf %w`.

### Hardening para Citus + PgBouncer-txn

- Topes duros por respuesta: `MaxResponseBytes` /
  `MaxResponseRows` con `CancelRequest` + drain +
  `*ResponseTooLargeError`.
- Retry en SQLSTATE 40001 / 40P01
  (`Config.RetryOnSerialization`).
- `DefaultQueryTimeout` con modelo de 3 capas de timeout
  documentado.

### Observabilidad

- Interfaz `Observer`: `OnQueryStart`, `OnQueryEnd`, `OnNotice`,
  `OnQuerySlow`.
- Contadores atomicos lock-free via `Stats()`.
- Subpackage `pgz/otel` — `Observer` con OpenTelemetry: spans y
  metricas (`pgz.query.duration`, `pgz.query.rows`,
  `pgz.query.errors`, `pgz.query.slow`). Cero costo si no se
  importa.

### Pool + ciclo de vida

- Pool LIFO acotado con reaper de idle.
- `MaxConnLifetime`, `PingAfterIdle`, `Discard()`.
- Retry transparente en `Acquire` (hasta 3x).
- `Drain(ctx)` y `WaitIdle(ctx)` para shutdown graceful.

### Adaptador `database/sql` (`pgz/stdlib`)

- Registrado como `"pgz"`.
- Path de lectura: `Query`, `QueryContext`, `QueryRow`, `Prepare`,
  `Ping`.
- Path de escritura: `Exec`, `ExecContext`, `stmt.Exec`,
  `stmt.ExecContext`.
- Transacciones: `Begin`, `BeginTx` con traduccion de nivel de
  aislamiento y read-only en un unico round-trip `BEGIN …`.
- INSERT … RETURNING via `db.QueryRow` funciona por
  `RawQueryAny`.
- `driver.ErrBadConn` devuelto solo en fallas de wire; `PGError`
  y cancelacion de ctx dejan la conexion usable.
- Compatible con sqlc, goose, golang-migrate, sqlx.

### Otros

- `Iterator` para acceso lazy a DataRow (`RawQuery` /
  `RawQueryAny`).
- Escape JSON SWAR opcional bajo build tag `pgz_simd`.
- Suite de fuzz para jsonwriter, pgerr, rows.

---

## Proximo

- **CI**: Workflow de GitHub Actions con contenedores de PG +
  PgBouncer corriendo suite de integracion, corpus de fuzz, y
  benchmarks del hot path como gate de regresion.
- **Soak test con trafico shadow** (8h / 500k usuarios). El diseno
  lo soporta; falta validacion real.

## Despues

- Decoders binarios de inet / cidr / macaddr / enum (el fallback
  texto funciona).
- Wrapper opcional de zstd / gzip para la API de streaming.
- Negociacion de protocolo al conectar (3.1+ / grease).
- Escape SIMD real de strings JSON (AVX2 + NEON) via assembly Go.
- Auto-retry driven por `PGError` en clases transitorias mas alla
  de 40001 / 40P01.

## No planeado

- LISTEN / NOTIFY.
- Protocolo de replicacion.
- Features de ORM.
- Path generico de "scan into anything" mas alla de `ScanStruct`.

pgz gana por foco. Si necesitas alguno de estos, usa pgx en
paralelo.
