# Roadmap

## Implementado

### Wire + protocolo

- Conexion TCP con TCP keepalive (`Config.Keepalive`, default 30s).
- Negociacion TLS (`SSLRequest`), modos `disable / prefer / require /
  verify-ca / verify-full`.
- Auth: trust, cleartext, MD5, SCRAM-SHA-256 (vector RFC 7677 validado;
  PBKDF2 solo stdlib).
- Startup + BackendKeyData + ParameterStatus + ReadyForQuery.
- El handshake respeta `ctx.Deadline()` del caller para todo el
  intercambio TLS + auth.
- Simple Query (`Q`) y Extended Query
  (`Parse` / `Describe` / `Bind` / `Execute` / `Sync`).
- Cache de prepared statements por conexion, acotado, con batching de
  `Close` server-side.
- PgBouncer-txn safe: SQLSTATE 26000 dispara re-`Parse` transparente
  cuando no se flushearon bytes.
- `CancelRequest` real en conexion lateral manejado por
  `context.Cancel`.
- Reader con bufio (32 KiB).

### Decoders de tipo

- Formato binario para: bool, int2/4/8, oid, float4/8, uuid, json,
  jsonb, bytea, date, timestamp, timestamptz.
- Decoders binarios de interval / time / timetz. Interval emite
  duracion ISO-8601.
- Decoder binario de arrays, recursivo, multi-dim, despacha por el
  encoder binario escalar segun OID del elemento.
- Fallback text-format para todo lo demas.
- Plan de encoder compilado por shape, construido una vez por
  `RowDescription`, reutilizado para cada fila. Hot loop: 0 allocs/op.

### Output + streaming

- Modos de salida JSON: array de objetos, NDJSON, columnar, TOON.
- Writer JSON directo con escape RFC 8259 y tabla lookup de 256
  entradas.
- API streaming `io.Writer` flusheando por umbral de bytes y tiempo
  transcurrido.
- API bufferizada `[]byte`.
- Header diferido hasta `BindComplete + primera DataRow`.

### Soporte DML

- `Exec(ctx, sql, args...)` para INSERT / UPDATE / DELETE / CALL / DDL.
  Devuelve `ExecResult` con `RowsAffected` y tag raw de
  `CommandComplete`.
- `ExecReturning(ctx, w, sql, args...)` streamea resultados de clausulas
  RETURNING como NDJSON, reutilizando el path completo de streaming
  JSON.
- `ExecReturningJSON(ctx, sql, args...)` variante bufferizada.
- Cache de statements y re-prepare PgBouncer-txn compartido con el path
  SELECT.

### Struct scan

- `ScanStruct[T]` -- scan tipado a struct desde resultados SELECT.
- `ScanStructBatched[T]` -- scan por batch con memoria acotada y
  callback.
- Soporta: escalares, `*T` nullable, `sql.Null*`, `sql.Scanner`,
  arrays 1-D y 2-D, embedded structs, range types (`pgz.RangeBytes`),
  composite types (declarar OID en `Config.BinaryOIDs`).

### Hardening para Citus + PgBouncer-txn

- Topes duros por respuesta: `MaxResponseBytes` / `MaxResponseRows` con
  `CancelRequest` + drain + `*ResponseTooLargeError`.
- Retry en SQLSTATE 40001 / 40P01 (`Config.RetryOnSerialization`).
- `DefaultQueryTimeout` con modelo de 3 capas de timeout documentado.

### Observabilidad

- Interfaz `Observer`: `OnQueryStart`, `OnQueryEnd`, `OnNotice`,
  `OnQuerySlow`.
- Contadores atomicos lock-free via `Stats()`.

### Pool + ciclo de vida

- Pool LIFO acotado con reaper de idle.
- `MaxConnLifetime`, `PingAfterIdle`, `Discard()`.
- Retry transparente en `Acquire` (hasta 3x).
- `Drain(ctx)` y `WaitIdle(ctx)` para shutdown graceful.

### Otros

- Adapter `database/sql` (`pgz/stdlib`), registrado como `"pgz"`.
- `Iterator` para acceso lazy a DataRow (`RawQuery`).
- Escape JSON SWAR opcional bajo build tag `pgz_simd`.
- Suite de fuzz para jsonwriter, pgerr, rows.

---

## Proximo

- **CI**: Workflow de GitHub Actions con contenedores de PG + PgBouncer
  corriendo suite de integracion, corpus de fuzz, y benchmarks del hot
  path como gate de regresion.
- **Soak test con trafico shadow** (8h / 500k usuarios). El diseno lo
  soporta; falta validacion real.
- **COPY-binary fast export** (`COPY (SELECT ...) TO STDOUT WITH
  (FORMAT binary)`). Saltea overhead por fila del protocolo; proxima
  palanca para bulk-export arriba del techo de ~165 MB/s.
- **Decoder binario de numeric**. Texto funciona hoy; binario ahorraria
  un paso de validacion para workloads con muchos numerics.
- **Tag v0.1.0** + CHANGELOG.md.

## Despues

- Decoders binarios de inet / cidr / macaddr / enum (el fallback texto
  funciona).
- Wrapper opcional de zstd / gzip para la API de streaming.
- Negociacion de protocolo al conectar (3.1+ / grease).
- Escape SIMD real de strings JSON (AVX2 + NEON) via assembly Go.
- Modo pipeline (`SendBatch`-equivalente) para muchas queries chicas
  por request.

## No planeado

- LISTEN / NOTIFY.
- Protocolo de replicacion.
- Features de ORM.
- Path generico de "scan into anything".

pgz gana por foco. Si necesitas alguno de estos, usa pgx en paralelo.
