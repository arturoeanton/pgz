# Roadmap

## Shipped

### Wire + protocol

- TCP connect with TCP keepalive (`Config.Keepalive`, default 30s).
- TLS negotiation (`SSLRequest`), modes `disable / prefer / require /
  verify-ca / verify-full`.
- Auth: trust, cleartext, MD5, SCRAM-SHA-256 (RFC 7677 vector
  validated; stdlib-only PBKDF2).
- Startup + BackendKeyData + ParameterStatus + ReadyForQuery.
- Handshake honours the caller's `ctx.Deadline()` for the full TLS +
  auth exchange.
- Simple Query (`Q`) and Extended Query
  (`Parse` / `Describe` / `Bind` / `Execute` / `Sync`).
- Prepared statement cache per connection, bounded, with server-side
  `Close` batching.
- PgBouncer-txn safe: SQLSTATE 26000 triggers transparent re-`Parse`
  when no bytes have been flushed.
- Real `CancelRequest` on a side connection driven by
  `context.Cancel`.
- bufio-backed reader (32 KiB configurable).

### Type decoders

- Binary format for: bool, int2/4/8, oid, float4/8, uuid, json, jsonb,
  bytea, date, timestamp, timestamptz.
- Interval / time / timetz binary decoders. Interval emits ISO-8601
  duration.
- Binary array decoder, recursive, multi-dim, dispatches through the
  scalar binary encoder by element OID.
- Opt-in binary `numeric` decoder via `Config.BinaryNumeric` (default
  text; binary wins on high-RTT links and wide precisions).
- Text-format fallback for everything else.
- Per-shape compiled column encoder plan, built once per
  `RowDescription`, reused for every row. Hot loop: 0 allocs/op.

### Output + streaming

- JSON output modes: array of objects, NDJSON, columnar, TOON.
- Direct JSON writer with RFC 8259 escaping and 256-entry lookup
  table.
- Streaming `io.Writer` API flushing by byte threshold and elapsed
  time.
- Buffered `[]byte` API.
- Header deferred until `BindComplete + first DataRow`.

### Parameters

- Binary parameter encoding for bool, int2/4/8, oid, float4/8, bytea,
  timestamp, timestamptz. Text fallback for strings, numeric, and
  unknown OIDs.
- OID-driven: uses the server's `ParameterDescription` to pick the
  correct binary width per parameter. Zero allocations per scalar
  argument.

### DML support

- `Exec(ctx, sql, args...)` for INSERT / UPDATE / DELETE / CALL / DDL.
  Returns `ExecResult` with `RowsAffected` and raw `CommandComplete`
  tag.
- `ExecReturning(ctx, w, sql, args...)` streams RETURNING clause
  results as NDJSON, reusing the full JSON streaming path.
- `ExecReturningJSON(ctx, sql, args...)` buffered variant.
- Statement cache and PgBouncer-txn re-prepare shared with SELECT
  path.

### COPY

- `CopyFrom(ctx, sql, r io.Reader)` — text / CSV import. Streams
  from any `io.Reader` into `CopyData` frames. 3 allocations for a
  100 000-row import.
- `CopyFromBinary(ctx, sql, fields, emit)` — binary COPY import with
  a typed `CopyWriter`. Frames tuples in place; one `CopyData`
  message per flush (no intermediate memcpy). Measured +44 %
  throughput vs `pgx.CopyFrom` on the same input.
- `CopyTo(ctx, sql, w io.Writer)` — text / CSV export. Pumps bytes
  straight to the writer (no per-row allocation).
- `CopyToBinary(ctx, sql, fields, handler)` — binary COPY export
  with a typed `CopyReader`. Parses tuples in-place from the wire
  buffer.

### Pipeline / batch

- `NewBatch()` + `Client.SendBatch(ctx, b)` ship every queued
  statement in one TCP write. Per-batch Parse caching promotes
  first-of-kind SQL into the persistent statement cache so
  subsequent items (and future batches) skip `Parse + Describe`.
- Measured +16 % throughput, 10.6× less memory, 8× fewer allocs
  than `pgx.SendBatch` on a 100-row INSERT batch.

### Struct scan

- `ScanStruct[T]` — typed struct scan from SELECT results.
- `ScanStructBatched[T]` — memory-bounded batched scan with
  callback.
- Supports: scalars, `*T` nullable, `sql.Null*`, `sql.Scanner`,
  1-D and 2-D arrays, embedded structs, range types
  (`pgz.RangeBytes`), composite types (declare OID in
  `Config.BinaryOIDs`).

### Errors

- `pgz.PGError` — structured PostgreSQL error with all wire fields
  (`Severity`, `Code`, `Message`, `Detail`, `Hint`, `Where`, raw
  `Fields` map).
- Predicate helpers: `IsUniqueViolation`, `IsForeignKeyViolation`,
  `IsCheckViolation`, `IsNotNullViolation`, `IsExclusionViolation`,
  `IsIntegrityViolation`, `IsSerializationFailure`, `IsDeadlock`,
  `IsQueryCanceled`, `IsAdminShutdown`,
  `IsInvalidSQLStatementName`.
- `SQLState()` and `SQLStateClass()` accessors. `errors.As` works
  through `fmt.Errorf %w` wrappers.

### Hardening for Citus + PgBouncer-txn

- Hard caps per response: `MaxResponseBytes` / `MaxResponseRows`
  with `CancelRequest` + drain + `*ResponseTooLargeError`.
- Retry on SQLSTATE 40001 / 40P01
  (`Config.RetryOnSerialization`).
- `DefaultQueryTimeout` with documented 3-layer timeout model.

### Observability

- `Observer` interface: `OnQueryStart`, `OnQueryEnd`, `OnNotice`,
  `OnQuerySlow`.
- Lock-free atomic `Stats()` counters.
- `pgz/otel` subpackage — OpenTelemetry-backed `Observer` with
  spans and metrics (`pgz.query.duration`, `pgz.query.rows`,
  `pgz.query.errors`, `pgz.query.slow`). Zero cost when not
  imported.

### Pool + lifecycle

- Bounded LIFO pool with idle reaper.
- `MaxConnLifetime`, `PingAfterIdle`, `Discard()`.
- Transparent `Acquire` retry (up to 3x).
- `Drain(ctx)` and `WaitIdle(ctx)` for graceful shutdown.

### `database/sql` adapter (`pgz/stdlib`)

- Registered as `"pgz"`.
- Read path: `Query`, `QueryContext`, `QueryRow`, `Prepare`,
  `Ping`.
- Write path: `Exec`, `ExecContext`, `stmt.Exec`,
  `stmt.ExecContext`.
- Transactions: `Begin`, `BeginTx` with isolation level and
  read-only translation into a single `BEGIN …` round-trip.
- INSERT … RETURNING via `db.QueryRow` works through `RawQueryAny`.
- `driver.ErrBadConn` returned only on wire-level failures;
  `PGError` and ctx cancellation leave the connection usable.
- Compatible with sqlc, goose, golang-migrate, sqlx.

### Other

- `Iterator` for lazy DataRow access (`RawQuery` / `RawQueryAny`).
- Optional SWAR JSON escape under `pgz_simd` build tag.
- Fuzz suite for jsonwriter, pgerr, rows.

---

## Next

- **CI**: GitHub Actions workflow with PG + PgBouncer service
  containers running integration suite, fuzz corpus, and hot-path
  benchmarks as regression gate.
- **Shadow-traffic soak test** (8h / 500k-user). Design targets it;
  real-world validation pending.
- **v0.1.0 tag** + CHANGELOG.md.

## Later

- inet / cidr / macaddr / enum binary decoders (text fallback
  works).
- Optional zstd / gzip writer wrapper for the streaming API.
- Connect-time protocol negotiation (3.1+ / grease).
- True SIMD JSON string escape (AVX2 + NEON) via Go assembly.
- PgError-driven auto-retry on transient classes beyond 40001 /
  40P01.

## Not planned

- LISTEN / NOTIFY.
- Replication protocol.
- ORM features.
- Generic "scan into anything" path beyond `ScanStruct`.

pgz wins by focus. If you need any of these, use pgx alongside.
