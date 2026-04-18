# Changelog

Format loosely based on Keep a Changelog. This project follows
Semantic Versioning.

## v0.1.0 — 2026-04-18

First tagged release. Full feature inventory in
[ROADMAP.md](ROADMAP.md) under "Shipped". Headline capabilities:

### Wire + protocol
- TCP + TLS (`disable / prefer / require / verify-ca / verify-full`).
- Auth: trust, cleartext, MD5, SCRAM-SHA-256 (stdlib-only PBKDF2,
  RFC 7677 vector validated).
- Simple Query and Extended Query
  (`Parse` / `Describe` / `Bind` / `Execute` / `Sync`).
- Per-connection prepared statement cache, bounded, batched Close.
- PgBouncer-txn safe: transparent re-`Parse` on SQLSTATE 26000 when
  no bytes have been flushed.
- Real `CancelRequest` on a side connection, driven by
  `context.Cancel`.
- TCP keepalive and configurable bufio reader.

### Types
- Binary decoders: bool, int2/4/8, oid, float4/8, uuid, json, jsonb,
  bytea, date, timestamp, timestamptz, interval, time, timetz, arrays
  (multi-dim), opt-in numeric.
- Text fallback for everything else.
- Per-shape compiled column encoder plan, 0 allocs/row in the hot
  loop.

### Output
- JSON modes: array of objects, NDJSON, columnar, TOON.
- Streaming `io.Writer` API with byte/time flush thresholds.
- Buffered `[]byte` API.
- Header deferred to first DataRow, so failed queries write zero
  bytes downstream.

### DML
- `Exec` (INSERT / UPDATE / DELETE / CALL / DDL) returning
  `ExecResult`.
- `ExecReturning` (streams RETURNING as NDJSON) and
  `ExecReturningJSON` (buffered).

### COPY
- `CopyFrom` and `CopyFromBinary` (typed `CopyWriter`, in-place
  framing).
- `CopyTo` (text pump, no per-row allocation) and `CopyToBinary`
  (typed `CopyReader`).

### Pipeline
- `NewBatch` + `Client.SendBatch`: one TCP write for all queued
  statements, per-batch Parse cache promoted into the persistent
  statement cache.

### Struct scan
- `ScanStruct[T]` and memory-bounded `ScanStructBatched[T]`.
- Supports scalars, `*T`, `sql.Null*`, `sql.Scanner`, arrays (1-D and
  2-D), embedded structs, range types, composite types.

### Errors
- `pgz.PGError` exposing all wire fields.
- SQLSTATE predicate helpers (`IsUniqueViolation`,
  `IsForeignKeyViolation`, `IsSerializationFailure`, `IsDeadlock`,
  `IsQueryCanceled`, etc.).
- `SQLState()` / `SQLStateClass()` accessors, `errors.As`-compatible.

### Hardening
- `MaxResponseBytes` / `MaxResponseRows` with `CancelRequest` + drain
  and `*ResponseTooLargeError`.
- Opt-in retry on SQLSTATE 40001 / 40P01
  (`Config.RetryOnSerialization`).
- Three-layer timeout model documented in `ARCHITECTURE.md`.

### Observability
- `Observer` interface (`OnQueryStart`, `OnQueryEnd`, `OnNotice`,
  `OnQuerySlow`).
- Atomic `Stats()` counters.
- `pgz/otel` subpackage: spans + metrics (`pgz.query.duration`,
  `pgz.query.rows`, `pgz.query.errors`, `pgz.query.slow`). Zero cost
  when not imported.

### Pool
- Bounded LIFO pool with idle reaper, `MaxConnLifetime`,
  `PingAfterIdle`, `Discard()`.
- Transparent `Acquire` retry (up to 3x).
- `Drain(ctx)` and `WaitIdle(ctx)` for graceful shutdown.

### `database/sql` adapter (`pgz/stdlib`)
- Registered as `"pgz"`.
- Read, write, prepared statements, transactions with isolation level
  and read-only translation into a single `BEGIN …`.
- INSERT … RETURNING via `db.QueryRow`.
- `driver.ErrBadConn` only on wire-level failures; `PGError` and
  `ctx` cancellation leave the connection usable.
- Compatible with sqlc, goose, golang-migrate, sqlx.

### Other
- `Iterator` for lazy DataRow access.
- Optional SWAR JSON escape under `pgz_simd` build tag.
- Fuzz suite for jsonwriter, pgerr, rows.
