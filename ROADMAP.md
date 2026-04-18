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
- bufio-backed reader (32 KiB).

### Type decoders

- Binary format for: bool, int2/4/8, oid, float4/8, uuid, json, jsonb,
  bytea, date, timestamp, timestamptz.
- Interval / time / timetz binary decoders. Interval emits ISO-8601
  duration.
- Binary array decoder, recursive, multi-dim, dispatches through the
  scalar binary encoder by element OID.
- Text-format fallback for everything else.
- Per-shape compiled column encoder plan, built once per
  `RowDescription`, reused for every row. Hot loop: 0 allocs/op.

### Output + streaming

- JSON output modes: array of objects, NDJSON, columnar, TOON.
- Direct JSON writer with RFC 8259 escaping and 256-entry lookup table.
- Streaming `io.Writer` API flushing by byte threshold and elapsed time.
- Buffered `[]byte` API.
- Header deferred until `BindComplete + first DataRow`.

### DML support

- `Exec(ctx, sql, args...)` for INSERT / UPDATE / DELETE / CALL / DDL.
  Returns `ExecResult` with `RowsAffected` and raw `CommandComplete` tag.
- `ExecReturning(ctx, w, sql, args...)` streams RETURNING clause results
  as NDJSON, reusing the full JSON streaming path.
- `ExecReturningJSON(ctx, sql, args...)` buffered variant.
- Statement cache and PgBouncer-txn re-prepare shared with SELECT path.

### Struct scan

- `ScanStruct[T]` -- typed struct scan from SELECT results.
- `ScanStructBatched[T]` -- memory-bounded batched scan with callback.
- Supports: scalars, `*T` nullable, `sql.Null*`, `sql.Scanner`,
  1-D and 2-D arrays, embedded structs, range types (`pgz.RangeBytes`),
  composite types (declare OID in `Config.BinaryOIDs`).

### Hardening for Citus + PgBouncer-txn

- Hard caps per response: `MaxResponseBytes` / `MaxResponseRows` with
  `CancelRequest` + drain + `*ResponseTooLargeError`.
- Retry on SQLSTATE 40001 / 40P01 (`Config.RetryOnSerialization`).
- `DefaultQueryTimeout` with documented 3-layer timeout model.

### Observability

- `Observer` interface: `OnQueryStart`, `OnQueryEnd`, `OnNotice`,
  `OnQuerySlow`.
- Lock-free atomic `Stats()` counters.

### Pool + lifecycle

- Bounded LIFO pool with idle reaper.
- `MaxConnLifetime`, `PingAfterIdle`, `Discard()`.
- Transparent `Acquire` retry (up to 3x).
- `Drain(ctx)` and `WaitIdle(ctx)` for graceful shutdown.

### Other

- `database/sql` adapter (`pgz/stdlib`), registered as `"pgz"`.
- `Iterator` for lazy DataRow access (`RawQuery`).
- Optional SWAR JSON escape under `pgz_simd` build tag.
- Fuzz suite for jsonwriter, pgerr, rows.

---

## Next

- **CI**: GitHub Actions workflow with PG + PgBouncer service containers
  running integration suite, fuzz corpus, and hot-path benchmarks as
  regression gate.
- **Shadow-traffic soak test** (8h / 500k-user). Design targets it;
  real-world validation pending.
- **COPY-binary fast export** (`COPY (SELECT ...) TO STDOUT WITH
  (FORMAT binary)`). Skips per-row protocol overhead; next lever for
  bulk-export above the ~165 MB/s ceiling.
- **Numeric binary decoder**. Text works today; binary would save one
  validation pass for numeric-heavy workloads.
- **v0.1.0 tag** + CHANGELOG.md.

## Later

- inet / cidr / macaddr / enum binary decoders (text fallback works).
- Optional zstd / gzip writer wrapper for the streaming API.
- Connect-time protocol negotiation (3.1+ / grease).
- True SIMD JSON string escape (AVX2 + NEON) via Go assembly.
- Pipeline mode (`SendBatch`-equivalent) for many small queries per
  request.

## Not planned

- LISTEN / NOTIFY.
- Replication protocol.
- ORM features.
- Generic "scan into anything" path.

pgz wins by focus. If you need any of these, use pgx alongside.
