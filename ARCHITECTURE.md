# Architecture

This document explains the design of pgz step by step. It covers why
the code is structured the way it is, where the performance wins come
from, and which tradeoffs were made deliberately.

---

## 1. What pgz is

A PostgreSQL driver for Go that turns query results into JSON bytes
(or typed Go structs) with minimal allocations, executes DML natively,
moves bulk data via COPY in both directions, and pipelines many small
queries per request. The deployment target is a data gateway in front
of Citus + PgBouncer in transaction mode.

It is not an ORM. The `database/sql` adapter is feature-complete (both
read and write paths, transactions, prepared statements) so `sqlc`,
`goose`, `golang-migrate`, and `sqlx` compile against it, but the
native API remains the fastest path when you own the code end-to-end.

---

## 2. Layer diagram

```
+-------------------------------------------------------------------+
| pgz (public API)                                                  |
|   Client · Open/Close · Query/Stream · Exec/ExecReturning         |
|   ScanStruct · CopyFrom/CopyTo · Batch/SendBatch · Config         |
|   Observer · Pool · PGError                                       |
+-------------------------------------------------------------------+
| rows  (encoder plan · DataRow hot loop · column metadata)         |
+-------------------------------------------------------------------+
| jsonwriter            types  (per-OID text + binary encoders)     |
|   Append* funcs       · numeric · jsonpass · bytea · times        |
|   256-entry escape    · binary (int/float/bool/uuid/json/date/ts) |
|     lookup table      · array  (recursive, multi-dim)             |
|                       · interval · time · timetz                  |
+-------------------------------------------------------------------+
| protocol (message codes, OIDs, constants from PG headers)         |
+-------------------------------------------------------------------+
| wire (framed reader/writer + bufio over net.Conn + scratch buf)   |
+-------------------------------------------------------------------+
| auth (MD5 · SCRAM-SHA-256 RFC 7677 · stdlib-only PBKDF2)         |
+-------------------------------------------------------------------+
| pgerr (ErrorResponse + NoticeResponse decoder)                    |
+-------------------------------------------------------------------+
| bufferpool (sync.Pool of []byte · optional live cap)              |
+-------------------------------------------------------------------+
```

Everything below `pgz/` is in `internal/`. Public surface: `Client`,
`Config`, `ExecResult`, `Observer`, `PGError`, `Batch`/`BatchResults`,
`CopyWriter`/`CopyReader`, `Iterator`, `pool.Pool`,
`ResponseTooLargeError`, the Mode / Stats types, plus the
`pgz/stdlib` and `pgz/otel` subpackages.

---

## 3. Wire layer (`internal/wire`)

The wire layer owns the `net.Conn`, a 128 KiB `bufio.Reader` (default,
configurable), and a single growable `readBuf` for assembling message
bodies that overflow the bufio buffer.

`ReadMessage()` returns the type byte and a slice into either the
bufio buffer (zero-copy fast path for messages ≤ `bufio` size) or
`readBuf`. The slice is valid only until the next `ReadMessage` call.
Every caller respects this contract.

Writes go through a per-connection write buffer. Length prefixing is
computed by patching the slice in place after the body is built.
`Builder.Finish()` leaves the frame in the buffer so multiple messages
can be coalesced into one TCP write; `Send()` is `Finish + Flush`.

This pattern is what lets the pipeline/batch and COPY paths ship many
messages in a single TCP write.

---

## 4. Protocol parsing (`internal/protocol`)

Constants mirrored from `src/include/libpq/protocol.h`:

- Frontend/backend message type codes, including the COPY family
  (`CopyInResponse`, `CopyOutResponse`, `CopyData`, `CopyDone`,
  `CopyFail`).
- Authentication sub-codes (`AuthOK`, `AuthSASL`, etc.).
- A curated list of type OIDs covering every type with a specialized
  encoder. `ArrayElem(oid)` maps array OIDs to element OIDs so the
  binary decoder picks the right per-element function.

---

## 5. Encoder plan (`internal/rows`)

After receiving a `RowDescription`, pgz walks the column descriptors
and builds a plan:

```go
type Column struct {
    Name      string
    TypeOID   protocol.OID
    Format    int16
    Encoder   types.Encoder      // picked from OID + format
    KeyPrefix []byte              // pre-built `"name":`
}
```

`KeyPrefix` is built once and reused per row. The row loop becomes a
tight sequence of `append` calls with no string formatting.

`ApplyFormatsEx(formats, binaryNumeric)` swaps each column's encoder
to the binary variant when the server agreed to ship binary. The
binaryNumeric flag piggy-backs on the same dispatch: default off,
opt-in via `Config.BinaryNumeric`.

---

## 6. JSON writer (`internal/jsonwriter`)

A set of `Append*` functions that write into a caller-supplied
`[]byte`. No writer struct. Passing the slice around lets the
compiler keep the header in registers, matching the
`strconv.AppendInt` idiom.

String escaping uses a 256-entry lookup table (`escapeFlag`). The
hot loop walks input, accumulates a "copy span", and only branches
to the slow path when an escapable byte appears. Plain ASCII passes
through with one `append`.

### Optional SWAR path

Under the `pgz_simd` build tag, the scalar escape is replaced by a
SWAR (SIMD-Within-A-Register) implementation that checks 8 bytes at
once using `uint64` arithmetic. When a chunk is entirely escape-free,
the loop advances by 8 with no per-byte branch. Measured ~4× faster
on medium/long ASCII. Pure Go, compiles everywhere.

---

## 7. Type encoders (`internal/types`)

One file per family:

- **`encoder.go`** — text-format encoders. Numbers and booleans pass
  through validated. Strings are escaped through `jsonwriter`.
- **`binary.go`** — binary encoders for int2/4/8, float4/8, bool,
  uuid, json, jsonb (strips version byte), bytea, date, timestamp,
  timestamptz. Timestamps decode directly from `int64` microseconds
  to ISO-8601 without going through `time.Time`.
- **`interval.go`** — interval/time/timetz. Interval emits ISO-8601
  duration. Trailing fractional zeros are stripped.
- **`array.go`** — recursive multi-dim binary array decoder.
- **`numeric.go`** — text-format passthrough with fast validation.
  `NaN`/`Infinity` are routed to JSON strings.
- **`numeric_bin.go`** — binary numeric decoder. Not auto-selected
  unless `Config.BinaryNumeric` is true (the server's C-side
  formatter beats the Go decoder on loopback).
- **`bytea.go`**, **`jsonpass.go`**, **`uuid.go`** — domain-specific
  fast paths.

The encoder signature:

```go
type Encoder func(dst []byte, raw []byte) []byte
```

`raw == nil` means SQL NULL. Every encoder handles it.

`PickBinaryEx(oid, binaryNumeric)` and `HasBinaryEx(oid,
binaryNumeric)` are the dispatch entrypoints; the Ex suffix marks
the variant that accepts the BinaryNumeric toggle.

---

## 8. Hot row loop

The per-row loop in `internal/rows/datarow.go`:

```go
buf = append(buf, '{')
for i, col := range plan.Columns {
    if i > 0 { buf = append(buf, ',') }
    buf = append(buf, col.KeyPrefix...)
    buf = col.Encoder(buf, cells[i])
}
buf = append(buf, '}')
```

`cells[i]` is a slice into the wire scratch buffer. No copy between
the socket and here. The loop allocates zero bytes, enforced by a
benchmark that must report `0 allocs/op` on every change.

---

## 9. Streaming

`StreamJSON` and `StreamNDJSON` build a `flushingWriter` around the
caller's `io.Writer`:

- Flushes when the buffer crosses `Config.FlushBytes` (default
  32 KiB)
- Flushes when `Config.FlushInterval` has elapsed since last flush
  (inline check, no goroutine, no timer)

Flushes happen on row boundaries only, so NDJSON line scanners never
see a split row.

**Key invariant:** no bytes leave the writer before `BindComplete` +
the first `DataRow`. A query that fails before any row writes zero
bytes downstream.

---

## 10. Parameter encoding (`pgz/args.go`)

`writeBindParams(bd, oids, args)` assembles the Bind message's
parameter block using the server's `ParameterDescription` to pick
the per-parameter wire format:

- Binary for `bool`, `int2/4/8`, `oid`, `float4/8`, `bytea`,
  `timestamp`, `timestamptz`. Width chosen from the server OID,
  not the Go type — `database/sql` always hands us `int64`, which
  needs to be narrowed to int4 if the column is int4.
- Text for strings (binary format = same bytes), numeric (server's
  text formatter is faster on loopback), and any OID the server
  couldn't infer (unknown = fall back to text).

Values are appended directly into the wire builder via
`bd.Int16`/`bd.Int32`. Zero heap allocations for scalar arguments;
the previous text path allocated one short string per int/float via
`strconv.FormatInt`/`FormatFloat`.

---

## 11. DML path (`Exec` / `ExecReturning`)

`Exec` uses the same extended-query protocol as SELECT but expects
no `DataRow` messages. It parses the `CommandComplete` tag to
extract `RowsAffected`. The statement cache is shared — PgBouncer-txn
re-prepare works identically.

`ExecReturning` handles DML with `RETURNING` clauses. On the wire,
a `INSERT ... RETURNING *` produces `RowDescription + DataRow* +
CommandComplete` — identical to a SELECT. `ExecReturning` reuses
the full JSON streaming path, bypassing the SELECT-only guard.

---

## 12. COPY (`pgz/copy.go`, `pgz/copy_to.go`)

Four entrypoints, all using the simple-query protocol so they share
no state with the stmt cache:

- **`CopyFrom(ctx, sql, r io.Reader)`** — text / CSV import.
  `pumpCopyData` reads into a reusable 256 KiB slab with 5 bytes
  reserved at offset 0 for the CopyData header, then writes the
  framed block in one `WriteRaw` call per flush. No memcpy through
  `wire.Builder`.
- **`CopyFromBinary(ctx, sql, fieldCount, emit)`** — binary import
  via `CopyWriter`. The writer reserves the 2-byte tuple field-count
  prefix, lets the caller append typed fields directly into the
  outbound buffer, then patches the prefix in place. Flushes happen
  on row boundaries when the buffer crosses 256 KiB. Measured at
  +44 % throughput vs `pgx.CopyFrom`.
- **`CopyTo(ctx, sql, w io.Writer)`** — text / CSV export. The wire
  body of each `CopyData` message is written straight to `w` — the
  slice aliases the bufio buffer, zero-copy from kernel to user
  `io.Writer`.
- **`CopyToBinary(ctx, sql, fieldCount, handler)`** — binary export
  via `CopyReader`. Parses the PGCOPY header (possibly split across
  messages), then per-tuple walks fields in place. Typed readers
  (`Int4`, `Text`, `Bool`, etc.) return slices aliasing the wire
  buffer.

---

## 13. Pipeline / batch (`pgz/pipeline.go`)

`Batch.Queue(sql, args...)` accumulates items in memory.
`SendBatch(ctx, b)`:

1. Walks items and, for each unique SQL, emits `Parse + Describe`
   once (named statement), then `Bind + Execute` per occurrence.
   Within-batch deduplication and the persistent stmt cache share
   a provisional entry so second occurrences in the same batch
   already skip Parse.
2. Emits a single trailing `Sync`.
3. Flushes everything in one TCP write.

`BatchResults.Exec()` / `Query()` read the per-item response block
(ParseComplete / BindComplete / CommandComplete for DML, plus
RowDescription for Query). On the first successful CommandComplete
for a given SQL, the provisional stmt entry is promoted to the
persistent cache so the next batch (or plain `Exec`) skips
Parse + Describe entirely.

Measured at +16 % throughput, 10.6× less memory, 8× fewer
allocations than `pgx.SendBatch` on a 100-row INSERT batch.

---

## 14. Cancellation

`context.Context` cancellation is handled by a watcher goroutine
for the duration of each query. On cancel:

1. Opens a side TCP connection to the same server.
2. Sends a `CancelRequest` with the backend key data. The server
   aborts with SQLSTATE 57014 (`query_canceled`).
3. Sets a past deadline on the main socket so in-flight reads
   return immediately.

The watcher is torn down on normal query completion.

---

## 15. Prepared statement cache and PgBouncer-txn

The extended-query path caches prepared statements by SQL text.
Cache miss pays for `Parse + Describe`; hit goes straight to
`Bind + Execute + Sync`.

PgBouncer in transaction mode rotates the physical backend between
transactions. A cached statement name from a previous transaction
won't exist on the new backend, and `Bind` fails with SQLSTATE
26000. The cache detects this, invalidates the entry, and retries
with a fresh `Parse` — but only if no bytes have been flushed
downstream yet.

`SendBatch` shares this cache: the first batch containing a new SQL
adds it; subsequent batches skip Parse + Describe.

---

## 16. Serialization retry (Citus)

SQLSTATE 40001 (serialization_failure) and 40P01 (deadlock_detected)
are retried transparently when `Config.RetryOnSerialization = true`
and no bytes have been committed. Up to 3 attempts, exponential
backoff from 10 ms.

---

## 17. Errors (`pgz.PGError`)

`pgz.PGError` is a type alias for the internal ErrorResponse
decoder's struct. Every query method returns `*PGError` directly
(no wrapping) so `errors.As` lands cleanly. Predicate helpers
cover the SQLSTATEs a gateway routinely branches on:

- Integrity: `IsUniqueViolation` (23505),
  `IsForeignKeyViolation` (23503), `IsCheckViolation` (23514),
  `IsNotNullViolation` (23502), `IsExclusionViolation` (23P01),
  `IsIntegrityViolation` (class 23).
- Concurrency: `IsSerializationFailure` (40001),
  `IsDeadlock` (40P01).
- Cancel/admin: `IsQueryCanceled` (57014),
  `IsAdminShutdown` (57P01).
- PgBouncer: `IsInvalidSQLStatementName` (26000).

Plus `SQLState()` / `SQLStateClass()` for arbitrary branching.

---

## 18. Timeout model

Three layers, documented so they are not confused:

1. **`postgresql.conf` `statement_timeout`** — DBA-owned hard
   ceiling. Final say. Protects against misbehaving clients.
2. **`Config.DefaultQueryTimeout`** — gateway-level default applied
   when the caller's ctx has no deadline. Fires a real
   `CancelRequest`.
3. **`ctx.WithTimeout` at the HTTP handler** — per-request
   override. Wins if shorter than (2).

Layers 2 and 3 are convenience. Layer 1 is security.

---

## 19. Hard caps per response

`Config.MaxResponseBytes` and `Config.MaxResponseRows` trigger a
`CancelRequest` + drain when crossed. The error is
`*ResponseTooLargeError`. The `Committed` field tells the caller
whether partial JSON reached downstream.

The check is one branch per row, outside the per-cell hot loop.

---

## 20. Connection pool (`pgz/pool`)

Bounded LIFO pool with:

- `Acquire(ctx)` blocks on an empty-and-full pool
- `MaxConnLifetime` — closes and reopens past the cap
- `PingAfterIdle` — validates idle conns before handing out
- `Discard()` — retires poisoned conns
- Transparent retry (up to 3×) for lifetime and ping failures
- `Drain(ctx)` / `WaitIdle(ctx)` for graceful shutdown
- `Stats()` snapshot: Open, Idle, InUse, Waiting, Max

---

## 21. Observer and OpenTelemetry

Single-struct hook with:

- `OnQueryStart(sql)` — before wire traffic
- `OnQueryEnd(QueryEvent)` — always, with
  duration/bytes/rows/error/SQLSTATE
- `OnQuerySlow(QueryEvent)` — when `SlowQueryThreshold` is
  exceeded
- `OnNotice(*pgerr.Error)` — every NoticeResponse

Implementations must be safe for concurrent use. Default no-op
adds zero overhead.

The optional `pgz/otel` subpackage implements the Observer
interface backed by OpenTelemetry tracers and meters: one span
per query, plus four metrics (`pgz.query.duration` histogram,
`pgz.query.rows`, `pgz.query.errors`, `pgz.query.slow`
counters). OTel dependencies ship only when you import the
subpackage.

---

## 22. `database/sql` adapter (`pgz/stdlib`)

Registered as `"pgz"`. Full read + write surface:

- `Query`, `QueryContext`, `QueryRow`, `Prepare`, `Ping`.
- `Exec`, `ExecContext`, `Stmt.ExecContext`.
- `Begin`, `BeginTx` with isolation level and read-only translated
  into a single `BEGIN …` round-trip.
- INSERT … RETURNING via `QueryRow` works because the adapter
  calls `RawQueryAny` (the variant without the SELECT-only
  guard).

The adapter shares the native `pgz.Client`'s stmt cache, so
repeated `db.Exec` of the same SQL skips Parse + Describe after
the first call — matching or beating `pgx/stdlib` and `lib/pq`
on every INSERT pattern we measured.

---

## 23. Tradeoffs

- **No reflection anywhere.** Encoders are picked by OID with a
  switch.
- **Stdlib only at runtime.** SCRAM uses hand-rolled PBKDF2. pgx
  is a dev dependency (benchmarks only). OpenTelemetry is only
  pulled if you import `pgz/otel`.
- **SELECT methods reject DML by default.** `Query`/`Stream`
  guard against non-SELECT SQL; DML with RETURNING goes through
  `ExecReturning` (or `RawQueryAny` + database/sql).
- **Buffer-pool backpressure is soft.** When the cap is hit,
  `Get` returns a fresh non-pooled buffer. Blocking would add
  latency.

---

## 24. What is not here (and why)

- **LISTEN / NOTIFY / replication** — different workload; use pgx
  alongside.
- **Binary decoders for `inet`/`cidr`/`macaddr`/`enum`** — text
  fallback works and is measurably faster on loopback; revisit
  if a real WAN workload demands it.
- **Kerberos / GSSAPI** — rare in cloud-native deployments where
  pgz is aimed.
- **ORM / reflection-heavy scan** — `ScanStruct[T]` already
  covers the 95 % case with zero reflection in the hot path.

---

## 25. Performance budget

A single-row encode on a 6-column mixed shape lives inside ~45 ns
and allocates 0 bytes. At that budget, the ceiling is the wire
protocol itself: on loopback we saturate at ~170 MB/s of JSON
output, matching a hand-tuned pgx+RawValues implementation within
±2 %. COPY binary export tops out at ~190 MB/s import and
~4.8 M rows/s export. Pipeline / SendBatch reaches 280 k rows/s
on a 100-INSERT batch. See [BENCHMARKS.md](BENCHMARKS.md) for
the full matrix.
