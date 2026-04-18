# Architecture

This document explains the design of pgz step by step. It covers why the
code is structured the way it is, where the performance wins come from,
and which tradeoffs were made deliberately.

---

## 1. What pgz is

A PostgreSQL driver for Go that turns query results into JSON bytes (or
typed Go structs) with minimal allocations. It also executes DML
statements (INSERT, UPDATE, DELETE, CALL) natively. The deployment target
is a data gateway in front of Citus + PgBouncer in transaction mode.

It is not an ORM. It is not `database/sql`-compatible by default (though
an adapter exists). It wins by doing less.

---

## 2. Layer diagram

```
+-------------------------------------------------------------------+
| pgz (public API)                                                  |
|   Client · Open/Close · Query/Stream · Exec/ExecReturning         |
|   ScanStruct · Config · Observer · Pool                           |
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

Everything below `pgz/` is in `internal/`. The public surface is small:
`Client`, `Config`, `ExecResult`, `Observer`, `pool.Pool`,
`ResponseTooLargeError`, and the Mode / Stats types.

---

## 3. Wire layer (`internal/wire`)

The wire layer owns the `net.Conn`, a 32 KiB `bufio.Reader`, and a
single growable `readBuf` for assembling message bodies.

`ReadMessage()` returns the type byte and a slice into `readBuf`. The
slice is valid only until the next `ReadMessage` call. Every caller
respects this contract.

Two key allocation reductions:

1. **No per-message allocation.** `readBuf` is a single growing slab.
   The returned slice aliases it directly.
2. **Buffered reads.** Without `bufio`, each `ReadMessage` made two
   `io.ReadFull` syscalls (header + body). For 100k narrow rows that
   meant 200k context switches. The bufio layer amortizes this to ~1
   read per 32 KB. On a narrow-int query this was the difference between
   20 MB/s and 100 MB/s.

Writes go through a per-connection write buffer. Length prefixing is
computed by patching the slice in place after the body is built.

---

## 4. Protocol parsing (`internal/protocol`)

A small package of constants:

- Frontend/backend message type codes, mirrored from
  `src/include/libpq/protocol.h`.
- Authentication sub-codes (`AuthOK`, `AuthSASL`, etc.).
- A curated list of type OIDs covering every type with a specialized
  encoder. `ArrayElem(oid)` maps array OIDs to element OIDs so the
  binary decoder picks the right per-element function.

---

## 5. Encoder plan (`internal/rows`)

After receiving a `RowDescription`, pgz walks the column descriptors and
builds a plan:

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

When opening an extended-protocol `Bind`, pgz requests binary format
for every column with a specialized binary encoder. The plan's `Encoder`
is swapped for the binary variant. Text format remains as a fallback.

---

## 6. JSON writer (`internal/jsonwriter`)

A set of `Append*` functions that write into a caller-supplied `[]byte`.
No writer struct. Passing the slice around lets the compiler keep the
header in registers, matching the `strconv.AppendInt` idiom.

String escaping uses a 256-entry lookup table (`escapeFlag`). The hot
loop walks input, accumulates a "copy span", and only branches to the
slow path when an escapable byte appears. Plain ASCII passes through
with one `append`.

### Optional SWAR path

Under the `pgz_simd` build tag, the scalar escape is replaced by a
SWAR (SIMD-Within-A-Register) implementation that checks 8 bytes at
once using `uint64` arithmetic. When a chunk is entirely escape-free,
the loop advances by 8 with no per-byte branch. Measured ~4x faster on
medium/long ASCII. Pure Go, compiles everywhere.

---

## 7. Type encoders (`internal/types`)

One file per family:

- **`encoder.go`** -- text-format encoders. Numbers and booleans pass
  through validated (the server's text form is already valid JSON).
  Strings are escaped through `jsonwriter`.
- **`binary.go`** -- binary encoders for int2/4/8, float4/8, bool, uuid,
  json, jsonb (strips version byte), bytea, date, timestamp,
  timestamptz. Timestamps decode directly from `int64` microseconds to
  ISO-8601 without going through `time.Time`.
- **`interval.go`** -- interval/time/timetz. Interval emits ISO-8601
  duration. Trailing fractional zeros are stripped.
- **`array.go`** -- recursive multi-dim binary array decoder. Reads the
  array header, dispatches each element through the scalar encoder.
  SQL NULL elements become JSON `null`.
- **`numeric.go`** -- text-format passthrough with fast validation.
  `NaN`/`Infinity` are routed to JSON strings.
- **`bytea.go`** -- `bytea_output = hex` is emitted as `"\\x..."`.
- **`jsonpass.go`** -- json/jsonb passed through verbatim. jsonb binary
  has a 1-byte version prefix which is checked and stripped.
- **`uuid.go`** -- hex-formatted directly from 16 raw bytes without
  `encoding/hex` (~4x faster).

The encoder signature:

```go
type Encoder func(dst []byte, raw []byte) []byte
```

`raw == nil` means SQL NULL. Every encoder handles it.

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

`cells[i]` is a slice into the wire scratch buffer. No copy between the
socket and here. The loop allocates zero bytes, enforced by a benchmark
that must report `0 allocs/op` on every change.

---

## 9. Streaming

`StreamJSON` and `StreamNDJSON` build a `flushingWriter` around the
caller's `io.Writer`:

- Flushes when the buffer crosses `Config.FlushBytes` (default 32 KiB)
- Flushes when `Config.FlushInterval` has elapsed since last flush
  (inline check, no goroutine, no timer)

Flushes happen on row boundaries only, so NDJSON line scanners never
see a split row.

**Key invariant:** no bytes leave the writer before `BindComplete` +
the first `DataRow`. A query that fails before any row writes zero bytes
downstream.

---

## 10. DML path (`Exec` / `ExecReturning`)

`Exec` uses the same extended-query protocol as SELECT but expects no
`DataRow` messages. It parses the `CommandComplete` tag to extract
`RowsAffected`. The statement cache is shared -- PgBouncer-txn
re-prepare works identically.

`ExecReturning` handles DML with `RETURNING` clauses. On the wire, a
`INSERT ... RETURNING *` produces `RowDescription + DataRow* +
CommandComplete` -- identical to a SELECT. `ExecReturning` reuses the
full JSON streaming path, bypassing the SELECT-only guard that protects
the `Query`/`Stream` methods.

---

## 11. Cancellation

`context.Context` cancellation is handled by a watcher goroutine for
the duration of each query. On cancel:

1. Opens a side TCP connection to the same server.
2. Sends a `CancelRequest` with the backend key data. The server aborts
   with SQLSTATE 57014 (`query_canceled`).
3. Sets a past deadline on the main socket so in-flight reads return
   immediately.

The watcher is torn down on normal query completion.

---

## 12. Prepared statement cache and PgBouncer-txn

The extended-query path caches prepared statements by SQL text. Cache
miss pays for `Parse + Describe`; hit goes straight to
`Bind + Execute + Sync`.

PgBouncer in transaction mode rotates the physical backend between
transactions. A cached statement name from a previous transaction
won't exist on the new backend, and `Bind` fails with SQLSTATE 26000.
The cache detects this, invalidates the entry, and retries with a fresh
`Parse` -- but only if no bytes have been flushed downstream yet.

---

## 13. Serialization retry (Citus)

SQLSTATE 40001 (serialization_failure) and 40P01 (deadlock_detected)
are retried transparently when `Config.RetryOnSerialization = true` and
no bytes have been committed. Up to 3 attempts, exponential backoff
from 10ms.

---

## 14. Timeout model

Three layers, documented so they are not confused:

1. **`postgresql.conf` `statement_timeout`** -- DBA-owned hard ceiling.
   Final say. Protects against misbehaving clients.
2. **`Config.DefaultQueryTimeout`** -- gateway-level default applied
   when the caller's ctx has no deadline. Fires a real `CancelRequest`.
3. **`ctx.WithTimeout` at the HTTP handler** -- per-request override.
   Wins if shorter than (2).

Layers 2 and 3 are convenience. Layer 1 is security.

---

## 15. Hard caps per response

`Config.MaxResponseBytes` and `Config.MaxResponseRows` trigger a
`CancelRequest` + drain when crossed. The error is
`*ResponseTooLargeError`. The `Committed` field tells the caller
whether partial JSON reached downstream.

The check is one branch per row, outside the per-cell hot loop.

---

## 16. Connection pool (`pgz/pool`)

Bounded LIFO pool with:

- `Acquire(ctx)` blocks on an empty-and-full pool
- `MaxConnLifetime` -- closes and reopens past the cap
- `PingAfterIdle` -- validates idle conns before handing out
- `Discard()` -- retires poisoned conns
- Transparent retry (up to 3x) for lifetime and ping failures
- `Drain(ctx)` / `WaitIdle(ctx)` for graceful shutdown
- `Stats()` snapshot: Open, Idle, InUse, Waiting, Max

---

## 17. Observer

Single-struct hook with:

- `OnQueryStart(sql)` -- before wire traffic
- `OnQueryEnd(QueryEvent)` -- always, with duration/bytes/rows/error/SQLSTATE
- `OnQuerySlow(QueryEvent)` -- when `SlowQueryThreshold` is exceeded
- `OnNotice(*pgerr.Error)` -- every NoticeResponse

Implementations must be safe for concurrent use. Default no-op adds
zero overhead. Atomic counters (`Stats()`) are a lock-free alternative.

---

## 18. Tradeoffs

- **No reflection anywhere.** Encoders are picked by OID with a switch.
- **No `database/sql` adapter in the hot path.** The adapter exists for
  drop-in compatibility but adds interface boxing overhead.
- **Stdlib only at runtime.** SCRAM uses hand-rolled PBKDF2. pgx is a
  dev dependency (benchmarks only).
- **SELECT methods reject DML.** The JSON-corruption-free guarantee
  depends on this. DML goes through `Exec`/`ExecReturning`.
- **Buffer-pool backpressure is soft.** When the cap is hit, `Get`
  returns a fresh non-pooled buffer. Blocking would add latency.

---

## 19. What is not here (and why)

- **COPY fast-export** -- on the roadmap, would beat extended-query for
  bulk exports.
- **LISTEN / NOTIFY / replication** -- different workload.
- **ORM / struct mapping beyond ScanStruct** -- different tool.
- **Numeric binary decoder** -- text form is already JSON-number-shaped.

---

## 20. Performance budget

A single-row encode on a 6-column mixed shape lives inside ~55 ns and
allocates 0 bytes. At that budget, the ceiling is the wire protocol
itself: on loopback we saturate at ~165 MB/s of JSON output, matching a
hand-tuned pgx+RawValues implementation within +/-2%. The next lever is
COPY binary, not further cell-encoder optimization.
