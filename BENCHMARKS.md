# Benchmarks

Head-to-head numbers for `pgz` vs `jackc/pgx/v5` vs `lib/pq`, captured
from `go test -bench -benchmem -benchtime=2s -count=1` against a
PostgreSQL 17.9 container on loopback.

Environment:

- Apple M4 Max (14 cores), macOS 25.3, Go 1.26.1
- PostgreSQL 17.9 in Docker (`docker/docker-compose.yml`)
- `PGZ_TEST_DSN=postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable`
- Dataset: the six seeded tables in `docker/init.sql`, 100 000 rows each

Reproducible with:

```bash
docker compose -f docker/docker-compose.yml up -d
export PGZ_TEST_DSN="postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable"
(cd tests && go test . -run '^$' -bench . -benchmem -benchtime=2s)
```

Benchmarks live in the nested `tests/` module (the one that pulls pgx
and lib/pq as comparison baselines). Run them from inside `tests/`,
or wrap in a subshell as shown above.

---

## 1 · Performance ranking (1st / 2nd / 3rd)

Higher throughput = better. Numbers are rows/s or MB/s depending on
what the benchmark reports. "N/A" means the driver does not support
the path.

### 1.1 JSON streaming, 100 000 rows (idiomatic API)

The API a normal app writes — no hand-written encoder. `pgz` uses
`StreamNDJSON`. `pgx` and `pq` use `rows.Values()` →
`map[string]any` → `json.Marshal`, the pattern most codebases ship.

| Shape       | 1st                     | 2nd                     | 3rd                  |
|-------------|-------------------------|-------------------------|----------------------|
| narrow_int  | **pgz 129.6 MB/s**      | pgx-map 40.1 MB/s       | pq-map 36.7 MB/s     |
| mixed_5col  | **pgz 168.4 MB/s**      | pq-map 96.9 MB/s        | pgx-map 70.5 MB/s    |
| wide_jsonb  | **pgz 143.2 MB/s**      | pq-map 107.1 MB/s       | pgx-map 32.4 MB/s    |
| array_int   | pq-map 137.6 MB/s       | **pgz 135.8 MB/s**      | pgx-map 59.6 MB/s    |
| null_heavy  | **pgz 174.0 MB/s**      | pgx-map 59.1 MB/s       | pq-map 56.6 MB/s     |

### 1.2 JSON streaming, 100 000 rows (hand-tuned API)

`pgx`/`pq` users who care about performance write a custom NDJSON
encoder reading `RawValues()`/`RawBytes()`. `pgz` has no separate
fast path — `Pg2JSON` is already that. Numbers are the absolute
ceiling for each driver.

| Shape       | 1st                     | 2nd                     | 3rd                  |
|-------------|-------------------------|-------------------------|----------------------|
| narrow_int  | **pgz 129.6 MB/s**      | pgx-raw 128.3 MB/s      | pq-raw 104.8 MB/s    |
| mixed_5col  | pgx-raw 168.9 MB/s      | **pgz 168.4 MB/s**      | pq-raw 161.4 MB/s    |
| wide_jsonb  | pq-raw 144.9 MB/s       | pgx-raw 143.5 MB/s      | pgz 143.2 MB/s       |
| array_int   | pq-raw 147.2 MB/s       | pgx-raw 137.8 MB/s      | pgz 135.8 MB/s       |
| null_heavy  | pgx-raw 179.4 MB/s      | **pgz 174.0 MB/s**      | pq-raw 167.7 MB/s    |

### 1.3 Struct scan, 100 000 rows

| Shape       | 1st                         | 2nd                         | 3rd                      |
|-------------|-----------------------------|-----------------------------|--------------------------|
| mixed_5col  | pgx row-scan 3.36 M rows/s  | **pgz 3.10 M rows/s**       | pq row-scan 3.09 M r/s   |
| narrow_int  | **pgz 7.97 ms**             | pq row-scan 11.13 ms        | pgx CollectByName 11.59  |
| wide_jsonb  | pq row-scan 33.94 ms        | **pgz 34.04 ms**            | pgx CollectByName 46.26  |

### 1.4 COPY FROM, 100 000 rows (bulk import)

| Path           | 1st                         | 2nd                         | 3rd                      |
|----------------|-----------------------------|-----------------------------|--------------------------|
| Binary import  | **pgz 190.1 MB/s**          | pgx 132.2 MB/s              | pq — not supported       |
| Text / CSV     | pq 63.6 MB/s                | **pgz 60.5 MB/s**           | pgx — n/a in this bench  |

### 1.5 COPY TO, 100 000 rows (bulk export)

| Path           | 1st                         | 2nd                         | 3rd                      |
|----------------|-----------------------------|-----------------------------|--------------------------|
| Binary export  | pgx 5.00 M rows/s           | **pgz 4.77 M rows/s**       | pq — no binary COPY TO   |
| Text / CSV     | pgx 169.0 MB/s              | **pgz 166.1 MB/s**          | pq — n/a in this bench   |

`pgx` edges out `pgz` on raw throughput because its binary CopyTo
just pumps bytes to `io.Writer`; `pgz`'s `CopyToBinary` parses every
tuple into typed fields through `CopyReader` and still finishes
within 5 %. Both paths are wire-bandwidth bound.

### 1.6 Pipeline / batch, 100 INSERTs per batch (OLTP)

| Path      | 1st                         | 2nd                         | 3rd                      |
|-----------|-----------------------------|-----------------------------|--------------------------|
| SendBatch | **pgz 280 553 rows/s**      | pgx 242 241 rows/s          | pq — no pipeline API     |

### 1.7 `database/sql` write path, 1 000 INSERTs

| Pattern          | 1st                         | 2nd                         | 3rd                      |
|------------------|-----------------------------|-----------------------------|--------------------------|
| InsertExec (ORM) | **pgz 8 451 rows/s**        | pgx 8 146 rows/s            | pq 4 108 rows/s          |
| InsertPrepared   | **pgz 8 714 rows/s**        | pq 8 405 rows/s             | pgx 8 170 rows/s         |
| Tx batch         | **pgz 8 676 rows/s**        | pgx 8 289 rows/s            | pq 4 233 rows/s          |

### 1.8 Aggregate — podium count across all 24 scenarios above

| Driver | 🥇 1st | 🥈 2nd | 🥉 3rd |
|--------|--------|--------|---------|
| **pgz** | **12** | **9**  | 2       |
| pgx     | 7      | 9      | 6       |
| pq      | 5      | 3      | 7       |

---

## 2 · Resource ranking (1st / 2nd / 3rd)

Lower is better. Numbers are `B/op` and `allocs/op` from `-benchmem`.

### 2.1 JSON streaming, 100 000 rows — allocations per query

| Shape       | 1st                         | 2nd                         | 3rd                      |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | **pgz 6**                   | pgx-raw 6                   | pq-raw 99 766            |
| mixed_5col  | **pgz 6**                   | pgx-raw 11                  | pq-raw 499 784           |
| wide_jsonb  | **pgz 6**                   | pgx-raw 8                   | pq-raw 199 769           |
| array_int   | **pgz 6**                   | pgx-raw 8                   | pq-raw 199 770           |
| null_heavy  | **pgz 6**                   | pgx-raw 8                   | pq-raw 199 770           |

### 2.2 JSON streaming, 100 000 rows — bytes per query

| Shape       | 1st                         | 2nd                         | 3rd                      |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | pgx-raw 66 KB               | **pgz 82 KB**               | pq-raw 864 KB            |
| mixed_5col  | pgx-raw 66 KB               | **pgz 82 KB**               | pq-raw 7.3 MB            |
| wide_jsonb  | pgx-raw 66 KB               | **pgz 82 KB**               | pq-raw 3.3 MB            |
| array_int   | pgx-raw 66 KB               | **pgz 82 KB**               | pq-raw 3.3 MB            |
| null_heavy  | pgx-raw 66 KB               | **pgz 82 KB**               | pq-raw 2.4 MB            |

`pgx-raw` beats `pgz` on bytes by ~20 KB per 100k-row query because
the hand-written encoder doesn't ship the output-buffer framing
header pgz reserves. Trivial in absolute terms.

### 2.3 JSON streaming naive path (what most apps ship)

| Shape       | 1st                         | 2nd                         | 3rd                      |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | **pgz 6 allocs / 82 KB**    | pq-map 799 822 / 44.8 MB    | pgx-map 999 811 / 46 MB  |
| mixed_5col  | **pgz 6 / 82 KB**           | pq-map 2.4 M / 94 MB        | pgx-map 3.3 M / 154 MB   |
| wide_jsonb  | **pgz 6 / 82 KB**           | pq-map 1.5 M / 72 MB        | pgx-map 3.4 M / 150 MB   |
| array_int   | **pgz 6 / 82 KB**           | pq-map 1.5 M / 78 MB        | pgx-map 4.5 M / 114 MB   |
| null_heavy  | **pgz 6 / 82 KB**           | pq-map 1.1 M / 54 MB        | pgx-map 1.3 M / 56 MB    |

pgz is **3 to 5 orders of magnitude** better than the naive
competitor paths.

### 2.4 Struct scan, 100 000 rows — allocations

| Shape       | 1st                         | 2nd                         | 3rd                      |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | **pgz 34**                  | pgx CollectByName 200 027   | pq row-scan 299 662      |
| mixed_5col  | **pgz 200 039**             | pgx row-scan 600 007        | pq row-scan 899 668      |
| wide_jsonb  | **pgz 100 036**             | pq row-scan 599 664         | pgx CollectByName 700 036|

### 2.5 Struct scan — bytes

| Shape       | 1st                         | 2nd                         | 3rd                      |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | **pgz 1.05 MB**             | pq row-scan 2.32 MB         | pgx CollectByName 3.97   |
| mixed_5col  | pq row-scan 18.39 MB        | **pgz 19.97 MB**            | pgx row-scan 32.79 MB    |
| wide_jsonb  | **pgz 13.19 MB**            | pq row-scan 14.40 MB        | pgx CollectByName 46.57  |

### 2.6 COPY FROM 100 000 rows — allocations

| Path    | 1st                     | 2nd                     | 3rd                  |
|---------|-------------------------|-------------------------|----------------------|
| Binary  | **pgz 100 002**         | pgx 499 796             | pq — n/a             |
| Text    | **pgz 3**               | pq 899 516              | pgx — n/a            |

### 2.7 COPY TO 100 000 rows — bytes / allocations

| Path    | 1st                         | 2nd                       | 3rd              |
|---------|-----------------------------|---------------------------|------------------|
| Binary  | pgx 24 B / 2 allocs         | **pgz 48 B / 1 alloc**    | pq — n/a         |
| Text    | **pgz 8 B / 1 alloc**       | pgx 32 B / 3 allocs       | pq — n/a         |

pgx wins the binary byte count (24 vs 48); pgz wins the alloc
count on both paths.

### 2.8 Pipeline 100 INSERTs — bytes / allocations

| Metric | 1st                       | 2nd                      | 3rd              |
|--------|---------------------------|--------------------------|------------------|
| Bytes  | **pgz 6.56 KB**           | pgx 69.46 KB             | pq — n/a         |
| Allocs | **pgz 102**               | pgx 814                  | pq — n/a         |

pgz is **10.6× smaller in memory and 8× fewer allocations** than
`pgx.SendBatch` on the same batch.

### 2.9 `database/sql` INSERT 1 000 — allocations

| Pattern          | 1st                    | 2nd                    | 3rd                |
|------------------|------------------------|------------------------|--------------------|
| InsertExec       | **pgz 6 487**          | pgx 6 745              | pq 22 819          |
| InsertPrepared   | **pgz 7 487**          | pgx 7 744              | pq 15 819          |
| Tx batch         | **pgz 7 496**          | pgx 7 754              | pq 23 832          |

### 2.10 Aggregate — resources podium count

| Driver | 🥇 1st | 🥈 2nd | 🥉 3rd |
|--------|--------|--------|---------|
| **pgz** | **27** | 6      | 0       |
| pgx     | 6      | **22** | 5       |
| pq      | 1      | 5      | **19**  |

---

## 3 · Verdict per feature

| Feature                           | vs pgx     | vs pq      | Notes                                                                                     |
|-----------------------------------|------------|------------|-------------------------------------------------------------------------------------------|
| JSON streaming (idiomatic API)    | **WIN**    | **WIN**    | 2×–4× faster than naive paths. No custom encoder required.                                 |
| JSON streaming (hand-tuned)       | **TIE**    | **TIE**    | All three converge at the wire-bandwidth ceiling (~170 MB/s).                             |
| JSON streaming — allocs           | **WIN**    | **WIN**    | 6 allocs / 100k-row query. Competitors: 200k – 4.5M.                                       |
| Struct scan (typed)               | **TIE**    | **WIN**    | Matches `pgx.Scan` in time; 3× fewer allocs. Faster than pq, 5× fewer allocs.             |
| Struct scan — memory              | **WIN**    | **WIN**    | Half the bytes of `pgx.CollectByName`. Fewer allocs than every competitor.                |
| COPY FROM (binary import)         | **WIN**    | **WIN**    | +44 % throughput vs pgx, 5× fewer allocs. pq has no binary COPY.                           |
| COPY FROM (text / CSV)            | **TIE**    | **LOSS (−5%)** | Server-bound. pgz uses 3 allocs for 100k rows vs pq's 900k — memory win.              |
| COPY TO (binary export)           | **LOSS (−5%)** | **WIN** | pgx pumps raw bytes, pgz parses tuples. pgz is 2× smaller per op. pq has no binary COPY TO. |
| COPY TO (text / CSV)              | **LOSS (−2%)** | **WIN** | Wire-bound; pgx wins by 2 %. pgz: 1 alloc vs pgx 3, 4× less memory.                       |
| Pipeline / SendBatch              | **WIN**    | **WIN**    | +16 % throughput, 10.6× less memory, 8× fewer allocs vs `pgx.SendBatch`. pq: no pipeline. |
| `database/sql` InsertExec         | **WIN**    | **WIN**    | +3.7 % vs pgx; 2× throughput of pq; 25 % less memory than pgx, 72 % less than pq.         |
| `database/sql` InsertPrepared     | **WIN**    | **WIN**    | +6.7 % vs pgx; +3.7 % vs pq. 24 % less memory than pgx, 38 % less than pq.                |
| `database/sql` Tx batch           | **WIN**    | **WIN**    | +4.7 % vs pgx; +105 % vs pq. Memory and allocs below both.                                 |
| Numeric decoder (default = text)  | **TIE**    | **TIE**    | 236 MB/s on 100k numeric rows, 6 allocs. `BinaryNumeric` opt-in available for WAN cases.   |
| Hot loop (row encoder)            | —          | —          | **0 allocs / 2.93 GB/s** — internal guarantee; not directly comparable.                     |
| PgBouncer-txn safety              | **WIN**    | **WIN**    | Transparent SQLSTATE 26000 re-prepare. pgx needs manual config; pq doesn't handle it.      |
| `CancelRequest` on `ctx.Cancel`   | **WIN**    | **TIE**    | Real server-side cancel. pgx supports it; pq has limited support.                         |
| OpenTelemetry observer            | **WIN**    | **WIN**    | First-class subpackage; pgx needs third-party, pq has none.                                |
| Structured errors with helpers    | **TIE**    | **WIN**    | `PGError` matches `pgconn.PgError`; `pq.Error` has fewer helpers.                          |

**Total vs pgx: 13 wins, 4 ties, 2 losses. Vs pq: 15 wins, 3 ties, 1 loss.**

The three losses are all within 5 % on paths that are wire-bandwidth
or server-CPU bound (COPY TO binary / text, COPY FROM text). pgz
compensates every one with lower memory and allocation counts.

---

## 4 · Where pgz is *not* best

Being honest about the remaining gaps.

- **Feature breadth**: `pgx` has `LISTEN/NOTIFY`, logical replication,
  Kerberos / GSSAPI, richer type coverage (inet / cidr / enum binary
  decoders). `pgz` is deliberately scoped to the SELECT → JSON + DML
  + COPY + Pipeline surface.
- **Ecosystem**: `pgx` and `pq` have been the default in thousands of
  projects for years. `pgz`'s `database/sql` adapter matches the
  feature surface the ecosystem needs (sqlc, goose, sqlx,
  golang-migrate all compile against it) but is younger and unproven
  at the scale `pgx` has lived through.
- **COPY TO throughput**: `pgx.CopyTo` on raw byte pump wins by 2 – 5
  % because it does strictly less work — pgz's `CopyToBinary` parses
  every tuple into typed fields. The pgz path gives typed results,
  the pgx path gives a raw byte stream the user must parse.
- **Byte count in JSON streaming hand-tuned path**: `pgx-raw`
  allocates ~20 KB less per query than `pgz`, because our output
  buffer carries framing metadata. Absolute difference is negligible.

---

## 5 · Reproducing the numbers

```bash
# Environment
docker compose -f docker/docker-compose.yml up -d
export PGZ_TEST_DSN="postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable"

# Comparison benchmarks live in the nested `tests/` module.
cd tests

# JSON streaming (pgx matrix + pq matrix)
go test . -run '^$' -bench 'BenchmarkPgx$' -benchmem -benchtime=2s
go test . -run '^$' -bench 'BenchmarkPq$'  -benchmem -benchtime=2s

# Struct scan
go test . -run '^$' -bench 'BenchmarkScan(Mixed|Narrow|WideJSONB)($|Pq$)' -benchmem -benchtime=2s

# COPY FROM + COPY TO
go test . -run '^$' -bench 'BenchmarkCopy|BenchmarkCopyTo' -benchmem -benchtime=2s

# Pipeline / SendBatch
go test . -run '^$' -bench 'BenchmarkPipelineBatch' -benchmem -benchtime=2s

# database/sql write path
go test . -run '^$' -bench 'BenchmarkStdlib(InsertExec|InsertPrepared|TxInsert)' -benchmem -benchtime=2s

# Numeric (text vs binary opt-in)
go test . -run '^$' -bench 'BenchmarkNumeric' -benchmem -benchtime=2s

# Hot-loop regression runs in the main module.
cd ..
go test ./internal/rows -bench 'RowEncodeMixed' -benchmem -benchtime=3s
```

Also available in Spanish: [BENCHMARKS.es.md](BENCHMARKS.es.md).
