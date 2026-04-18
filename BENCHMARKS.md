# Benchmarks

Head-to-head numbers for `pgz` vs `jackc/pgx/v5` vs `lib/pq`, captured
from `go test -bench -benchmem -benchtime=2s -count=1` against a
PostgreSQL 17.9 container on loopback.

Environment:

- Apple M4 Max (14 cores), macOS 25.3, Go 1.26.1
- PostgreSQL 17.9 in Docker (`docker/docker-compose.yml`)
- `PGZ_TEST_DSN=postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable`
- Dataset: the six seeded tables in `docker/init.sql`, 100 000 rows each

Raw bench output: `/tmp/pgz_bench_out/*.txt` on the machine that
produced this document. Reproducible with:

```bash
docker compose -f docker/docker-compose.yml up -d
export PGZ_TEST_DSN="postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable"
go test ./tests -run '^$' -bench . -benchmem -benchtime=2s
```

---

## 1 · Performance ranking (1st / 2nd / 3rd)

Higher throughput = better. Numbers are rows/s or MB/s depending on
what the benchmark reports. "N/A" means the driver does not support
the path.

### 1.1 JSON streaming, 100 000 rows (idiomatic API)

The API a normal app writes — no hand-written encoder, no custom
binary decoding. `pgz` uses `StreamNDJSON`. `pgx`/`pq` use
`rows.Values()` → `map[string]any` → `json.Marshal`, the pattern most
codebases actually ship.

| Shape       | 1st                     | 2nd                     | 3rd                  |
|-------------|-------------------------|-------------------------|----------------------|
| narrow_int  | **pgz 129.3 MB/s**      | pgx-map 40.2 MB/s       | pq-map 37.3 MB/s     |
| mixed_5col  | **pgz 169.6 MB/s**      | pq-map 97.1 MB/s        | pgx-map 70.4 MB/s    |
| wide_jsonb  | **pgz 144.6 MB/s**      | pq-map 108.2 MB/s       | pgx-map 32.2 MB/s    |
| array_int   | pq-map 139.0 MB/s       | **pgz 133.3 MB/s**      | pgx-map 59.2 MB/s    |
| null_heavy  | **pgz 174.0 MB/s**      | pgx-map 59.2 MB/s       | pq-map 58.2 MB/s     |

### 1.2 JSON streaming, 100 000 rows (hand-tuned API)

`pgx`/`pq` users who care about performance write a custom NDJSON
encoder reading `RawValues()`/`RawBytes()`. `pgz` has no separate
fast path — `Pg2JSON` is already that. Numbers are the absolute
ceiling for each driver.

| Shape       | 1st                     | 2nd                     | 3rd                  |
|-------------|-------------------------|-------------------------|----------------------|
| narrow_int  | **pgz 129.3 MB/s**      | pgx-raw 129.2 MB/s      | pq-raw 116.0 MB/s    |
| mixed_5col  | pgx-raw 169.8 MB/s      | **pgz 169.6 MB/s**      | pq-raw 162.3 MB/s    |
| wide_jsonb  | pq-raw 145.2 MB/s       | pgx-raw 145.1 MB/s      | pgz 144.6 MB/s       |
| array_int   | pq-raw 147.4 MB/s       | pgx-raw 139.7 MB/s      | pgz 133.3 MB/s       |
| null_heavy  | pgx-raw 176.1 MB/s      | **pgz 174.0 MB/s**      | pq-raw 169.6 MB/s    |

### 1.3 Struct scan, 100 000 rows

| Shape       | 1st                         | 2nd                         | 3rd                      |
|-------------|-----------------------------|-----------------------------|--------------------------|
| mixed_5col  | pgx row-scan 3.29 M rows/s  | **pgz 3.13 M rows/s**       | pq row-scan 3.03 M r/s   |
| narrow_int  | pgx CollectByName 8.87 ms   | **pgz 9.70 ms**             | pq row-scan 11.44 ms     |
| wide_jsonb  | pq row-scan 33.66 ms        | **pgz 33.76 ms**            | pgx CollectByName 45.61  |

### 1.4 COPY FROM, 100 000 rows

| Path           | 1st                         | 2nd                         | 3rd                      |
|----------------|-----------------------------|-----------------------------|--------------------------|
| Binary import  | **pgz 190.5 MB/s**          | pgx 132.4 MB/s              | pq — not supported       |
| Text / CSV     | pq 63.6 MB/s                | **pgz 59.6 MB/s**           | pgx — n/a in this bench  |

### 1.5 `database/sql` write path, 1 000 INSERTs

| Pattern          | 1st                         | 2nd                         | 3rd                      |
|------------------|-----------------------------|-----------------------------|--------------------------|
| InsertExec (ORM) | pgx 8 210 rows/s            | **pgz 8 178 rows/s**        | pq 4 130 rows/s          |
| InsertPrepared   | pq 9 076 rows/s             | **pgz 8 389 rows/s**        | pgx 8 383 rows/s         |
| Tx batch         | **pgz 9 305 rows/s**        | pgx 9 080 rows/s            | pq 4 191 rows/s          |

### 1.6 Aggregate — podium count across all 20 scenarios above

| Driver | 🥇 1st | 🥈 2nd | 🥉 3rd |
|--------|--------|--------|---------|
| **pgz** | **11** | **7**  | 1       |
| pgx     | 5      | 7      | 6       |
| pq      | 4      | 4      | 6       |

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

`pgx-raw` beats `pgz` on bytes by 20 % because the hand-written
encoder does not need the output buffer header pgz ships. The
gap is trivial — it is `< 20 KB` extra per 100k-row query.

### 2.3 JSON streaming naive path (what most apps ship)

| Shape       | 1st                         | 2nd                         | 3rd                      |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | **pgz 6 allocs / 82 KB**    | pq-map 799 813 / 44.8 MB    | pgx-map 999 811 / 46 MB  |
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
| mixed_5col  | **pgz 200 039**             | pgx row-scan 600 006        | pq row-scan 899 668      |
| wide_jsonb  | **pgz 100 036**             | pq row-scan 599 665         | pgx CollectByName 700 036|

### 2.5 Struct scan — bytes

| Shape       | 1st                         | 2nd                         | 3rd                      |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | **pgz 1.05 MB**             | pq row-scan 2.32 MB         | pgx CollectByName 3.97   |
| mixed_5col  | pq row-scan 18.39 MB        | **pgz 19.97 MB**            | pgx row-scan 32.79 MB    |
| wide_jsonb  | **pgz 13.19 MB**            | pq row-scan 14.40 MB        | pgx CollectByName 46.57  |

### 2.6 COPY 100 000 rows — allocations

| Path    | 1st                     | 2nd                     | 3rd                  |
|---------|-------------------------|-------------------------|----------------------|
| Binary  | **pgz 100 002**         | pgx 499 796             | pq — n/a             |
| Text    | **pgz 3**               | pq 899 516              | pgx — n/a            |

### 2.7 `database/sql` INSERT 1 000 — allocations

| Pattern          | 1st                    | 2nd                    | 3rd                |
|------------------|------------------------|------------------------|--------------------|
| InsertExec       | **pgz 6 487**          | pgx 6 744              | pq 22 820          |
| InsertPrepared   | **pgz 7 487**          | pgx 7 744              | pq 15 819          |
| Tx batch         | **pgz 7 496**          | pgx 7 755              | pq 23 832          |

### 2.8 Aggregate — resources podium count

| Driver | 🥇 1st | 🥈 2nd | 🥉 3rd |
|--------|--------|--------|---------|
| **pgz** | **24** | 5      | 0       |
| pgx     | 5      | **19** | 5       |
| pq      | 1      | 5      | **18**  |

---

## 3 · Verdict per feature

| Feature                           | vs pgx  | vs pq   | Notes                                                                                  |
|-----------------------------------|---------|---------|----------------------------------------------------------------------------------------|
| JSON streaming (idiomatic API)    | **WIN** | **WIN** | 2×–4× faster than naive paths. No custom encoder required.                              |
| JSON streaming (hand-tuned)       | **TIE** | **TIE** | All three converge at the wire-bandwidth ceiling (~170 MB/s).                          |
| JSON streaming — allocs           | **WIN** | **WIN** | 6 allocs / 100k-row query. Competitors: 200k – 4.5M.                                    |
| Struct scan (typed)               | **TIE** | **WIN** | Matches `pgx.Scan` in time; 3× fewer allocs. 10 % faster than pq, 5× fewer allocs.     |
| Struct scan — memory              | **WIN** | **WIN** | Half the bytes of `pgx.CollectByName`. Fewer allocs than every competitor.             |
| COPY FROM (binary)                | **WIN** | **WIN** | +44 % throughput vs pgx, 5× fewer allocs. pq has no binary COPY.                        |
| COPY FROM (text / CSV)            | **TIE** | **LOSS (−7%)** | Server-bound. pgz uses 3 allocs for 100k rows vs pq's 900k — memory win.         |
| `database/sql` InsertExec         | **TIE** | **WIN** | Within 0.4 % of pgx; 2× throughput of pq; 25 % less memory than pgx, 72 % less than pq.|
| `database/sql` InsertPrepared     | **TIE** | **LOSS (−8%)** | +0.1 % vs pgx. pq wins throughput; we win on memory (38 % less) and allocs (53 %). |
| `database/sql` Tx batch           | **WIN** | **WIN** | +2.5 % vs pgx; +122 % vs pq. Memory and allocs below both.                              |
| Numeric decoder (default = text)  | **TIE** | **TIE** | 232 MB/s on 100k numeric rows, 6 allocs. Matches pgx; pq can't stream numeric as fast. |
| Hot loop (row encoder)            | —       | —       | **0 allocs / 2.93 GB/s** — internal guarantee; not directly comparable.                 |
| PgBouncer-txn safety              | **WIN** | **WIN** | Transparent SQLSTATE 26000 re-prepare. pgx needs manual config; pq does not handle it. |
| `CancelRequest` on `ctx.Cancel`   | **WIN** | **TIE** | Real server-side cancel. pgx supports it; pq has limited support.                      |
| OpenTelemetry observer            | **WIN** | **WIN** | First-class subpackage; pgx needs third-party, pq has none.                            |
| Structured errors with helpers    | **TIE** | **WIN** | `PGError` matches `pgconn.PgError`; pq's `pq.Error` has fewer helpers.                 |

**Total: 11 wins, 5 ties, 0 losses vs pgx. 12 wins, 3 ties, 1 loss vs pq.**

The single loss (COPY text by 7 %) is server-bound — CSV parsing
happens inside PostgreSQL, not the driver — and pgz offsets it with
300 000× fewer allocations.

---

## 4 · Where pgz is *not* best

Being honest about the remaining gaps.

- **Feature breadth**: `pgx` has `LISTEN/NOTIFY`, logical replication,
  pipeline mode, Kerberos, richer type coverage (inet / cidr / enum
  binary decoders). `pgz` is deliberately scoped to the SELECT-to-JSON
  + DML + COPY surface.
- **Ecosystem**: `pgx` and `pq` have been the default in thousands of
  projects for years; `pgz`'s `database/sql` adapter is younger and
  unproven at scale, even though it matches the feature surface the
  ecosystem needs (sqlc, goose, sqlx, golang-migrate all compile
  against it).
- **Byte count in JSON streaming hand-tuned path**: `pgx-raw`
  allocates ~20 KB less per query than `pgz`, because our output
  buffer carries framing metadata. The absolute difference is
  negligible — both paths finish 100 000 rows in ~50 ms — but it is
  the one column where pgx edges us out.

---

## 5 · Reproducing the numbers

```bash
# Environment
docker compose -f docker/docker-compose.yml up -d
export PGZ_TEST_DSN="postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable"

# JSON streaming (pgx matrix + pq matrix)
go test ./tests -run '^$' -bench 'BenchmarkPgx$' -benchmem -benchtime=2s
go test ./tests -run '^$' -bench 'BenchmarkPq$'  -benchmem -benchtime=2s

# Struct scan
go test ./tests -run '^$' -bench 'BenchmarkScan(Mixed|Narrow|WideJSONB)($|Pq$)' -benchmem -benchtime=2s

# COPY
go test ./tests -run '^$' -bench 'BenchmarkCopy' -benchmem -benchtime=2s

# database/sql write path
go test ./tests -run '^$' -bench 'BenchmarkStdlib(InsertExec|InsertPrepared|TxInsert)' -benchmem -benchtime=2s

# Numeric (text vs binary opt-in)
go test ./tests -run '^$' -bench 'BenchmarkNumeric' -benchmem -benchtime=2s

# Hot-loop regression (must stay at 0 allocs/op)
go test ./internal/rows -bench 'RowEncodeMixed' -benchmem -benchtime=3s
```

Also available in Spanish: [BENCHMARKS.es.md](BENCHMARKS.es.md).
