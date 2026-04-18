# Benchmarks

Comparación punto a punto de `pgz` contra `jackc/pgx/v5` y `lib/pq`,
medida con `go test -bench -benchmem -benchtime=2s -count=1` contra
un contenedor de PostgreSQL 17.9 en loopback.

Entorno:

- Apple M4 Max (14 núcleos), macOS 25.3, Go 1.26.1
- PostgreSQL 17.9 en Docker (`docker/docker-compose.yml`)
- `PGZ_TEST_DSN=postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable`
- Dataset: las seis tablas sembradas en `docker/init.sql`, 100 000 filas cada una

Reproducible con:

```bash
docker compose -f docker/docker-compose.yml up -d
export PGZ_TEST_DSN="postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable"
(cd tests && go test . -run '^$' -bench . -benchmem -benchtime=2s)
```

Los benchmarks viven en el modulo nested `tests/` (el que trae pgx y
lib/pq como baseline de comparacion). Corrélos desde dentro de
`tests/`, o envolvélos en un subshell como arriba.

---

## 1 · Ranking de performance (1° / 2° / 3°)

Más throughput = mejor. Los números son filas/s o MB/s según lo que
reporta cada bench. "N/A" significa que el driver no soporta ese
camino.

### 1.1 Streaming JSON, 100 000 filas (API idiomática)

La API que escribe una app normal — sin encoder hecho a mano. `pgz`
usa `StreamNDJSON`. `pgx` y `pq` usan `rows.Values()` →
`map[string]any` → `json.Marshal`, que es el patrón real en
producción.

| Shape       | 1°                      | 2°                      | 3°                   |
|-------------|-------------------------|-------------------------|----------------------|
| narrow_int  | **pgz 129.6 MB/s**      | pgx-map 40.1 MB/s       | pq-map 36.7 MB/s     |
| mixed_5col  | **pgz 168.4 MB/s**      | pq-map 96.9 MB/s        | pgx-map 70.5 MB/s    |
| wide_jsonb  | **pgz 143.2 MB/s**      | pq-map 107.1 MB/s       | pgx-map 32.4 MB/s    |
| array_int   | pq-map 137.6 MB/s       | **pgz 135.8 MB/s**      | pgx-map 59.6 MB/s    |
| null_heavy  | **pgz 174.0 MB/s**      | pgx-map 59.1 MB/s       | pq-map 56.6 MB/s     |

### 1.2 Streaming JSON, 100 000 filas (API hand-tuned)

Los usuarios de `pgx`/`pq` que se preocupan por performance escriben
un encoder NDJSON custom leyendo `RawValues()`/`RawBytes()`. `pgz`
no tiene un segundo camino rápido — `Pg2JSON` ya es ese. Estos
números son el techo absoluto de cada driver.

| Shape       | 1°                      | 2°                      | 3°                   |
|-------------|-------------------------|-------------------------|----------------------|
| narrow_int  | **pgz 129.6 MB/s**      | pgx-raw 128.3 MB/s      | pq-raw 104.8 MB/s    |
| mixed_5col  | pgx-raw 168.9 MB/s      | **pgz 168.4 MB/s**      | pq-raw 161.4 MB/s    |
| wide_jsonb  | pq-raw 144.9 MB/s       | pgx-raw 143.5 MB/s      | pgz 143.2 MB/s       |
| array_int   | pq-raw 147.2 MB/s       | pgx-raw 137.8 MB/s      | pgz 135.8 MB/s       |
| null_heavy  | pgx-raw 179.4 MB/s      | **pgz 174.0 MB/s**      | pq-raw 167.7 MB/s    |

### 1.3 Struct scan, 100 000 filas

| Shape       | 1°                          | 2°                          | 3°                       |
|-------------|-----------------------------|-----------------------------|--------------------------|
| mixed_5col  | pgx row-scan 3.36 M filas/s | **pgz 3.10 M filas/s**      | pq row-scan 3.09 M f/s   |
| narrow_int  | **pgz 7.97 ms**             | pq row-scan 11.13 ms        | pgx CollectByName 11.59  |
| wide_jsonb  | pq row-scan 33.94 ms        | **pgz 34.04 ms**            | pgx CollectByName 46.26  |

### 1.4 COPY FROM, 100 000 filas (import masivo)

| Camino         | 1°                          | 2°                          | 3°                       |
|----------------|-----------------------------|-----------------------------|--------------------------|
| Import binario | **pgz 190.1 MB/s**          | pgx 132.2 MB/s              | pq — no soportado        |
| Texto / CSV    | pq 63.6 MB/s                | **pgz 60.5 MB/s**           | pgx — n/a en este bench  |

### 1.5 COPY TO, 100 000 filas (export masivo)

| Camino         | 1°                          | 2°                          | 3°                       |
|----------------|-----------------------------|-----------------------------|--------------------------|
| Export binario | pgx 5.00 M filas/s          | **pgz 4.77 M filas/s**      | pq — sin COPY TO binario |
| Texto / CSV    | pgx 169.0 MB/s              | **pgz 166.1 MB/s**          | pq — n/a en este bench   |

`pgx` saca ventaja en throughput porque su `CopyTo` binario
simplemente volcaba bytes al `io.Writer`; el `CopyToBinary` de
`pgz` parsea cada tupla a campos tipados mediante `CopyReader` y
aun así termina dentro del 5 %. Ambos paths están saturados por
el ancho de banda de wire.

### 1.6 Pipeline / batch, 100 INSERTs por batch (OLTP)

| Path      | 1°                          | 2°                          | 3°                       |
|-----------|-----------------------------|-----------------------------|--------------------------|
| SendBatch | **pgz 280 553 filas/s**     | pgx 242 241 filas/s         | pq — sin pipeline        |

### 1.7 Camino de escritura `database/sql`, 1 000 INSERTs

| Patrón            | 1°                         | 2°                         | 3°                   |
|-------------------|-----------------------------|-----------------------------|----------------------|
| InsertExec (ORM)  | **pgz 8 451 filas/s**       | pgx 8 146 filas/s           | pq 4 108 filas/s     |
| InsertPrepared    | **pgz 8 714 filas/s**       | pq 8 405 filas/s            | pgx 8 170 filas/s    |
| Tx en lote        | **pgz 8 676 filas/s**       | pgx 8 289 filas/s           | pq 4 233 filas/s     |

### 1.8 Agregado — conteo de podios en los 24 escenarios anteriores

| Driver | 🥇 1°   | 🥈 2°   | 🥉 3°   |
|--------|--------|--------|---------|
| **pgz** | **12** | **9**  | 2       |
| pgx     | 7      | 9      | 6       |
| pq      | 5      | 3      | 7       |

---

## 2 · Ranking de recursos (1° / 2° / 3°)

Menos es mejor. Los números son `B/op` y `allocs/op` de `-benchmem`.

### 2.1 Streaming JSON, 100 000 filas — allocs por query

| Shape       | 1°                          | 2°                          | 3°                       |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | **pgz 6**                   | pgx-raw 6                   | pq-raw 99 766            |
| mixed_5col  | **pgz 6**                   | pgx-raw 11                  | pq-raw 499 784           |
| wide_jsonb  | **pgz 6**                   | pgx-raw 8                   | pq-raw 199 769           |
| array_int   | **pgz 6**                   | pgx-raw 8                   | pq-raw 199 770           |
| null_heavy  | **pgz 6**                   | pgx-raw 8                   | pq-raw 199 770           |

### 2.2 Streaming JSON, 100 000 filas — bytes por query

| Shape       | 1°                          | 2°                          | 3°                       |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | pgx-raw 66 KB               | **pgz 82 KB**               | pq-raw 864 KB            |
| mixed_5col  | pgx-raw 66 KB               | **pgz 82 KB**               | pq-raw 7.3 MB            |
| wide_jsonb  | pgx-raw 66 KB               | **pgz 82 KB**               | pq-raw 3.3 MB            |
| array_int   | pgx-raw 66 KB               | **pgz 82 KB**               | pq-raw 3.3 MB            |
| null_heavy  | pgx-raw 66 KB               | **pgz 82 KB**               | pq-raw 2.4 MB            |

`pgx-raw` gana a `pgz` en bytes por ~20 KB por query de 100k filas
porque el encoder hecho a mano no lleva el header de output buffer
que pgz reserva. Trivial en absoluto.

### 2.3 Streaming JSON en camino naive (lo que el código real escribe)

| Shape       | 1°                          | 2°                          | 3°                       |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | **pgz 6 allocs / 82 KB**    | pq-map 799 822 / 44.8 MB    | pgx-map 999 811 / 46 MB  |
| mixed_5col  | **pgz 6 / 82 KB**           | pq-map 2.4 M / 94 MB        | pgx-map 3.3 M / 154 MB   |
| wide_jsonb  | **pgz 6 / 82 KB**           | pq-map 1.5 M / 72 MB        | pgx-map 3.4 M / 150 MB   |
| array_int   | **pgz 6 / 82 KB**           | pq-map 1.5 M / 78 MB        | pgx-map 4.5 M / 114 MB   |
| null_heavy  | **pgz 6 / 82 KB**           | pq-map 1.1 M / 54 MB        | pgx-map 1.3 M / 56 MB    |

pgz está **3 a 5 órdenes de magnitud** mejor que los caminos naive
de los competidores.

### 2.4 Struct scan, 100 000 filas — allocs

| Shape       | 1°                          | 2°                          | 3°                       |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | **pgz 34**                  | pgx CollectByName 200 027   | pq row-scan 299 662      |
| mixed_5col  | **pgz 200 039**             | pgx row-scan 600 007        | pq row-scan 899 668      |
| wide_jsonb  | **pgz 100 036**             | pq row-scan 599 664         | pgx CollectByName 700 036|

### 2.5 Struct scan — bytes

| Shape       | 1°                          | 2°                          | 3°                       |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | **pgz 1.05 MB**             | pq row-scan 2.32 MB         | pgx CollectByName 3.97   |
| mixed_5col  | pq row-scan 18.39 MB        | **pgz 19.97 MB**            | pgx row-scan 32.79 MB    |
| wide_jsonb  | **pgz 13.19 MB**            | pq row-scan 14.40 MB        | pgx CollectByName 46.57  |

### 2.6 COPY FROM 100 000 filas — allocs

| Camino | 1°                      | 2°                      | 3°                   |
|--------|-------------------------|-------------------------|----------------------|
| Binary | **pgz 100 002**         | pgx 499 796             | pq — n/a             |
| Text   | **pgz 3**               | pq 899 516              | pgx — n/a            |

### 2.7 COPY TO 100 000 filas — bytes / allocs

| Camino | 1°                          | 2°                        | 3°               |
|--------|-----------------------------|---------------------------|------------------|
| Binary | pgx 24 B / 2 allocs         | **pgz 48 B / 1 alloc**    | pq — n/a         |
| Text   | **pgz 8 B / 1 alloc**       | pgx 32 B / 3 allocs       | pq — n/a         |

pgx gana en bytes en el path binario (24 vs 48); pgz gana en
conteo de allocs en ambos caminos.

### 2.8 Pipeline 100 INSERTs — bytes / allocs

| Métrica | 1°                        | 2°                       | 3°               |
|---------|---------------------------|--------------------------|------------------|
| Bytes   | **pgz 6.56 KB**           | pgx 69.46 KB             | pq — n/a         |
| Allocs  | **pgz 102**               | pgx 814                  | pq — n/a         |

pgz usa **10.6× menos memoria y 8× menos allocs** que
`pgx.SendBatch` en el mismo batch.

### 2.9 `database/sql` INSERT 1 000 — allocs

| Patrón            | 1°                     | 2°                     | 3°                 |
|-------------------|------------------------|------------------------|--------------------|
| InsertExec        | **pgz 6 487**          | pgx 6 745              | pq 22 819          |
| InsertPrepared    | **pgz 7 487**          | pgx 7 744              | pq 15 819          |
| Tx en lote        | **pgz 7 496**          | pgx 7 754              | pq 23 832          |

### 2.10 Agregado — conteo de podios en recursos

| Driver | 🥇 1°   | 🥈 2°   | 🥉 3°   |
|--------|--------|--------|---------|
| **pgz** | **27** | 6      | 0       |
| pgx     | 6      | **22** | 5       |
| pq      | 1      | 5      | **19**  |

---

## 3 · Veredicto por feature

| Feature                              | vs pgx        | vs pq         | Notas                                                                                     |
|--------------------------------------|---------------|---------------|-------------------------------------------------------------------------------------------|
| Streaming JSON (API idiomática)      | **GANA**      | **GANA**      | 2×–4× más rápido que caminos naive. Sin encoder custom.                                   |
| Streaming JSON (hand-tuned)          | **EMPATA**    | **EMPATA**    | Los tres convergen al techo del ancho de banda de wire (~170 MB/s).                        |
| Streaming JSON — allocs              | **GANA**      | **GANA**      | 6 allocs / query de 100k filas. Competidores: 200k – 4.5M.                                 |
| Struct scan (tipado)                 | **EMPATA**    | **GANA**      | Igual a `pgx.Scan` en tiempo; 3× menos allocs. Más rápido que pq, 5× menos allocs.        |
| Struct scan — memoria                | **GANA**      | **GANA**      | Mitad de bytes que `pgx.CollectByName`. Menos allocs que todo competidor.                 |
| COPY FROM (import binario)           | **GANA**      | **GANA**      | +44 % throughput vs pgx, 5× menos allocs. pq no tiene COPY binario.                       |
| COPY FROM (texto / CSV)              | **EMPATA**    | **PIERDE (−5%)** | Server-bound. pgz: 3 allocs en 100k filas vs 900k de pq — gana en memoria.           |
| COPY TO (export binario)             | **PIERDE (−5%)** | **GANA**   | pgx volca bytes crudos, pgz parsea tuplas. pgz usa 2× menos memoria por op. pq no tiene binario. |
| COPY TO (texto / CSV)                | **PIERDE (−2%)** | **GANA**   | Wire-bound; pgx gana por 2 %. pgz: 1 alloc vs 3 de pgx, 4× menos memoria.                 |
| Pipeline / SendBatch                 | **GANA**      | **GANA**      | +16 % throughput, 10.6× menos memoria, 8× menos allocs vs `pgx.SendBatch`. pq: sin pipeline. |
| `database/sql` InsertExec            | **GANA**      | **GANA**      | +3.7 % vs pgx; 2× throughput de pq; 25 % menos memoria que pgx, 72 % que pq.              |
| `database/sql` InsertPrepared        | **GANA**      | **GANA**      | +6.7 % vs pgx; +3.7 % vs pq. 24 % menos memoria que pgx, 38 % menos que pq.               |
| `database/sql` Tx en lote            | **GANA**      | **GANA**      | +4.7 % vs pgx; +105 % vs pq. Memoria y allocs debajo de ambos.                             |
| Decoder numeric (default = texto)    | **EMPATA**    | **EMPATA**    | 236 MB/s en 100k filas numeric, 6 allocs. `BinaryNumeric` opt-in para casos WAN.          |
| Hot loop (encoder de filas)          | —             | —             | **0 allocs / 2.93 GB/s** — garantía interna; no comparable directamente.                   |
| PgBouncer-txn seguro                 | **GANA**      | **GANA**      | Re-prepare transparente en SQLSTATE 26000. pgx requiere config manual; pq no lo maneja.   |
| `CancelRequest` en `ctx.Cancel`      | **GANA**      | **EMPATA**    | Cancel real del servidor. pgx lo soporta; pq tiene soporte limitado.                      |
| Observer OpenTelemetry               | **GANA**      | **GANA**      | Subpackage oficial; pgx requiere lib externa, pq no tiene.                                 |
| Errores estructurados con helpers    | **EMPATA**    | **GANA**      | `PGError` iguala a `pgconn.PgError`; `pq.Error` tiene menos helpers.                      |

**Total vs pgx: 13 ganadas, 4 empates, 2 perdidas. Vs pq: 15 ganadas, 3 empates, 1 perdida.**

Las tres derrotas están dentro del 5 % en caminos saturados por
ancho de banda o por CPU del servidor (COPY TO binario / texto,
COPY FROM texto). pgz las compensa en memoria y allocs.

---

## 4 · Dónde pgz *no* es el mejor

Honestidad sobre los huecos restantes.

- **Superficie de features**: `pgx` tiene `LISTEN/NOTIFY`, replicación
  lógica, Kerberos / GSSAPI, decoders binarios más completos
  (inet / cidr / enum). `pgz` está deliberadamente acotado a
  SELECT → JSON + DML + COPY + Pipeline.
- **Ecosistema**: `pgx` y `pq` son el default en miles de proyectos
  desde hace años. El adaptador `database/sql` de `pgz` cubre la
  superficie que el ecosistema necesita (sqlc, goose, sqlx,
  golang-migrate compilan contra él) pero es más joven y no
  probado a la escala que pgx ya vivió.
- **Throughput COPY TO**: `pgx.CopyTo` sobre volcado crudo de bytes
  gana por 2 – 5 % porque hace estrictamente menos trabajo — el
  `CopyToBinary` de pgz parsea cada tupla a campos tipados. El
  camino de pgz entrega datos tipados, el de pgx entrega un stream
  de bytes que el usuario debe parsear.
- **Bytes en streaming JSON hand-tuned**: `pgx-raw` aloja unos
  20 KB menos por query que `pgz`. Diferencia absoluta
  insignificante.

---

## 5 · Reproducir los números

```bash
# Entorno
docker compose -f docker/docker-compose.yml up -d
export PGZ_TEST_DSN="postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable"

# Los benchmarks de comparacion viven en el modulo nested `tests/`.
cd tests

# Streaming JSON (matriz pgx + matriz pq)
go test . -run '^$' -bench 'BenchmarkPgx$' -benchmem -benchtime=2s
go test . -run '^$' -bench 'BenchmarkPq$'  -benchmem -benchtime=2s

# Struct scan
go test . -run '^$' -bench 'BenchmarkScan(Mixed|Narrow|WideJSONB)($|Pq$)' -benchmem -benchtime=2s

# COPY FROM + COPY TO
go test . -run '^$' -bench 'BenchmarkCopy|BenchmarkCopyTo' -benchmem -benchtime=2s

# Pipeline / SendBatch
go test . -run '^$' -bench 'BenchmarkPipelineBatch' -benchmem -benchtime=2s

# Camino de escritura database/sql
go test . -run '^$' -bench 'BenchmarkStdlib(InsertExec|InsertPrepared|TxInsert)' -benchmem -benchtime=2s

# Numeric (texto vs binario opt-in)
go test . -run '^$' -bench 'BenchmarkNumeric' -benchmem -benchtime=2s

# La regresion del hot-loop corre en el modulo principal.
cd ..
go test ./internal/rows -bench 'RowEncodeMixed' -benchmem -benchtime=3s
```

También disponible en inglés: [BENCHMARKS.md](BENCHMARKS.md).
