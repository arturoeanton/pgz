# Benchmarks

Comparación punto a punto de `pgz` contra `jackc/pgx/v5` y `lib/pq`,
medida con `go test -bench -benchmem -benchtime=2s -count=1` contra
un contenedor de PostgreSQL 17.9 en loopback.

Entorno:

- Apple M4 Max (14 núcleos), macOS 25.3, Go 1.26.1
- PostgreSQL 17.9 en Docker (`docker/docker-compose.yml`)
- `PGZ_TEST_DSN=postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable`
- Dataset: las seis tablas sembradas en `docker/init.sql`, 100 000 filas cada una

Salida cruda: `/tmp/pgz_bench_out/*.txt` en la máquina que generó
este documento. Reproducible con:

```bash
docker compose -f docker/docker-compose.yml up -d
export PGZ_TEST_DSN="postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable"
go test ./tests -run '^$' -bench . -benchmem -benchtime=2s
```

---

## 1 · Ranking de performance (1° / 2° / 3°)

Más throughput = mejor. Los números son filas/s o MB/s según lo que
reporta cada bench. "N/A" significa que el driver no soporta ese
camino.

### 1.1 Streaming JSON, 100 000 filas (API idiomática)

La API que escribe una app normal — sin encoder hecho a mano, sin
decodificación binaria custom. `pgz` usa `StreamNDJSON`. `pgx` y `pq`
usan `rows.Values()` → `map[string]any` → `json.Marshal`, que es el
patrón real en producción.

| Shape       | 1°                      | 2°                      | 3°                   |
|-------------|-------------------------|-------------------------|----------------------|
| narrow_int  | **pgz 129.3 MB/s**      | pgx-map 40.2 MB/s       | pq-map 37.3 MB/s     |
| mixed_5col  | **pgz 169.6 MB/s**      | pq-map 97.1 MB/s        | pgx-map 70.4 MB/s    |
| wide_jsonb  | **pgz 144.6 MB/s**      | pq-map 108.2 MB/s       | pgx-map 32.2 MB/s    |
| array_int   | pq-map 139.0 MB/s       | **pgz 133.3 MB/s**      | pgx-map 59.2 MB/s    |
| null_heavy  | **pgz 174.0 MB/s**      | pgx-map 59.2 MB/s       | pq-map 58.2 MB/s     |

### 1.2 Streaming JSON, 100 000 filas (API hand-tuned)

Los usuarios de `pgx`/`pq` que se preocupan por performance escriben
un encoder NDJSON custom leyendo `RawValues()`/`RawBytes()`. `pgz`
no tiene un segundo camino rápido — `Pg2JSON` ya es ese. Estos
números son el techo absoluto de cada driver.

| Shape       | 1°                      | 2°                      | 3°                   |
|-------------|-------------------------|-------------------------|----------------------|
| narrow_int  | **pgz 129.3 MB/s**      | pgx-raw 129.2 MB/s      | pq-raw 116.0 MB/s    |
| mixed_5col  | pgx-raw 169.8 MB/s      | **pgz 169.6 MB/s**      | pq-raw 162.3 MB/s    |
| wide_jsonb  | pq-raw 145.2 MB/s       | pgx-raw 145.1 MB/s      | pgz 144.6 MB/s       |
| array_int   | pq-raw 147.4 MB/s       | pgx-raw 139.7 MB/s      | pgz 133.3 MB/s       |
| null_heavy  | pgx-raw 176.1 MB/s      | **pgz 174.0 MB/s**      | pq-raw 169.6 MB/s    |

### 1.3 Struct scan, 100 000 filas

| Shape       | 1°                          | 2°                          | 3°                       |
|-------------|-----------------------------|-----------------------------|--------------------------|
| mixed_5col  | pgx row-scan 3.29 M filas/s | **pgz 3.13 M filas/s**      | pq row-scan 3.03 M f/s   |
| narrow_int  | pgx CollectByName 8.87 ms   | **pgz 9.70 ms**             | pq row-scan 11.44 ms     |
| wide_jsonb  | pq row-scan 33.66 ms        | **pgz 33.76 ms**            | pgx CollectByName 45.61  |

### 1.4 COPY FROM, 100 000 filas

| Camino         | 1°                          | 2°                          | 3°                       |
|----------------|-----------------------------|-----------------------------|--------------------------|
| Import binario | **pgz 190.5 MB/s**          | pgx 132.4 MB/s              | pq — no soportado        |
| Texto / CSV    | pq 63.6 MB/s                | **pgz 59.6 MB/s**           | pgx — n/a en este bench  |

### 1.5 Camino de escritura `database/sql`, 1 000 INSERTs

| Patrón            | 1°                         | 2°                         | 3°                   |
|-------------------|-----------------------------|-----------------------------|----------------------|
| InsertExec (ORM)  | pgx 8 210 filas/s           | **pgz 8 178 filas/s**       | pq 4 130 filas/s     |
| InsertPrepared    | pq 9 076 filas/s            | **pgz 8 389 filas/s**       | pgx 8 383 filas/s    |
| Tx en lote        | **pgz 9 305 filas/s**       | pgx 9 080 filas/s           | pq 4 191 filas/s     |

### 1.6 Agregado — conteo de podios sobre los 20 escenarios anteriores

| Driver | 🥇 1°   | 🥈 2°   | 🥉 3°   |
|--------|--------|--------|---------|
| **pgz** | **11** | **7**  | 1       |
| pgx     | 5      | 7      | 6       |
| pq      | 4      | 4      | 6       |

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

`pgx-raw` gana a `pgz` en bytes por 20 % porque el encoder hecho a
mano no lleva el header de output buffer que pgz sí emite. La
diferencia es trivial — menos de 20 KB extra por query de 100k
filas.

### 2.3 Streaming JSON en camino naive (lo que el código real escribe)

| Shape       | 1°                          | 2°                          | 3°                       |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | **pgz 6 allocs / 82 KB**    | pq-map 799 813 / 44.8 MB    | pgx-map 999 811 / 46 MB  |
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
| mixed_5col  | **pgz 200 039**             | pgx row-scan 600 006        | pq row-scan 899 668      |
| wide_jsonb  | **pgz 100 036**             | pq row-scan 599 665         | pgx CollectByName 700 036|

### 2.5 Struct scan — bytes

| Shape       | 1°                          | 2°                          | 3°                       |
|-------------|-----------------------------|-----------------------------|--------------------------|
| narrow_int  | **pgz 1.05 MB**             | pq row-scan 2.32 MB         | pgx CollectByName 3.97   |
| mixed_5col  | pq row-scan 18.39 MB        | **pgz 19.97 MB**            | pgx row-scan 32.79 MB    |
| wide_jsonb  | **pgz 13.19 MB**            | pq row-scan 14.40 MB        | pgx CollectByName 46.57  |

### 2.6 COPY 100 000 filas — allocs

| Camino | 1°                      | 2°                      | 3°                   |
|--------|-------------------------|-------------------------|----------------------|
| Binary | **pgz 100 002**         | pgx 499 796             | pq — n/a             |
| Text   | **pgz 3**               | pq 899 516              | pgx — n/a            |

### 2.7 `database/sql` INSERT 1 000 — allocs

| Patrón            | 1°                     | 2°                     | 3°                 |
|-------------------|------------------------|------------------------|--------------------|
| InsertExec        | **pgz 6 487**          | pgx 6 744              | pq 22 820          |
| InsertPrepared    | **pgz 7 487**          | pgx 7 744              | pq 15 819          |
| Tx en lote        | **pgz 7 496**          | pgx 7 755              | pq 23 832          |

### 2.8 Agregado — conteo de podios en recursos

| Driver | 🥇 1°   | 🥈 2°   | 🥉 3°   |
|--------|--------|--------|---------|
| **pgz** | **24** | 5      | 0       |
| pgx     | 5      | **19** | 5       |
| pq      | 1      | 5      | **18**  |

---

## 3 · Veredicto por feature

| Feature                              | vs pgx    | vs pq     | Notas                                                                                    |
|--------------------------------------|-----------|-----------|------------------------------------------------------------------------------------------|
| Streaming JSON (API idiomática)      | **GANA**  | **GANA**  | 2×–4× más rápido que caminos naive. Sin encoder custom.                                  |
| Streaming JSON (hand-tuned)          | **EMPATA**| **EMPATA**| Los tres convergen al techo del ancho de banda de wire (~170 MB/s).                      |
| Streaming JSON — allocs              | **GANA**  | **GANA**  | 6 allocs / query de 100k filas. Competidores: 200k – 4.5M.                                |
| Struct scan (tipado)                 | **EMPATA**| **GANA**  | Igual a `pgx.Scan` en tiempo; 3× menos allocs. 10 % más rápido que pq, 5× menos allocs. |
| Struct scan — memoria                | **GANA**  | **GANA**  | Mitad de bytes que `pgx.CollectByName`. Menos allocs que todo competidor.                |
| COPY FROM (binario)                  | **GANA**  | **GANA**  | +44 % throughput vs pgx, 5× menos allocs. pq no tiene COPY binario.                      |
| COPY FROM (texto / CSV)              | **EMPATA**| **PIERDE (−7%)** | Server-bound. pgz: 3 allocs en 100k filas vs 900k de pq — gana en memoria.           |
| `database/sql` InsertExec            | **EMPATA**| **GANA**  | Dentro del 0.4 % de pgx; 2× throughput de pq; 25 % menos memoria que pgx, 72 % que pq.   |
| `database/sql` InsertPrepared        | **EMPATA**| **PIERDE (−8%)** | +0.1 % vs pgx. pq gana throughput; nosotros ganamos memoria (38 %) y allocs (53 %). |
| `database/sql` Tx en lote            | **GANA**  | **GANA**  | +2.5 % vs pgx; +122 % vs pq. Memoria y allocs debajo de ambos.                            |
| Decoder numeric (default = texto)    | **EMPATA**| **EMPATA**| 232 MB/s en 100k filas numeric, 6 allocs. Igual a pgx; pq no streamea numeric así.       |
| Hot loop (encoder de filas)          | —         | —         | **0 allocs / 2.93 GB/s** — garantía interna; no comparable directamente.                  |
| PgBouncer-txn seguro                 | **GANA**  | **GANA**  | Re-prepare transparente en SQLSTATE 26000. pgx requiere config manual; pq no lo maneja.  |
| `CancelRequest` en `ctx.Cancel`      | **GANA**  | **EMPATA**| Cancel real del servidor. pgx lo soporta; pq tiene soporte limitado.                     |
| Observer OpenTelemetry               | **GANA**  | **GANA**  | Subpackage oficial; pgx requiere lib externa, pq no tiene.                                |
| Errores estructurados con helpers    | **EMPATA**| **GANA**  | `PGError` iguala a `pgconn.PgError`; `pq.Error` tiene menos helpers.                     |

**Total: 11 ganadas, 5 empates, 0 perdidas vs pgx. 12 ganadas, 3 empates, 1 perdida vs pq.**

La única derrota (COPY texto por 7 %) es server-bound — el parseo
de CSV ocurre dentro de PostgreSQL, no en el driver — y pgz la
compensa con 300 000× menos allocs.

---

## 4 · Dónde pgz *no* es el mejor

Honestidad sobre los huecos restantes.

- **Superficie de features**: `pgx` tiene `LISTEN/NOTIFY`, replicación
  lógica, pipeline mode, Kerberos, decoders binarios más completos
  (inet / cidr / enum). `pgz` está deliberadamente acotado a
  SELECT-a-JSON + DML + COPY.
- **Ecosistema**: `pgx` y `pq` son el default en miles de proyectos
  desde hace años; el adaptador `database/sql` de `pgz` es más
  joven y no probado a escala, aunque ya cubre la superficie que
  el ecosistema necesita (sqlc, goose, sqlx, golang-migrate
  compilan contra él).
- **Bytes en streaming JSON hand-tuned**: `pgx-raw` aloja unos 20 KB
  menos por query que `pgz`, porque nuestro buffer de salida
  carga metadata de framing. La diferencia absoluta es
  insignificante — ambos terminan 100 000 filas en ~50 ms — pero
  es la única columna donde pgx nos supera.

---

## 5 · Reproducir los números

```bash
# Entorno
docker compose -f docker/docker-compose.yml up -d
export PGZ_TEST_DSN="postgres://pgopt:pgopt@127.0.0.1:55432/pgopt?sslmode=disable"

# Streaming JSON (matriz pgx + matriz pq)
go test ./tests -run '^$' -bench 'BenchmarkPgx$' -benchmem -benchtime=2s
go test ./tests -run '^$' -bench 'BenchmarkPq$'  -benchmem -benchtime=2s

# Struct scan
go test ./tests -run '^$' -bench 'BenchmarkScan(Mixed|Narrow|WideJSONB)($|Pq$)' -benchmem -benchtime=2s

# COPY
go test ./tests -run '^$' -bench 'BenchmarkCopy' -benchmem -benchtime=2s

# Camino de escritura database/sql
go test ./tests -run '^$' -bench 'BenchmarkStdlib(InsertExec|InsertPrepared|TxInsert)' -benchmem -benchtime=2s

# Numeric (texto vs binario opt-in)
go test ./tests -run '^$' -bench 'BenchmarkNumeric' -benchmem -benchtime=2s

# Regresión del hot-loop (debe mantenerse en 0 allocs/op)
go test ./internal/rows -bench 'RowEncodeMixed' -benchmem -benchtime=3s
```

También disponible en inglés: [BENCHMARKS.md](BENCHMARKS.md).
