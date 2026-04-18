# Arquitectura

Este documento explica el diseno de pgz paso a paso. Cubre por que el
codigo esta estructurado asi, de donde vienen las ganancias de
performance, y que tradeoffs se hicieron deliberadamente.

---

## 1. Que es pgz

Un driver PostgreSQL para Go que convierte resultados de queries en
bytes JSON (o structs Go tipados) con allocations minimas, ejecuta
DML nativo, mueve datos en masa via COPY en ambas direcciones, y
pipelinea muchas queries chicas por request. El target de deployment
es un gateway de datos frente a Citus + PgBouncer en modo
transaccion.

No es un ORM. El adaptador `database/sql` esta completo (paths de
lectura y escritura, transacciones, prepared statements), asi que
`sqlc`, `goose`, `golang-migrate` y `sqlx` compilan contra el, pero
la API nativa sigue siendo el camino mas rapido cuando uno maneja
el codigo de punta a punta.

---

## 2. Diagrama de capas

```
+-------------------------------------------------------------------+
| pgz (API publica)                                                 |
|   Client · Open/Close · Query/Stream · Exec/ExecReturning         |
|   ScanStruct · CopyFrom/CopyTo · Batch/SendBatch · Config         |
|   Observer · Pool · PGError                                       |
+-------------------------------------------------------------------+
| rows  (plan de encoder · hot loop de DataRow · metadata columnas) |
+-------------------------------------------------------------------+
| jsonwriter            types  (encoders text + binary per OID)     |
|   funciones Append*   · numeric · jsonpass · bytea · times        |
|   tabla de escape     · binary (int/float/bool/uuid/json/date/ts) |
|     256 entradas      · array  (recursivo, multi-dim)             |
|                       · interval · time · timetz                  |
+-------------------------------------------------------------------+
| protocol (codigos de mensaje, OIDs, constantes de headers PG)     |
+-------------------------------------------------------------------+
| wire (reader/writer framed + bufio sobre net.Conn + scratch buf)  |
+-------------------------------------------------------------------+
| auth (MD5 · SCRAM-SHA-256 RFC 7677 · PBKDF2 solo stdlib)         |
+-------------------------------------------------------------------+
| pgerr (decoder de ErrorResponse + NoticeResponse)                 |
+-------------------------------------------------------------------+
| bufferpool (sync.Pool de []byte · cap live opcional)              |
+-------------------------------------------------------------------+
```

Todo lo que esta debajo de `pgz/` vive en `internal/`. Superficie
publica: `Client`, `Config`, `ExecResult`, `Observer`, `PGError`,
`Batch`/`BatchResults`, `CopyWriter`/`CopyReader`, `Iterator`,
`pool.Pool`, `ResponseTooLargeError`, los tipos Mode / Stats, y los
subpackages `pgz/stdlib` y `pgz/otel`.

---

## 3. Capa wire (`internal/wire`)

La capa wire es duena del `net.Conn`, un `bufio.Reader` de 128 KiB
(default, configurable), y un unico `readBuf` creciente para
ensamblar cuerpos de mensajes que excedan el bufio.

`ReadMessage()` devuelve el byte de tipo y un slice dentro del bufio
(fast path zero-copy para mensajes ≤ tamano de bufio) o de `readBuf`.
El slice es valido solo hasta la proxima llamada a `ReadMessage`.
Todos los callers respetan este contrato.

Las escrituras pasan por un buffer de escritura por conexion. El
prefijo de longitud se computa patcheando el slice in-place despues
de construir el cuerpo. `Builder.Finish()` deja el frame en el
buffer para que multiples mensajes se coalesquen en una unica
escritura TCP; `Send()` es `Finish + Flush`.

Este patron es lo que permite que los caminos de pipeline/batch y
COPY envien muchos mensajes en una sola escritura TCP.

---

## 4. Parsing del protocolo (`internal/protocol`)

Constantes espejadas de `src/include/libpq/protocol.h`:

- Codigos de tipo de mensaje frontend/backend, incluyendo la familia
  COPY (`CopyInResponse`, `CopyOutResponse`, `CopyData`, `CopyDone`,
  `CopyFail`).
- Sub-codigos de autenticacion (`AuthOK`, `AuthSASL`, etc.).
- Una lista curada de OIDs de tipo cubriendo cada tipo con encoder
  especializado. `ArrayElem(oid)` mapea OIDs de array a OIDs de
  elemento para que el decoder binario elija la funcion correcta.

---

## 5. Plan de encoder (`internal/rows`)

Despues de recibir un `RowDescription`, pgz recorre los descriptores
de columna y construye un plan:

```go
type Column struct {
    Name      string
    TypeOID   protocol.OID
    Format    int16
    Encoder   types.Encoder      // elegido por OID + formato
    KeyPrefix []byte              // `"nombre":` pre-construido
}
```

`KeyPrefix` se construye una vez y se reutiliza por fila. El loop
de filas se convierte en una secuencia ajustada de `append` sin
formateo de strings.

`ApplyFormatsEx(formats, binaryNumeric)` intercambia el encoder de
cada columna por la variante binaria cuando el servidor acordo
enviar binario. El flag binaryNumeric viaja junto: default off,
opt-in via `Config.BinaryNumeric`.

---

## 6. JSON writer (`internal/jsonwriter`)

Un conjunto de funciones `Append*` que escriben en un `[]byte`
suministrado por el caller. Sin struct writer. Pasar el slice
permite al compilador mantener el header en registros, siguiendo el
idiom de `strconv.AppendInt`.

El escape de strings usa una tabla lookup de 256 entradas
(`escapeFlag`). El hot loop recorre el input, acumula un "span de
copia", y solo salta al slow path cuando aparece un byte escapable.
ASCII plano pasa con un solo `append`.

### Path SWAR opcional

Bajo el build tag `pgz_simd`, el escape escalar se reemplaza por
una implementacion SWAR (SIMD-Within-A-Register) que chequea 8
bytes a la vez usando aritmetica `uint64`. Cuando un chunk no
necesita escape, el loop avanza de a 8 sin branch por byte. Medido
~4× mas rapido en ASCII medio/largo. Go puro, compila en todas las
plataformas.

---

## 7. Encoders de tipo (`internal/types`)

Un archivo por familia:

- **`encoder.go`** — encoders text-format. Numeros y booleanos
  pasan validados. Strings se escapan via `jsonwriter`.
- **`binary.go`** — encoders binarios para int2/4/8, float4/8,
  bool, uuid, json, jsonb (strip byte de version), bytea, date,
  timestamp, timestamptz. Timestamps decodifican directamente de
  `int64` microsegundos a ISO-8601 sin pasar por `time.Time`.
- **`interval.go`** — interval/time/timetz. Interval emite
  duracion ISO-8601.
- **`array.go`** — decoder binario recursivo multi-dim de arrays.
- **`numeric.go`** — passthrough text-format con validacion rapida.
  `NaN`/`Infinity` se rutean a strings JSON.
- **`numeric_bin.go`** — decoder binario de numeric. No se
  autoselecciona salvo que `Config.BinaryNumeric` este en true (el
  formateador C del servidor le gana al decoder Go en loopback).
- **`bytea.go`**, **`jsonpass.go`**, **`uuid.go`** — fast paths
  especificos de dominio.

La firma del encoder:

```go
type Encoder func(dst []byte, raw []byte) []byte
```

`raw == nil` significa SQL NULL. Cada encoder lo maneja.

`PickBinaryEx(oid, binaryNumeric)` y `HasBinaryEx(oid,
binaryNumeric)` son los entrypoints de dispatch; el sufijo Ex marca
la variante que acepta el toggle de BinaryNumeric.

---

## 8. Hot loop de filas

El loop por fila en `internal/rows/datarow.go`:

```go
buf = append(buf, '{')
for i, col := range plan.Columns {
    if i > 0 { buf = append(buf, ',') }
    buf = append(buf, col.KeyPrefix...)
    buf = col.Encoder(buf, cells[i])
}
buf = append(buf, '}')
```

`cells[i]` es un slice dentro del scratch buffer del wire. Cero
copias entre el socket y aca. El loop alloca cero bytes, impuesto
por un benchmark que debe reportar `0 allocs/op` en cada cambio.

---

## 9. Streaming

`StreamJSON` y `StreamNDJSON` construyen un `flushingWriter`
alrededor del `io.Writer` del caller:

- Flushea cuando el buffer cruza `Config.FlushBytes` (default
  32 KiB)
- Flushea cuando paso `Config.FlushInterval` desde el ultimo flush
  (chequeo inline, sin goroutine, sin timer)

Los flushes solo ocurren en limites de fila, asi los scanners
NDJSON por linea nunca ven una fila partida.

**Invariante clave:** ningun byte sale del writer antes de
`BindComplete` + la primera `DataRow`. Una query que falla antes
de cualquier fila escribe cero bytes downstream.

---

## 10. Encoding de parametros (`pgz/args.go`)

`writeBindParams(bd, oids, args)` arma el bloque de parametros del
mensaje Bind usando la `ParameterDescription` del servidor para
elegir el formato por parametro:

- Binario para `bool`, `int2/4/8`, `oid`, `float4/8`, `bytea`,
  `timestamp`, `timestamptz`. El ancho se elige a partir del OID
  del servidor, no del tipo Go — `database/sql` siempre nos pasa
  `int64`, que hay que angostar a int4 si la columna es int4.
- Texto para strings (el binario son los mismos bytes), numeric
  (el formateador texto del servidor es mas rapido en loopback),
  y cualquier OID que el servidor no pudo inferir.

Los valores se appendean directamente al builder via
`bd.Int16`/`bd.Int32`. Cero heap allocations para argumentos
escalares; el camino de texto anterior alojaba un string corto por
int/float via `strconv.FormatInt`/`FormatFloat`.

---

## 11. Path DML (`Exec` / `ExecReturning`)

`Exec` usa el mismo protocolo extended-query que SELECT pero no
espera mensajes `DataRow`. Parsea el tag `CommandComplete` para
extraer `RowsAffected`. El cache de statements es compartido — el
re-prepare de PgBouncer-txn funciona identico.

`ExecReturning` maneja DML con clausulas `RETURNING`. En el wire,
un `INSERT ... RETURNING *` produce `RowDescription + DataRow* +
CommandComplete` — identico a un SELECT. `ExecReturning` reutiliza
el path completo de streaming JSON, salteando el guard de
solo-SELECT.

---

## 12. COPY (`pgz/copy.go`, `pgz/copy_to.go`)

Cuatro entrypoints, todos usando el protocolo simple-query asi que
no comparten estado con el stmt cache:

- **`CopyFrom(ctx, sql, r io.Reader)`** — import texto / CSV.
  `pumpCopyData` lee a un slab reutilizable de 256 KiB con 5 bytes
  reservados en offset 0 para el header de CopyData, despues
  escribe el bloque frameado en una llamada `WriteRaw` por flush.
  Sin memcpy por `wire.Builder`.
- **`CopyFromBinary(ctx, sql, fieldCount, emit)`** — import
  binario via `CopyWriter`. El writer reserva el prefijo de 2
  bytes del field-count de la tupla, deja que el caller appendee
  campos tipados directo al buffer de salida, y despues parchea
  el prefijo in-place. Los flushes ocurren en limite de fila
  cuando el buffer cruza 256 KiB. Medido +44 % throughput vs
  `pgx.CopyFrom`.
- **`CopyTo(ctx, sql, w io.Writer)`** — export texto / CSV. El
  body wire de cada `CopyData` se escribe directo a `w` — el
  slice apunta al buffer de bufio, zero-copy del kernel al
  `io.Writer` del usuario.
- **`CopyToBinary(ctx, sql, fieldCount, handler)`** — export
  binario via `CopyReader`. Parsea el header PGCOPY (posiblemente
  partido entre mensajes), despues por tupla camina los campos
  in-place. Los readers tipados (`Int4`, `Text`, `Bool`, etc.)
  devuelven slices apuntando al buffer del wire.

---

## 13. Pipeline / batch (`pgz/pipeline.go`)

`Batch.Queue(sql, args...)` acumula items en memoria.
`SendBatch(ctx, b)`:

1. Recorre los items y, para cada SQL unico, emite
   `Parse + Describe` una vez (statement nombrado), despues
   `Bind + Execute` por ocurrencia. La deduplicacion dentro del
   batch y el cache persistente de statements comparten una
   entrada provisional para que las segundas ocurrencias en el
   mismo batch ya salteen Parse.
2. Emite un unico `Sync` al final.
3. Flushea todo en una escritura TCP.

`BatchResults.Exec()` / `Query()` leen el bloque de respuesta por
item (ParseComplete / BindComplete / CommandComplete para DML, mas
RowDescription para Query). En el primer CommandComplete exitoso
para un SQL dado, la entrada provisional se promueve al cache
persistente para que el proximo batch (o `Exec` normal) saltee
Parse + Describe del todo.

Medido +16 % throughput, 10.6× menos memoria, 8× menos allocations
que `pgx.SendBatch` en un batch de 100 INSERTs.

---

## 14. Cancelacion

La cancelacion por `context.Context` se maneja con una goroutine
watcher durante cada query. Al cancelar:

1. Abre una conexion TCP lateral al mismo server.
2. Envia un `CancelRequest` con los datos de backend key. El
   server aborta con SQLSTATE 57014 (`query_canceled`).
3. Pone un deadline pasado en el socket principal para que las
   lecturas en vuelo retornen inmediatamente.

El watcher se destruye al completar la query normalmente.

---

## 15. Cache de prepared statements y PgBouncer-txn

El path extended-query cachea prepared statements por texto SQL.
Un miss paga `Parse + Describe`; un hit va directo a
`Bind + Execute + Sync`.

PgBouncer en modo transaccion rota el backend fisico entre
transacciones. Un nombre de statement cacheado de una transaccion
anterior no va a existir en el nuevo backend, y `Bind` falla con
SQLSTATE 26000. El cache detecta esto, invalida la entrada, y
reintenta con un `Parse` fresco — pero solo si no se flushearon
bytes downstream.

`SendBatch` comparte este cache: el primer batch que contiene un
SQL nuevo lo agrega; los batches siguientes saltean
Parse + Describe.

---

## 16. Retry de serializacion (Citus)

SQLSTATE 40001 (serialization_failure) y 40P01 (deadlock_detected)
se reintentan transparentemente cuando
`Config.RetryOnSerialization = true` y no se committearon bytes.
Hasta 3 intentos, backoff exponencial desde 10 ms.

---

## 17. Errores (`pgz.PGError`)

`pgz.PGError` es un type alias al struct del decoder interno de
ErrorResponse. Cada metodo de query devuelve `*PGError` directo
(sin wrap) asi `errors.As` aterriza limpio. Los helpers cubren
los SQLSTATEs donde un gateway rutinariamente ramifica:

- Integridad: `IsUniqueViolation` (23505),
  `IsForeignKeyViolation` (23503), `IsCheckViolation` (23514),
  `IsNotNullViolation` (23502), `IsExclusionViolation` (23P01),
  `IsIntegrityViolation` (clase 23).
- Concurrencia: `IsSerializationFailure` (40001),
  `IsDeadlock` (40P01).
- Cancel/admin: `IsQueryCanceled` (57014),
  `IsAdminShutdown` (57P01).
- PgBouncer: `IsInvalidSQLStatementName` (26000).

Mas `SQLState()` / `SQLStateClass()` para ramificacion
arbitraria.

---

## 18. Modelo de timeouts

Tres capas, documentadas para no confundirlas:

1. **`postgresql.conf` `statement_timeout`** — techo duro del DBA.
   Ultima palabra. Protege contra clientes mal comportados.
2. **`Config.DefaultQueryTimeout`** — default a nivel gateway
   aplicado cuando el ctx del caller no tiene deadline. Dispara
   un `CancelRequest` real.
3. **`ctx.WithTimeout` en el handler HTTP** — override por
   request. Gana si es mas corto que (2).

Las capas 2 y 3 son conveniencia. La capa 1 es seguridad.

---

## 19. Topes duros por respuesta

`Config.MaxResponseBytes` y `Config.MaxResponseRows` disparan un
`CancelRequest` + drain al cruzarlos. El error es
`*ResponseTooLargeError`. El campo `Committed` le dice al caller
si JSON parcial llego downstream.

El chequeo es un branch por fila, fuera del hot loop por celda.

---

## 20. Pool de conexiones (`pgz/pool`)

Pool LIFO acotado con:

- `Acquire(ctx)` bloquea en un pool lleno y vacio
- `MaxConnLifetime` — cierra y reabre al pasar el cap
- `PingAfterIdle` — valida conns idle antes de entregarlas
- `Discard()` — retira conns envenenadas
- Retry transparente (hasta 3×) por lifetime y fallas de ping
- `Drain(ctx)` / `WaitIdle(ctx)` para shutdown graceful
- `Stats()` snapshot: Open, Idle, InUse, Waiting, Max

---

## 21. Observer y OpenTelemetry

Hook de struct unico con:

- `OnQueryStart(sql)` — antes del trafico wire
- `OnQueryEnd(QueryEvent)` — siempre, con
  duracion/bytes/filas/error/SQLSTATE
- `OnQuerySlow(QueryEvent)` — cuando se excede
  `SlowQueryThreshold`
- `OnNotice(*pgerr.Error)` — cada NoticeResponse

Las implementaciones deben ser safe para uso concurrente. El no-op
por default agrega cero overhead.

El subpackage opcional `pgz/otel` implementa la interfaz Observer
usando tracers y meters de OpenTelemetry: un span por query, mas
cuatro metricas (histograma `pgz.query.duration`, counters
`pgz.query.rows`, `pgz.query.errors`, `pgz.query.slow`). Las
dependencias de OTel solo se cargan si importas el subpackage.

---

## 22. Adaptador `database/sql` (`pgz/stdlib`)

Registrado como `"pgz"`. Superficie lectura + escritura completa:

- `Query`, `QueryContext`, `QueryRow`, `Prepare`, `Ping`.
- `Exec`, `ExecContext`, `Stmt.ExecContext`.
- `Begin`, `BeginTx` con traduccion de nivel de aislamiento y
  read-only en un unico round-trip `BEGIN …`.
- INSERT … RETURNING via `QueryRow` funciona porque el adaptador
  llama a `RawQueryAny` (la variante sin el guard de solo
  SELECT).

El adaptador comparte el stmt cache del `pgz.Client` nativo, asi
que repetidos `db.Exec` del mismo SQL saltean Parse + Describe
despues de la primera llamada — igualando o ganandole a
`pgx/stdlib` y `lib/pq` en cada patron de INSERT que medimos.

---

## 23. Tradeoffs

- **Cero reflexion.** Los encoders se eligen por OID con un
  switch.
- **Solo stdlib en runtime.** SCRAM usa PBKDF2 escrito a mano. pgx
  es dependencia dev (solo benchmarks). OpenTelemetry solo se
  jala si importas `pgz/otel`.
- **Los metodos SELECT rechazan DML por default.** `Query`/`Stream`
  protegen contra SQL no-SELECT; DML con RETURNING va por
  `ExecReturning` (o `RawQueryAny` + database/sql).
- **La backpressure del buffer-pool es soft.** Cuando se alcanza
  el cap, `Get` devuelve un buffer fresco sin pool. Bloquear
  agregaria latencia.

---

## 24. Que no esta (y por que)

- **LISTEN / NOTIFY / replicacion** — workload diferente; usar
  pgx en paralelo.
- **Decoders binarios para `inet`/`cidr`/`macaddr`/`enum`** — el
  fallback texto funciona y es medidamente mas rapido en loopback;
  revisitar si un workload WAN real lo demanda.
- **Kerberos / GSSAPI** — raro en deployments cloud-native donde
  apunta pgz.
- **ORM / scan reflection-heavy** — `ScanStruct[T]` ya cubre el
  95 % de los casos con cero reflexion en el hot path.

---

## 25. Presupuesto de performance

Un encode de una fila con 6 columnas mixtas vive dentro de ~45 ns
y alloca 0 bytes. Con ese presupuesto, el techo es el protocolo
wire mismo: en loopback saturamos a ~170 MB/s de output JSON,
empatando una implementacion hand-tuned de pgx+RawValues dentro
de ±2 %. Export COPY binario topa a ~190 MB/s de import y
~4.8 M filas/s de export. Pipeline / SendBatch alcanza 280 k
filas/s en un batch de 100 INSERTs. Ver
[BENCHMARKS.md](BENCHMARKS.md) para la matriz completa.
