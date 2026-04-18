# Arquitectura

Este documento explica el diseno de pgz paso a paso. Cubre por que el
codigo esta estructurado asi, de donde vienen las ganancias de
performance, y que tradeoffs se hicieron deliberadamente.

---

## 1. Que es pgz

Un driver PostgreSQL para Go que convierte resultados de queries en
bytes JSON (o structs Go tipados) con allocations minimas. Tambien
ejecuta sentencias DML (INSERT, UPDATE, DELETE, CALL) de forma nativa.
El target de deployment es un gateway de datos frente a Citus +
PgBouncer en modo transaccion.

No es un ORM. No es compatible con `database/sql` por default (aunque
existe un adapter). Gana por hacer menos.

---

## 2. Diagrama de capas

```
+-------------------------------------------------------------------+
| pgz (API publica)                                                 |
|   Client · Open/Close · Query/Stream · Exec/ExecReturning         |
|   ScanStruct · Config · Observer · Pool                           |
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

Todo lo que esta debajo de `pgz/` vive en `internal/`. La superficie
publica es chica: `Client`, `Config`, `ExecResult`, `Observer`,
`pool.Pool`, `ResponseTooLargeError`, y los tipos Mode / Stats.

---

## 3. Capa wire (`internal/wire`)

La capa wire es duena del `net.Conn`, un `bufio.Reader` de 32 KiB, y un
unico `readBuf` creciente para ensamblar los cuerpos de los mensajes.

`ReadMessage()` devuelve el byte de tipo y un slice dentro de `readBuf`.
El slice es valido solo hasta la proxima llamada a `ReadMessage`. Todos
los callers respetan este contrato.

Dos reducciones clave de allocations:

1. **Cero allocation por mensaje.** `readBuf` es un unico slab
   creciente. El slice devuelto apunta directo a el.
2. **Lecturas buffereadas.** Sin `bufio`, cada `ReadMessage` hacia dos
   syscalls `io.ReadFull` (header + body). Para 100k filas narrow eso
   significaba 200k context switches. La capa bufio amortiza esto a ~1
   lectura cada 32 KB. En una query narrow-int esta fue la diferencia
   entre 20 MB/s y 100 MB/s.

Las escrituras pasan por un buffer de escritura por conexion. El
prefijo de longitud se computa patcheando el slice in-place despues de
construir el cuerpo.

---

## 4. Parsing del protocolo (`internal/protocol`)

Un paquete chico de constantes:

- Codigos de tipo de mensaje frontend/backend, espejados de
  `src/include/libpq/protocol.h`.
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

`KeyPrefix` se construye una vez y se reutiliza por fila. El loop de
filas se convierte en una secuencia ajustada de `append` sin formateo
de strings.

Al abrir un `Bind` de extended-protocol, pgz pide formato binario para
cada columna con encoder binario especializado. El `Encoder` del plan
se intercambia por la variante binaria. El formato texto queda como
fallback.

---

## 6. JSON writer (`internal/jsonwriter`)

Un conjunto de funciones `Append*` que escriben en un `[]byte`
suministrado por el caller. Sin struct writer. Pasar el slice permite
al compilador mantener el header en registros, siguiendo el idiom de
`strconv.AppendInt`.

El escape de strings usa una tabla lookup de 256 entradas
(`escapeFlag`). El hot loop recorre el input, acumula un "span de
copia", y solo salta al slow path cuando aparece un byte escapable.
ASCII plano pasa con un solo `append`.

### Path SWAR opcional

Bajo el build tag `pgz_simd`, el escape escalar se reemplaza por una
implementacion SWAR (SIMD-Within-A-Register) que chequea 8 bytes a la
vez usando aritmetica `uint64`. Cuando un chunk no necesita escape, el
loop avanza de a 8 sin branch por byte. Medido ~4x mas rapido en ASCII
medio/largo. Go puro, compila en todas las plataformas.

---

## 7. Encoders de tipo (`internal/types`)

Un archivo por familia:

- **`encoder.go`** -- encoders text-format. Numeros y booleanos pasan
  validados (la forma texto del server ya es JSON valido). Strings se
  escapan via `jsonwriter`.
- **`binary.go`** -- encoders binarios para int2/4/8, float4/8, bool,
  uuid, json, jsonb (strip byte de version), bytea, date, timestamp,
  timestamptz. Timestamps decodifican directamente de `int64`
  microsegundos a ISO-8601 sin pasar por `time.Time`.
- **`interval.go`** -- interval/time/timetz. Interval emite duracion
  ISO-8601. Los ceros fraccionarios al final se eliminan.
- **`array.go`** -- decoder binario recursivo multi-dim de arrays. Lee
  el header del array, despacha cada elemento por el encoder escalar.
  Elementos SQL NULL se vuelven JSON `null`.
- **`numeric.go`** -- passthrough text-format con validacion rapida.
  `NaN`/`Infinity` se rutean a strings JSON.
- **`bytea.go`** -- `bytea_output = hex` se emite como `"\\x..."`.
- **`jsonpass.go`** -- json/jsonb pasan verbatim. jsonb binario tiene un
  prefijo de version de 1 byte que se chequea y elimina.
- **`uuid.go`** -- formateado hex directo desde 16 bytes raw sin
  `encoding/hex` (~4x mas rapido).

La firma del encoder:

```go
type Encoder func(dst []byte, raw []byte) []byte
```

`raw == nil` significa SQL NULL. Cada encoder lo maneja.

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

`cells[i]` es un slice dentro del scratch buffer del wire. Cero copias
entre el socket y aca. El loop alloca cero bytes, impuesto por un
benchmark que debe reportar `0 allocs/op` en cada cambio.

---

## 9. Streaming

`StreamJSON` y `StreamNDJSON` construyen un `flushingWriter` alrededor
del `io.Writer` del caller:

- Flushea cuando el buffer cruza `Config.FlushBytes` (default 32 KiB)
- Flushea cuando paso `Config.FlushInterval` desde el ultimo flush
  (chequeo inline, sin goroutine, sin timer)

Los flushes solo ocurren en limites de fila, asi los scanners NDJSON
por linea nunca ven una fila partida.

**Invariante clave:** ningun byte sale del writer antes de
`BindComplete` + la primera `DataRow`. Una query que falla antes de
cualquier fila escribe cero bytes downstream.

---

## 10. Path DML (`Exec` / `ExecReturning`)

`Exec` usa el mismo protocolo extended-query que SELECT pero no espera
mensajes `DataRow`. Parsea el tag `CommandComplete` para extraer
`RowsAffected`. El cache de statements es compartido -- el re-prepare
de PgBouncer-txn funciona identico.

`ExecReturning` maneja DML con clausulas `RETURNING`. En el wire, un
`INSERT ... RETURNING *` produce `RowDescription + DataRow* +
CommandComplete` -- identico a un SELECT. `ExecReturning` reutiliza el
path completo de streaming JSON, salteando el guard de solo-SELECT que
protege los metodos `Query`/`Stream`.

---

## 11. Cancelacion

La cancelacion por `context.Context` se maneja con una goroutine
watcher durante cada query. Al cancelar:

1. Abre una conexion TCP lateral al mismo server.
2. Envia un `CancelRequest` con los datos de backend key. El server
   aborta con SQLSTATE 57014 (`query_canceled`).
3. Pone un deadline pasado en el socket principal para que las lecturas
   en vuelo retornen inmediatamente.

El watcher se destruye al completar la query normalmente.

---

## 12. Cache de prepared statements y PgBouncer-txn

El path extended-query cachea prepared statements por texto SQL. Un miss
paga `Parse + Describe`; un hit va directo a `Bind + Execute + Sync`.

PgBouncer en modo transaccion rota el backend fisico entre
transacciones. Un nombre de statement cacheado de una transaccion
anterior no va a existir en el nuevo backend, y `Bind` falla con
SQLSTATE 26000. El cache detecta esto, invalida la entrada, y reintenta
con un `Parse` fresco -- pero solo si no se flushearon bytes downstream.

---

## 13. Retry de serializacion (Citus)

SQLSTATE 40001 (serialization_failure) y 40P01 (deadlock_detected) se
reintentan transparentemente cuando `Config.RetryOnSerialization = true`
y no se committearon bytes. Hasta 3 intentos, backoff exponencial desde
10ms.

---

## 14. Modelo de timeouts

Tres capas, documentadas para no confundirlas:

1. **`postgresql.conf` `statement_timeout`** -- techo duro del DBA.
   Ultima palabra. Protege contra clientes mal comportados.
2. **`Config.DefaultQueryTimeout`** -- default a nivel gateway aplicado
   cuando el ctx del caller no tiene deadline. Dispara un
   `CancelRequest` real.
3. **`ctx.WithTimeout` en el handler HTTP** -- override por request.
   Gana si es mas corto que (2).

Las capas 2 y 3 son conveniencia. La capa 1 es seguridad.

---

## 15. Topes duros por respuesta

`Config.MaxResponseBytes` y `Config.MaxResponseRows` disparan un
`CancelRequest` + drain al cruzarlos. El error es
`*ResponseTooLargeError`. El campo `Committed` le dice al caller si
JSON parcial llego downstream.

El chequeo es un branch por fila, fuera del hot loop por celda.

---

## 16. Pool de conexiones (`pgz/pool`)

Pool LIFO acotado con:

- `Acquire(ctx)` bloquea en un pool lleno y vacio
- `MaxConnLifetime` -- cierra y reabre al pasar el cap
- `PingAfterIdle` -- valida conns idle antes de entregarlas
- `Discard()` -- retira conns envenenadas
- Retry transparente (hasta 3x) por lifetime y fallas de ping
- `Drain(ctx)` / `WaitIdle(ctx)` para shutdown graceful
- `Stats()` snapshot: Open, Idle, InUse, Waiting, Max

---

## 17. Observer

Hook de struct unico con:

- `OnQueryStart(sql)` -- antes del trafico wire
- `OnQueryEnd(QueryEvent)` -- siempre, con duracion/bytes/filas/error/SQLSTATE
- `OnQuerySlow(QueryEvent)` -- cuando se excede `SlowQueryThreshold`
- `OnNotice(*pgerr.Error)` -- cada NoticeResponse

Las implementaciones deben ser safe para uso concurrente. El no-op por
default agrega cero overhead. Los contadores atomicos (`Stats()`) son
una alternativa lock-free.

---

## 18. Tradeoffs

- **Cero reflexion.** Los encoders se eligen por OID con un switch.
- **Sin adapter `database/sql` en el hot path.** El adapter existe para
  compatibilidad drop-in pero agrega overhead de interface boxing.
- **Solo stdlib en runtime.** SCRAM usa PBKDF2 escrito a mano. pgx es
  dependencia dev (solo benchmarks).
- **Los metodos SELECT rechazan DML.** La garantia de cero-corrupcion-
  JSON depende de esto. DML va por `Exec`/`ExecReturning`.
- **La backpressure del buffer-pool es soft.** Cuando se alcanza el cap,
  `Get` devuelve un buffer fresco sin pool. Bloquear agregaria latencia.

---

## 19. Que no esta (y por que)

- **COPY fast-export** -- en el roadmap, venceria al extended-query para
  exports masivos.
- **LISTEN / NOTIFY / replicacion** -- workload diferente.
- **ORM / mapping de structs mas alla de ScanStruct** -- herramienta
  diferente.
- **Decoder binario de numeric** -- la forma texto ya tiene forma de
  JSON number.

---

## 20. Presupuesto de performance

Un encode de una fila con 6 columnas mixtas vive dentro de ~55 ns y
alloca 0 bytes. Con ese presupuesto, el techo es el protocolo wire
mismo: en loopback saturamos a ~165 MB/s de output JSON, empatando una
implementacion hand-tuned de pgx+RawValues dentro de +/-2%. La proxima
palanca es COPY binario, no mas optimizacion de encoders por celda.
