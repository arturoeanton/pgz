package pgz

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/arturoeanton/pgz/internal/pgerr"
	"github.com/arturoeanton/pgz/internal/protocol"
)

// CopyFrom streams bytes from r into the server using COPY ... FROM STDIN.
// The SQL statement must be a valid COPY statement, e.g.:
//
//	"COPY users (id, name, email) FROM STDIN (FORMAT csv)"
//	"COPY log FROM STDIN"                    // default: TEXT
//
// r's bytes are chunked into CopyData messages. The format of those bytes
// (CSV, TEXT, binary header + tuples) must match whatever the COPY
// statement declared; this method is format-agnostic.
//
// Returns the row count reported by the server on CommandComplete.
//
// CopyFrom bypasses the prepared-statement cache entirely (COPY uses the
// simple-query protocol), so this call never affects SELECT / Exec hot
// paths. Observer.OnQueryStart/End fire exactly as for any other query.
func (c *Client) CopyFrom(ctx context.Context, sql string, r io.Reader) (int64, error) {
	if d := c.cfg.DefaultQueryTimeout; d > 0 {
		if _, hasDL := ctx.Deadline(); !hasDL {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
	}
	c.observer.OnQueryStart(sql)
	start := time.Now()
	var rows int64
	var err error
	defer func() {
		c.recordEnd(sql, int(rows), 0, time.Since(start), err)
	}()
	if err = c.attachContext(ctx); err != nil {
		return 0, err
	}
	defer c.detachContext()

	rows, err = c.copyFrom(sql, r)
	return rows, err
}

// CopyFromBinary runs COPY ... FROM STDIN (FORMAT binary) and drives the
// binary wire format directly. The caller emits rows through the
// CopyWriter handed to emit; return io.EOF from emit to signal clean end
// of stream. The statement must declare binary format, e.g.:
//
//	"COPY users (id, name, created_at) FROM STDIN (FORMAT binary)"
//
// fieldCount is the number of columns each tuple carries; it is enforced
// on every row so a mismatched column count is caught client-side before
// the server sees a malformed tuple.
func (c *Client) CopyFromBinary(ctx context.Context, sql string,
	fieldCount int, emit func(w *CopyWriter) error) (int64, error) {
	if fieldCount <= 0 || fieldCount > 1<<15-1 {
		return 0, fmt.Errorf("pgz: CopyFromBinary: invalid fieldCount %d", fieldCount)
	}
	if d := c.cfg.DefaultQueryTimeout; d > 0 {
		if _, hasDL := ctx.Deadline(); !hasDL {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
	}
	c.observer.OnQueryStart(sql)
	start := time.Now()
	var rows int64
	var err error
	defer func() {
		c.recordEnd(sql, int(rows), 0, time.Since(start), err)
	}()
	if err = c.attachContext(ctx); err != nil {
		return 0, err
	}
	defer c.detachContext()

	rows, err = c.copyFromBinary(sql, fieldCount, emit)
	return rows, err
}

// copyFrom drives the simple-query COPY IN protocol: send Query, read
// CopyInResponse, pump CopyData, send CopyDone, wait for
// CommandComplete + ReadyForQuery.
func (c *Client) copyFrom(sql string, r io.Reader) (int64, error) {
	q := c.conn.Begin(protocol.MsgQuery)
	q.CString(sql)
	if err := q.Send(); err != nil {
		return 0, err
	}
	if err := c.awaitCopyInResponse(); err != nil {
		return 0, err
	}
	if err := c.pumpCopyData(r); err != nil {
		_ = c.sendCopyFail(err.Error())
		c.drainUntilReady()
		return 0, err
	}
	if err := c.sendCopyDone(); err != nil {
		return 0, err
	}
	return c.awaitCopyComplete()
}

// copyFromBinary is the binary-format variant. It writes the required
// PGCOPY header, then one tuple per emit callback invocation, then the
// -1 trailer. The CopyWriter frames its own CopyData messages in place
// and writes them directly to the socket — no intermediate memcpy
// through wire.Builder, no small-message syscalls between rows.
func (c *Client) copyFromBinary(sql string, fieldCount int,
	emit func(w *CopyWriter) error) (int64, error) {
	q := c.conn.Begin(protocol.MsgQuery)
	q.CString(sql)
	if err := q.Send(); err != nil {
		return 0, err
	}
	if err := c.awaitCopyInResponse(); err != nil {
		return 0, err
	}

	cw := newCopyWriter(c, fieldCount)
	// PGCOPY header: 11-byte magic + int32 flags + int32 header-extension.
	// Layout is fixed in PG's adt/copy.c; all zero, no extensions.
	cw.buf = append(cw.buf,
		'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xff, '\r', '\n', 0,
		0, 0, 0, 0, // flags
		0, 0, 0, 0, // header extension length
	)

	var rows int64
	for {
		// Reserve the tuple's int16 fieldCount prefix; fields append
		// directly into cw.buf.
		cw.rowStart = len(cw.buf)
		cw.buf = append(cw.buf, 0, 0)
		cw.written = 0

		err := emit(cw)
		if err == io.EOF {
			// Roll back the unused row header we just reserved.
			cw.buf = cw.buf[:cw.rowStart]
			break
		}
		if err != nil {
			_ = c.sendCopyFail(err.Error())
			c.drainUntilReady()
			return 0, err
		}
		if cw.written != fieldCount {
			e := fmt.Errorf("pgz: CopyFromBinary: row %d wrote %d fields, expected %d",
				rows, cw.written, fieldCount)
			_ = c.sendCopyFail(e.Error())
			c.drainUntilReady()
			return 0, e
		}
		// Patch the fieldCount prefix in place.
		binary.BigEndian.PutUint16(cw.buf[cw.rowStart:cw.rowStart+2], uint16(fieldCount))
		rows++
		if len(cw.buf) >= copyFlushThreshold {
			if err := cw.flush(); err != nil {
				return 0, err
			}
		}
	}
	// Trailer: int16 = -1.
	cw.buf = append(cw.buf, 0xff, 0xff)
	if err := cw.flush(); err != nil {
		return 0, err
	}
	if err := c.sendCopyDone(); err != nil {
		return 0, err
	}
	_, err := c.awaitCopyComplete()
	return rows, err
}

// awaitCopyInResponse reads messages until the server signals
// CopyInResponse. Surfaces ErrorResponse and unexpected message types
// as driver errors.
func (c *Client) awaitCopyInResponse() error {
	var pgError *pgerr.Error
	for {
		t, body, err := c.conn.ReadMessage()
		if err != nil {
			return err
		}
		switch t {
		case protocol.MsgCopyInResponse:
			return nil
		case protocol.MsgErrorResponse:
			pgError = pgerr.Parse(body)
		case protocol.MsgReadyForQuery:
			if pgError != nil {
				return pgError
			}
			return fmt.Errorf("pgz: ReadyForQuery before CopyInResponse")
		case protocol.MsgNoticeResponse:
			c.observer.OnNotice(pgerr.Parse(body))
		case protocol.MsgParameterStatus:
			k, v := splitParameter(body)
			c.params[k] = v
		case protocol.MsgCommandComplete:
			return fmt.Errorf("pgz: COPY completed without accepting data")
		default:
			return fmt.Errorf("pgz: unexpected msg %q while awaiting CopyInResponse", t)
		}
	}
}

// pumpCopyData copies r into CopyData messages. The chunk size lines up
// with the binary writer's flush threshold so the wire sees the same
// pacing regardless of format.
func (c *Client) pumpCopyData(r io.Reader) error {
	buf := make([]byte, copyFlushThreshold)
	// Reserve bytes 0..4 for our in-place CopyData header so we can
	// WriteRaw without a second memcpy through wire.Builder.
	const hdrLen = 5
	frame := make([]byte, hdrLen+len(buf))
	frame[0] = protocol.MsgCopyData
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			copy(frame[hdrLen:], buf[:n])
			binary.BigEndian.PutUint32(frame[1:5], uint32(n+4))
			if werr := c.conn.WriteRaw(frame[:hdrLen+n]); werr != nil {
				return werr
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (c *Client) sendCopyDone() error {
	m := c.conn.Begin(protocol.MsgCopyDone)
	return m.Send()
}

func (c *Client) sendCopyFail(reason string) error {
	m := c.conn.Begin(protocol.MsgCopyFail)
	m.CString(reason)
	return m.Send()
}

// awaitCopyComplete reads until ReadyForQuery and returns the row count
// parsed from CommandComplete. Notices are forwarded to the Observer.
func (c *Client) awaitCopyComplete() (int64, error) {
	var rows int64
	var pgError *pgerr.Error
	for {
		t, body, err := c.conn.ReadMessage()
		if err != nil {
			return 0, err
		}
		switch t {
		case protocol.MsgCommandComplete:
			tag := string(body[:len(body)-1])
			rows = parseRowsAffected(tag)
		case protocol.MsgNoticeResponse:
			c.observer.OnNotice(pgerr.Parse(body))
		case protocol.MsgParameterStatus:
			k, v := splitParameter(body)
			c.params[k] = v
		case protocol.MsgErrorResponse:
			pgError = pgerr.Parse(body)
		case protocol.MsgReadyForQuery:
			if pgError != nil {
				return 0, pgError
			}
			return rows, nil
		case protocol.MsgEmptyQueryResponse:
			// No-op COPY; drain and report zero.
		default:
			return 0, fmt.Errorf("pgz: unexpected msg %q during COPY completion", t)
		}
	}
}

// drainUntilReady swallows messages up to and including ReadyForQuery
// so the connection is reusable after a failed COPY. Errors while
// draining are silent — the caller already has one.
func (c *Client) drainUntilReady() {
	for {
		t, _, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		if t == protocol.MsgReadyForQuery {
			return
		}
	}
}

// copyFlushThreshold is the CopyData payload target per TCP write. 256
// KiB keeps framing overhead under 0.02 % and matches the amount of row
// data the server can parse between ack stalls on loopback.
const copyFlushThreshold = 256 * 1024

// copyHdrLen is the size of a CopyData wire header (1-byte type + int32
// length). Reserved at buf[0:5] so flush can WriteRaw in one syscall.
const copyHdrLen = 5

// newCopyWriter returns a writer with buf pre-sized to the flush
// threshold + slack, and the CopyData header byte installed at buf[0].
// The length field at buf[1:5] is patched per flush.
func newCopyWriter(c *Client, fieldCount int) *CopyWriter {
	w := &CopyWriter{c: c, fieldCount: fieldCount}
	w.buf = make([]byte, copyHdrLen, copyFlushThreshold+64*1024)
	w.buf[0] = protocol.MsgCopyData
	return w
}

// CopyWriter is the binary-row builder for CopyFromBinary. Fields must
// match the column order in the COPY statement. All methods append into
// a single reusable buffer that is framed and flushed in-place; a
// finished row is never copied a second time between construction and
// the TCP write.
type CopyWriter struct {
	c          *Client
	fieldCount int

	buf      []byte // owned output buffer; buf[0:5] reserved for CopyData header
	rowStart int    // offset in buf where the current row's fieldCount prefix sits
	written  int    // fields appended to the current row

	scratch [16]byte
}

func (w *CopyWriter) writeLen(n int32) {
	binary.BigEndian.PutUint32(w.scratch[:4], uint32(n))
	w.buf = append(w.buf, w.scratch[:4]...)
}

// Null writes a SQL NULL placeholder (-1 length).
func (w *CopyWriter) Null() {
	w.buf = append(w.buf, 0xff, 0xff, 0xff, 0xff)
	w.written++
}

// Int2 writes an int16 column.
func (w *CopyWriter) Int2(v int16) {
	w.buf = append(w.buf,
		0, 0, 0, 2,
		byte(v>>8), byte(v))
	w.written++
}

// Int4 writes an int32 column.
func (w *CopyWriter) Int4(v int32) {
	w.buf = append(w.buf,
		0, 0, 0, 4,
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	w.written++
}

// Int8 writes an int64 column.
func (w *CopyWriter) Int8(v int64) {
	w.buf = append(w.buf,
		0, 0, 0, 8,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	w.written++
}

// Bool writes a PG bool (1 byte, 0 or 1).
func (w *CopyWriter) Bool(v bool) {
	b := byte(0)
	if v {
		b = 1
	}
	w.buf = append(w.buf, 0, 0, 0, 1, b)
	w.written++
}

// Float4 writes a float32 in IEEE-754 big-endian.
func (w *CopyWriter) Float4(v float32) {
	u := math.Float32bits(v)
	w.buf = append(w.buf,
		0, 0, 0, 4,
		byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
	w.written++
}

// Float8 writes a float64 in IEEE-754 big-endian.
func (w *CopyWriter) Float8(v float64) {
	u := math.Float64bits(v)
	w.buf = append(w.buf,
		0, 0, 0, 8,
		byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
		byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
	w.written++
}

// Text writes a UTF-8 string (PG text, varchar, bpchar).
func (w *CopyWriter) Text(s string) {
	w.writeLen(int32(len(s)))
	w.buf = append(w.buf, s...)
	w.written++
}

// Bytes writes a bytea column.
func (w *CopyWriter) Bytes(b []byte) {
	w.writeLen(int32(len(b)))
	w.buf = append(w.buf, b...)
	w.written++
}

// UUID writes a uuid as its 16 binary bytes.
func (w *CopyWriter) UUID(u [16]byte) {
	w.buf = append(w.buf, 0, 0, 0, 16)
	w.buf = append(w.buf, u[:]...)
	w.written++
}

// Timestamp writes a PG timestamp (microseconds since 2000-01-01 UTC).
func (w *CopyWriter) Timestamp(t time.Time) {
	us := t.Sub(pgEpoch).Microseconds()
	w.buf = append(w.buf,
		0, 0, 0, 8,
		byte(us>>56), byte(us>>48), byte(us>>40), byte(us>>32),
		byte(us>>24), byte(us>>16), byte(us>>8), byte(us))
	w.written++
}

// TimestampTZ writes a PG timestamptz. Wire format is identical to
// timestamp; PG stores UTC microseconds since 2000-01-01.
func (w *CopyWriter) TimestampTZ(t time.Time) {
	us := t.UTC().Sub(pgEpoch).Microseconds()
	w.buf = append(w.buf,
		0, 0, 0, 8,
		byte(us>>56), byte(us>>48), byte(us>>40), byte(us>>32),
		byte(us>>24), byte(us>>16), byte(us>>8), byte(us))
	w.written++
}

// flush patches the CopyData length prefix in place and writes the
// entire framed buffer to the socket. The type byte stays at buf[0];
// the payload length (bytes after the type byte, including the 4-byte
// length field itself) goes at buf[1:5].
func (w *CopyWriter) flush() error {
	if len(w.buf) <= copyHdrLen {
		return nil
	}
	binary.BigEndian.PutUint32(w.buf[1:5], uint32(len(w.buf)-1))
	if err := w.c.conn.WriteRaw(w.buf); err != nil {
		return err
	}
	// Keep the CopyData header, drop the payload.
	w.buf = w.buf[:copyHdrLen]
	return nil
}
