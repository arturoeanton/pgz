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

// CopyTo runs COPY ... TO STDOUT and pipes the server's output straight
// to w. The SQL must be a valid COPY statement, e.g.:
//
//	"COPY (SELECT * FROM users) TO STDOUT (FORMAT csv)"
//	"COPY logs TO STDOUT"                                 // default TEXT
//
// CopyData bodies are written to w as they arrive — the bytes are
// aliased from the wire's internal buffer so the pump does not copy
// through an intermediate scratch. Returns the row count reported by
// the server on CommandComplete.
//
// CopyTo uses the simple-query protocol and shares no state with the
// prepared-statement cache or the SELECT hot path.
func (c *Client) CopyTo(ctx context.Context, sql string, w io.Writer) (int64, error) {
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
	var bytesOut int64
	var err error
	defer func() {
		c.recordEnd(sql, int(rows), int(bytesOut), time.Since(start), err)
	}()
	if err = c.attachContext(ctx); err != nil {
		return 0, err
	}
	defer c.detachContext()
	rows, bytesOut, err = c.copyTo(sql, w)
	return rows, err
}

// CopyToBinary runs COPY ... TO STDOUT (FORMAT binary) and delivers
// each tuple to handler through a CopyReader. The SQL must request
// binary format:
//
//	"COPY (SELECT id, name, created_at FROM users) TO STDOUT (FORMAT binary)"
//
// fieldCount is the number of columns per tuple declared by the COPY
// statement. Enforced per row: a server tuple with a different width
// aborts the stream. handler may return io.EOF to stop early.
//
// The CopyReader fields are read directly from the wire buffer with
// no per-field allocation; Text and Bytes return slices aliasing the
// message buffer so the handler must copy if it wants to keep them.
func (c *Client) CopyToBinary(ctx context.Context, sql string, fieldCount int,
	handler func(r *CopyReader) error) (int64, error) {
	if fieldCount <= 0 || fieldCount > 1<<15-1 {
		return 0, fmt.Errorf("pgz: CopyToBinary: invalid fieldCount %d", fieldCount)
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
	rows, err = c.copyToBinary(sql, fieldCount, handler)
	return rows, err
}

// copyTo pumps CopyData bodies directly to w.
func (c *Client) copyTo(sql string, w io.Writer) (int64, int64, error) {
	q := c.conn.Begin(protocol.MsgQuery)
	q.CString(sql)
	if err := q.Send(); err != nil {
		return 0, 0, err
	}
	if err := c.awaitCopyOutResponse(); err != nil {
		return 0, 0, err
	}
	var bytesOut int64
	for {
		t, body, err := c.conn.ReadMessage()
		if err != nil {
			return 0, bytesOut, err
		}
		switch t {
		case protocol.MsgCopyData:
			n, werr := w.Write(body)
			bytesOut += int64(n)
			if werr != nil {
				c.drainUntilReady()
				return 0, bytesOut, werr
			}
		case protocol.MsgCopyDone:
			// keep reading; CommandComplete follows.
		case protocol.MsgCommandComplete:
			// tag strips trailing NUL.
			tag := string(body[:len(body)-1])
			rows := parseRowsAffected(tag)
			if err := c.waitForReady(); err != nil {
				return rows, bytesOut, err
			}
			return rows, bytesOut, nil
		case protocol.MsgErrorResponse:
			pgErr := pgerr.Parse(body)
			c.drainUntilReady()
			return 0, bytesOut, pgErr
		case protocol.MsgNoticeResponse:
			c.observer.OnNotice(pgerr.Parse(body))
		case protocol.MsgParameterStatus:
			k, v := splitParameter(body)
			c.params[k] = v
		default:
			return 0, bytesOut, fmt.Errorf("pgz: unexpected msg %q during COPY TO", t)
		}
	}
}

// copyToBinary parses the PGCOPY header and streams tuples to handler.
func (c *Client) copyToBinary(sql string, fieldCount int,
	handler func(*CopyReader) error) (int64, error) {
	q := c.conn.Begin(protocol.MsgQuery)
	q.CString(sql)
	if err := q.Send(); err != nil {
		return 0, err
	}
	if err := c.awaitCopyOutResponse(); err != nil {
		return 0, err
	}

	// PGCOPY header: consume from the first CopyData chunks until we
	// have 19 bytes: 11-byte magic + int32 flags + int32 header-ext-len.
	// The server sends the header either in its own CopyData message or
	// concatenated with the first tuple.
	var hdrCursor int
	var headerSeen bool
	cr := &CopyReader{fieldCount: fieldCount}

	var rows int64
	for {
		t, body, err := c.conn.ReadMessage()
		if err != nil {
			return rows, err
		}
		switch t {
		case protocol.MsgCopyData:
			if !headerSeen {
				body, hdrCursor, headerSeen, err = consumePGCopyHeader(body, hdrCursor)
				if err != nil {
					c.drainUntilReady()
					return rows, err
				}
				if !headerSeen {
					continue
				}
			}
			for len(body) > 0 {
				// int16 field count (or -1 for trailer).
				if len(body) < 2 {
					return rows, fmt.Errorf("pgz: truncated CopyData tuple header")
				}
				fc := int16(binary.BigEndian.Uint16(body[0:2]))
				if fc == -1 {
					// trailer; the server will send CopyDone next.
					body = body[2:]
					continue
				}
				if int(fc) != fieldCount {
					return rows, fmt.Errorf("pgz: CopyToBinary: server tuple has %d fields, expected %d",
						fc, fieldCount)
				}
				body = body[2:]
				cr.reset(body)
				if err := handler(cr); err != nil {
					if err == io.EOF {
						c.drainUntilReady()
						return rows, nil
					}
					c.drainUntilReady()
					return rows, err
				}
				if cr.err != nil {
					c.drainUntilReady()
					return rows, cr.err
				}
				rows++
				body = cr.body
			}
		case protocol.MsgCopyDone:
			// continue; CommandComplete follows.
		case protocol.MsgCommandComplete:
			if err := c.waitForReady(); err != nil {
				return rows, err
			}
			return rows, nil
		case protocol.MsgErrorResponse:
			pgErr := pgerr.Parse(body)
			c.drainUntilReady()
			return rows, pgErr
		case protocol.MsgNoticeResponse:
			c.observer.OnNotice(pgerr.Parse(body))
		case protocol.MsgParameterStatus:
			k, v := splitParameter(body)
			c.params[k] = v
		default:
			return rows, fmt.Errorf("pgz: unexpected msg %q during COPY TO binary", t)
		}
	}
}

// consumePGCopyHeader walks through the 19-byte PGCOPY header which may
// be split across multiple CopyData messages. Returns the remaining
// body (past the header), the updated cursor, and whether the header
// has been fully consumed.
func consumePGCopyHeader(body []byte, cursor int) ([]byte, int, bool, error) {
	const hdrLen = 19
	if cursor == 0 && len(body) >= 11 {
		// Validate magic once.
		if string(body[:11]) != "PGCOPY\n\xff\r\n\x00" {
			return nil, cursor, false, fmt.Errorf("pgz: invalid PGCOPY magic")
		}
	}
	remaining := hdrLen - cursor
	if len(body) < remaining {
		return nil, cursor + len(body), false, nil
	}
	return body[remaining:], hdrLen, true, nil
}

// awaitCopyOutResponse reads until the server signals CopyOutResponse.
func (c *Client) awaitCopyOutResponse() error {
	var pgError *pgerr.Error
	for {
		t, body, err := c.conn.ReadMessage()
		if err != nil {
			return err
		}
		switch t {
		case protocol.MsgCopyOutResponse:
			return nil
		case protocol.MsgErrorResponse:
			pgError = pgerr.Parse(body)
		case protocol.MsgReadyForQuery:
			if pgError != nil {
				return pgError
			}
			return fmt.Errorf("pgz: ReadyForQuery before CopyOutResponse")
		case protocol.MsgNoticeResponse:
			c.observer.OnNotice(pgerr.Parse(body))
		case protocol.MsgParameterStatus:
			k, v := splitParameter(body)
			c.params[k] = v
		case protocol.MsgCommandComplete:
			return fmt.Errorf("pgz: COPY completed before producing data")
		default:
			return fmt.Errorf("pgz: unexpected msg %q while awaiting CopyOutResponse", t)
		}
	}
}

// waitForReady reads until ReadyForQuery (used between CommandComplete
// and the conn being reusable). Notices are forwarded.
func (c *Client) waitForReady() error {
	for {
		t, body, err := c.conn.ReadMessage()
		if err != nil {
			return err
		}
		switch t {
		case protocol.MsgReadyForQuery:
			return nil
		case protocol.MsgNoticeResponse:
			c.observer.OnNotice(pgerr.Parse(body))
		case protocol.MsgParameterStatus:
			k, v := splitParameter(body)
			c.params[k] = v
		}
	}
}

// CopyReader is the binary-tuple reader surface handed to
// CopyToBinary's handler callback. Field order must match the COPY
// column order. The slices returned by Text and Bytes alias the
// internal wire buffer and are valid only until the next handler
// invocation — the caller must copy to keep them.
type CopyReader struct {
	fieldCount int
	body       []byte
	err        error
}

func (r *CopyReader) reset(body []byte) {
	r.body = body
	r.err = nil
}

// fieldHeader reads the int32 length and returns (length, isNull).
// Sets r.err and returns isNull=false on malformed input.
func (r *CopyReader) fieldHeader() (int32, bool) {
	if r.err != nil {
		return 0, false
	}
	if len(r.body) < 4 {
		r.err = fmt.Errorf("pgz: CopyReader: truncated field header")
		return 0, false
	}
	l := int32(binary.BigEndian.Uint32(r.body[0:4]))
	r.body = r.body[4:]
	if l == -1 {
		return 0, true
	}
	if l < 0 || int(l) > len(r.body) {
		r.err = fmt.Errorf("pgz: CopyReader: bad field length %d", l)
		return 0, false
	}
	return l, false
}

// IsNull peeks at the next field's length header without consuming the
// value bytes. Handy when the caller wants to branch before picking a
// typed reader.
func (r *CopyReader) IsNull() bool {
	if r.err != nil {
		return false
	}
	if len(r.body) < 4 {
		r.err = fmt.Errorf("pgz: CopyReader: truncated field header")
		return false
	}
	l := int32(binary.BigEndian.Uint32(r.body[0:4]))
	return l == -1
}

// SkipNull consumes a NULL field (-1 length). Returns true if the next
// field was NULL and has been consumed; false otherwise (field still
// pending). Convenient with Null() follow-up readers.
func (r *CopyReader) SkipNull() bool {
	if r.err != nil || len(r.body) < 4 {
		return false
	}
	if int32(binary.BigEndian.Uint32(r.body[0:4])) == -1 {
		r.body = r.body[4:]
		return true
	}
	return false
}

// Int2 reads a 2-byte int. NULL becomes 0 with the bool set to true.
func (r *CopyReader) Int2() (int16, bool) {
	l, null := r.fieldHeader()
	if null || r.err != nil {
		return 0, null
	}
	if l != 2 {
		r.err = fmt.Errorf("pgz: CopyReader.Int2: expected 2 bytes, got %d", l)
		return 0, false
	}
	v := int16(binary.BigEndian.Uint16(r.body[:2]))
	r.body = r.body[2:]
	return v, false
}

// Int4 reads a 4-byte int.
func (r *CopyReader) Int4() (int32, bool) {
	l, null := r.fieldHeader()
	if null || r.err != nil {
		return 0, null
	}
	if l != 4 {
		r.err = fmt.Errorf("pgz: CopyReader.Int4: expected 4 bytes, got %d", l)
		return 0, false
	}
	v := int32(binary.BigEndian.Uint32(r.body[:4]))
	r.body = r.body[4:]
	return v, false
}

// Int8 reads an 8-byte int.
func (r *CopyReader) Int8() (int64, bool) {
	l, null := r.fieldHeader()
	if null || r.err != nil {
		return 0, null
	}
	if l != 8 {
		r.err = fmt.Errorf("pgz: CopyReader.Int8: expected 8 bytes, got %d", l)
		return 0, false
	}
	v := int64(binary.BigEndian.Uint64(r.body[:8]))
	r.body = r.body[8:]
	return v, false
}

// Bool reads a 1-byte PG bool.
func (r *CopyReader) Bool() (bool, bool) {
	l, null := r.fieldHeader()
	if null || r.err != nil {
		return false, null
	}
	if l != 1 {
		r.err = fmt.Errorf("pgz: CopyReader.Bool: expected 1 byte, got %d", l)
		return false, false
	}
	v := r.body[0] != 0
	r.body = r.body[1:]
	return v, false
}

// Float4 reads a 4-byte IEEE-754 big-endian.
func (r *CopyReader) Float4() (float32, bool) {
	l, null := r.fieldHeader()
	if null || r.err != nil {
		return 0, null
	}
	if l != 4 {
		r.err = fmt.Errorf("pgz: CopyReader.Float4: expected 4 bytes, got %d", l)
		return 0, false
	}
	v := math.Float32frombits(binary.BigEndian.Uint32(r.body[:4]))
	r.body = r.body[4:]
	return v, false
}

// Float8 reads an 8-byte IEEE-754 big-endian.
func (r *CopyReader) Float8() (float64, bool) {
	l, null := r.fieldHeader()
	if null || r.err != nil {
		return 0, null
	}
	if l != 8 {
		r.err = fmt.Errorf("pgz: CopyReader.Float8: expected 8 bytes, got %d", l)
		return 0, false
	}
	v := math.Float64frombits(binary.BigEndian.Uint64(r.body[:8]))
	r.body = r.body[8:]
	return v, false
}

// Text returns a slice aliasing the wire buffer. Valid only until the
// next handler invocation — copy if you need to keep it.
func (r *CopyReader) Text() ([]byte, bool) {
	l, null := r.fieldHeader()
	if null || r.err != nil {
		return nil, null
	}
	v := r.body[:l]
	r.body = r.body[l:]
	return v, false
}

// Bytes is an alias for Text for bytea-typed columns (identical wire
// format at this layer; the server already delivered raw bytes).
func (r *CopyReader) Bytes() ([]byte, bool) { return r.Text() }

// UUID reads a 16-byte UUID.
func (r *CopyReader) UUID() ([16]byte, bool) {
	var out [16]byte
	l, null := r.fieldHeader()
	if null || r.err != nil {
		return out, null
	}
	if l != 16 {
		r.err = fmt.Errorf("pgz: CopyReader.UUID: expected 16 bytes, got %d", l)
		return out, false
	}
	copy(out[:], r.body[:16])
	r.body = r.body[16:]
	return out, false
}

// Timestamp returns the raw int64 microseconds since the PG epoch
// (2000-01-01 UTC). Use time.Time(...) conversion at the call site if
// a Go time is needed.
func (r *CopyReader) Timestamp() (int64, bool) {
	l, null := r.fieldHeader()
	if null || r.err != nil {
		return 0, null
	}
	if l != 8 {
		r.err = fmt.Errorf("pgz: CopyReader.Timestamp: expected 8 bytes, got %d", l)
		return 0, false
	}
	v := int64(binary.BigEndian.Uint64(r.body[:8]))
	r.body = r.body[8:]
	return v, false
}

// TimestampTime returns a time.Time built from the Timestamp int64.
func (r *CopyReader) TimestampTime() (time.Time, bool) {
	us, null := r.Timestamp()
	if null || r.err != nil {
		return time.Time{}, null
	}
	return pgEpoch.Add(time.Duration(us) * time.Microsecond), false
}

// Err returns any error that occurred during field reads. Handlers
// may check this after each row instead of every individual reader.
func (r *CopyReader) Err() error { return r.err }
