package pgz

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/arturoeanton/pgz/internal/bufferpool"
	"github.com/arturoeanton/pgz/internal/pgerr"
	"github.com/arturoeanton/pgz/internal/protocol"
	"github.com/arturoeanton/pgz/internal/rows"
)

// ExecResult holds the outcome of a non-SELECT statement.
type ExecResult struct {
	// RowsAffected is the number of rows inserted, updated, or deleted.
	// For CALL / DDL it is 0.
	RowsAffected int64
	// Tag is the raw CommandComplete tag, e.g. "INSERT 0 5", "DELETE 3".
	Tag string
}

// Exec executes a DML statement (INSERT, UPDATE, DELETE, CALL, etc.) that
// does not return rows. For statements with RETURNING clauses use
// ExecReturning instead.
//
// The extended-query protocol is used so parameters are supported. The
// statement cache is shared with the SELECT path — PgBouncer-txn
// re-prepare works identically.
func (c *Client) Exec(ctx context.Context, sql string, args ...any) (ExecResult, error) {
	if d := c.cfg.DefaultQueryTimeout; d > 0 {
		if _, hasDL := ctx.Deadline(); !hasDL {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
	}
	c.observer.OnQueryStart(sql)
	start := time.Now()
	var res ExecResult
	var err error
	defer func() {
		c.recordEnd(sql, 0, 0, time.Since(start), err)
	}()
	if err = c.attachContext(ctx); err != nil {
		return res, err
	}
	defer c.detachContext()

	const maxRetries = 3
	backoff := 10 * time.Millisecond
	for attempt := 0; ; attempt++ {
		res, err = c.execCached(sql, args)
		if err == nil || !c.cfg.RetryOnSerialization {
			return res, err
		}
		if attempt+1 >= maxRetries || !isRetryableSerialization(err) {
			return res, err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return res, err
		}
		select {
		case <-ctx.Done():
			return res, err
		case <-time.After(backoff):
		}
		backoff *= 2
	}
}

// execCached runs a DML statement via the extended-query protocol. It
// mirrors runCached but expects no DataRows and parses CommandComplete.
func (c *Client) execCached(sql string, args []any) (ExecResult, error) {
	for attempt := 0; attempt < 2; attempt++ {
		st := c.stmts.get(sql)
		fromCache := st != nil
		if st == nil {
			var err error
			st, err = c.execPrepare(sql)
			if err != nil {
				c.stmts.invalidate(sql)
				return ExecResult{}, err
			}
			c.stmts.put(sql, st)
		}
		res, err := c.execBind(st, args)
		if err == nil {
			return res, nil
		}
		if attempt == 0 && fromCache && isStaleStmtError(err) {
			c.stmts.invalidate(sql)
			continue
		}
		return res, err
	}
	return ExecResult{}, nil
}

// execPrepare runs Parse + Describe + Sync for a DML statement.
// Unlike prepareAndDescribe, it accepts NoData (no RowDescription)
// which is normal for INSERT/UPDATE/DELETE without RETURNING.
func (c *Client) execPrepare(sql string) (*preparedStmt, error) {
	name := c.stmts.nextName()

	for _, dead := range c.stmts.takePendingClose() {
		cl := c.conn.Begin(protocol.MsgClose)
		cl.Byte('S')
		cl.CString(dead)
		cl.Finish()
	}
	p := c.conn.Begin(protocol.MsgParse)
	p.CString(name)
	p.CString(sql)
	p.Int16(0)
	p.Finish()
	d := c.conn.Begin(protocol.MsgDescribe)
	d.Byte('S')
	d.CString(name)
	d.Finish()
	c.conn.Begin(protocol.MsgSync).Finish()
	if err := c.conn.Flush(); err != nil {
		return nil, err
	}

	var plan *rows.Plan
	var noData bool
	var pgError *pgerr.Error
	for {
		t, body, err := c.conn.ReadMessage()
		if err != nil {
			return nil, err
		}
		switch t {
		case protocol.MsgParseComplete, protocol.MsgParameterDescription,
			protocol.MsgCloseComplete:
		case protocol.MsgNoData:
			noData = true
		case protocol.MsgRowDescription:
			// DML with RETURNING — has a result set like SELECT.
			plan, err = rows.ParseRowDescription(body)
			if err != nil {
				return nil, err
			}
		case protocol.MsgErrorResponse:
			pgError = pgerr.Parse(body)
		case protocol.MsgNoticeResponse:
			c.observer.OnNotice(pgerr.Parse(body))
		case protocol.MsgParameterStatus:
			k, v := splitParameter(body)
			c.params[k] = v
		case protocol.MsgReadyForQuery:
			if pgError != nil {
				return nil, pgError
			}
			if noData {
				// No result columns — pure DML. plan stays nil.
				return &preparedStmt{name: name}, nil
			}
			if plan == nil {
				return nil, fmt.Errorf("pgz: exec prepare returned neither NoData nor RowDescription")
			}
			fmts := rows.PickResultFormatsEx(plan.Columns, c.cfg.BinaryNumeric)
			if len(c.cfg.BinaryOIDs) > 0 {
				for i, col := range plan.Columns {
					for _, oid := range c.cfg.BinaryOIDs {
						if protocol.OID(oid) == col.TypeOID {
							fmts[i] = 1
							break
						}
					}
				}
			}
			plan.ApplyFormatsEx(fmts, c.cfg.BinaryNumeric)
			return &preparedStmt{name: name, plan: plan, resultFmts: fmts}, nil
		default:
			return nil, fmt.Errorf("pgz: unexpected msg %q during exec prepare", t)
		}
	}
}

// execBind runs Bind + Execute + Sync for a DML statement (no DataRows
// expected). Returns the parsed CommandComplete tag.
func (c *Client) execBind(st *preparedStmt, args []any) (ExecResult, error) {
	bd := c.conn.Begin(protocol.MsgBind)
	bd.CString("")
	bd.CString(st.name)
	bd.Int16(0)
	bd.Int16(int16(len(args)))
	for _, a := range args {
		s, isNull, err := encodeArg(a)
		if err != nil {
			return ExecResult{}, err
		}
		if isNull {
			bd.Int32(-1)
		} else {
			bd.Int32(int32(len(s)))
			bd.String(s)
		}
	}
	bd.Int16(0) // no result format codes (no result columns)
	bd.Finish()
	ex := c.conn.Begin(protocol.MsgExecute)
	ex.CString("")
	ex.Int32(0)
	ex.Finish()
	c.conn.Begin(protocol.MsgSync).Finish()
	if err := c.conn.Flush(); err != nil {
		return ExecResult{}, err
	}

	var res ExecResult
	var pgError *pgerr.Error
	for {
		t, body, err := c.conn.ReadMessage()
		if err != nil {
			return res, err
		}
		switch t {
		case protocol.MsgBindComplete:
		case protocol.MsgCommandComplete:
			res.Tag = string(body[:len(body)-1]) // strip trailing NUL
			res.RowsAffected = parseRowsAffected(res.Tag)
		case protocol.MsgEmptyQueryResponse:
		case protocol.MsgNoticeResponse:
			c.observer.OnNotice(pgerr.Parse(body))
		case protocol.MsgParameterStatus:
			k, v := splitParameter(body)
			c.params[k] = v
		case protocol.MsgErrorResponse:
			pgError = pgerr.Parse(body)
		case protocol.MsgReadyForQuery:
			if pgError != nil {
				return res, pgError
			}
			return res, nil
		default:
			return res, fmt.Errorf("pgz: unexpected msg %q during exec", t)
		}
	}
}

// ExecReturning executes a DML statement with a RETURNING clause and
// streams the returned rows as NDJSON to w. On the wire this is identical
// to a SELECT — the server sends RowDescription + DataRow* +
// CommandComplete. We reuse the full JSON streaming path, bypassing the
// SELECT-only guard.
func (c *Client) ExecReturning(ctx context.Context, w io.Writer, sql string, args ...any) error {
	return c.runQueryInner(ctx, c.newFlushWriter(w), ModeNDJSON, sql, args)
}

// ExecReturningJSON is the buffered variant of ExecReturning. Returns
// the JSON output as []byte (NDJSON).
func (c *Client) ExecReturningJSON(ctx context.Context, sql string, args ...any) ([]byte, error) {
	bp := bufferpool.Get()
	defer bufferpool.Put(bp)
	*bp = (*bp)[:0]
	w := &bufWriter{p: bp, rowsHint: c.cfg.RowsHint}
	if err := c.runQueryInner(ctx, w, ModeNDJSON, sql, args); err != nil {
		return nil, err
	}
	out := make([]byte, len(*bp))
	copy(out, *bp)
	return out, nil
}

// parseRowsAffected extracts the row count from a CommandComplete tag.
// Tag format examples:
//
//	"INSERT 0 5"  → 5
//	"UPDATE 3"    → 3
//	"DELETE 10"   → 10
//	"SELECT 100"  → 100
//	"CALL"        → 0
//	"CREATE TABLE"→ 0
func parseRowsAffected(tag string) int64 {
	// The row count is always the last space-separated token for DML.
	idx := strings.LastIndexByte(tag, ' ')
	if idx < 0 {
		return 0
	}
	n, err := strconv.ParseInt(tag[idx+1:], 10, 64)
	if err != nil {
		return 0
	}
	return n
}
