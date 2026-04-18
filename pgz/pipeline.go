package pgz

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/arturoeanton/pgz/internal/pgerr"
	"github.com/arturoeanton/pgz/internal/protocol"
	"github.com/arturoeanton/pgz/internal/rows"
)

// Batch accumulates queries to be flushed to the server in a single
// round-trip.
//
//	b := pgz.NewBatch()
//	b.Queue("UPDATE users SET last_seen = now() WHERE id = $1", 42)
//	b.Queue("INSERT INTO audit (user_id, action) VALUES ($1, $2)", 42, "login")
//	br := c.SendBatch(ctx, b)
//	for range b.Len() { _, err := br.Exec() ; ... }
//	br.Close()
//
// All queued items ship in one TCP write. The server executes them
// sequentially; an error in one item aborts the rest with
// ErrBatchAborted. SendBatch keeps the shared prepared-statement
// cache warm — the first occurrence of a given SQL triggers Parse +
// Describe + Bind + Execute, and subsequent items in the same batch
// (and future batches) only pay Bind + Execute.
type Batch struct {
	items []batchItem
}

type batchItem struct {
	sql  string
	args []any
}

// NewBatch allocates an empty batch. Reusing a batch across
// SendBatch calls is allowed — items are not cleared automatically.
func NewBatch() *Batch { return &Batch{} }

// Queue appends a statement + args to the batch.
func (b *Batch) Queue(sql string, args ...any) {
	b.items = append(b.items, batchItem{sql: sql, args: args})
}

// Len returns the number of queued items.
func (b *Batch) Len() int { return len(b.items) }

// Reset clears the batch so the underlying slice can be reused.
func (b *Batch) Reset() { b.items = b.items[:0] }

// ErrBatchAborted is returned from BatchResults.Exec / Query for
// items that follow one that errored.
var ErrBatchAborted = fmt.Errorf("pgz: batch aborted by earlier item")

// BatchResults consumes the ordered response stream produced by
// SendBatch. Call Exec (or Query) once per queued item, in order;
// Close drains the stream and must be called before using the
// underlying Client for anything else.
type BatchResults struct {
	c         *Client
	n         int
	idx       int
	done      bool
	aborted   bool
	closed    bool
	ctxStop   context.CancelFunc
	sendStart time.Time

	// plan[i] is non-nil for items whose first-of-kind Parse appears
	// immediately before them in the pipeline. readItemExec uses it
	// to know how many response messages precede CommandComplete.
	parsePlan []parseStage
}

// parseStage records what the server will emit before BindComplete
// for a single batch item: either nothing (cached stmt) or the full
// Parse/Describe response block.
type parseStage struct {
	hasParse     bool // ParseComplete expected
	hasDescribe  bool // ParameterDescription + (RowDescription|NoData) expected
	sql          string
	name         string // server-side stmt name (when hasParse)
	cached       *preparedStmt
}

// SendBatch pipelines every queued item in a single TCP write and
// returns the BatchResults iterator. The first occurrence of each
// unique SQL inside the batch is Parsed + Described and cached on
// the Client so the next batch (or plain Exec) skips the Parse.
func (c *Client) SendBatch(ctx context.Context, b *Batch) *BatchResults {
	br := &BatchResults{c: c, n: len(b.items)}
	if d := c.cfg.DefaultQueryTimeout; d > 0 {
		if _, hasDL := ctx.Deadline(); !hasDL {
			ctx, br.ctxStop = context.WithTimeout(ctx, d)
		}
	}
	if err := c.attachContext(ctx); err != nil {
		br.done = true
		br.aborted = true
		return br
	}
	c.observer.OnQueryStart("pgz.SendBatch")
	br.sendStart = time.Now()

	// Drain any pending stmt Close messages piggy-backed from previous
	// evictions — same trick Exec uses.
	for _, dead := range c.stmts.takePendingClose() {
		cl := c.conn.Begin(protocol.MsgClose)
		cl.Byte('S')
		cl.CString(dead)
		cl.Finish()
	}

	br.parsePlan = make([]parseStage, len(b.items))
	seen := map[string]*preparedStmt{}

	for i, it := range b.items {
		stage := parseStage{sql: it.sql}

		// Prefer the persistent Client stmtCache; within-batch duplicate
		// SQL piggy-backs on the same cached entry.
		if cached, ok := seen[it.sql]; ok {
			stage.cached = cached
		} else if cached := c.stmts.get(it.sql); cached != nil {
			stage.cached = cached
			seen[it.sql] = cached
		} else {
			stage.hasParse = true
			stage.hasDescribe = true
			stage.name = c.stmts.nextName()
			// Pre-insert a provisional cache entry so the next item
			// with the same SQL in this batch picks it up. We finalise
			// the paramOIDs / plan after reading the response.
			stage.cached = &preparedStmt{name: stage.name}
			seen[it.sql] = stage.cached

			p := c.conn.Begin(protocol.MsgParse)
			p.CString(stage.name)
			p.CString(it.sql)
			p.Int16(0)
			p.Finish()
			d := c.conn.Begin(protocol.MsgDescribe)
			d.Byte('S')
			d.CString(stage.name)
			d.Finish()
		}

		if err := c.writeBindExecute(stage.cached, it.args); err != nil {
			br.done = true
			br.aborted = true
			c.detachContext()
			return br
		}
		br.parsePlan[i] = stage
	}
	// Single Sync closes the pipeline.
	c.conn.Begin(protocol.MsgSync).Finish()
	if err := c.conn.Flush(); err != nil {
		br.done = true
		br.aborted = true
		c.detachContext()
		return br
	}
	return br
}

// writeBindExecute appends Bind + Execute for one batch item. Uses
// the same writeBindParams the rest of the driver uses, so binary
// params come along for free once the stmt's paramOIDs are known
// (second and later occurrences of a cached SQL).
func (c *Client) writeBindExecute(st *preparedStmt, args []any) error {
	bd := c.conn.Begin(protocol.MsgBind)
	bd.CString("")       // unnamed portal
	bd.CString(st.name)  // "" for first-of-kind (Parse of unnamed), else named
	if err := writeBindParams(bd, st.paramOIDs, args); err != nil {
		return err
	}
	bd.Int16(0) // text result format (DML / RETURNING coverage); no result cols for pure DML
	bd.Finish()
	ex := c.conn.Begin(protocol.MsgExecute)
	ex.CString("")
	ex.Int32(0)
	ex.Finish()
	return nil
}

// Exec consumes the next item as a DML statement and returns its
// row count. Returns ErrBatchAborted once a previous item has failed.
func (br *BatchResults) Exec() (ExecResult, error) {
	var zero ExecResult
	if br.closed {
		return zero, fmt.Errorf("pgz: BatchResults closed")
	}
	if br.idx >= br.n {
		return zero, io.EOF
	}
	stage := br.parsePlan[br.idx]
	br.idx++
	if br.aborted {
		return zero, ErrBatchAborted
	}
	res, err := br.readItemExec(stage)
	if err != nil {
		br.aborted = true
	}
	return res, err
}

// Query consumes the next item as a row-producing statement and
// returns an Iterator. The caller must drive the iterator to
// completion before calling the next result method.
func (br *BatchResults) Query() (*Iterator, error) {
	if br.closed {
		return nil, fmt.Errorf("pgz: BatchResults closed")
	}
	if br.idx >= br.n {
		return nil, io.EOF
	}
	stage := br.parsePlan[br.idx]
	br.idx++
	if br.aborted {
		return nil, ErrBatchAborted
	}
	it, err := br.readItemQuery(stage)
	if err != nil {
		br.aborted = true
	}
	return it, err
}

// Close drains remaining items and the trailing RFQ, releases the
// ctx watcher, and reports the batch duration to the Observer.
func (br *BatchResults) Close() error {
	if br.closed {
		return nil
	}
	br.closed = true
	for br.idx < br.n && !br.done {
		if err := br.skipItem(); err != nil {
			break
		}
		br.idx++
	}
	if !br.done {
		br.drainUntilReady()
	}
	br.c.detachContext()
	if br.ctxStop != nil {
		br.ctxStop()
		br.ctxStop = nil
	}
	br.c.recordEnd("pgz.SendBatch", br.n, 0, time.Since(br.sendStart), nil)
	return nil
}

// readItemExec reads the response block for one DML batch item.
// Stage tells us which preamble (ParseComplete + ParameterDescription
// + NoData / RowDescription) precedes the BindComplete.
func (br *BatchResults) readItemExec(stage parseStage) (ExecResult, error) {
	var res ExecResult
	var pgError *pgerr.Error
	for {
		t, body, err := br.c.conn.ReadMessage()
		if err != nil {
			return res, err
		}
		switch t {
		case protocol.MsgParseComplete:
			// First-of-kind item; fall through.
		case protocol.MsgParameterDescription:
			if stage.cached != nil {
				stage.cached.paramOIDs = parseParamOIDs(body)
			}
		case protocol.MsgRowDescription:
			if stage.cached != nil {
				plan, perr := rows.ParseRowDescription(body)
				if perr == nil {
					stage.cached.plan = plan
				}
			}
		case protocol.MsgNoData:
			// Pure DML; plan stays nil.
		case protocol.MsgBindComplete:
			// Bind succeeded; expect CommandComplete next.
		case protocol.MsgDataRow:
			// User called Exec on a Query-shaped item. Drain silently.
		case protocol.MsgCommandComplete:
			res.Tag = string(body[:len(body)-1])
			res.RowsAffected = parseRowsAffected(res.Tag)
			if pgError != nil {
				return res, pgError
			}
			// Promote the provisional cache entry to persistent.
			br.commitStage(stage)
			return res, nil
		case protocol.MsgEmptyQueryResponse:
			if pgError != nil {
				return res, pgError
			}
			br.commitStage(stage)
			return res, nil
		case protocol.MsgErrorResponse:
			pgError = pgerr.Parse(body)
		case protocol.MsgNoticeResponse:
			br.c.observer.OnNotice(pgerr.Parse(body))
		case protocol.MsgParameterStatus:
			k, v := splitParameter(body)
			br.c.params[k] = v
		case protocol.MsgReadyForQuery:
			br.done = true
			if pgError != nil {
				return res, pgError
			}
			return res, ErrBatchAborted
		default:
			return res, fmt.Errorf("pgz: unexpected msg %q in batch Exec", t)
		}
	}
}

// readItemQuery waits for the RowDescription (or NoData if the
// statement returns no rows) and hands the caller an Iterator that
// owns the wire until its Close.
func (br *BatchResults) readItemQuery(stage parseStage) (*Iterator, error) {
	var pgError *pgerr.Error
	for {
		t, body, err := br.c.conn.ReadMessage()
		if err != nil {
			return nil, err
		}
		switch t {
		case protocol.MsgParseComplete, protocol.MsgBindComplete:
		case protocol.MsgParameterDescription:
			if stage.cached != nil {
				stage.cached.paramOIDs = parseParamOIDs(body)
			}
		case protocol.MsgRowDescription:
			plan, perr := rows.ParseRowDescription(body)
			if perr != nil {
				return nil, perr
			}
			if stage.cached != nil {
				stage.cached.plan = plan
			}
			it := &Iterator{
				c:     br.c,
				plan:  plan,
				bound: true,
				st:    stage.cached,
			}
			br.commitStage(stage)
			return it, nil
		case protocol.MsgNoData:
			it := &Iterator{c: br.c, bound: true, done: false, st: stage.cached}
			br.commitStage(stage)
			return it, nil
		case protocol.MsgErrorResponse:
			pgError = pgerr.Parse(body)
		case protocol.MsgNoticeResponse:
			br.c.observer.OnNotice(pgerr.Parse(body))
		case protocol.MsgParameterStatus:
			k, v := splitParameter(body)
			br.c.params[k] = v
		case protocol.MsgReadyForQuery:
			br.done = true
			if pgError != nil {
				return nil, pgError
			}
			return nil, ErrBatchAborted
		default:
			return nil, fmt.Errorf("pgz: unexpected msg %q in batch Query", t)
		}
	}
}

// commitStage promotes a first-of-kind stage's provisional stmt entry
// into the persistent cache. Idempotent for subsequent items that
// already shared the entry through the batch-local seen map.
func (br *BatchResults) commitStage(stage parseStage) {
	if !stage.hasParse || stage.cached == nil {
		return
	}
	br.c.stmts.put(stage.sql, stage.cached)
}

// skipItem swallows one response block without interpreting it.
func (br *BatchResults) skipItem() error {
	for {
		t, body, err := br.c.conn.ReadMessage()
		if err != nil {
			return err
		}
		switch t {
		case protocol.MsgCommandComplete, protocol.MsgEmptyQueryResponse:
			return nil
		case protocol.MsgReadyForQuery:
			br.done = true
			return nil
		case protocol.MsgErrorResponse:
			_ = pgerr.Parse(body)
		case protocol.MsgNoticeResponse:
			br.c.observer.OnNotice(pgerr.Parse(body))
		case protocol.MsgParameterStatus:
			k, v := splitParameter(body)
			br.c.params[k] = v
		}
	}
}

// drainUntilReady reads until the batch's terminating RFQ.
func (br *BatchResults) drainUntilReady() {
	for !br.done {
		t, _, err := br.c.conn.ReadMessage()
		if err != nil {
			return
		}
		if t == protocol.MsgReadyForQuery {
			br.done = true
			return
		}
	}
}
