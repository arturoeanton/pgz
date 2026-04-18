// Package stdlib registers pgz as a database/sql driver named
// "pgz". Import it for side effects:
//
//	import (
//	    "database/sql"
//	    _ "github.com/arturoeanton/pgz/pgz/stdlib"
//	)
//
//	db, err := sql.Open("pgz", "postgres://user:pass@host/db?sslmode=disable")
//	rows, err := db.Query("SELECT id, name FROM users WHERE active = $1", true)
//	_, err = db.Exec("INSERT INTO users (name) VALUES ($1)", "alice")
//
// Both read and write paths are supported. Transactions go through
// BeginTx / Commit / Rollback. Positional parameters ($1..$N) only —
// named parameters return an error.
//
// The adapter shares the same prepared-statement cache and the same
// wire connection as the native API, so sql.DB using this driver pays
// the standard database/sql interface-boxing cost on top of that, but
// no second pool, no duplicated handshake, no duplicated stmt cache.
//
// The native API (pgz.ScanStruct, pgz.StreamNDJSON, pgz.Exec,
// pgz.CopyFromBinary) remains the fastest path when you own the code
// end-to-end. Use this adapter when you need sqlc / goose / sqlx /
// golang-migrate compatibility or when an existing codebase already
// builds on database/sql.
package stdlib

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/arturoeanton/pgz/internal/protocol"
	"github.com/arturoeanton/pgz/internal/rows"
	"github.com/arturoeanton/pgz/pgz"
)

func init() {
	sql.Register("pgz", &Driver{})
}

// Driver is the registered database/sql driver. Users typically do
// not reference this type directly — sql.Open("pgz", ...) is all
// that is required.
type Driver struct{}

// Open parses dsn and connects. Used by sql.DB when no Connector is
// registered. Prefer NewConnector if you need to customise Config
// fields that ParseDSN does not expose (TLS callbacks, RuntimeParams,
// etc.).
func (Driver) Open(dsn string) (driver.Conn, error) {
	cfg, err := pgz.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c, err := pgz.Open(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	return &Conn{client: c}, nil
}

// OpenConnector returns a database/sql Connector bound to dsn.
// sql.DB uses this to open new connections on demand.
func (Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := pgz.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{cfg: cfg}, nil
}

// NewConnector builds a Connector from an already-constructed Config.
// Useful when ParseDSN cannot express the settings you need (for
// example, a TLS config with a custom RootCAs pool).
func NewConnector(cfg pgz.Config) driver.Connector { return &connector{cfg: cfg} }

type connector struct {
	cfg pgz.Config
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	cl, err := pgz.Open(ctx, c.cfg)
	if err != nil {
		return nil, err
	}
	return &Conn{client: cl}, nil
}

func (c *connector) Driver() driver.Driver { return Driver{} }

// Conn implements driver.Conn / ConnPrepareContext / ConnBeginTx /
// ExecerContext / QueryerContext / Pinger / Validator. The inTx flag
// short-circuits BeginTx when a transaction is already open — the
// database/sql layer would catch it too, but surfacing it here keeps
// the error message specific.
type Conn struct {
	client *pgz.Client
	bad    bool
	inTx   bool
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &Stmt{c: c, query: query, numInput: -1}, nil
}

func (c *Conn) Close() error {
	if c.client == nil {
		return nil
	}
	err := c.client.Close()
	c.client = nil
	return err
}

// Begin implements driver.Conn by delegating to BeginTx with default
// options. database/sql calls this for legacy BeginTx-less code paths.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx opens a transaction. The isolation level and read-only flag
// are translated to their SQL equivalents and issued as part of the
// opening statement. Nesting is rejected; database/sql normally
// prevents it but we surface a specific error.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.bad || c.client == nil {
		return nil, driver.ErrBadConn
	}
	if c.inTx {
		return nil, errors.New("pgz/stdlib: nested transaction not supported")
	}
	stmt := buildBeginStmt(opts)
	if _, err := c.client.Exec(ctx, stmt); err != nil {
		c.markBad(err)
		return nil, err
	}
	c.inTx = true
	return &pgzTx{c: c}, nil
}

// ExecContext runs a DML / DDL statement. On PG protocol-level errors
// (ErrorResponse) we return a *pgz.PGError untouched so callers can
// errors.As it; on wire-level errors we mark the conn bad and return
// driver.ErrBadConn so database/sql evicts it from the pool.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.bad || c.client == nil {
		return nil, driver.ErrBadConn
	}
	anyArgs, err := namedToAny(args)
	if err != nil {
		return nil, err
	}
	res, err := c.client.Exec(ctx, query, anyArgs...)
	if err != nil {
		c.markBad(err)
		return nil, err
	}
	return execResult{rows: res.RowsAffected}, nil
}

// markBad flags the conn as unreusable when err is a wire-level
// failure. PG-protocol errors (ErrorResponse wrapped in *pgz.PGError)
// leave the connection in a recoverable state — PG already sent
// ReadyForQuery — so we do NOT mark bad for those. Everything else
// (net.OpError, broken pipe, short read) means the conn is dead.
func (c *Conn) markBad(err error) {
	var pgErr *pgz.PGError
	if errors.As(err, &pgErr) {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// ctx cancellation does not mean the socket is corrupt; the
		// pgz CancelRequest path leaves the conn reusable.
		return
	}
	c.bad = true
}

// QueryContext is the fast path for database/sql.Query without a
// separate Prepare step. args are converted via namedToAny. Uses
// RawQueryAny so INSERT/UPDATE/DELETE … RETURNING work through
// db.QueryRow exactly like SELECT.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.bad || c.client == nil {
		return nil, driver.ErrBadConn
	}
	anyArgs, err := namedToAny(args)
	if err != nil {
		return nil, err
	}
	it, err := c.client.RawQueryAny(ctx, query, anyArgs...)
	if err != nil {
		c.markBad(err)
		return nil, err
	}
	return newRows(it)
}

// Ping forwards to the underlying client.
func (c *Conn) Ping(ctx context.Context) error {
	if c.client == nil {
		return driver.ErrBadConn
	}
	return c.client.Ping(ctx)
}

// IsValid is called by sql.DB's connection validator.
func (c *Conn) IsValid() bool { return c.client != nil && !c.bad }

// Stmt implements driver.Stmt / StmtQueryContext / StmtExecContext.
type Stmt struct {
	c        *Conn
	query    string
	numInput int // -1 means unknown; database/sql tolerates that.
}

func (s *Stmt) Close() error { return nil } // nothing to release; real caching lives in pgz.stmts

func (s *Stmt) NumInput() int { return s.numInput }

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamed(args))
}

// ExecContext reuses the shared native stmt cache by SQL key. The
// second call with the same query skips Parse + Describe and goes
// straight to Bind + Execute + Sync.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.c.bad || s.c.client == nil {
		return nil, driver.ErrBadConn
	}
	anyArgs, err := namedToAny(args)
	if err != nil {
		return nil, err
	}
	res, err := s.c.client.Exec(ctx, s.query, anyArgs...)
	if err != nil {
		s.c.markBad(err)
		return nil, err
	}
	return execResult{rows: res.RowsAffected}, nil
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamed(args))
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.c.bad || s.c.client == nil {
		return nil, driver.ErrBadConn
	}
	anyArgs, err := namedToAny(args)
	if err != nil {
		return nil, err
	}
	it, err := s.c.client.RawQueryAny(ctx, s.query, anyArgs...)
	if err != nil {
		s.c.markBad(err)
		return nil, err
	}
	return newRows(it)
}

// --- Rows ---------------------------------------------------------------

// Inline-dispatch opcodes for Rows.Next. Mirrors the fast-path
// design from fillStructRow: identify the common column shapes by
// a single byte so the hot loop can handle them without a function
// call. Anything we do not specifically recognise falls back to the
// generic decoder.
const (
	opNextFallback uint8 = iota
	opNextInt2
	opNextInt4
	opNextInt8
	opNextFloat4
	opNextFloat8
	opNextBool
	opNextText // text / varchar / bpchar / name — emit string
	opNextJSONB
	opNextJSON
	opNextBytea
)

type pgRows struct {
	it       *pgz.Iterator
	plan     *rows.Plan
	decoders []func([]byte) any
	ops      []uint8
	names    []string
}

func newRows(it *pgz.Iterator) (*pgRows, error) {
	plan := it.Plan()
	if plan == nil {
		// Shouldn't happen — RawQuery resolves the plan before return.
		_ = it.Close()
		return nil, fmt.Errorf("pgz/stdlib: no plan resolved for query")
	}
	names := make([]string, len(plan.Columns))
	decoders := make([]func([]byte) any, len(plan.Columns))
	ops := make([]uint8, len(plan.Columns))
	for i, col := range plan.Columns {
		names[i] = col.Name
		decoders[i] = pgz.DriverValueDecoder(uint32(col.TypeOID), col.Format)
		ops[i] = opForNext(col.TypeOID, col.Format)
	}
	return &pgRows{it: it, plan: plan, decoders: decoders, ops: ops, names: names}, nil
}

// opForNext maps a (OID, format) pair to an inline opcode for
// Rows.Next. Returns opNextFallback for anything not in the fast
// path — the caller then uses the pre-built decoder function.
func opForNext(oid protocol.OID, format int16) uint8 {
	if format != 1 {
		return opNextFallback
	}
	switch oid {
	case protocol.OIDBool:
		return opNextBool
	case protocol.OIDInt2:
		return opNextInt2
	case protocol.OIDInt4, protocol.OIDOID:
		return opNextInt4
	case protocol.OIDInt8:
		return opNextInt8
	case protocol.OIDFloat4:
		return opNextFloat4
	case protocol.OIDFloat8:
		return opNextFloat8
	case protocol.OIDText, protocol.OIDVarchar, protocol.OIDBPChar, protocol.OIDName:
		return opNextText
	case protocol.OIDJSONB:
		return opNextJSONB
	case protocol.OIDJSON:
		return opNextJSON
	case protocol.OIDBytea:
		return opNextBytea
	}
	return opNextFallback
}

func (r *pgRows) Columns() []string { return r.names }

func (r *pgRows) Close() error { return r.it.Close() }

func (r *pgRows) Next(dest []driver.Value) error {
	body, err := r.it.NextRaw()
	if err == io.EOF {
		return io.EOF
	}
	if err != nil {
		return err
	}
	if len(body) < 2 {
		return fmt.Errorf("pgz/stdlib: short DataRow")
	}
	n := int(int16(binary.BigEndian.Uint16(body[0:2])))
	if n != len(r.decoders) {
		return fmt.Errorf("pgz/stdlib: column count mismatch (got %d, plan %d)", n, len(r.decoders))
	}
	body = body[2:]
	for i := 0; i < n; i++ {
		if len(body) < 4 {
			return fmt.Errorf("pgz/stdlib: short column header")
		}
		l := int32(uint32(body[0])<<24 | uint32(body[1])<<16 |
			uint32(body[2])<<8 | uint32(body[3]))
		body = body[4:]
		if l == -1 {
			dest[i] = nil
			continue
		}
		if l < 0 || int(l) > len(body) {
			return fmt.Errorf("pgz/stdlib: bad column length %d", l)
		}
		raw := body[:l]
		body = body[l:]
		// Inline fast path for the common binary shapes. Each case
		// avoids the function-pointer call to r.decoders[i] and lets
		// the switch lower to a jump table. Anything uncovered falls
		// through to the generic decoder at the bottom.
		switch r.ops[i] {
		case opNextInt4:
			if len(raw) == 4 {
				v := int32(uint32(raw[0])<<24 | uint32(raw[1])<<16 |
					uint32(raw[2])<<8 | uint32(raw[3]))
				dest[i] = int64(v)
				continue
			}
		case opNextInt8:
			if len(raw) == 8 {
				dest[i] = int64(uint64(raw[0])<<56 | uint64(raw[1])<<48 |
					uint64(raw[2])<<40 | uint64(raw[3])<<32 |
					uint64(raw[4])<<24 | uint64(raw[5])<<16 |
					uint64(raw[6])<<8 | uint64(raw[7]))
				continue
			}
		case opNextInt2:
			if len(raw) == 2 {
				dest[i] = int64(int16(uint16(raw[0])<<8 | uint16(raw[1])))
				continue
			}
		case opNextBool:
			if len(raw) == 1 {
				dest[i] = raw[0] != 0
				continue
			}
		case opNextFloat8:
			if len(raw) == 8 {
				u := uint64(raw[0])<<56 | uint64(raw[1])<<48 |
					uint64(raw[2])<<40 | uint64(raw[3])<<32 |
					uint64(raw[4])<<24 | uint64(raw[5])<<16 |
					uint64(raw[6])<<8 | uint64(raw[7])
				dest[i] = math.Float64frombits(u)
				continue
			}
		case opNextFloat4:
			if len(raw) == 4 {
				u := uint32(raw[0])<<24 | uint32(raw[1])<<16 |
					uint32(raw[2])<<8 | uint32(raw[3])
				dest[i] = float64(math.Float32frombits(u))
				continue
			}
		case opNextText:
			dest[i] = string(raw)
			continue
		case opNextJSONB:
			// jsonb binary: 1-byte version prefix + body. Strip it so
			// consumers see canonical JSON bytes.
			if len(raw) > 0 && raw[0] == 1 {
				b := make([]byte, len(raw)-1)
				copy(b, raw[1:])
				dest[i] = b
				continue
			}
		case opNextJSON, opNextBytea:
			b := make([]byte, len(raw))
			copy(b, raw)
			dest[i] = b
			continue
		}
		dest[i] = r.decoders[i](raw)
	}
	return nil
}

// --- Result / Tx --------------------------------------------------------

// execResult is the driver.Result returned from any non-Query Exec.
// LastInsertId is unsupported on PostgreSQL — the canonical pattern
// is RETURNING id via db.QueryRow. We surface that in the error
// message rather than lie about a zero value.
type execResult struct {
	rows int64
}

func (r execResult) LastInsertId() (int64, error) {
	return 0, errors.New("pgz/stdlib: LastInsertId not supported — use INSERT ... RETURNING id with QueryRow")
}

func (r execResult) RowsAffected() (int64, error) { return r.rows, nil }

// pgzTx is the driver.Tx implementation. Commit/Rollback release the
// inTx flag and clear the conn so database/sql returns it to the pool.
type pgzTx struct {
	c *Conn
}

func (t *pgzTx) Commit() error   { return t.finish("COMMIT") }
func (t *pgzTx) Rollback() error { return t.finish("ROLLBACK") }

func (t *pgzTx) finish(stmt string) error {
	if t.c == nil {
		return errors.New("pgz/stdlib: tx already finished")
	}
	c := t.c
	t.c = nil
	if !c.inTx {
		return nil
	}
	c.inTx = false
	if c.bad || c.client == nil {
		return driver.ErrBadConn
	}
	_, err := c.client.Exec(context.Background(), stmt)
	if err != nil {
		c.markBad(err)
	}
	return err
}

// buildBeginStmt emits a single BEGIN … statement that carries the
// requested isolation level and read-only flag. Doing it in one
// statement (vs BEGIN; SET TRANSACTION …) saves a round-trip.
func buildBeginStmt(opts driver.TxOptions) string {
	base := "BEGIN"
	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
		// no-op
	case sql.LevelReadUncommitted:
		base += " ISOLATION LEVEL READ UNCOMMITTED"
	case sql.LevelReadCommitted:
		base += " ISOLATION LEVEL READ COMMITTED"
	case sql.LevelRepeatableRead:
		base += " ISOLATION LEVEL REPEATABLE READ"
	case sql.LevelSerializable:
		base += " ISOLATION LEVEL SERIALIZABLE"
	}
	if opts.ReadOnly {
		if base == "BEGIN" {
			base += " READ ONLY"
		} else {
			base += " READ ONLY"
		}
	}
	return base
}

// --- helpers ------------------------------------------------------------

func namedToAny(args []driver.NamedValue) ([]any, error) {
	if len(args) == 0 {
		return nil, nil
	}
	out := make([]any, len(args))
	for i, a := range args {
		if a.Name != "" {
			// Named parameters would require pgz to expose the
			// $name → $N translation; database/sql passes them through
			// only when the driver opts in via NamedValueChecker. We
			// do not today, so a non-empty Name means the caller tried
			// to use named params — surface it rather than mis-bind.
			return nil, fmt.Errorf("pgz/stdlib: named parameters not supported (got %q); use positional $1..$N", a.Name)
		}
		out[i] = a.Value
	}
	return out, nil
}

func valuesToNamed(args []driver.Value) []driver.NamedValue {
	if len(args) == 0 {
		return nil
	}
	out := make([]driver.NamedValue, len(args))
	for i, v := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return out
}

// Compile-time interface assertions so a missing method surfaces at
// build time rather than as a confusing runtime error.
var (
	_ driver.Driver          = Driver{}
	_ driver.DriverContext   = Driver{}
	_ driver.Connector       = (*connector)(nil)
	_ driver.Conn            = (*Conn)(nil)
	_ driver.ConnBeginTx     = (*Conn)(nil)
	_ driver.ConnPrepareContext = (*Conn)(nil)
	_ driver.ExecerContext   = (*Conn)(nil)
	_ driver.QueryerContext  = (*Conn)(nil)
	_ driver.Pinger          = (*Conn)(nil)
	_ driver.Validator       = (*Conn)(nil)
	_ driver.Stmt            = (*Stmt)(nil)
	_ driver.StmtExecContext = (*Stmt)(nil)
	_ driver.StmtQueryContext = (*Stmt)(nil)
	_ driver.Rows            = (*pgRows)(nil)
)
