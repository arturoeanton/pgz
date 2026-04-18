// database/sql write-path tests against the pgz driver.
// Skipped without PGZ_TEST_DSN.
package tests

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/arturoeanton/pgz/pgz"
	_ "github.com/arturoeanton/pgz/pgz/stdlib"
)

func openPgzSQL(t testing.TB) *sql.DB {
	t.Helper()
	dsn := os.Getenv("PGZ_TEST_DSN")
	if dsn == "" {
		t.Skip("PGZ_TEST_DSN not set")
	}
	db, err := sql.Open("pgz", dsn)
	if err != nil {
		t.Fatal(err)
	}
	// 1 conn keeps the test deterministic — the driver is
	// single-connection and stmt cache is per-connection.
	db.SetMaxOpenConns(1)
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

func ensureUsersTable(t testing.TB, db *sql.DB) {
	t.Helper()
	_, _ = db.Exec("DROP TABLE IF EXISTS pgz_std_users")
	_, err := db.Exec(`CREATE UNLOGGED TABLE pgz_std_users (
		id    int4 PRIMARY KEY,
		name  text NOT NULL,
		email text
	)`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStdlibExecInsertUpdateDelete(t *testing.T) {
	db := openPgzSQL(t)
	defer db.Close()
	ensureUsersTable(t, db)

	res, err := db.Exec(`INSERT INTO pgz_std_users (id, name) VALUES ($1, $2)`,
		1, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("INSERT rows=%d", n)
	}

	res, err = db.Exec(`UPDATE pgz_std_users SET email = $1 WHERE id = $2`,
		"a@example.com", 1)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("UPDATE rows=%d", n)
	}

	var email string
	if err := db.QueryRow(`SELECT email FROM pgz_std_users WHERE id = $1`, 1).
		Scan(&email); err != nil {
		t.Fatal(err)
	}
	if email != "a@example.com" {
		t.Fatalf("email=%q", email)
	}

	res, err = db.Exec(`DELETE FROM pgz_std_users WHERE id = $1`, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("DELETE rows=%d", n)
	}

	_, _ = db.Exec("DROP TABLE pgz_std_users")
}

func TestStdlibTxCommitRollback(t *testing.T) {
	db := openPgzSQL(t)
	defer db.Close()
	ensureUsersTable(t, db)

	// Commit path.
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO pgz_std_users (id, name) VALUES ($1, $2)`,
		10, "bob"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := db.QueryRow(`SELECT count(*)::int FROM pgz_std_users WHERE id = $1`, 10).
		Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("commit lost the row")
	}

	// Rollback path.
	tx, err = db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO pgz_std_users (id, name) VALUES ($1, $2)`,
		11, "charlie"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*)::int FROM pgz_std_users WHERE id = $1`, 11).
		Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("rollback kept the row")
	}

	_, _ = db.Exec("DROP TABLE pgz_std_users")
}

func TestStdlibTxReadOnlyRejectsWrite(t *testing.T) {
	db := openPgzSQL(t)
	defer db.Close()
	ensureUsersTable(t, db)

	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`INSERT INTO pgz_std_users (id, name) VALUES ($1, $2)`, 99, "x")
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("expected INSERT to fail in READ ONLY tx")
	}
	var pgErr *pgz.PGError
	if !errors.As(err, &pgErr) {
		_ = tx.Rollback()
		t.Fatalf("want *pgz.PGError, got %T: %v", err, err)
	}
	// 25006 = read_only_sql_transaction
	if pgErr.Code != "25006" {
		_ = tx.Rollback()
		t.Fatalf("unexpected SQLSTATE %q: %v", pgErr.Code, err)
	}
	// Release the conn before the top-level cleanup; with
	// MaxOpenConns=1 the DROP TABLE below would otherwise deadlock
	// against the still-open tx.
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	_, _ = db.Exec("DROP TABLE pgz_std_users")
}

// TestStdlibPreparedInsert exercises repeated Exec via db.Prepare — the
// path sqlc / goose / sqlx use in production.
func TestStdlibPreparedInsert(t *testing.T) {
	db := openPgzSQL(t)
	defer db.Close()
	ensureUsersTable(t, db)

	stmt, err := db.Prepare(`INSERT INTO pgz_std_users (id, name) VALUES ($1, $2)`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec(i, "u"); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := db.QueryRow(`SELECT count(*)::int FROM pgz_std_users`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatalf("n=%d", n)
	}
	_, _ = db.Exec("DROP TABLE pgz_std_users")
}

// TestStdlibInsertReturning confirms the RETURNING-as-row pattern works
// since we do not support LastInsertId.
func TestStdlibInsertReturning(t *testing.T) {
	db := openPgzSQL(t)
	defer db.Close()
	_, _ = db.Exec("DROP TABLE IF EXISTS pgz_std_ret")
	if _, err := db.Exec(
		`CREATE UNLOGGED TABLE pgz_std_ret (id serial PRIMARY KEY, name text)`); err != nil {
		t.Fatal(err)
	}
	var id int
	err := db.QueryRow(
		`INSERT INTO pgz_std_ret (name) VALUES ($1) RETURNING id`, "x").Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("id=%d", id)
	}
	_, _ = db.Exec("DROP TABLE pgz_std_ret")
}
