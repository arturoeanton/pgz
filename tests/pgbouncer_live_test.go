// Live test against a real PgBouncer in transaction mode. Set
// PGZ_PGBOUNCER_DSN to enable. The test uses a tiny default_pool_size
// in the PgBouncer config plus many concurrent clients to force backend
// rotation between transactions, which is precisely what would expose
// the SQLSTATE 26000 ("prepared statement does not exist") issue if our
// statement cache wasn't PgBouncer-aware.
package tests

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/arturoeanton/pgz/pgz"
)

func TestLivePgBouncerRotation(t *testing.T) {
	dsn := os.Getenv("PGZ_PGBOUNCER_DSN")
	if dsn == "" {
		t.Skip("PGZ_PGBOUNCER_DSN not set; skipping live PgBouncer test")
	}
	cfg, err := pgz.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	const N = 20
	const Q = 50
	var clients [N]*pgz.Client
	for i := 0; i < N; i++ {
		c, err := pgz.Open(context.Background(), cfg)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		clients[i] = c
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	var wg sync.WaitGroup
	var fails int64
	for i, c := range clients {
		wg.Add(1)
		go func(i int, c *pgz.Client) {
			defer wg.Done()
			for j := 0; j < Q; j++ {
				_, err := c.QueryJSON(context.Background(),
					"SELECT $1::int AS i, $2::text AS s",
					i*1000+j, "hello")
				if err != nil {
					atomic.AddInt64(&fails, 1)
					t.Errorf("client %d query %d: %v", i, j, err)
					return
				}
			}
		}(i, c)
	}
	wg.Wait()
	if fails != 0 {
		t.Fatalf("%d failures across %d queries", fails, N*Q)
	}
}
