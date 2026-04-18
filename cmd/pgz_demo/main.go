// pgz_demo runs a SELECT and prints the resulting JSON to stdout.
//
// Usage:
//   PGZ_DSN=postgres://user:pass@host:5432/db go run ./cmd/pgz_demo \
//     -mode array "SELECT 1 AS a, 'hi' AS b"
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/arturoeanton/pgz/pgz"
)

func main() {
	mode := flag.String("mode", "array", "array | ndjson | columnar")
	flag.Parse()

	dsn := os.Getenv("PGZ_DSN")
	if dsn == "" {
		fatal("PGZ_DSN not set")
	}
	if flag.NArg() < 1 {
		fatal("usage: pgz_demo [flags] <SQL>")
	}
	sql := flag.Arg(0)

	cfg, err := pgz.ParseDSN(dsn)
	if err != nil {
		fatal("dsn: " + err.Error())
	}
	ctx := context.Background()
	c, err := pgz.Open(ctx, cfg)
	if err != nil {
		fatal("open: " + err.Error())
	}
	defer c.Close()

	switch *mode {
	case "array":
		buf, err := c.QueryJSON(ctx, sql)
		if err != nil {
			fatal("query: " + err.Error())
		}
		os.Stdout.Write(buf)
		fmt.Println()
	case "ndjson":
		if err := c.StreamNDJSON(ctx, os.Stdout, sql); err != nil {
			fatal("stream: " + err.Error())
		}
	case "columnar":
		if err := c.StreamColumnar(ctx, os.Stdout, sql); err != nil {
			fatal("stream: " + err.Error())
		}
		fmt.Println()
	default:
		fatal("unknown mode: " + *mode)
	}
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
