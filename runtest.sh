#!/usr/bin/env bash
#
# runtest.sh -- run the full pgz test suite.
#
# Usage:
#   ./runtest.sh                     # unit tests only
#   PGZ_TEST_DSN='postgres://...' ./runtest.sh   # unit + integration
#
# Exit code is non-zero if any step fails.

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC} $1"; }
fail() { echo -e "${RED}FAIL${NC} $1"; echo -e "${RED}$2${NC}"; exit 1; }
info() { echo -e "${YELLOW}----${NC} $1"; }

# 1. Vet
info "go vet ./..."
VET_OUT=$(go vet ./... 2>&1) || fail "go vet" "$VET_OUT"
pass "go vet"

# 2. Build
info "go build ./..."
BUILD_OUT=$(go build ./... 2>&1) || fail "go build" "$BUILD_OUT"
pass "go build"

# 3. Unit tests
info "go test ./... (unit)"
TEST_OUT=$(go test ./... -count=1 -timeout=120s 2>&1) || fail "unit tests" "$TEST_OUT"
echo "$TEST_OUT"
pass "unit tests"

# 4. Integration tests (if DSN is set)
if [ -n "${PGZ_TEST_DSN:-}" ]; then
    info "integration tests (PGZ_TEST_DSN set)"
    INT_OUT=$(go test ./tests/ -count=1 -timeout=120s -v 2>&1) || fail "integration tests" "$INT_OUT"
    echo "$INT_OUT"
    pass "integration tests"

    # 5. Hot-path bench (zero-alloc check)
    info "hot-path benchmarks (zero-alloc enforcement)"
    BENCH_OUT=$(go test ./internal/rows/ -bench='RowEncodeMixed' -benchmem -count=1 2>&1) || fail "hot-path bench" "$BENCH_OUT"
    if echo "$BENCH_OUT" | grep -q '0 allocs/op'; then
        pass "zero-alloc hot loop"
    else
        fail "zero-alloc hot loop" "$BENCH_OUT"
    fi
else
    info "skipping integration tests (PGZ_TEST_DSN not set)"
fi

echo ""
echo -e "${GREEN}All checks passed.${NC}"
