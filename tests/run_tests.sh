#!/usr/bin/env bash
set -u

ITERATIONS=${1:-10}
URL=${2:-http://localhost:18080}
DIR=$(cd "$(dirname "$0")" && pwd)
failed=0

run() {
    local name=$1; shift
    echo "== $name"
    if ! python3 "$DIR/read_update_test.py" --url "$URL" -x "$ITERATIONS" "$@"; then
        failed=1
    fi
}

run "test 1: 50 keys"  --keys 50  --prefix read-update-50-
run "test 2: 150 keys" --keys 150 --prefix read-update-150-

if [ "$failed" -ne 0 ]; then
    echo "SOME TESTS FAILED"
    exit 1
fi
echo "ALL TESTS PASSED"
