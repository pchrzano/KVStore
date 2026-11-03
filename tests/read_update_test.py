#!/usr/bin/env python3
"""Read/update test for kv-app.

Creates `--keys` keys (default 50) with values, then `--iterations` times:
  1. reads all the keys and checks each value is the latest one written,
  2. updates all the values.

Uses only the Python standard library. Exits with status 1 on any failure.

    tests/read_update_test.py --iterations 10
    tests/read_update_test.py --keys 150 --url http://localhost:18080 --iterations 100
"""

import argparse
import http.client
import json
import sys
import time
import urllib.parse


def make_value(key_index: int, iteration: int) -> str:
    # Non-ASCII text, to exercise UTF-8 handling.
    return f"wartość-{key_index}-iteracja-{iteration}"


class Client:
    def __init__(self, url: str):
        parsed = urllib.parse.urlsplit(url)
        self.conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=30)

    def request(self, method: str, key: str, body: str | None = None) -> tuple[int, bytes]:
        path = "/api/v1/kv/" + urllib.parse.quote(key, safe="")
        data = body.encode() if body is not None else None
        self.conn.request(method, path, body=data)
        resp = self.conn.getresponse()
        return resp.status, resp.read()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:18080", help="server base URL")
    parser.add_argument("-k", "--keys", type=int, default=50, help="number of keys")
    parser.add_argument("-x", "--iterations", type=int, default=10, help="read+update rounds")
    parser.add_argument("--prefix", default="read-update-", help="key name prefix")
    args = parser.parse_args()

    client = Client(args.url)
    keys = [f"{args.prefix}{i}" for i in range(args.keys)]
    expected: dict[str, str] = {}
    failures = 0

    def fail(msg: str) -> None:
        nonlocal failures
        failures += 1
        print(f"FAIL: {msg}", file=sys.stderr)

    # Create (a key left over from an earlier run is updated instead: 200).
    start = time.perf_counter()
    for i, key in enumerate(keys):
        value = make_value(i, 0)
        status, _ = client.request("PUT", key, value)
        if status not in (200, 201):
            fail(f"create {key}: HTTP {status}")
        expected[key] = value
    print(f"created {args.keys} keys in {time.perf_counter() - start:.3f}s")

    reads = updates = 0
    read_time = update_time = 0.0
    for n in range(1, args.iterations + 1):
        start = time.perf_counter()
        for key in keys:
            status, body = client.request("GET", key)
            reads += 1
            if status != 200:
                fail(f"iteration {n}: GET {key}: HTTP {status}")
                continue
            got = json.loads(body)
            if got.get("key") != key or got.get("value") != expected[key]:
                fail(f"iteration {n}: GET {key}: expected {expected[key]!r}, got {got!r}")
        read_time += time.perf_counter() - start

        start = time.perf_counter()
        for i, key in enumerate(keys):
            value = make_value(i, n)
            status, _ = client.request("PUT", key, value)
            updates += 1
            if status != 200:
                fail(f"iteration {n}: update {key}: HTTP {status} (expected 200)")
            expected[key] = value
        update_time += time.perf_counter() - start

    def rate(count: int, seconds: float) -> str:
        return f"{count / seconds:.0f}/s" if seconds > 0 else "-"

    print(f"{args.iterations} iterations: {reads} reads in {read_time:.3f}s ({rate(reads, read_time)}), "
          f"{updates} updates in {update_time:.3f}s ({rate(updates, update_time)})")
    if failures:
        print(f"FAILED: {failures} failures", file=sys.stderr)
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
