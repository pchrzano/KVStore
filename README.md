# KVStore

A small sharded key-value store written in C++ on top of [Seastar](https://github.com/scylladb/seastar),
the framework behind ScyllaDB. It exposes a REST API and persists data to per-shard append-only files.

## How it works

- **Shards.** Seastar runs one reactor thread per CPU core (a *shard*). Every shard runs its own HTTP
  server and owns its own `Store`; shards share nothing and communicate only by message passing.
- **Key ownership.** A key belongs to shard `hash(key) % shard_count`, using a stable hash
  (FNV-1a), since the data files depend on it. Whichever shard receives a
  request forwards the work to the owner with `invoke_on`, so each `Store` is only ever touched by its
  own thread — no locks.
- **Storage.** Each shard appends records to `store-g<generation>-<shard>.dat` in the working
  directory and keeps an in-memory index `std::map<Key, offset>` pointing at each key's latest
  record. Reads go to the file, except for the most recently used values, which each shard keeps
  in an LRU cache (`--cache-size` pairs per shard, default 100). Newly written values go into the
  cache too. `kv.meta` records the format version, shard count and generation.
- **Durability.** Appends are serialized per shard, written with aligned DMA (`O_DIRECT`) writes and
  `fdatasync`ed before the request is answered.
- **Listing.** `GET /api/v1/kv` collects each shard's (already sorted) keys and k-way merges them.

### Record format

All integers are little-endian:

```
[type: u8][key size: u64][key bytes][value size: u64][value bytes]
type: 1 = put, 2 = delete (tombstone; value size 0, no value bytes)
```

Replaying a file from start to end gives the current state: the last record for a key wins.

### Startup and resharding

Before serving, the server rebuilds its index from the data files ([`store_loader.hh`](store_loader.hh)):

- **No `kv.meta`:** a new data directory (generation 1).
- **Same shard count:** every shard replays its own file. An incomplete record at the end (a crash
  during an append) is truncated away.
- **Different `--smp`:** the data is resharded. Each old file is replayed (every key lives in exactly
  one) and its live records are sent to their new owner shards, which write them to the next
  generation. Then `kv.meta` is atomically switched and the old files deleted. Files of any other
  generation are leftovers of an interrupted resharding and are deleted at startup, so a crash at
  any point leaves either the old or the new generation complete. Resharding also drops old
  versions and tombstones.

### Limits

- Keys: valid UTF-8, 1–255 bytes. In the URL a key is percent-encoded, so a key containing `/` is
  written as `%2F` (e.g. `/api/v1/kv/a%2Fb` is the key `a/b`); a raw `/` gives 404. Values: valid
  UTF-8, any length.
- Files are only compacted by resharding; otherwise old versions and tombstones stay.
- Records have no checksum, so a torn write whose bytes happen to look like a valid record would
  not be detected.
- The shard count (`--smp`) is fixed while the process runs; changing it between runs reshards.

## API

| Method | Path | Body | Responses |
|---|---|---|---|
| `PUT` | `/api/v1/kv/{key}` | the value (raw UTF-8) | `201` created, `200` updated, `400` invalid key or value |
| `GET` | `/api/v1/kv/{key}` | — | `200` `{"key":…,"value":…}`, `404`, `400` |
| `DELETE` | `/api/v1/kv/{key}` | — | `204` deleted, `404`, `400` |
| `GET` | `/api/v1/kv` | — | `200` `{"keys":[…]}` — all keys, sorted by bytes |

Swagger UI is served at **`/docs`**, and the OpenAPI spec at `/api/v1/openapi.yaml`
(source: [`openapi.yaml`](openapi.yaml), embedded into the binary at build time).

```bash
curl -X PUT --data-binary 'world' http://localhost:18080/api/v1/kv/hello   # {"key":"hello","created":true}
curl http://localhost:18080/api/v1/kv/hello                                # {"key":"hello","value":"world"}
curl http://localhost:18080/api/v1/kv                                      # {"keys":["hello"]}
curl -X DELETE http://localhost:18080/api/v1/kv/hello                      # 204
```

## Building and running

Seastar builds and runs on Linux only, so on macOS everything happens in Docker
(this setup uses [Colima](https://github.com/abiosoft/colima)).

### 1. Get Seastar and the dev image

Clone Seastar **next to** this repository (the scripts expect `../seastar`):

```
parent/
├── seastar/   ← github.com/scylladb/seastar (tested with b3d32700)
└── KVStore/   ← this repository
```

```bash
git clone https://github.com/scylladb/seastar.git
cd seastar
docker build -t seastar-dev -f docker/dev/Dockerfile .
```

### 2. Build Seastar

Seastar must be mounted at `/seastar`: its CMake package config contains absolute paths.

```bash
docker run --rm -v "$PWD":/seastar -w /seastar seastar-dev \
    bash -c './configure.py --mode=release && ninja -C build/release'
```

### 3. Build and run the app

```bash
cd ../KVStore
scripts/docker-run.sh                 # builds build/release, serves on http://localhost:18080
scripts/docker-run.sh --smp 4         # pass Seastar/app options (default: --smp 2 --memory 1G)
HOST_PORT=19000 scripts/docker-run.sh # use another port on the host
```

Building by hand inside the container:

```bash
cmake -S . -B build/release -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DSeastar_DIR=/seastar/build/release -DCMAKE_MODULE_PATH=/seastar/cmake
ninja -C build/release
```

### Options

| Option | Default | |
|---|---|---|
| `--address` | `0.0.0.0` | HTTP listen address |
| `--port` | `8080` | HTTP listen port (inside the container) |
| `--smp` | all cores | number of shards (Seastar) |
| `--memory` | most of RAM | memory for the app (Seastar) |
| `--cache-size` | `100` | key-value pairs cached per shard; `0` disables the cache |
| `--unsafe-bypass-fsync 1` | off | skip `fdatasync` — faster, may lose data; for testing only |
| `--logger-log-level <logger>=<level>` | `info` | log level per logger, e.g. `kv-store=debug:kv-http=debug` (see [Logging](#logging)) |

## Logging

Each part of the app has its own logger, so its level can be set on its own
(`--logger-log-level kv-store=debug:kv-http=debug`; separate loggers with `:`).

| Logger | `info` / `warn` / `error` | `debug` |
|---|---|---|
| `kvapp` | start (options), fsync bypass warning, listening, stopping, stopped | — |
| `kv-loader` | what startup does (new directory, load or reshard), summary with key count and time, `kv.meta` switch, removed files, startup failure with what it was doing | each migrated file |
| `kv-store` | cut-off broken file tail, slow appends (≥ 100 ms, with the fsync time), corrupt data | every put, get (cache hit or miss) and delete, per-shard load |
| `kv-http` | every request that failed with 500, with the error | access log: method, path, status, time, receiving and owner shard |

At the default level (`info`) nothing is logged per request. Values are never logged, only their size.

## Running the tests

The tests talk to a running server over HTTP and need only Python 3.9+ (standard library).
Start the server (e.g. `scripts/docker-run.sh`), then from the host:

```bash
tests/run_tests.sh                                # 10 iterations against http://localhost:18080
tests/run_tests.sh 20 http://localhost:19000      # iterations, server URL
```

`run_tests.sh` runs two tests, each one creating its keys and then, every iteration, reading all of
them (checking each value is the latest one written) and updating all of them:

| Test | Keys |
|---|---|
| 1 | 50 |
| 2 | 150 — more than the default cache of 100 pairs per shard with `--smp 1`, so reads go to disk |

Each prints its read and update times and exits with status 1 on any wrong value or status code.
To run one test with your own settings:

```bash
tests/read_update_test.py --keys 150 --iterations 20 --url http://localhost:18080
```

## Debugging in CLion

1. Build the debug image (Seastar image + gdb configured for Seastar's signals):
   `docker build -t seastar-dev-debug -f docker/Dockerfile.debug docker`
2. In CLion, add a **Docker toolchain** named `Seastar Docker` with image `seastar-dev-debug`,
   mount `../seastar` at `/seastar`, and container options
   `--cap-add=SYS_PTRACE --security-opt seccomp=unconfined --network host`.
3. Use the **Debug (Docker)** CMake preset ([`CMakePresets.json`](CMakePresets.json)) and the
   **kv-app (Debug)** run configuration (`--smp 2 --memory 1G --port 18081`).
   **kv-app (Docker)** runs `scripts/docker-run.sh` instead.

To see which shard a stopped thread belongs to, in gdb:
`p 'seastar::internal::this_shard_id_ptr()::g_this_shard_id'`.

## Source layout

| File | |
|---|---|
| `main.cc` | startup: options, `sharded<WebApi>`, HTTP server, signal handling |
| `webapi.hh/.cc` | REST handlers, routing to owner shards, Swagger UI |
| `store.hh/.cc` | per-shard store: index, file format, DMA append and read, replay |
| `store_loader.hh/.cc` | startup: `kv.meta`, loading, resharding, cleanup of old generations |
| `lru_cache.hh/.cc` | `LRUCache`: least recently used cache of a fixed number of key-value pairs |
| `key.hh/.cc` | `Key`: UTF-8 string up to 255 bytes, stored inline |
| `value.hh/.cc` | `Value`: UTF-8 string of any length |
| `utf8.hh/.cc` | UTF-8 validation |
| `log.hh` | the loggers (`kvapp`, `kv-loader`, `kv-store`, `kv-http`) |
| `tests/` | HTTP tests: `run_tests.sh`, `read_update_test.py` |
| `openapi.yaml` | API specification |
