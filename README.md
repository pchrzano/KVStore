# KVStore

A small sharded key-value store written in C++ on top of [Seastar](https://github.com/scylladb/seastar),
the framework behind ScyllaDB. It exposes a REST API and persists data to per-shard append-only files.

## How it works

- **Shards.** Seastar runs one reactor thread per CPU core (a *shard*). Every shard runs its own HTTP
  server and owns its own `Store`; shards share nothing and communicate only by message passing.
- **Key ownership.** A key belongs to shard `std::hash(key) % shard_count`. Whichever shard receives a
  request forwards the work to the owner with `invoke_on`, so each `Store` is only ever touched by its
  own thread — no locks.
- **Storage.** Each shard appends records to `store-<shard>.dat` in the working directory and keeps an
  in-memory index `std::map<Key, offset>` pointing at each key's latest record. Reads go to the file.
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

### Limits

- Keys: valid UTF-8, 0–255 bytes, no `/`. Values: valid UTF-8, any length.
- The index is in memory only: it is **not rebuilt from the files on restart** yet, so a restarted
  server starts empty. The files are never compacted (old versions and tombstones stay).
- The shard count (`--smp`) is fixed for the life of the process.

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
| `--unsafe-bypass-fsync 1` | off | skip `fdatasync` — faster, may lose data; for testing only |
| `--logger-log-level kvapp=warn` | `info` | silence the per-request "Putting…" log |

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
| `store.hh/.cc` | per-shard store: index, file format, DMA append and read |
| `key.hh/.cc` | `Key`: UTF-8 string up to 255 bytes, stored inline |
| `value.hh/.cc` | `Value`: UTF-8 string of any length |
| `utf8.hh/.cc` | UTF-8 validation |
| `log.hh` | the `kvapp` logger |
| `openapi.yaml` | API specification |
