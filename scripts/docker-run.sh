#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEASTAR_DIR="${SEASTAR_DIR:-$(cd "$APP_DIR/../seastar" && pwd)}"
IMAGE="${IMAGE:-seastar-dev}"
HOST_PORT="${HOST_PORT:-18080}"
NAME=kv-app

if ! docker info >/dev/null 2>&1; then
    echo "==> Docker not reachable, starting Colima..."
    colima start
fi

MOUNTS=(-v "$SEASTAR_DIR":/seastar -v "$APP_DIR":/app)

echo "==> Building kv-app (release)"
docker run --rm "${MOUNTS[@]}" -w /app "$IMAGE" bash -c '
    set -e
    if [ ! -f build/release/CMakeCache.txt ]; then
        cmake -S . -B build/release -G Ninja -Wno-dev \
            -DCMAKE_BUILD_TYPE=Release \
            -DSeastar_DIR=/seastar/build/release \
            -DCMAKE_MODULE_PATH=/seastar/cmake
    fi
    ninja -C build/release
'

docker rm -f "$NAME" >/dev/null 2>&1 || true

TTY=()
if [ -t 0 ] && [ -t 1 ]; then
    TTY=(-it)
fi

if [ $# -eq 0 ]; then
    set -- --smp 2 --memory 1G
fi

echo "==> Starting kv-app on http://localhost:$HOST_PORT  (Ctrl-C to stop)"
echo "    try: curl http://localhost:$HOST_PORT/api/v1/kv/hello"
exec docker run --rm ${TTY[@]+"${TTY[@]}"} --name "$NAME" -p "$HOST_PORT":8080 \
    "${MOUNTS[@]}" -w /app "$IMAGE" ./build/release/kv-app "$@"
