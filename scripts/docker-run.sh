#!/usr/bin/env bash
# Build kv-app (release) and run it in a Docker container.
#
#   scripts/docker-run.sh [app args...]      e.g. scripts/docker-run.sh --smp 4
#
# Environment overrides:
#   HOST_PORT    port on the Mac (default 18080)
#   SEASTAR_DIR  Seastar checkout (default ../seastar next to kv-app)
#   IMAGE        Docker image (default seastar-dev)
#
# Stop with Ctrl-C (or CLion's stop button); the container is removed on exit.
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEASTAR_DIR="${SEASTAR_DIR:-$(cd "$APP_DIR/../seastar" && pwd)}"
IMAGE="${IMAGE:-seastar-dev}"
HOST_PORT="${HOST_PORT:-18080}"
NAME=kv-app

# Docker runs inside the Colima VM; start it if it isn't up.
if ! docker info >/dev/null 2>&1; then
    echo "==> Docker not reachable, starting Colima..."
    colima start
fi

# Seastar must be mounted at /seastar: its CMake config contains that path.
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

# Replace a container left over from a previous run.
docker rm -f "$NAME" >/dev/null 2>&1 || true

# -it only when there is a terminal (CLion's terminal or a shell), so the
# script also works from a plain run console.
TTY=()
if [ -t 0 ] && [ -t 1 ]; then
    TTY=(-it)
fi

if [ $# -eq 0 ]; then
    set -- --smp 2 --memory 1G
fi

echo "==> Starting kv-app on http://localhost:$HOST_PORT  (Ctrl-C to stop)"
echo "    try: curl http://localhost:$HOST_PORT/api/v1/kv/hello"
# ${TTY[@]+...} keeps an empty array safe under `set -u` in macOS's bash 3.2.
exec docker run --rm ${TTY[@]+"${TTY[@]}"} --name "$NAME" -p "$HOST_PORT":8080 \
    "${MOUNTS[@]}" -w /app "$IMAGE" ./build/release/kv-app "$@"
