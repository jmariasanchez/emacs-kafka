#!/usr/bin/env bash
# Build and test kafka.el on Linux inside an Apple `container` (or docker /
# podman) VM: Debian trixie, Emacs 30.1, stable Rust, a local Kafka broker.
# Usage: scripts/linux-check.sh [quick|snapshot]
#   quick: build + clippy + unit tests only, no broker.
#   snapshot: use a Linux Emacs installed in $CACHE/emacs-snapshot/install.
set -euo pipefail
cd "$(dirname "$0")/.."
RUNTIME="${CONTAINER_RUNTIME:-$(command -v container || command -v docker || command -v podman)}"
IMAGE=kafka-el-linux:latest
CACHE="${KAFKA_EL_CACHE:-$HOME/.cache/kafka-el}"
mkdir -p "$CACHE/linux-target" "$CACHE/linux-cargo-registry" "$CACHE/emacs-snapshot"
if ! "$RUNTIME" image ls 2>/dev/null | grep -q kafka-el-linux; then
  "$RUNTIME" build -t "$IMAGE" -f ci/linux/Containerfile ci/linux
fi
MODE="${1:-full}"
exec "$RUNTIME" run --rm --memory 8g --cpus 6 \
  -v "$PWD:/src" \
  -v "$CACHE/linux-target:/target" \
  -v "$CACHE/emacs-snapshot:/build" \
  -v "$CACHE/linux-cargo-registry:/opt/cargo/registry" \
  -v "$CACHE/kafka_2.13-${KAFKA_VERSION:-4.4.0}:/kafka-dist:ro" \
  -e CARGO_TARGET_DIR=/target -e MODE="$MODE" -e KAFKA_VERSION="${KAFKA_VERSION:-4.4.0}" \
  "$IMAGE" bash -euo pipefail -c '
    cd /src
    export EMACS=emacs
    if [ "$MODE" = snapshot ]; then export EMACS=/build/install/bin/emacs; fi
    echo "== $(uname -sm), $(rustc --version), $("$EMACS" --version | head -1), $(java -version 2>&1 | head -1)"
    if [ "$MODE" = snapshot ]; then git -C /build/source rev-parse HEAD; fi
    cargo clippy --workspace --all-targets -- -D warnings
    "$EMACS" -Q --batch -L lisp --eval "(setq byte-compile-dest-file-function (lambda (f) (concat \"/tmp/\" (file-name-nondirectory f) \"c\")))" \
      --eval "(setq byte-compile-error-on-warn t)" -f batch-byte-compile lisp/*.el
    if [ "$MODE" = quick ]; then cargo test --workspace; exit 0; fi
    export KAFKA_EL_CACHE=/tmp/kcache LOG_DIR=/tmp/kafka-logs
    mkdir -p "$KAFKA_EL_CACHE" "$LOG_DIR"
    ln -s /kafka-dist $KAFKA_EL_CACHE/kafka_2.13-$KAFKA_VERSION
    trap '\''status=$?; if [ "$status" -ne 0 ]; then find /tmp/kcache -name "*.log" -type f -exec tail -80 {} + 2>/dev/null || true; fi'\'' EXIT
    scripts/test-all.sh
  '
