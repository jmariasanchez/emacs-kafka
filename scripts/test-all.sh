#!/usr/bin/env bash
# Run every test suite against a local dev broker (started if needed).
set -euo pipefail
cd "$(dirname "$0")/.."
VER="${KAFKA_VERSION:-4.4.0}"
PORT="${KAFKA_PORT:-19092}"
CACHE="${KAFKA_EL_CACHE:-$HOME/.cache/kafka-el}"
scripts/kafka-dev.sh status | grep -q running || scripts/kafka-dev.sh start
export KAFKA_EL_TEST_BOOTSTRAP="127.0.0.1:$PORT"
export KAFKA_EL_TEST_KAFKA_HOME="$CACHE/kafka_2.13-$VER"
cargo test --workspace
cargo build -p kafka-bridge
KAFKA_EL_BRIDGE="$PWD/target/debug/kafka-bridge" emacs -Q --batch -L lisp -L test/ert \
  -l kafka-tests.el -f ert-run-tests-batch-and-exit
