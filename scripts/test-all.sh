#!/usr/bin/env bash
# Run every test suite against a local dev broker (started if needed).
# Secure-broker and 3-node-cluster tests run only when their env vars are set
# (see scripts/kafka-secure.sh env and scripts/kafka-cluster.sh env), or when
# KAFKA_EL_TEST_FULL=1, which starts both.
set -euo pipefail
cd "$(dirname "$0")/.."
VER="${KAFKA_VERSION:-4.4.0}"
PORT="${KAFKA_PORT:-19092}"
CACHE="${KAFKA_EL_CACHE:-$HOME/.cache/kafka-el}"
scripts/kafka-dev.sh status | grep -q running || scripts/kafka-dev.sh start
export KAFKA_EL_TEST_BOOTSTRAP="127.0.0.1:$PORT"
export KAFKA_EL_TEST_KAFKA_HOME="$CACHE/kafka_2.13-$VER"
if [ "${KAFKA_EL_TEST_FULL:-}" = 1 ]; then
  scripts/kafka-secure.sh start
  scripts/kafka-cluster.sh start
  eval "$(scripts/kafka-secure.sh env)"
  eval "$(scripts/kafka-cluster.sh env)"
fi
cargo test --workspace
cargo build -p kafka-bridge
ERT_FILES=()
for f in test/ert/*-tests.el; do ERT_FILES+=(-l "$(basename "$f")"); done
KAFKA_EL_BRIDGE="$PWD/target/debug/kafka-bridge" emacs -Q --batch -L lisp -L test/ert \
  "${ERT_FILES[@]}" -f ert-run-tests-batch-and-exit
