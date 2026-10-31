#!/usr/bin/env bash
# Start/stop a single-node KRaft broker for local development and tests.
# Usage: scripts/kafka-dev.sh start|stop|status
# Env: KAFKA_VERSION (default 4.4.0), KAFKA_PORT (default 19092), KAFKA_EL_CACHE
set -euo pipefail
VER="${KAFKA_VERSION:-4.4.0}"
PORT="${KAFKA_PORT:-19092}"
CPORT=$((PORT + 1))
CACHE="${KAFKA_EL_CACHE:-$HOME/.cache/kafka-el}"
HOME_DIR="$CACHE/kafka_2.13-$VER"
RUN="$CACHE/run-$PORT"
mkdir -p "$CACHE"

fetch() {
  [ -d "$HOME_DIR" ] && return
  curl -sfL "https://downloads.apache.org/kafka/$VER/kafka_2.13-$VER.tgz" -o "$CACHE/k.tgz" \
    || curl -sfL "https://archive.apache.org/dist/kafka/$VER/kafka_2.13-$VER.tgz" -o "$CACHE/k.tgz"
  tar xzf "$CACHE/k.tgz" -C "$CACHE" && rm -f "$CACHE/k.tgz"
}

case "${1:-}" in
  start)
    fetch
    mkdir -p "$RUN"
    cat > "$RUN/server.properties" <<P
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:$CPORT
listeners=PLAINTEXT://127.0.0.1:$PORT,CONTROLLER://127.0.0.1:$CPORT
advertised.listeners=PLAINTEXT://127.0.0.1:$PORT
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=$RUN/data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
P
    if [ ! -f "$RUN/data/meta.properties" ]; then
      "$HOME_DIR/bin/kafka-storage.sh" format -t "$("$HOME_DIR/bin/kafka-storage.sh" random-uuid)" \
        -c "$RUN/server.properties" --standalone >/dev/null 2>&1 \
      || "$HOME_DIR/bin/kafka-storage.sh" format -t "$("$HOME_DIR/bin/kafka-storage.sh" random-uuid)" \
        -c "$RUN/server.properties" >/dev/null
    fi
    nohup "$HOME_DIR/bin/kafka-server-start.sh" "$RUN/server.properties" >"$RUN/server.log" 2>&1 &
    echo $! > "$RUN/pid"
    for _ in $(seq 1 60); do
      if "$HOME_DIR/bin/kafka-broker-api-versions.sh" --bootstrap-server "127.0.0.1:$PORT" >/dev/null 2>&1; then
        echo "kafka $VER up on 127.0.0.1:$PORT"; exit 0
      fi
      sleep 1
    done
    echo "kafka failed to start; see $RUN/server.log" >&2; exit 1 ;;
  stop)
    [ -f "$RUN/pid" ] && kill "$(cat "$RUN/pid")" 2>/dev/null || true
    rm -f "$RUN/pid"; echo stopped ;;
  status)
    [ -f "$RUN/pid" ] && kill -0 "$(cat "$RUN/pid")" 2>/dev/null && echo running || echo stopped ;;
  *) echo "usage: $0 start|stop|status" >&2; exit 2 ;;
esac
