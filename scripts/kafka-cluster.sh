#!/usr/bin/env bash
# Three-node KRaft cluster (combined broker+controller) for multi-broker,
# leader election, reassignment and broker-restart tests.
# Usage: scripts/kafka-cluster.sh start|stop|status|kill N|restart N|env
#   Brokers on 127.0.0.1:39092, :39093, :39094; controllers on :39192..:39194.
set -euo pipefail
VER="${KAFKA_VERSION:-4.4.0}"
BASE="${KAFKA_CLUSTER_PORT:-39092}"
CACHE="${KAFKA_EL_CACHE:-$HOME/.cache/kafka-el}"
HOME_DIR="$CACHE/kafka_2.13-$VER"
RUN="$CACHE/cluster-$BASE"
N=3
here="$(cd "$(dirname "$0")" && pwd)"

port() { echo $((BASE + $1 - 1)); }
cport() { echo $((BASE + 100 + $1 - 1)); }
voters() { local v=""; for i in $(seq 1 $N); do v="$v${v:+,}$i@127.0.0.1:$(cport "$i")"; done; echo "$v"; }
bootstrap() { local v=""; for i in $(seq 1 $N); do v="$v${v:+,}127.0.0.1:$(port "$i")"; done; echo "$v"; }

write_props() {
  local i=$1 d="$RUN/n$i"
  mkdir -p "$d"
  cat > "$d/server.properties" <<P
process.roles=broker,controller
node.id=$i
controller.quorum.voters=$(voters)
listeners=PLAINTEXT://127.0.0.1:$(port "$i"),CONTROLLER://127.0.0.1:$(cport "$i")
advertised.listeners=PLAINTEXT://127.0.0.1:$(port "$i")
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=$d/data
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
default.replication.factor=3
min.insync.replicas=1
group.initial.rebalance.delay.ms=0
P
}

start_node() {
  local i=$1 d="$RUN/n$i"
  if [ -f "$d/pid" ] && kill -0 "$(cat "$d/pid")" 2>/dev/null; then return; fi
  nohup "$HOME_DIR/bin/kafka-server-start.sh" "$d/server.properties" >"$d/server.log" 2>&1 &
  echo $! > "$d/pid"
}

stop_node() {
  local i=$1 d="$RUN/n$i" sig=${2:-TERM}
  if [ -f "$d/pid" ]; then
    local pid; pid="$(cat "$d/pid")"
    kill "-$sig" "$pid" 2>/dev/null || true
    for _ in $(seq 1 60); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
    rm -f "$d/pid"
  fi
}

wait_up() {
  local i=$1
  for _ in $(seq 1 90); do
    if "$HOME_DIR/bin/kafka-broker-api-versions.sh" --bootstrap-server "127.0.0.1:$(port "$i")" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "node $i failed to start; see $RUN/n$i/server.log" >&2; return 1
}

case "${1:-}" in
  start)
    KAFKA_VERSION="$VER" "$here/kafka-dev.sh" fetch
    id_file="$RUN/cluster.id"; mkdir -p "$RUN"
    [ -f "$id_file" ] || "$HOME_DIR/bin/kafka-storage.sh" random-uuid > "$id_file"
    for i in $(seq 1 $N); do
      write_props "$i"
      [ -f "$RUN/n$i/data/meta.properties" ] || \
        "$HOME_DIR/bin/kafka-storage.sh" format -t "$(cat "$id_file")" -c "$RUN/n$i/server.properties" >/dev/null
    done
    for i in $(seq 1 $N); do start_node "$i"; done
    for i in $(seq 1 $N); do wait_up "$i"; done
    echo "kafka $VER 3-node cluster up on $(bootstrap)" ;;
  stop)
    for i in $(seq 1 $N); do stop_node "$i"; done; echo stopped ;;
  status)
    up=0; for i in $(seq 1 $N); do [ -f "$RUN/n$i/pid" ] && kill -0 "$(cat "$RUN/n$i/pid")" 2>/dev/null && up=$((up + 1)); done
    [ "$up" -eq "$N" ] && echo running || echo "stopped ($up/$N up)" ;;
  kill) stop_node "${2:?node}" KILL; echo "node $2 killed" ;;
  restart) stop_node "${2:?node}"; start_node "$2"; wait_up "$2"; echo "node $2 restarted" ;;
  start-node) start_node "${2:?node}"; wait_up "$2"; echo "node $2 started" ;;
  env)
    echo "export KAFKA_EL_TEST_CLUSTER=$(bootstrap)"
    echo "export KAFKA_EL_TEST_CLUSTER_SCRIPT=$here/kafka-cluster.sh" ;;
  *) echo "usage: $0 start|stop|status|kill N|restart N|start-node N|env" >&2; exit 2 ;;
esac
