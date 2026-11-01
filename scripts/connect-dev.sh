#!/usr/bin/env bash
# Local distributed Connect worker. Never stops the shared Kafka broker.
set -euo pipefail
HOME_DIR="${KAFKA_EL_CACHE:-$HOME/.cache/kafka-el}/kafka_2.13-4.4.0"
RUN="${KAFKA_EL_CACHE:-$HOME/.cache/kafka-el}/connect-18083"
URL=http://127.0.0.1:18083
running() { [ -f "$RUN/pid" ] && kill -0 "$(cat "$RUN/pid")" 2>/dev/null; }
case "${1:-}" in
 start)
  if running; then curl -fsS "$URL/"; exit; fi
  mkdir -p "$RUN"
  PLUGIN=$(find "$HOME_DIR/libs" "$HOME_DIR/plugins" -name 'connect-file-*.jar' 2>/dev/null | head -1 || true)
  [ -n "$PLUGIN" ] || { echo 'FileStream plugin not found' >&2; exit 1; }
  ID="kel-connect-18083-$(date +%s)"
  cat > "$RUN/worker.properties" <<P
bootstrap.servers=127.0.0.1:19092
group.id=$ID
listeners=http://127.0.0.1:18083
rest.advertised.host.name=127.0.0.1
rest.advertised.port=18083
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
config.storage.topic=$ID-configs
offset.storage.topic=$ID-offsets
status.storage.topic=$ID-status
config.storage.replication.factor=1
offset.storage.replication.factor=1
status.storage.replication.factor=1
offset.storage.partitions=1
status.storage.partitions=1
offset.flush.interval.ms=1000
plugin.path=$PLUGIN
P
  nohup "$HOME_DIR/bin/connect-distributed.sh" "$RUN/worker.properties" >"$RUN/worker.log" 2>&1 &
  echo $! > "$RUN/pid"
  for _ in $(seq 1 90); do
   if curl -fsS "$URL/" >/dev/null 2>&1; then echo "Connect ready at $URL"; exit 0; fi
   running || break
   sleep 1
  done
  echo "Connect failed: $RUN/worker.log" >&2; exit 1 ;;
 stop)
  if running; then kill "$(cat "$RUN/pid")"; fi
  rm -f "$RUN/pid" ;;
 status) if running; then curl -fsS "$URL/"; else echo stopped; exit 1; fi ;;
 env) echo "export KAFKA_EL_TEST_CONNECT=$URL" ;;
 *) echo "usage: $0 start|stop|status|env" >&2; exit 2 ;;
esac
