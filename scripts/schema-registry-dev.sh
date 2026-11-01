#!/usr/bin/env bash
# Real Karapace registry backed by the shared plain Kafka broker.
set -euo pipefail
cache="${KAFKA_EL_CACHE:-$HOME/.cache/kafka-el}"
root="$cache/registry-18081"
venv="$cache/karapace-venv"
url=http://127.0.0.1:18081
mkdir -p "$root"
running() { [[ -f "$root/pid" ]] && kill -0 "$(cat "$root/pid")" 2>/dev/null; }
case "${1:-status}" in
  start)
    if running; then echo "Registry already running: $url"; exit 0; fi
    if [[ ! -x "$venv/bin/karapace" ]]; then
      if [[ "$(uname)" == Darwin ]]; then
        export CC=/usr/bin/clang SDKROOT="$(xcrun --show-sdk-path)"
      fi
      [[ -x "$venv/bin/python" ]] || uv venv --python 3.12 "$venv"
      UV_CACHE_DIR="$cache/uv" uv pip install --python "$venv/bin/python" 'karapace @ git+https://github.com/Aiven-Open/karapace.git@6.2.0'
    fi
    cat > "$root/config.json" <<'JSON'
{"host":"127.0.0.1","port":18081,"advertised_hostname":"127.0.0.1","bootstrap_uri":"127.0.0.1:19092","client_id":"kafka-el-registry","group_id":"kafka-el-registry-18081","topic_name":"_kafka_el_schemas","replication_factor":1,"karapace_registry":true,"karapace_rest":false,"log_level":"WARNING","compatibility":"BACKWARD"}
JSON
    nohup "$venv/bin/python" -m karapace "$root/config.json" > "$root/server.log" 2>&1 < /dev/null &
    echo $! > "$root/pid"
    for _ in $(seq 1 90); do
      if curl -fsS "$url/subjects" > /dev/null; then echo "Registry ready: $url"; exit 0; fi
      if ! running; then tail -40 "$root/server.log"; exit 1; fi
      sleep 1
    done
    echo "Registry startup timed out. See $root/server.log" >&2; exit 1
    ;;
  stop)
    if running; then kill "$(cat "$root/pid")"; fi
    rm -f "$root/pid"
    ;;
  status)
    if running && curl -fsS "$url/subjects" > /dev/null; then echo "Registry running: $url"; else echo "Registry stopped"; exit 1; fi
    ;;
  env) printf 'export KAFKA_EL_TEST_REGISTRY=%s\n' "$url" ;;
  *) echo "Usage: $0 start|stop|status|env" >&2; exit 2 ;;
esac
