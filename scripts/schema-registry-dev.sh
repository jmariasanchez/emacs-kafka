#!/usr/bin/env bash
# Real Karapace registry backed by the shared plain Kafka broker.
# KAFKA_EL_REGISTRY_AUTH=1 selects the authenticated fixture on port 18082.
# KAFKA_EL_REGISTRY_TLS=1 selects the mutual-TLS fixture on port 18084.
set -euo pipefail
cache="${KAFKA_EL_CACHE:-$HOME/.cache/kafka-el}"
port=18081
if [[ "${KAFKA_EL_REGISTRY_AUTH:-0}" == 1 ]]; then port=18082; fi
if [[ "${KAFKA_EL_REGISTRY_TLS:-0}" == 1 ]]; then port=18084; fi
root="$cache/registry-$port"
venv="$cache/karapace-venv"
url="http://127.0.0.1:$port"
if [[ "$port" == 18084 ]]; then
  url="https://127.0.0.1:$port"
fi
mkdir -p "$root"
running() {
  [[ -f "$root/pid" ]] && kill -0 "$(cat "$root/pid")" 2>/dev/null &&
    [[ "$(ps -p "$(cat "$root/pid")" -o command=)" == *"$venv/bin/python -m karapace"* ]]
}
ready() {
  if [[ "$port" == 18084 ]]; then
    curl --cacert "$root/ca.pem" --cert "$root/client.pem" --key "$root/client.key" \
      --max-time 3 -fsS "$url/master_available" 2>/dev/null
  else
    curl --max-time 3 -fsS "$url/master_available" 2>/dev/null
  fi | grep -q '"master_available"[[:space:]]*:[[:space:]]*true'
}
case "${1:-status}" in
  start)
    if running; then
      if ready; then echo "Registry already running: $url"; exit 0; fi
      echo "Registry process exists but is not ready. See $root/server.log" >&2; exit 1
    fi
    if [[ ! -x "$venv/bin/karapace" ]]; then
      if [[ "$(uname)" == Darwin ]]; then
        export CC=/usr/bin/clang SDKROOT="$(xcrun --show-sdk-path)"
      fi
      [[ -x "$venv/bin/python" ]] || uv venv --python 3.12 "$venv"
      UV_CACHE_DIR="$cache/uv" uv pip install --python "$venv/bin/python" 'karapace @ git+https://github.com/Aiven-Open/karapace.git@6.2.0'
    fi
    authfile=""
    tls_cert="" tls_key="" tls_ca="" tls_auth=none
    if [[ "$port" == 18084 ]]; then
      if [[ ! -f "$root/client.pem" ]] || ! openssl x509 -in "$root/client.pem" -checkend 60 -noout >/dev/null; then
        (umask 077
         openssl req -x509 -newkey rsa:2048 -nodes -days 7 -subj /CN=kafka-el-registry-ca \
           -keyout "$root/ca.key" -out "$root/ca.pem" >/dev/null 2>&1
         openssl req -newkey rsa:2048 -nodes -subj /CN=localhost \
           -keyout "$root/server.key" -out "$root/server.csr" >/dev/null 2>&1
         printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > "$root/server.ext"
         openssl x509 -req -in "$root/server.csr" -CA "$root/ca.pem" -CAkey "$root/ca.key" \
           -CAcreateserial -days 7 -extfile "$root/server.ext" -out "$root/server.pem" >/dev/null 2>&1
         openssl req -newkey rsa:2048 -nodes -subj /CN=registry-client \
           -keyout "$root/client.key" -out "$root/client.csr" >/dev/null 2>&1
         openssl x509 -req -in "$root/client.csr" -CA "$root/ca.pem" -CAkey "$root/ca.key" \
           -CAcreateserial -days 7 -out "$root/client.pem" >/dev/null 2>&1)
      fi
      tls_cert="$root/server.pem" tls_key="$root/server.key" tls_ca="$root/ca.pem" tls_auth=required
    fi
    if [[ "$port" == 18082 ]]; then
      authfile="$root/auth.json"
      # Public test-only credentials: registry-user / registry-test-password.
      cat > "$authfile" <<'JSON'
{"users":[{"username":"registry-user","algorithm":"sha512","salt":"u4nxBknxZO_8huyNTETTIw","password_hash":"UNGht0dpP2mZGO+/1SYX+6Vlu648qrKBQOqBJd5K+SJPzZiSaso7rj0IwprhDHKf+8z2E81/IhlByUs5+fwEbw=="}],"permissions":[{"username":"registry-user","operation":"Write","resource":".*"}]}
JSON
    fi
    nohup env KARAPACE_SERVER_TLS_CERTFILE="$tls_cert" KARAPACE_SERVER_TLS_KEYFILE="$tls_key" \
      KARAPACE_SERVER_TLS_CAFILE="$tls_ca" KARAPACE_SERVER_TLS_CLIENT_AUTH="$tls_auth" \
      KARAPACE_HOST=127.0.0.1 KARAPACE_PORT="$port" KARAPACE_ADVERTISED_PORT="$port" \
      KARAPACE_ADVERTISED_HOSTNAME=127.0.0.1 KARAPACE_BOOTSTRAP_URI=127.0.0.1:19092 \
      KARAPACE_CLIENT_ID="kafka-el-registry-$port" KARAPACE_GROUP_ID="kafka-el-registry-$port" \
      KARAPACE_TOPIC_NAME="_kafka_el_schemas_$port" KARAPACE_REPLICATION_FACTOR=1 \
      KARAPACE_KARAPACE_REGISTRY=true KARAPACE_KARAPACE_REST=false \
      KARAPACE_REGISTRY_AUTHFILE="$authfile" KARAPACE_LOG_LEVEL=WARNING KARAPACE_COMPATIBILITY=BACKWARD \
      "$venv/bin/python" -m karapace > "$root/server.log" 2>&1 < /dev/null &
    echo $! > "$root/pid"
    for _ in $(seq 1 90); do
      if ready; then echo "Registry ready: $url"; exit 0; fi
      if ! running; then tail -40 "$root/server.log"; exit 1; fi
      sleep 1
    done
    echo "Registry startup timed out. See $root/server.log" >&2; exit 1
    ;;
  stop)
    if running; then
      kill "$(cat "$root/pid")"
      for _ in $(seq 1 30); do if ! running; then break; fi; sleep 1; done
      if running; then echo "Registry still stopping" >&2; exit 1; fi
    fi
    rm -f "$root/pid"
    ;;
  status)
    if running && ready; then echo "Registry running: $url"; else echo "Registry stopped or not ready"; exit 1; fi
    ;;
  env)
    if [[ "$port" == 18084 ]]; then printf 'export KAFKA_EL_TEST_REGISTRY_TLS=%s\nexport KAFKA_EL_TEST_REGISTRY_CERTS=%q\n' "$url" "$root"
    elif [[ "$port" == 18082 ]]; then printf 'export KAFKA_EL_TEST_REGISTRY_AUTH=%s\n' "$url"
    else printf 'export KAFKA_EL_TEST_REGISTRY=%s\n' "$url"; fi
    ;;
  *) echo "Usage: $0 start|stop|status|env" >&2; exit 2 ;;
esac
