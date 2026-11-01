#!/usr/bin/env bash
# Single-node KRaft broker with SASL, TLS, mTLS and the StandardAuthorizer.
# Usage: scripts/kafka-secure.sh start|stop|status|env
#
# Listeners (all on 127.0.0.1):
#   29092 SASL_PLAINTEXT  PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
#   29093 SSL             mTLS, client cert required (principal User:CN=...)
#   29094 SASL_SSL        server TLS + SASL, no client cert
#   29099 CONTROLLER      PLAINTEXT (internal)
#
# Users: admin/admin-secret (super user, PLAIN + SCRAM-256/512),
#        alice/alice-secret (no ACLs, PLAIN + SCRAM-256/512).
# Certs in $RUN/certs: ca.pem, client.pem/client.key (CN=admin-client, super
# user), alice-client.pem/.key (CN=alice-client, no ACLs), expired.pem/.key
# (CN=expired-client, notAfter in the past), and rogue-ca.pem (untrusted CA).
#
# `env` prints shell exports for tests (KAFKA_EL_TEST_SECURE_*).
# KAFKA_SECURE_MECHANISMS=PLAIN with a different KAFKA_SECURE_PORT creates
# an isolated fixture that rejects otherwise supported SCRAM mechanisms.
set -euo pipefail
VER="${KAFKA_VERSION:-4.4.0}"
BASE="${KAFKA_SECURE_PORT:-29092}"
CACHE="${KAFKA_EL_CACHE:-$HOME/.cache/kafka-el}"
HOME_DIR="$CACHE/kafka_2.13-$VER"
RUN="$CACHE/secure-$BASE"
CERTS="$RUN/certs"
P_SASL=$BASE; P_SSL=$((BASE + 1)); P_SASLSSL=$((BASE + 2)); P_CTRL=$((BASE + 7))
PASS=changeit
mkdir -p "$CACHE"
here="$(cd "$(dirname "$0")" && pwd)"

fetch() { KAFKA_VERSION="$VER" "$here/kafka-dev.sh" fetch; }

# Pick an OpenSSL that supports -not_after (OpenSSL 3.x). LibreSSL on macOS
# does not, so the expired cert falls back to `openssl ca` with -enddate.
OPENSSL="${OPENSSL:-openssl}"

gen_certs() {
  [ -f "$CERTS/.done" ] && return
  rm -rf "$CERTS"; mkdir -p "$CERTS"; cd "$CERTS"
  "$OPENSSL" req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 3650 \
    -subj "/CN=kafka-el-test-ca" >/dev/null 2>&1
  "$OPENSSL" req -x509 -newkey rsa:2048 -nodes -keyout rogue-ca.key -out rogue-ca.pem -days 3650 \
    -subj "/CN=rogue-ca" >/dev/null 2>&1
  printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' > san.ext
  leaf() { # name cn [ext]
    "$OPENSSL" req -newkey rsa:2048 -nodes -keyout "$1.key" -out "$1.csr" -subj "/CN=$2" >/dev/null 2>&1
    "$OPENSSL" x509 -req -in "$1.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -out "$1.pem" \
      -days 3650 ${3:+-extfile "$3"} >/dev/null 2>&1
  }
  leaf broker localhost san.ext
  leaf client admin-client
  leaf alice-client alice-client
  # Expired client cert: valid 2020-01-01 .. 2020-01-02.
  "$OPENSSL" req -newkey rsa:2048 -nodes -keyout expired.key -out expired.csr -subj "/CN=expired-client" >/dev/null 2>&1
  mkdir -p ca-db/new; : > ca-db/index.txt; echo 1000 > ca-db/serial
  cat > ca.cnf <<C
[ca]
default_ca=d
[d]
dir=$CERTS/ca-db
database=\$dir/index.txt
new_certs_dir=\$dir/new
serial=\$dir/serial
default_md=sha256
policy=p
[p]
commonName=supplied
C
  "$OPENSSL" ca -batch -config ca.cnf -cert ca.pem -keyfile ca.key -in expired.csr -out expired.pem \
    -startdate 20200101000000Z -enddate 20200102000000Z -notext >/dev/null 2>&1
  "$OPENSSL" pkcs12 -export -in broker.pem -inkey broker.key -name broker -out broker.p12 \
    -passout "pass:$PASS" >/dev/null 2>&1
  touch .done
  cd - >/dev/null
}

jaas_plain='org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret" user_admin="admin-secret" user_alice="alice-secret";'
jaas_scram='org.apache.kafka.common.security.scram.ScramLoginModule required;'

case "${1:-}" in
  start)
    if "$0" status | grep -q running; then echo "already running"; exit 0; fi
    fetch; gen_certs; mkdir -p "$RUN"
    cat > "$RUN/server.properties" <<P
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:$P_CTRL
listeners=SASL_PLAINTEXT://127.0.0.1:$P_SASL,SSL://127.0.0.1:$P_SSL,SASL_SSL://127.0.0.1:$P_SASLSSL,CONTROLLER://127.0.0.1:$P_CTRL
advertised.listeners=SASL_PLAINTEXT://127.0.0.1:$P_SASL,SSL://127.0.0.1:$P_SSL,SASL_SSL://127.0.0.1:$P_SASLSSL
controller.listener.names=CONTROLLER
listener.security.protocol.map=SASL_PLAINTEXT:SASL_PLAINTEXT,SSL:SSL,SASL_SSL:SASL_SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SASL_PLAINTEXT
sasl.enabled.mechanisms=${KAFKA_SECURE_MECHANISMS:-PLAIN,SCRAM-SHA-256,SCRAM-SHA-512}
sasl.mechanism.inter.broker.protocol=PLAIN
listener.name.sasl_plaintext.plain.sasl.jaas.config=$jaas_plain
listener.name.sasl_plaintext.scram-sha-256.sasl.jaas.config=$jaas_scram
listener.name.sasl_plaintext.scram-sha-512.sasl.jaas.config=$jaas_scram
listener.name.sasl_ssl.plain.sasl.jaas.config=$jaas_plain
listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=$jaas_scram
listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=$jaas_scram
ssl.keystore.type=PKCS12
ssl.keystore.location=$CERTS/broker.p12
ssl.keystore.password=$PASS
ssl.key.password=$PASS
ssl.truststore.type=PEM
ssl.truststore.location=$CERTS/ca.pem
listener.name.ssl.ssl.client.auth=required
ssl.principal.mapping.rules=RULE:^CN=([^,]*).*\$/\$1/,DEFAULT
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin;User:ANONYMOUS;User:admin-client
allow.everyone.if.no.acl.found=false
log.dirs=$RUN/data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
P
    if [ ! -f "$RUN/data/meta.properties" ]; then
      id="$("$HOME_DIR/bin/kafka-storage.sh" random-uuid)"
      "$HOME_DIR/bin/kafka-storage.sh" format -t "$id" -c "$RUN/server.properties" \
        -S 'SCRAM-SHA-256=[name=admin,password=admin-secret]' \
        -S 'SCRAM-SHA-512=[name=admin,password=admin-secret]' \
        -S 'SCRAM-SHA-256=[name=alice,password=alice-secret]' \
        -S 'SCRAM-SHA-512=[name=alice,password=alice-secret]' >/dev/null
    fi
    cat > "$RUN/admin.properties" <<A
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";
A
    nohup "$HOME_DIR/bin/kafka-server-start.sh" "$RUN/server.properties" >"$RUN/server.log" 2>&1 &
    echo $! > "$RUN/pid"
    for _ in $(seq 1 90); do
      if "$HOME_DIR/bin/kafka-broker-api-versions.sh" --bootstrap-server "127.0.0.1:$P_SASL" \
           --command-config "$RUN/admin.properties" >/dev/null 2>&1; then
        echo "secure kafka $VER up: SASL_PLAINTEXT :$P_SASL, SSL(mTLS) :$P_SSL, SASL_SSL :$P_SASLSSL"; exit 0
      fi
      sleep 1
    done
    echo "secure kafka failed to start; see $RUN/server.log" >&2; exit 1 ;;
  stop)
    [ -f "$RUN/pid" ] && kill "$(cat "$RUN/pid")" 2>/dev/null || true
    rm -f "$RUN/pid"; echo stopped ;;
  status)
    [ -f "$RUN/pid" ] && kill -0 "$(cat "$RUN/pid")" 2>/dev/null && echo running || echo stopped ;;
  env)
    cat <<E
export KAFKA_EL_TEST_SECURE_SASL=127.0.0.1:$P_SASL
export KAFKA_EL_TEST_SECURE_SSL=127.0.0.1:$P_SSL
export KAFKA_EL_TEST_SECURE_SASL_SSL=127.0.0.1:$P_SASLSSL
export KAFKA_EL_TEST_SECURE_CERTS=$CERTS
export KAFKA_EL_TEST_SECURE_ADMIN_PROPS=$RUN/admin.properties
E
    ;;
  *) echo "usage: $0 start|stop|status|env" >&2; exit 2 ;;
esac
