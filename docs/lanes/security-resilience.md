# Security, resilience and load lane

Validated on 2026-11-01, macOS aarch64, Kafka 4.4.0, librdkafka 2.12.1,
Emacs 31.1 and 30.2. All changes are on `lane/security-resilience`.

## Focused fixes and ownership overlap

- `crates/kafka-core/src/connection.rs`: a short-lived verification consumer
  polls native connection diagnostics while requesting metadata. Authentication,
  unsupported mechanisms and TLS errors become fixed, actionable messages. Raw
  broker diagnostics are never echoed. Client PEM validity is checked locally
  because this broker returns a generic TLS alert for an expired client cert.
- `crates/kafka-core/src/errors.rs`: additive `Security { code, message }` error.
- `crates/kafka-bridge/src/convert.rs`: map security failures to RPC 1000 with
  `data.kafkaCode`, plus a regression test.
- **Overlap outside the original file list:** `crates/kafka-core/src/cluster.rs`,
  only `connect`, `describe_topic` and `partitions`. Connect has one overall
  deadline, including verification and metadata. Topic metadata errors retain
  their native Kafka codes instead of becoming `NotFound` without a code.
- `crates/kafka-core/src/records.rs`: one bounded-fetch deadline includes setup,
  metadata, timestamp resolution, watermarks and polling. Watermark calls share
  the remaining deadline rather than multiplying it by partition count.
  Producer delivery futures also have an explicit overall timeout, returning
  `MessageTimedOut` if native delivery does not finish within the profile budget.
- `crates/kafka-core/Cargo.toml`, `Cargo.lock`: add `openssl = { version = "0.10",
  features = ["vendored"] }` for PEM validity checks, reusing the existing
  vendored OpenSSL. No new native crypto implementation or CMake dependency.
- `scripts/kafka-cluster.sh`: fix `local i=$1 d="$RUN/n$i"` evaluating `i`
  before assignment under `set -u`. This broke standalone kill/start-node.
  Add validated `pause N`/`resume N` controls for SIGSTOP/SIGCONT.
  If graceful shutdown exceeds 30 seconds, force termination rather than
  deleting the PID file and incorrectly reporting a still-listening node stopped.
- `scripts/kafka-secure.sh`: optional `KAFKA_SECURE_MECHANISMS`, default unchanged,
  for a separate PLAIN-only broker used to test broker-side SCRAM rejection.
- New owned suites: `tests/security.rs`, `tests/resilience.rs`, `tests/load.rs`
  under kafka-bridge, and `test/ert/kafka-security-tests.el`.
- No changes to dispatch, Lisp connect parameters, RPC structs, capability
  implementation flags, or shared documentation.

## Security evidence

`security.rs` has four real-process integration tests:

1. `sasl_matrix_and_acl`: PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 connect as
   admin and alice. Explicit PLAIN produce/fetch round trip, optionally verified
   by the official `kafka-console-consumer.sh` using the admin properties file.
   Incorrect passwords for all three mechanisms produce
   `SaslAuthenticationFailed` and an actionable authentication message.
   Unknown mechanisms and uncompiled GSSAPI produce `UnsupportedSaslMechanism`.
   Alice topic describe and produce return `TopicAuthorizationFailed`, not a
   generic error. Group listing completes and returns the authorized empty
   catalog. Kafka filters ListGroups rather than denying this request outright.
2. `tls_matrix`: mTLS succeeds through both 127.0.0.1 and localhost. Missing
   client cert, expired cert and rogue CA produce distinct actionable messages
   with `kafkaCode: SSL`. SASL_SSL PLAIN succeeds on port 29094. The initial
   expired-client run exercised the broker's rejection before the local
   expiration diagnostic was added.
3. `passwords_never_reach_stdout_or_stderr_even_with_debug`: captures both
   process pipes and checks a sentinel password, with and without native
   `debug=all` plus `RUST_LOG=trace`. Stdout must contain only valid framed RPC.
   Connection enumeration never returns credentials.
4. `broker_disabled_mechanism`: a separate PLAIN-only broker on 59092 rejects
   otherwise supported SCRAM-SHA-256 with `UnsupportedSaslMechanism` and a
   clear mechanism-not-enabled message. Shared secure broker is never restarted.

Connect probes have a 3000 ms budget and a 5 second hard test bound, including
process scheduling tolerance. They do not use the harness's 60 second default.
The Emacs wrong-password test uses 1500 ms and asserts completion within 3.5 s.

ERT reads SCRAM-SHA-512 credentials exclusively from a temporary mode-0600
netrc file with a bound `auth-sources` and disabled credential cache. It connects
as admin to the actual secure broker. Cleanup stops the bridge and deletes the
file. A second ERT test checks actionable wrong-password errors and no connected
state on failure.

## Resilience evidence

The destructive suite refuses any bootstrap string other than the owned
`127.0.0.1:49092,127.0.0.1:49093,127.0.0.1:49094`. It never touches 39092.

- RF=3 stream with credit while 100 unique records are produced across SIGKILL
  of the discovered partition leader. All 100 acknowledged offsets arrive
  exactly once, values are unique, and a bounded snapshot contains exactly
  those 100 records plus the warm-up record.
- Connecting with the dead broker still in the bootstrap list succeeds.
- RF=1 leader SIGSTOP: produce errors within its message budget and fetch with
  timeoutMs=500 completes/errors within the 2 second scheduling bound.
  SIGCONT restores the broker.
- Killing the leader during a bounded fetch cannot exceed that fetch bound.
- With all three brokers killed, metadata and produce fail promptly. A separate
  connection to the shared plain broker serves concurrent requests.
- Restarting the three nodes recovers the existing connection without manual
  reconnect. Therefore no Emacs reconnect workaround is necessary.
- A drop guard resumes paused processes and restores the owned cluster on test
  failure. The caller stops it after the suite.

A strengthened all-brokers-down produce assertion exposed a native delivery
wait exceeding the expected deadline after a prior leader pause. The explicit
producer future deadline addresses that path. The final all-enabled workspace
run passed the complete resilience sequence in 53.79 s. Paused-leader produce
returned in 1.506 s, the 500 ms fetch in 503.7 ms, the healthy connection's
concurrent topic list in 3.0 ms, and all-brokers-down produce in 1.505 s.

## Load/performance observations

Debug build, same machine while other lanes were active. These are sanity
measurements, not release performance promises.

| Probe | Observed result |
|---|---|
| 1 MiB value, produce plus bounded fetch | 1.234110166 s, byte-for-byte identical |
| 5000 topics, filtered topics/list | 69.358541 ms, exactly 5000 entries |
| 5000-topic creation setup | 3.558771333 s, excluding metadata convergence |
| 2000 kafka/connections stdio calls, no broker I/O | p50 61 us, p95 72 us, p99 99 us |

The 1 MiB probe raises topic and client message limits to 2 MiB to allow Kafka
record overhead. The catalog probe uses only the owned cluster and deletes its
5000 uniquely prefixed topics afterwards. CreateTopics acknowledges controller
persistence before all brokers apply their metadata images, so the probe waits
for catalog convergence before timing the list. An initial immediate list saw
1900 of 5000 topics, and producing during bulk catalog teardown timed out. Both
were fixture timing effects, not evidence of silent bridge truncation.

## Commands and validation

```sh
export CARGO_BUILD_JOBS=4
eval "$(scripts/kafka-secure.sh env)"
export KAFKA_EL_TEST_BOOTSTRAP=127.0.0.1:19092
export KAFKA_EL_TEST_KAFKA_HOME="$HOME/.cache/kafka-el/kafka_2.13-4.4.0"
KAFKA_SECURE_PORT=59092 KAFKA_SECURE_MECHANISMS=PLAIN scripts/kafka-secure.sh start
export KAFKA_EL_TEST_SECURE_PLAIN_ONLY=127.0.0.1:59092
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings

KAFKA_CLUSTER_PORT=49092 scripts/kafka-cluster.sh start
KAFKA_EL_TEST_RESILIENCE_CLUSTER=127.0.0.1:49092,127.0.0.1:49093,127.0.0.1:49094 \
KAFKA_EL_TEST_RESILIENCE_SCRIPT="$PWD/scripts/kafka-cluster.sh" \
cargo test -p kafka-bridge --test resilience -- --nocapture

KAFKA_EL_TEST_LOAD=1 KAFKA_EL_TEST_LOAD_BOOTSTRAP=127.0.0.1:49092 \
cargo test -p kafka-bridge --test load -- --ignored --nocapture --test-threads=1

KAFKA_EL_BRIDGE="$PWD/target/debug/kafka-bridge" emacs -Q --batch \
  -L lisp -L test/ert -l kafka-tests.el -l kafka-security-tests.el \
  -f ert-run-tests-batch-and-exit
# Repeat with /nix/store/np588f4wzi7qbz2q3kxv0ald5dsy7xw9-emacs-30.2/bin/emacs.
emacs -Q --batch -L lisp --eval '(setq byte-compile-error-on-warn t)' \
  -f batch-byte-compile lisp/*.el
find lisp -name '*.elc' -delete
KAFKA_CLUSTER_PORT=49092 scripts/kafka-cluster.sh stop
KAFKA_SECURE_PORT=59092 scripts/kafka-secure.sh stop
```

Final all-enabled `cargo test --workspace`: **31 passed, 0 failed, 3 ignored**.
This includes 4/4 real security tests, the CLI consumer oracle, 1/1 destructive
resilience test and 5/5 existing bridge e2e tests. Strict workspace clippy passed.
Full ERT passed 15/15 on Emacs 31.1 and 15/15 on 30.2, including both new
security tests. Byte compilation passed with warnings treated as errors.
The existing jsonrpc shutdown sentinel warnings still appear during test
cleanup, without failed assertions. All three opt-in load probes passed together
in a serial run (23.02 s). The owned 49092 cluster and separate 59092 secure
fixture were both stopped after the final run. Shared brokers were not stopped
or restarted.

## Documentation integration rows

### RPC.md

No new RPC methods or parameters. Update existing behavior notes:

| Method | Params | Notes |
|---|---|---|
| `kafka/connect` | unchanged | One timeoutMs budget covers verification. Authentication, mechanism and TLS failures have actionable messages and data.kafkaCode. |
| `kafka/records/fetch` | unchanged | timeoutMs includes metadata/offset setup, not only the poll loop. Deadline exhaustion may return error 1007. |
| `kafka/produce` | unchanged | Delivery wait is bounded by the connection timeout. MessageTimedOut does not prove a write was never committed. |

### PARITY.md suggested row changes

| Operation | State | Path | Status | Test |
|---|---|---|---|---|
| SASL PLAIN/SCRAM | existing | librdkafka config and diagnostic verification | implemented and live tested | security::sasl_matrix_and_acl, security::broker_disabled_mechanism |
| TLS, client certs | existing | vendored OpenSSL and librdkafka | implemented and live tested | security::tls_matrix |
| auth-source credentials | wrapper | kafka--secret | implemented and real netrc tested | kafka-security-auth-source-scram |
| Connection timeout | wrapper | native timeout plus overall verification deadline | implemented and live tested | security matrix, existing unreachable_broker_fails_fast |
| Idempotence and leader failover | existing | enable.idempotence=true | live tested across leader SIGKILL | resilience::leader_failover_outage_restart_and_pause |

Do not mark OAUTHBEARER success tested. A diagnostic exploration using an
unsecured OAuth token did not get as far as the broker mechanism handshake and
returned a generic transport error. OIDC/token refresh remains outside this
PLAIN/SCRAM lane and needs its own real-broker coverage. GSSAPI remains disabled
in this build.

### Capability note

| id | state | implemented | note |
|---|---|---|---|
| cluster.saslTls | existing | true | PLAIN, SCRAM-256/512, SASL_SSL and mTLS live tested, actionable auth/TLS errors. GSSAPI needs sasl feature. OAuth remains unverified. |

No new capability IDs or implementation-state changes.

### TESTING.md requirement-to-test rows

| Requirement | Test |
|---|---|
| SASL PLAIN/SCRAM credentials and ACL error codes | security::sasl_matrix_and_acl |
| Unsupported broker mechanism, not just invalid local config | security::broker_disabled_mechanism |
| mTLS, expired certificate, untrusted CA, hostname SANs, SASL_SSL | security::tls_matrix |
| No stdout/stderr secret leakage, including native debug logging | security::passwords_never_reach_stdout_or_stderr_even_with_debug |
| Real netrc auth-source credentials | kafka-security-auth-source-scram |
| Fast actionable authentication failure in Emacs | kafka-security-wrong-password-fast |
| Leader failover, no lost acknowledged records or duplicates, dead bootstrap | resilience::leader_failover_outage_restart_and_pause |
| Paused leader, restart during fetch, all-broker outage, cross-connection responsiveness | resilience::leader_failover_outage_restart_and_pause |
| 1 MiB payload integrity | load::one_mib_round_trip |
| Large metadata catalog | load::topics_catalog_5000 |
| p50/p95/p99 stdio overhead | load::rpc_overhead_2000 |

## Remaining limitations

- Security and resilience tests skip when their documented environment variables
  are absent. Load tests are ignored and additionally require KAFKA_EL_TEST_LOAD=1.
- Native blocking operations already started cannot be cancelled by dropping an
  async future. RPC deadlines are enforced, while remaining native work is still
  bounded by its configured native timeout. This is not a thread-pool load test.
- A timed-out produce can have an indeterminate delivery outcome. Callers must
  not blindly retry it as a new logical record and assume exactly-once semantics.
- SIGSTOP models an unresponsive broker, not asymmetric packet loss. No actual
  network proxy partition, Kerberos, OAuth success, or certificate rotation test
  was added.
