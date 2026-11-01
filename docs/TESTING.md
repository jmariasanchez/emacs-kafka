# TESTING.md

## Suites

| Suite | Location | Needs broker | What it proves |
|---|---|---|---|
| Rust unit | `crates/*/src` `#[cfg(test)]` | no | framing, base64 round trip, error mapping, lag/tail math, secret redaction, read-only guard before I/O, capability table consistency |
| Bridge e2e | `crates/kafka-bridge/tests/e2e.rs` | yes | the real binary over stdio: every RPC method, error codes, flow control, no commits, clean exit on stdin close, lag checked against the official CLI |
| ERT unit | `test/ert/kafka-tests.el` (`kafka-test-*`) | no | param building, auth-source, confirmations, producer parsing, row cap, hexdump |
| ERT integration | `test/ert/kafka-tests.el` (`kafka-it-*`) | yes | the real Emacs buffers end to end, bridge crash recovery, error surfacing |

Broker tests skip themselves (they don't fail) when `KAFKA_EL_TEST_BOOTSTRAP` is unset.

```sh
scripts/kafka-dev.sh start   # Kafka 4.4.0 KRaft on 127.0.0.1:19092
scripts/test-all.sh          # everything
```

## Requirement → test map (0.1)

| Requirement (master plan) | Test |
|---|---|
| No unintended offset commits | `e2e::full_workflow` (inspection groups have no offsets) |
| Group describe does not change offsets | `e2e::group_lag_is_reported` |
| Bounded batches + credit flow control | `e2e::full_workflow` (paused until credit) |
| Cancellation and cleanup | `e2e::full_workflow` (stop then unknown sub), `kafka-it-explorer-workflow` (kill buffer stops stream) |
| No leaked consumers on shutdown | `e2e::stdin_close_mid_stream_exits_and_leaks_nothing` |
| Binary-safe payloads, empty and null | `e2e::full_workflow`, `convert::tests::binary_safe` |
| Retrieval vs decode errors distinct | `convert::tests::bad_base64_is_decode_error` (1006 vs 1000) |
| Malformed frames | `transport::tests::*`, `e2e::protocol_errors_without_broker` |
| No duplicated or lost stream batches under interleaved sync requests | `kafka-it-stream-no-duplicate-or-lost-batches` (300 records, batch 7). Fails when a duplicate dispatch or a dropped batch is injected. |
| Subprocess death | `kafka-it-bridge-death-recovers` |
| Notifications not stuck behind sync replies (jsonrpc 1.0.16) | `kafka-it-explorer-workflow` live-tail step. Fails 3/3 on Emacs 29.4 with `kafka-rpc--drain` disabled. |
| Destructive guards | `e2e` (1002, 1003), `kafka-test-confirm-exact` |
| Partial admin failure reporting | `convert::tests::partial_carries_results` (unit only) |

## Extended suites

`docs/lanes/` preserves per-lane implementation and validation reports. Their
integration suggestions describe the state before merge. The current RPC
contract is `RPC.md`. Final counts and scope are recorded in `VALIDATION.md`,
with platform details in `COMPATIBILITY.md`.

| Suite | Real service coverage |
|---|---|
| `admin.rs` | ACLs, SCRAM, incremental config, committed offsets and safe resets, official CLI oracle |
| `wire.rs` | quota and replica changes, leader election, log dirs, share groups, TLS/SCRAM wire transport |
| `security.rs` | PLAIN, SCRAM-256/512, SASL_SSL, mTLS, bad password, expired certificate, wrong CA/SAN, ACL denial and secret capture |
| `resilience.rs` | owned three-broker fixture, leader kill/pause/restart, complete outage, concurrent healthy connection, acknowledged record integrity |
| `registry.rs` | real Karapace REST, Avro/JSON Schema/Protobuf round trips, Basic auth and mTLS |
| `connect.rs` | real distributed Connect FileStream connector and Streams WordCount observability |
| `txn.rs` | commit/abort isolation, explicit sessions, batches, disconnect/replacement/shutdown/EOF aborts with stable-offset sentinels |
| `load.rs` | ignored opt-in 1 MiB integrity, 5,000 topics and IPC quantiles |
| `test/ert/*-tests.el` | all Lisp suites loaded by `scripts/test-all.sh` |

Optional fixtures need Java (21 was exercised), `curl`, OpenSSL and `keytool`.
The Registry fixture additionally needs `uv`, which installs a Python 3.12
environment and Karapace 6.2.0 in the cache. Set up services before the combined
suite:

```sh
scripts/kafka-secure.sh start
scripts/kafka-cluster.sh start
eval "$(scripts/kafka-secure.sh env)"
eval "$(scripts/kafka-cluster.sh env)"
scripts/schema-registry-dev.sh start
KAFKA_EL_REGISTRY_AUTH=1 scripts/schema-registry-dev.sh start
KAFKA_EL_REGISTRY_TLS=1 scripts/schema-registry-dev.sh start
eval "$(scripts/schema-registry-dev.sh env)"
eval "$(KAFKA_EL_REGISTRY_AUTH=1 scripts/schema-registry-dev.sh env)"
eval "$(KAFKA_EL_REGISTRY_TLS=1 scripts/schema-registry-dev.sh env)"
scripts/connect-dev.sh start
eval "$(scripts/connect-dev.sh env)"
scripts/test-all.sh
```

The resilience test deliberately stops brokers. It refuses to use the shared
cluster. Start a separate owned fixture with `KAFKA_CLUSTER_PORT=49092` and set
`KAFKA_EL_TEST_RESILIENCE_CLUSTER=127.0.0.1:49092,127.0.0.1:49093,127.0.0.1:49094`
and `KAFKA_EL_TEST_RESILIENCE_SCRIPT=$PWD/scripts/kafka-cluster.sh`.
For the broker-disabled-mechanism test, start a separate secure fixture using
`KAFKA_SECURE_PORT=59092 KAFKA_SECURE_MECHANISMS=PLAIN` and set
`KAFKA_EL_TEST_SECURE_PLAIN_ONLY=127.0.0.1:59092`.
See `docs/lanes/security-resilience.md` for exact destructive-test and load-test
commands. Each fixture has a matching `stop` command. Preserve the same port
and authentication environment when stopping nondefault instances.

## Platform verification

`scripts/linux-check.sh` runs build, warning-free clippy, strict Lisp byte
compilation, Rust tests, and live-broker ERT in a local Linux VM. It uses Apple
`container`, Docker, or Podman and the cached Kafka distribution. No remote
source upload is required. `scripts/linux-check.sh quick` omits live services.
Use `EMACS=/path/to/emacs scripts/test-all.sh` for another local Emacs version.
GitHub Actions is configured separately in `.github/workflows/ci.yml`. A local
VM pass is not evidence that a hosted Actions job ran.

## Still not covered

* OAuth issuer/token refresh, GSSAPI, managed Kafka services and Connect auth.
* Asymmetric network partitions or packet loss through a network proxy.
* 10,000+ topic catalogs, certificate rotation and sustained thread-pool load.
* Schema Registry external-reference resolution and transactional consumer
  offset commits, which are not implemented.
