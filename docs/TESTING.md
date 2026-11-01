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

## Not yet covered (release gates for later versions)

* SASL and TLS brokers, invalid credentials, expired certificates, ACL denial.
* Multi-broker KRaft: leader election, broker restart mid-stream, network
  partition (toxiproxy).
* Large records and topic catalogs with 10k+ topics, plus p50/p95/p99 IPC
  overhead benchmarks.
* Transactions (not implemented).
