# Producer and transient lane

## Integration notes

Branch: `lane/producer-transient`.

The coordinator must call `ext::txn::abort_all(Some(connection_id)).await`
before disconnecting or replacing a connection, and
`ext::txn::abort_all(None).await` during shutdown and stdin-close cleanup.
The function drains matching sessions before aborting them. Requests already
queued on a removed session are rejected. This lane deliberately did not
change lifecycle dispatch ownership. Until those hooks are integrated,
client-side idle expiry is the cleanup fallback while the bridge is running.
Hard process termination cannot run Rust destructors, so Kafka's transaction
timeout remains the final fallback after a crash.

`crates/kafka-rpc-protocol/src/lib.rs` was changed only to add serde-defaulted
`isolation_level: Option<String>` fields to `FetchParams` and
`ConsumeStartParams`. `records.rs` validates and applies the option to the
inspection consumer configuration. `dispatch.rs` changes only pass it through
the two core calls. The core `fetch` and `stream` signatures now take a final
`Option<String>` isolation argument. All existing repository callers were
updated. No dependencies or Package-Requires changes were needed.

`kafka.el` already requires the lane's transient module in the shared scaffold.
`kafka-transient.el` requires `transient` and `kafka-workflows`. The records,
topics and groups `?` bindings are installed from this file. Workflow hooks and
record marking bindings are installed from `kafka-workflows.el`, without
editing `lisp/kafka-records.el`. No extra hook there is needed.

Optional menu commands are declared and guarded by both `:if` and
`:inapt-if-not` predicates using `fboundp`. Emacs 30's bundled Transient checks
undefined suffix commands before evaluating inapt predicates, so visibility
predicates are necessary as well. Missing commands become available on the
next menu opening after their implementation is loaded. The menu references
`kafka-delete-records` and `kafka-streams-apps` for those workflow entries.

## RPC.md rows

| Method | Params | Result |
|---|---|---|
| `kafka/txn/begin` | `connectionId, transactionalId?, idleTimeoutMs?` | `{txnId}`. Read-only guard before producer creation. One explicit session per connection. Idle timeout defaults to 300000 ms, range 1..86400000. |
| `kafka/txn/produce` | `txnId, topic, partition?, key?, value?, encoding?, headers?, timestamp?` | `{topic, partition, offset}`. Same payload decoding as `kafka/produce`. `connectionId` is unnecessary because the session owns its connection. |
| `kafka/txn/commit` | `txnId` | `{txnId, committed: true}`. Removes the session after successful commit. |
| `kafka/txn/abort` | `txnId` | `{txnId, committed: false}`. Removes the session after successful abort. |
| `kafka/produce/batch` | `connectionId, topic, records: [{key?, value?, partition?, headers?, encoding?, timestamp?}], transactional?` | `{results: [{index, ok, delivery?{topic,partition,offset}, error?}], transactional, committed}`. Accepts 1..10000 records. Headers can be the normal array or a name-to-value object. |

Add to the existing records rows:

| Method | Params |
|---|---|
| `kafka/records/fetch` | Existing params plus `isolationLevel?: "read_committed"\|"read_uncommitted"`, default `read_uncommitted`. Invalid values return -32602. |
| `kafka/consume/start` | Existing params plus the same `isolationLevel` option, validation and default. |

Batch partial delivery or decoding failures use error 1005 with the full
result object in `error.data`. Nontransactional successes remain delivered.
For transactional batches, any record error aborts the entire batch and sets
`committed: false`, even when an individual record has a successful delivery
report. A successful report is a broker acknowledgement, not evidence that
an enclosing transaction committed. A commit or abort failure returns the
underlying core error with per-record results and `committed: false`.

Session IDs are single-use and unknown, expired or finished IDs return -32602.
Each session has a dedicated `FutureProducer`, runs `init_transactions` once,
and serializes operations. Commit errors retain the explicit session so the
caller can retry or abort. Accepted transaction and batch writes finish even
if their RPC waiter is cancelled. This avoids concurrent commit/produce calls
on a producer when cancellation interrupts a blocking commit.

## PARITY.md row changes

| Operation | State | Path | Status | Test |
|---|---|---|---|---|
| Transactions | existing | dedicated FutureProducer init/begin/commit/abort | implemented, lifecycle hooks required at merge | `txn::transaction_abort_commit_isolation_and_cli_oracle`, `txn::unfinished_core_transaction_aborts_on_drop` |
| Batch produce from buffer/file | wrapper | bounded batch RPC, region and JSON Lines commands | implemented and tested | `txn::batch_partial_failure_and_atomic_abort`, `kafka-txn-it-produce-region-and-file` |
| Isolation level (read_committed) | existing | inspection consumer isolation.level | implemented and tested for fetch and stream | `txn::transaction_abort_commit_isolation_and_cli_oracle` |
| Transient menus | wrapper | builtin transient, context keymaps | implemented and tested | `kafka-txn-transient-menus` |
| Bookmarks and saved queries | wrapper | bookmark handlers and named query defcustom | implemented and tested | `kafka-txn-it-bookmark-round-trip`, `kafka-txn-it-saved-query-and-header-search` |
| Record diff and bounded search | wrapper | marked values to Ediff, bounded fetch and header-aware matching | implemented and tested | `kafka-txn-diff-setup`, `kafka-txn-it-saved-query-and-header-search` |

## Capability changes

| id | state | implemented | note |
|---|---|---|---|
| `produce.transactions` | existing | true | Dedicated session producer, idle expiry, abort on drop. Merge lifecycle abort_all hooks. |
| `produce.batch` (new) | wrapper | true | Up to 10000 records with per-record reports, partial failures, optional atomic commit. |
| `consume.isolation` (new) | existing | true | read_committed/read_uncommitted for bounded fetch and streaming inspection. |

## TESTING.md requirement-to-test rows

| Requirement | Test |
|---|---|
| Aborted transactions hidden from committed readers, visible to uncommitted readers | `txn::transaction_abort_commit_isolation_and_cli_oracle`: abort 3, assert 0/3, then commit 2, assert 2. Independent `kafka-console-consumer.sh --isolation-level read_committed` oracle checks the exact values. |
| Live stream honors isolation | Same test starts a committed stream and asserts exactly the two committed values. |
| Idle transaction cleanup and single-use sessions | `txn::idle_timeout_aborts_and_session_ids_are_one_shot` |
| Unfinished transaction destructor aborts | `txn::unfinished_core_transaction_aborts_on_drop` against the real broker |
| Batch partial results, atomic abort/commit, record count cap | `txn::batch_partial_failure_and_atomic_abort` |
| Read-only transaction and batch guards, invalid isolation | `txn::readonly_and_invalid_isolation_are_rejected` |
| Region/file publishing, headers, null/empty payloads, raw JSON, protected environments | `kafka-txn-it-produce-region-and-file`, `kafka-txn-lines-and-json`, `kafka-txn-batch-protection-before-network` |
| Every active transient suffix is a defined interactive command | `kafka-txn-transient-menus`, actual Transient setup on both Emacs versions |
| Bookmark survives buffer destruction with connection/topic/partitions/start/filter | `kafka-txn-it-bookmark-round-trip`, actual bookmark-set/bookmark-jump and broker fetch |
| Saved query filtering and bounded header search | `kafka-txn-it-saved-query-and-header-search`, actual produced broker records |
| Mark two records and pretty-print their values for Ediff | `kafka-txn-diff-setup`, actual record buffer marking, intercepted Ediff call verifies both buffer contents |

## UI usage

- `M-x kafka-dispatch` opens all workflow groups. `?` opens context menus in
  records, topics and groups lists.
- `kafka-produce-region` publishes one record per line. The first literal
  `kafka-producer-key-separator` (tab by default) splits key from value. Nil
  disables splitting. Empty lines are empty values, a final newline does not
  add an extra record. A prefix argument commits the batch transactionally.
- `kafka-produce-file` accepts record-envelope JSON objects or raw lines.
  JSON objects without any recognized envelope fields are raw JSON payloads.
  Malformed object syntax is rejected before publishing. Both commands honor
  protected-environment confirmation and report batch totals.
- `C-x r m` bookmarks topic, records and group buffers. Only the profile name
  is stored, never the connection's credentials.
- `kafka-saved-queries` contains `(name connection topic position filter)`
  entries. `kafka-run-saved-query` restores a bounded record browser.
- In records, `m` toggles up to two selections and `=` invokes Ediff on their
  pretty-printed values. Ediff buffers can be killed normally after use.
- `kafka-records-search` fetches at most N records and searches keys, values,
  header names and header values. It opens a snapshot in a records buffer.

## Limits

- Kafka's own absolute transaction timeout is separate from bridge idle
  timeout. Broker and producer properties still govern maximum transaction
  duration. Idle cleanup checks at most every second and abort is bounded by
  the connection timeout.
- Transactions cover producing only, not transactional consumer-offset commits.
- Batches await delivery sequentially. The 10000-record cap is not a promise
  that a batch completes within a particular RPC timeout.
- Search is bounded and client-side, not a whole-topic index. The result is a
  snapshot. Ordinary records refresh/live commands switch to their ordinary
  browser semantics. The normal records row cap still applies.
- The minimum supported Emacs is 29.1, but only the locally available Emacs
  30.2 and 31.1 were exercised here.

## Validation results

Validated on November 1, 2026 against the shared Kafka 4.4.0 broker at
`127.0.0.1:19092`:

- `CARGO_BUILD_JOBS=4 KAFKA_EL_TEST_BOOTSTRAP=127.0.0.1:19092 KAFKA_EL_TEST_KAFKA_HOME=$HOME/.cache/kafka-el/kafka_2.13-4.4.0 cargo test --workspace`: **29 passed**, including 5 transaction integration tests and all 5 existing bridge e2e tests. The official console-consumer oracle ran, not skipped.
- `CARGO_BUILD_JOBS=4 cargo clippy --workspace --all-targets -- -D warnings`: passed with no warnings.
- `KAFKA_EL_BRIDGE=$PWD/target/debug/kafka-bridge KAFKA_EL_TEST_BOOTSTRAP=127.0.0.1:19092 emacs -Q --batch -L lisp -L test/ert -l kafka-tests.el -l kafka-txn-tests.el -f ert-run-tests-batch-and-exit`: **21/21 passed** on Emacs 31.1, then **21/21 passed** using `/nix/store/np588f4wzi7qbz2q3kxv0ald5dsy7xw9-emacs-30.2/bin/emacs`. Includes 8 new lane tests and the 13 existing ERT tests.
- All `lisp/*.el` byte-compiled with `byte-compile-error-on-warn` true under both Emacs versions, zero warnings. Generated `.elc` files were deleted afterwards.
- `git diff --check`: passed.

The first ERT run caught a bookmark-handler current-buffer restoration bug.
The Emacs 30 run caught the undefined-suffix validation ordering difference.
Expanded file tests caught the need for an explicit empty JSON object when a
record envelope contains only null fields. All three were fixed and the full
suites rerun. Existing jsonrpc shutdown sentinel warnings still appear during
integration tests, but there are no failed or skipped final ERT cases.

## Post-review transaction hardening

`Transaction::produce` now wraps the complete delivery future in
`tokio::time::timeout` using the profile timeout, rather than treating
FutureProducer's queue-admission timeout as a total deadline. Expiry maps to
`KafkaError::MessageProduction(MessageTimedOut)`, matching the security lane's
normal producer behavior and preserving the Kafka error code on the wire.
As with ordinary produce, a timeout does not retract a record already sent.
Callers should abort a transaction after an uncertain delivery outcome.

`Drop` moves the entire producer handle to a named background OS thread for
both abort and destruction. Moving only a clone would still allow the last
native producer destructor to block an async worker. The destructor schedules
best-effort cleanup, it does not wait for completion. Explicit `finish(false)`
and the lifecycle `abort_all` hook still await an abort. Abrupt process exit
can terminate background cleanup, leaving the broker timeout as fallback.

The new core test `delivery_deadline_bounds_an_unresponsive_endpoint` uses a
real TCP listener that never answers Kafka requests and a 60-second native
delivery timeout. The wrapper must return `MessageTimedOut` within one second
with a 50 ms profile deadline. It bypasses transaction initialization only to
isolate the delivery deadline, and does not pause any shared broker.

The real-broker abort-on-drop test now checks prompt return from drop and
publishes a later nontransactional sentinel. Committed readers must see only
that sentinel, proving the background abort advanced the last stable offset,
not merely that an open transaction is temporarily invisible.

Follow-up validation: full `cargo test --workspace` with broker and CLI oracle
environment passed **30 tests**. Strict workspace/all-target Clippy passed.
The lane's 8 ERT tests also passed on both Emacs 31.1 and 30.2 against the
hardened bridge, including transactional file producing.
