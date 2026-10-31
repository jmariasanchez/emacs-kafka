# PARITY.md: Feature-parity audit

Milestone 1 of the master plan. Every operation is classified, mapped to an
implementation path, and linked to the test that verifies it.

Baseline versions (audited 2026-10-31):

| Component | Version |
|---|---|
| Apache Kafka (test broker) | 4.4.0, single-node KRaft |
| rust-rdkafka | 0.39.0 |
| librdkafka (bundled via rdkafka-sys) | 2.12.1 |
| Emacs | 29.1 minimum, tested on 31.1 |

The machine-readable version of this table is `kafka-core::capabilities`
(served over RPC as `kafka/capabilities`, shown in Emacs by
`M-x kafka-capabilities`). A unit test asserts that no `gap` entry is marked
implemented.

## Classification

| State | Meaning |
|---|---|
| **existing** | Direct support in a vetted dependency |
| **wrapper** | Composed from existing dependency APIs |
| **adapter** | Separate supported API or library (HTTP REST etc.) |
| **gap** | Needs implementation here or upstream |
| **unsupported** | Not available for the selected Kafka/client version |

## Matrix

Legend for Status: ✅ implemented + tested, ⏳ planned, 🚫 blocked on gap.

### Cluster and connections

| Operation | State | Path | Status | Test |
|---|---|---|---|---|
| Multiple simultaneous clusters | wrapper | bridge connection registry | ✅ | `e2e::full_workflow` (rw + ro) |
| Named profiles, env labels | wrapper | `kafka-clusters` defcustom | ✅ | `kafka-test-connect-params-and-secrets` |
| Bootstrap + metadata | existing | `Client::fetch_metadata` | ✅ | `e2e::full_workflow` |
| SASL PLAIN/SCRAM/OAUTHBEARER | existing | librdkafka config | ✅ wired, ⏳ tested | needs SASL broker in CI |
| SASL GSSAPI (Kerberos) | existing | needs `gssapi` cargo feature | ⏳ | |
| TLS, client certs | existing | `ssl-vendored` | ✅ wired, ⏳ tested | needs TLS broker in CI |
| Read-only profiles | wrapper | enforced in `kafka-core` before any I/O, and in Emacs | ✅ | `read_only_blocks_writes_before_network`, `e2e` |
| auth-source credentials | wrapper | `kafka--secret` | ✅ | `kafka-test-connect-params-and-secrets` |
| Connection timeout | existing | `socket.timeout.ms`, admin timeouts | ✅ | `unreachable_broker_fails_fast` |
| Destructive-action guards | wrapper | exact-name confirm (bridge + UI) | ✅ | `e2e` (code 1003), `kafka-test-confirm-exact` |

### Topics and partitions

| Operation | State | Path | Status | Test |
|---|---|---|---|---|
| List/filter topics | existing | metadata | ✅ | `e2e`, `kafka-it-explorer-workflow` |
| Create topic (+ validate-only) | existing | `create_topics` | ✅ | `e2e` |
| Delete topic | existing | `delete_topics` | ✅ | `e2e` |
| Describe configs | existing | `describe_configs` | ✅ | `e2e` |
| Alter configs | wrapper | legacy `AlterConfigs`, merged with current dynamic overrides | ✅ | `e2e` asserts overrides preserved |
| Incremental alter configs | **gap** | not exposed by rdkafka 0.39 (librdkafka has it) | 🚫 | |
| Increase partitions | existing | `create_partitions` | ✅ | `e2e` |
| Leaders, replicas, ISR | existing | metadata | ✅ | `e2e` |
| Watermarks | existing | `fetch_watermarks` | ✅ | `e2e` |
| Offsets for timestamp | existing | `offsets_for_times` | ✅ | `e2e` |
| Delete records | existing | `delete_records` | ⏳ | |
| Partition reassignment | **gap** | AlterPartitionReassignments not exposed | 🚫 | |
| Broker configs | existing | `describe_configs(Broker)` | ✅ (read) | |
| Describe log dirs | **gap** | not exposed | 🚫 | |
| Describe cluster features | **gap** | not exposed | 🚫 | |

### Producer

| Operation | State | Path | Status | Test |
|---|---|---|---|---|
| Key/value/headers/partition/timestamp | existing | `FutureProducer` | ✅ | `e2e` |
| Binary payloads | wrapper | base64 on the wire | ✅ | `e2e` (bytes `ff 00 41`) |
| Null key, tombstones | existing | | ✅ | `e2e` |
| Idempotence | existing | `enable.idempotence=true` | ✅ (always on) | |
| Delivery reports | existing | | ✅ | `e2e` asserts offsets |
| Transactions | existing | `init/begin/commit/abort_transaction` | ⏳ | |
| Batch produce from buffer/file | wrapper | | ⏳ | |

### Consumer (inspection)

| Operation | State | Path | Status | Test |
|---|---|---|---|---|
| Bounded fetch earliest/latest/offset/timestamp/tail | wrapper | manual `assign`, no group join | ✅ | `e2e` |
| Live stream with flow control | wrapper | credit semaphore + bounded channels | ✅ | `e2e` asserts pause until credit |
| Never commits offsets | wrapper | `enable.auto.commit=false`, never `commit`, unique throwaway group id | ✅ | `e2e` asserts no offsets on inspection groups |
| Isolation level (read_committed) | existing | config | ⏳ | |
| Subscribe as a real group member | existing | | ⏳ (explicit opt-in only) | |

### Groups

| Operation | State | Path | Status | Test |
|---|---|---|---|---|
| List groups | existing | `fetch_group_list` | ✅ | `e2e` |
| Describe members | existing | `fetch_group_list(Some)` | ✅ | |
| Committed offsets + lag | wrapper | OffsetFetch via never-subscribed consumer + watermarks | ✅ | `group_lag_is_reported` (oracle: official CLI) |
| Delete group | existing | `delete_groups` | ✅ | `group_lag_is_reported` |
| Reset/alter group offsets | **gap** | AlterConsumerGroupOffsets not exposed. Candidate wrapper: `commit` from an assigned consumer using the group id, only when group is Empty | ⏳ | |
| ListConsumerGroupOffsets (efficient) | **gap** | currently we OffsetFetch every partition of every topic, O(partitions) | ⏳ perf | |
| Share groups (KIP-932) | **gap** | no librdkafka/rdkafka support yet | 🚫 | |
| Streams groups (KIP-1071) | **gap** | | 🚫 | |

### Security and governance

| Operation | State | Status |
|---|---|---|
| Describe/Create/Delete ACLs | **gap** (rdkafka 0.39 does not bind librdkafka's ACL API) | 🚫 contribute upstream or add FFI wrapper |
| Client quotas | **gap** | 🚫 |
| SCRAM credential management | **gap** | 🚫 |
| Delegation tokens | **gap** | 🚫 |

### Ecosystem

| Operation | State | Status |
|---|---|---|
| Schema Registry (Avro/Protobuf/JSON Schema) | adapter (HTTP REST) | ⏳ phase 06 |
| Kafka Connect | adapter (REST) | ⏳ phase 07 |
| Streams observability | adapter (groups + metrics) | ⏳ phase 07. No Streams runtime is planned. |
| Metrics | existing (`statistics.interval.ms` callback) | ⏳ |

## Gap strategy

Most admin gaps exist in librdkafka 2.12 but are unbound in rust-rdkafka 0.39
(ACLs, incremental alter configs, ListConsumerGroupOffsets,
AlterConsumerGroupOffsets, DescribeCluster, user SCRAM). Preferred order:

1. Contribute bindings upstream to rust-rdkafka.
2. Meanwhile, a thin `unsafe` FFI module in `kafka-core` over `rdkafka-sys`,
   behind the same safe API, removed once upstream lands.
3. Partition reassignment and log dirs are not in librdkafka at all; they need
   either a Java-sidecar adapter or a small native implementation over the
   `kafka-protocol` crate (the Rust wire-protocol crate of the same name). This
   decision is deferred to phase 05.

## Semantic differences to remember

* `topic/alterConfig` uses the legacy non-incremental API. We read current
  dynamic overrides and re-send them, so the result is equivalent to an
  incremental set **except** for a race with a concurrent writer, and topics
  with sensitive dynamic configs are refused (their values cannot be read back).
* `records/fetch` stops at the high watermark captured at call start, so a
  busy topic returns a consistent snapshot instead of running forever.
* Lag is `max(0, high - committed)`. Partitions with no committed offset are
  omitted, not reported as lag 0.
