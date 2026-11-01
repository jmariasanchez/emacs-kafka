# Feature parity

Audit updated November 1, 2026. The original rust-rdkafka 0.39 admin gaps are
now implemented locally, without changing the upstream dependency. This is
an implementation audit, not a claim that every Kafka feature is supported.

## Baseline and classification

Kafka 4.4.0 KRaft, rust-rdkafka 0.39.0, bundled librdkafka 2.12.1.
`kafka-wire` uses crates.io `kafka-protocol` 0.18.0. Our editor RPC types live
in the distinct local `kafka-rpc-protocol` crate.

| State | Meaning |
|---|---|
| existing | Direct dependency API |
| wrapper | Composition or locally maintained safe binding/protocol client |
| adapter | Separate REST service |
| gap | Not implemented |
| unsupported | Not available for the selected version |

The machine-readable subset is `kafka-core::capabilities`, exposed by
`kafka/capabilities` and `M-x kafka-capabilities`. An implemented flag means
the documented operation exists, not that every variant or environment has
been tested. Version/security limits below apply.

## Cluster and security

| Operation | State | Implementation | Evidence |
|---|---|---|---|
| Multiple clusters, named profiles, read-only | wrapper | bridge registry, core guards before I/O | `e2e::full_workflow`, ERT |
| Metadata, topics, brokers, replicas, ISR, watermarks | existing | librdkafka | `e2e::full_workflow` |
| Cluster id, controller, authorized operations | wrapper | RAII FFI DescribeCluster | `admin` |
| SASL PLAIN and SCRAM-SHA-256/512 | existing | librdkafka configuration and actionable connection diagnostics | `security::sasl_matrix_and_acl` |
| TLS, mTLS, SASL_SSL | existing | vendored OpenSSL and librdkafka | `security::tls_matrix` |
| Invalid password, unsupported mechanism, expired cert, rogue CA, missing client cert | wrapper | bounded verification with sanitized diagnostics | `security` |
| auth-source | wrapper | profile secrets from auth-source | `kafka-security-auth-source-scram` |
| SASL OAUTHBEARER | existing | librdkafka properties only | wired, no successful live OAuth test |
| GSSAPI/Kerberos | gap | needs optional build feature and test realm | not built/tested |
| Describe/Create/Delete ACLs | wrapper | safe FFI, filtered list, exact-name deletion | `admin` with secure broker and CLI oracle |
| SCRAM credential describe/upsert/delete | wrapper | safe FFI, passwords never returned | `admin`, real login before/after credential removal |
| Client quotas | wrapper | native Describe/AlterClientQuotas, validate-only and removal confirmation | `wire::features_quotas_and_guards`, CLI oracle |
| Delegation tokens | gap | no API yet | not implemented |

## Topics and administration

| Operation | State | Implementation | Evidence |
|---|---|---|---|
| List/filter, create/validate, delete | existing | AdminClient, exact-name delete guard | `e2e` |
| Describe topic and broker configs | existing | DescribeConfigs, sensitive values redacted | `e2e`, `admin` |
| Alter topic configs | wrapper | IncrementalAlterConfigs SET, not legacy read/merge/replace | `e2e`, `admin` |
| Incremental set/delete/append/subtract | wrapper | RAII FFI, topic and broker resources, validate-only | `admin` |
| Increase partitions | existing | CreatePartitions | `e2e` |
| Delete records before partition offsets | existing | DeleteRecords, exact topic confirmation | `admin` |
| Offsets for timestamp | existing | offsets_for_times | `e2e`, `admin` |
| Partition reassignment submit/list/cancel | wrapper | native wire client, preview and exact confirmation | `wire`, three-node cluster and CLI verification |
| Preferred leader election | wrapper | native ElectLeaders | `wire` |
| Log directory sizes and replica lag | wrapper | DescribeLogDirs per broker | `wire` |
| Finalized feature levels | wrapper | negotiated ApiVersions | `wire` |

## Producing, inspection and groups

| Operation | State | Implementation | Evidence |
|---|---|---|---|
| Key/value/headers/partition/timestamp | existing | idempotent FutureProducer | `e2e` |
| Binary, null keys, tombstones, delivery reports | wrapper | explicit text/base64 RPC | `e2e`, conversion unit tests |
| Transactions | existing | dedicated sessions, init/begin/commit/abort, idle expiration, lifecycle abort | `txn` |
| Batch, region, JSON Lines file producing | wrapper | 1..10000 records, partial results, optional atomic batch | `txn`, ERT |
| Bounded fetch earliest/latest/offset/time/tail | wrapper | manual assignment and captured high watermarks | `e2e` |
| Live tail and credit flow control | wrapper | bounded channels, cancellation | `e2e`, 300-record ERT stress test |
| read_committed and read_uncommitted | existing | optional fetch/stream isolationLevel | `txn`, committed/aborted transaction and CLI oracle |
| Inspection never commits or joins a real group | wrapper | dedicated inspection consumers | `e2e`, `resilience` |
| List groups, members and assignments | wrapper | fetch_group_list plus DescribeConsumerGroups FFI | `admin` |
| Efficient group offsets and lag | wrapper | ListConsumerGroupOffsets FFI, then watermarks only for committed partitions | `admin`, `e2e::group_lag_is_reported` |
| Reset offsets | wrapper | AlterConsumerGroupOffsets, Empty checks, preview, exact group confirmation | `admin`, official CLI oracle |
| Delete group/topic offsets | wrapper | DeleteConsumerGroupOffsets | `admin` |
| Delete group | existing | DeleteGroups, exact-name confirmation | `e2e` |
| Share group list/describe/offset reset/delete | wrapper | native KIP-932 administration, negotiated broker versions, Empty precondition | `wire`, Kafka 4.4 three-node cluster and CLI oracle |
| Share record consumption/acknowledgement | gap | no share consumer runtime | not implemented |
| Opt-in real group subscription and per-partition pause/resume | gap | inspection only | not implemented |
| Full KIP-1071 Streams group administration | gap | discovery only when visible | not implemented |

## Ecosystem and Emacs workflows

| Operation | State | Implementation | Evidence |
|---|---|---|---|
| Schema Registry | adapter | subjects, versions, schemas, register, compatibility/config, soft/permanent delete | `registry`, real Karapace REST oracle |
| Avro, JSON Schema and Protobuf | adapter | Confluent framing, encode/decode, optional JSON Schema validation | codec unit tests, `registry` produce/fetch/decode |
| Registry auth and TLS | adapter | basic/bearer, CA and PEM client identity | real basic-auth and mTLS tests; bearer header unit test only |
| External schema references | gap | REST metadata retained, codec rejects references explicitly | not decoded yet |
| Kafka Connect | adapter | lifecycle, configs/validation, plugins, tasks/restarts, topics, stopped-connector offsets | `connect`, real FileStreamSource and curl oracle |
| Streams observability | wrapper | group/topic heuristic, members, lag, internal topics | real WordCountDemo test |
| Transient/context menus | wrapper | builtin transient, `?` context bindings | ERT |
| Bookmarks, saved queries, record diff, bounded search | wrapper | bookmark handlers, query defcustom, Ediff and header-aware search | ERT |
| Metrics collection/statistics dashboards | gap | no metrics runtime | not implemented |

## Maintaining the local gaps

`kafka-core/src/ffi*` owns native admin requests, options, queues, events and
result lifetimes through RAII. Requests run on blocking workers while holding
the cluster alive, including when their RPC waiter is cancelled. Move these
wrappers upstream to rust-rdkafka when feasible, then remove the local FFI.

`kafka-wire` handles APIs absent from librdkafka. It negotiates versions and
implements verified TLS, SASL PLAIN and SCRAM, but not OAuth, Kerberos,
ZooKeeper controller routing or arbitrary librdkafka property forwarding.
It has no implicit retries for writes with uncertain outcomes. Unsupported
broker APIs fail explicitly. Share APIs require a broker exposing the relevant
Kafka 4.x versions. The three-node fixture is required for live share offsets
because the default share-state internal topic replication factor is three.

REST adapters are optional. No registry or Connect runtime is embedded in
the bridge. Fixture scripts install/run real local services for tests.

## Semantic and safety limits

* Config previews are observations, not compare-and-swap transactions. Only
  requested incremental changes are sent, so unrelated overrides are preserved.
* Offset resets recheck Empty state before mutation, but Kafka exposes no
  atomic compare-and-reset against a group joining concurrently. Stop consumers
  administratively before executing a reset.
* Lag is max(0, high - committed). Missing committed offsets are not lag zero.
* A bounded fetch snapshots high watermarks. Read-committed inspection can
  encounter transaction control gaps and open transactions, so a timeout can
  be legitimate even with a nonzero physical high watermark.
* Produce timeout has an indeterminate delivery outcome. Do not blindly retry
  as a new logical record and infer exactly-once semantics.
* SIGKILL and SIGSTOP tests cover leader failover and unresponsive brokers,
  not asymmetric network partitions. OAuth success, Kerberos, certificate
  rotation, secured Connect endpoints, and hosted managed services remain
  unverified.

Full method contracts, commands, evidence and limitations are also retained
in `docs/lanes/*.md`. `docs/TESTING.md` describes reproducible suite entry points.
