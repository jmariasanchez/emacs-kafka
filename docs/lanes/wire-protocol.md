# Wire protocol lane

## Implementation

`crates/kafka-wire` implements bounded async Kafka request/response framing,
correlation checks, per-request timeouts, bootstrap failover, ApiVersions
negotiation, metadata discovery, and group coordinator lookup. Every request
uses the highest version supported by both broker and `kafka-protocol 0.18`.
ApiVersions v0 bootstraps negotiation. SASL authentication occurs before the
second, rich ApiVersions request because Kafka accepts only one pre-auth
ApiVersions exchange.

The dependency disables default features and enables `client`, `gzip`, and
`snappy`. These compression implementations do not require cmake. The client
uses a small local request/response association trait because the dependency's
`Request` trait requires both client and broker features. No generated protocol
code was copied. The client speaks admin APIs only, not record batch encoding.

TLS uses vendored OpenSSL and tokio-openssl, validates the CA and hostname, and
supports optional client certificates and encrypted PEM keys. SASL PLAIN,
SCRAM-SHA-256, and SCRAM-SHA-512 work with or without TLS. SCRAM uses SASLprep,
random nonces, PBKDF2/HMAC/SHA2, strict challenge checks and constant-time server
signature comparison. PBKDF2 runs on the blocking pool and its work factor is
bounded. Neither the socket client nor SCRAM secret state implements Debug.
Authentication error messages expose broker error names, not server text or
credentials.

KRaft controller APIs go to a broker, which forwards them. Log directories and
share-group lists fan out across the metadata broker list. Share descriptions
and offsets route through FindCoordinator. Preferred leader election is
implemented here, not via librdkafka, to keep one consistent RPC path.
Interrupted, timed-out, malformed, or correlation-mismatched connections are
poisoned and cannot be reused. Writes are never automatically retried after
an ambiguous transport failure.

## RPC.md additions

| Method | Params | Mutating |
|---|---|---|
| `kafka/reassignments/list` | `connectionId` -> `[{topic,partition,replicas,addingReplicas,removingReplicas}]` | no |
| `kafka/reassignments/alter` | `connectionId, topic, partitions[{partition,replicas}], dryRun?, confirmName?` -> `{dryRun,changes[{topic,partition,currentReplicas,targetReplicas,cancel}],results?}`. `replicas:null` cancels. Real submissions require exact topic confirmation. | yes, except preview |
| `kafka/leaders/elect` | `connectionId, topic, partitions[int]` -> `{results}`. Preferred election only. | yes |
| `kafka/logDirs/describe` | `connectionId` -> `[{brokerId,ok,directories[{path,errorCode,error,totalBytes,usableBytes,partitions[{topic,partition,size,offsetLag,isFuture}]}],error?}]` | no |
| `kafka/quotas/describe` | `connectionId` -> `[{entity[{entityType,entityName}],values[{key,value}]}]` | no |
| `kafka/quotas/alter` | `connectionId, entity[{entityType,entityName}], ops[{key,value}], validateOnly?, confirmName?` -> `{validateOnly,results}`. `value:null` removes a quota. | yes, including validation |
| `kafka/shareGroups/list` | `connectionId` -> `[{groupId,state,type,brokerId}]`, broker-side ListGroups share filter | no |
| `kafka/shareGroup/describe` | `connectionId, groupId` -> `{groupId,state,groupEpoch,assignmentEpoch,assignor,members[{memberId,clientId,clientHost,topics,assignment}]}` | no |
| `kafka/shareGroup/offsets` | `connectionId, groupId` -> `[{topic,partition,startOffset,leaderEpoch,ok,errorCode,error}]` | no |
| `kafka/shareGroup/resetOffsets` | `connectionId, groupId, confirmName, offsets?[{topic,partition,offset}], deleteTopics?[string]`. Supply exactly one of offsets or deleteTopics. Requires an Empty group with no members. | yes |
| `kafka/features/describe` | `connectionId` -> `{finalizedFeaturesEpoch,finalized[{name,minVersion,maxVersion}],supported[{name,minVersion,maxVersion}]}` | no |

Quota entity types are `user`, `client-id`, and `ip`. A null entityName selects
the default entity. Supported keys are `producer_byte_rate`,
`consumer_byte_rate`, `request_percentage`, `controller_mutation_rate`, and
`connection_creation_rate`. For removal, confirmName is the entity names in
request order, joined with `/`, with `<default>` for a null name. Validation-only
does not need confirmation but does enforce read-only profiles.

Per-resource mutation results carry `ok`, `errorCode`, and readable `error`.
A failed resource returns RPC 1005 with the whole result in `error.data`.
Top-level broker failures return 1000 with `data.kafkaCode` when available.
Timeouts return 1007, absent or incompatible APIs return 1008. A log-directory
broker failure is retained in that broker's row rather than discarding healthy
broker results. ElectionNotNeeded (84) and NoReassignmentInProgress (85) remain
explicit broker results rather than being silently converted into success.

All mutating RPCs call the core guard before opening a wire connection. The
reusable wire admin functions also reject read-only configs. Reassignments,
share offset changes and quota removal require exact confirmation. Replica
previews never mutate and are available on read-only connections. A share
reset's Empty check is a preflight, with the broker still authoritative if
membership changes concurrently.

## PARITY.md row replacements

| Operation | State | Path | Status | Test |
|---|---|---|---|---|
| Partition reassignment | adapter | kafka-wire List/AlterPartitionReassignments | implemented | `wire::reassign_elect_log_dirs_and_records`, `wire::cancel_active_reassignment` |
| Preferred leader election | adapter | kafka-wire ElectLeaders | implemented | `wire::reassign_elect_log_dirs_and_records` |
| Describe log dirs | adapter | metadata fanout + kafka-wire DescribeLogDirs | implemented | `wire::reassign_elect_log_dirs_and_records` |
| Describe cluster features | adapter | negotiated ApiVersions finalized features | implemented | `wire::features_quotas_and_guards` |
| Client quotas | adapter | kafka-wire Describe/AlterClientQuotas | implemented | `wire::features_quotas_and_guards`, `kafka-wire-it-quota-edit-and-delete` |
| Share groups (KIP-932) | adapter | kafka-wire ListGroups, ShareGroupDescribe, Describe/Alter/DeleteShareGroupOffsets | implemented, requires working share-state storage | `wire::share_group_describe_offsets_reset` |

## Capability changes for coordinator

| id | state | implemented | note |
|---|---|---|---|
| `partition.reassignment` | adapter | true | Native wire list/submit/cancel, preview, exact confirmation |
| `partition.electLeaders` (new) | adapter | true | Native preferred leader election |
| `logDirs.describe` | adapter | true | DescribeLogDirs per metadata broker |
| `quotas` | adapter | true | Native client quota describe/set/remove/validate |
| `groups.share` | adapter | true | Share list/describe/offsets/reset/delete, requires broker support and initialized share state |
| `features.describe` | adapter | true | ApiVersions finalized and supported features |

## TESTING.md additions

| Requirement | Test |
|---|---|
| Bounded framing, fragmented responses, correlation validation | `kafka_wire::tests::{framing_bounds,fragmented_frame_and_correlation}` |
| Highest mutual version and unsupported API detection | `kafka_wire::tests::version_intersection` |
| Timeout and no reuse after interruption | `kafka_wire::tests::timeout_poisoning_prevents_reuse` |
| SCRAM RFC 7677 proof and server signature | `kafka_wire::scram::tests::rfc7677_vector` |
| Reject bad nonce, duplicate attributes, mandatory extension, unsafe iteration counts | `kafka_wire::scram::tests::rejects_bad_challenges` |
| Readable Kafka error codes | `kafka_wire::tests::errors_are_named` |
| Read-only rejection before network | `kafka_wire::admin::tests::read_only_before_network`, `wire::features_quotas_and_guards` |
| RF1 broker 1 -> replicas 2,3 and unchanged records | `wire::reassign_elect_log_dirs_and_records`, Kafka topic describe and reassignment verify CLI oracles |
| Cancel an active reassignment | `wire::cancel_active_reassignment`, verifies successful cancellations restore original replicas |
| Nonzero replica disk size per broker | `wire::reassign_elect_log_dirs_and_records` |
| Quota validateOnly, set, remove, exact removal confirmation | `wire::features_quotas_and_guards`, kafka-configs CLI oracle |
| Share list, active members, offsets, Empty precondition, reset, delete | `wire::share_group_describe_offsets_reset`, kafka-share-groups CLI oracle |
| SASL_SSL PLAIN and SCRAM512 on native wire | `wire::sasl_ssl_plain_and_scram512` |
| Native SCRAM256, mTLS, bad password, untrusted CA | `wire::wire_mtls_scram256_and_auth_failures` |
| Replica plan parser and preview before exact confirmation | `kafka-wire-plan-*`, `kafka-wire-submit-*` |
| Real Lisp lists, editable replica preview/submission and read-only rejection | `kafka-wire-it-admin-views-and-plan` |
| Real Lisp quota set, validate and delete | `kafka-wire-it-quota-edit-and-delete` |

Run all fixtures:

```sh
CARGO_BUILD_JOBS=4 \
KAFKA_EL_TEST_BOOTSTRAP=127.0.0.1:19092 \
KAFKA_EL_TEST_CLUSTER_BOOTSTRAP=127.0.0.1:39092,127.0.0.1:39093,127.0.0.1:39094 \
KAFKA_EL_TEST_SHARE_BOOTSTRAP=127.0.0.1:39092 \
KAFKA_EL_TEST_SECURE_BOOTSTRAP=127.0.0.1:29092 \
KAFKA_EL_TEST_SSL_CA="$HOME/.cache/kafka-el/secure-29092/certs/ca.pem" \
KAFKA_EL_TEST_KAFKA_HOME="$HOME/.cache/kafka-el/kafka_2.13-4.4.0" \
cargo test --workspace
```

The dedicated SHARE_BOOTSTRAP variable avoids falsely treating a broker with
share.version enabled but broken share-state initialization as a usable share
fixture. Missing test environment variables skip those integration bodies.
Secure tests use the lane brief's fixed local ports and admin test credentials.
No shared broker is stopped or restarted. Test consumers are terminated on
both success and assertion failure.

## Lisp entry points

`kafka-reassignments`, `kafka-log-dirs`, `kafka-quotas`, and `kafka-share-groups`
take the connection name. Reassignment `e` opens an editable `partition:
replica,replica` buffer, or `partition: cancel`. `C-c C-p` previews the current
metadata diff. `C-c C-c` previews again and requires the exact topic. Quota `e`
sets a value after validateOnly, and `D` confirms the exact entity before
removal. Share list RET displays member assignments and offsets without
joining. Views use the existing connection header and tabulated-list patterns.

## Validation and limitations

Live validation on November 1, 2026:

- All six Rust wire integration tests passed with the environment above.
- Existing five Rust bridge e2e tests passed unchanged.
- Workspace `cargo test --workspace` passed 39 tests with all six wire broker
  tests enabled. Two additional guard/validation unit tests then brought the
  kafka-wire unit suite to 9 passing tests, for 41 passing tests in total.
- `cargo clippy --workspace --all-targets -- -D warnings` passed.
- All Lisp files byte-compiled with warnings treated as errors, and generated
  .elc files were removed.
- Existing and wire ERT passed 20/20 on Emacs 31.1 and 20/20 on Emacs 30.2,
  including live quota editing/removal and replica plan submission.
- A real SASL pre-auth ordering bug and JSON-vector UI iteration bug were found
  by these tests and corrected before final validation.
- The first reassignment convergence test exposed a transient librdkafka
  NotLeaderForPartition while querying watermarks after reassignment. Polling
  now treats that response as not-yet-converged rather than a permanent failure.

The requested single-node :19092 share-consumer test was attempted twice. Its
broker logs showed __share_group_state auto-creation failing because the
configured default replication factor is 3 but there is one broker. The
independent kafka-share-groups CLI also reported no offset information and zero
assigned partitions. List and describe worked, but no client can exercise
share offsets there until the fixture is repaired. The full offsets/reset/
delete workflow passed on the existing three-node cluster instead. No shared
internal topic or broker settings were changed by this lane.

Other intentional limits: no GSSAPI/OAUTHBEARER, no ZooKeeper-controller
routing, no background pooling, no automatic retry of uncertain writes, and no
arbitrary librdkafka `properties` forwarding into the native client. Security
uses the explicit ConnectionConfig fields. TLS verification cannot be disabled
through this client. Admin responses are capped at 64 MiB. The UI exposes share
inspection, while share reset/delete is currently RPC-only.

Cargo.lock was regenerated by cargo while resolving lane dependencies and left
uncommitted so the coordinator can regenerate one combined lockfile after all
lanes merge. No root manifest, shared protocol types, or capability registry
was edited.
