# Admin FFI lane

## Implementation and integration

The lane adds `rdkafka-sys = "4.10"` to kafka-core. Cargo.lock changes only
record that already-resolved dependency as direct. Native request objects,
queues, events, options and owned errors have RAII destructors. Requests run
on spawn_blocking with an Arc<Cluster> retained through completion, including
when the waiting future is cancelled. The native handle comes from
`cluster.admin_client().inner().native_ptr()`. Result data is copied before
its owning event is released. Native error codes become KafkaError::AdminOp.
The admin RPC converter extracts that variant explicitly to retain kafkaCode:
rust-rdkafka 0.39's rdkafka_error_code() unexpectedly returns None for AdminOp.

All mutations call guard_write before network I/O, including validateOnly
and reset dry runs. Destructive actions require confirmName. Neither SCRAM
request type implements Debug or Serialize. Malformed SCRAM RPC params
produce a generic error instead of reflecting input. The SCRAM UI suppresses
jsonrpc traffic logging for the password-bearing call and clears the input
string afterward.

No edits were made to cluster.rs, dispatch.rs, capabilities.rs, or the shared
docs. The existing stub module registrations and kafka.el requires already
load the new code. The only non-owned file change is Cargo.lock, generated
by Cargo for the explicit dependency.

### Coordinator follow-ups

* Change `kafka/topic/alterConfig` to call incremental_alter with set operations.
  This lane intentionally leaves its legacy merge-and-replace behavior alone.
  Decide how to retain its previous/applied response compatibility, and redact
  sensitive values if reusing that response shape.
* Change `kafka/group/describe` to use group_offsets, then fetch watermarks only
  for returned partitions. It currently still scans all topics. The new method
  returns `[{topic, partition, committed}]` and never joins or commits.
* Merge the following RPC, capability, parity and testing rows into shared docs.
* `features.describe` remains a gap. DescribeCluster is not DescribeFeatures.
* Consider moving the admin module's explicit AdminOp error-code extraction
  into the shared core_err converter so legacy admin methods retain codes too.

## RPC.md additions

All methods require connectionId. Params below include it explicitly.

| Method | Params | Mutating |
|---|---|---|
| `kafka/cluster/info` | `connectionId` -> `{clusterId, controller, nodes, authorizedOperations}` | no |
| `kafka/acls/describe` | `connectionId, filter?{resourceType?, name?, patternType?, principal?, host?, operation?, permission?}` -> bindings[] | no |
| `kafka/acls/create` | `connectionId, acl{resourceType,name,patternType?,principal,host,operation,permission}` | yes |
| `kafka/acls/delete` | `connectionId, filter, confirmName` -> matching deleted bindings[] | yes |
| `kafka/configs/incrementalAlter` | `connectionId, resourceType, name, changes[{name,operation,value?}], validateOnly?` -> `{applied,validateOnly}` | yes |
| `kafka/group/offsets` | `connectionId, groupId` -> `[{topic,partition,committed}]` | no |
| `kafka/group/members` | `connectionId, groupId` -> `{groupId,state,groupType,coordinator,members,partitionAssignor}` | no |
| `kafka/group/resetOffsets` | `connectionId, groupId, topics[{topic,partitions?}], spec, dryRun, confirmName?` -> `{dryRun,offsets[{topic,partition,currentCommitted,newOffset,low,high}]}` | yes unless dryRun |
| `kafka/group/deleteOffsets` | `connectionId, groupId, topic, confirmName` -> partition results[] | yes |
| `kafka/scram/describe` | `connectionId, users?[]` -> `[{user,credentials[{mechanism,iterations}]}]` | no |
| `kafka/scram/alter` | `connectionId, change{operation:"upsert",user,mechanism,password,iterations?}` or `change{operation:"delete",user,mechanism,confirmName}` | yes |
| `kafka/topic/deleteRecords` | `connectionId, topic, offsets[{partition,offset}], confirmName` -> `[{partition,lowWatermark}]` | yes |

ACL enum strings are case-insensitive. Resource types: topic, group, cluster
(alias broker), transactional-id. Filter resource type can be any. Pattern
is literal or prefixed, with any/match additionally available to filters.
Filter fields default to any or no constraint. Creation requires concrete
resource/operation/permission, name, principal and host. Pattern defaults to
literal. Operations: all, read, write, create, delete, alter, describe,
cluster-action, describe-configs, alter-configs, idempotent-write. Permissions:
allow or deny. Deletion requires a nonempty name in the filter and confirmName
identical to that name. A wildcard ACL resource is confirmed as the literal
`*`. Nameless bulk deletion is intentionally refused. Output bindings can be
passed directly as a deletion filter.

Incremental config resources are topic names or nonnegative broker ids encoded
as strings. Operations: set, delete, append, subtract. Delete omits value,
other operations require it. Config values are never returned by this RPC.
Cluster-wide default broker configuration (empty resource name) is not
supported by the native ConfigResource constructor in librdkafka 2.12.1.

Reset spec is one of `{type:"to-earliest"}`, `{type:"to-latest"}`,
`{type:"to-offset",offset:N}`, `{type:"to-datetime",timestamp:MS}`,
`{type:"shift-by",offset:N}`. Missing partitions selects all partitions of
that topic. Duplicates and unknown partitions are refused. Computed offsets
are clamped to current low/high watermarks, matching the Kafka CLI convention.
A timestamp later than available records selects high. Shift requires an
existing committed offset and rejects integer overflow. dryRun is mandatory.
Applying requires confirmName equal to groupId. DeleteOffsets also confirms
the groupId. DeleteRecords confirms the topic and deletes records strictly
before each supplied nonnegative offset.

Reset checks DescribeConsumerGroups and refuses any active or rebalancing
group. Empty, Dead, or Unknown is allowed only with zero members. The state is
checked again immediately before applying. Kafka offers no atomic
compare-and-reset, so keep consumers stopped throughout preview and execution.
Execution recalculates the spec rather than treating a preview as a reservation.
The preview UI explicitly mentions this in its final confirmation.

Member entries contain memberId, clientId, host, groupInstanceId and assignment
(topic/partition entries). Group type is classic, consumer or unknown. Native
API version/broker errors are surfaced instead of guessing state.

SCRAM supports SCRAM-SHA-256 and SCRAM-SHA-512. Iterations default to 4096 and
must be 4096..16384. Deletion confirms the exact username. Passwords are never
included in responses. SCRAM credential deletion affects fresh authentication,
not already-authenticated connections, so the test disconnects/reconnects.

## PARITY.md row changes

| Operation | State | Path | Status | Test |
|---|---|---|---|---|
| Incremental alter configs | wrapper | native IncrementalAlterConfigs via RAII FFI | implemented | `admin::native_configs_cluster_and_records` |
| Delete records | existing | AdminClient::delete_records with confirmation | implemented | `admin::native_configs_cluster_and_records` |
| Describe cluster | wrapper | native DescribeCluster | implemented | `admin::native_configs_cluster_and_records` |
| Describe members and assignments | wrapper | native DescribeConsumerGroups | implemented | `admin::active_group_refuses_reset_and_reports_assignments` |
| ListConsumerGroupOffsets | wrapper | one native OffsetFetch, no topic scan | implemented in new RPC | `admin::offsets_reset_and_cli_oracle` |
| Reset/alter group offsets | wrapper | native AlterConsumerGroupOffsets, empty-group guard | implemented | `admin::offsets_reset_and_cli_oracle` |
| Delete group offsets for a topic | wrapper | native DeleteConsumerGroupOffsets | implemented | `admin::offsets_reset_and_cli_oracle` |
| Describe/Create/Delete ACLs | wrapper | native ACL admin APIs | implemented | `admin::secure_acls_and_scram` |
| SCRAM credential management | wrapper | native Describe/AlterUserScramCredentials | implemented | `admin::secure_acls_and_scram` |

## Capability changes

| id | state | implemented | note |
|---|---|---|---|
| topic.incrementalAlterConfig | wrapper | true | Native RAII FFI, topic and individual broker resources |
| topic.deleteRecords | existing | true | Per-partition before-offset, exact topic confirmation |
| groups.resetOffsets | wrapper | true | Native admin reset, preview, empty-group check and exact confirmation |
| acls | wrapper | true | Native describe/create/delete, exact resource deletion confirmation |
| scram.users | wrapper | true | Native credential describe/upsert/delete, password redaction |
| cluster.info (new) | wrapper | true | Native DescribeCluster, controller, nodes and authorized operations |
| groups.offsets (new) | wrapper | true | Native ListConsumerGroupOffsets without metadata scans |
| groups.members (new) | wrapper | true | Native DescribeConsumerGroups with decoded assignments |
| groups.deleteOffsets (new) | wrapper | true | Native DeleteConsumerGroupOffsets, exact group confirmation |

## TESTING.md additions

| Requirement | Test |
|---|---|
| Native cluster id/controller/nodes | `admin::native_configs_cluster_and_records`, `kafka-admin-it-cluster-info` |
| Incremental set preserves other overrides, validateOnly, delete/append/subtract, broker validateOnly | `admin::native_configs_cluster_and_records` |
| Efficient offset fetch, dry run unchanged, reset CLI oracle, every reset spec, delete offsets | `admin::offsets_reset_and_cli_oracle` |
| Active group refuses reset, member assignments decoded | `admin::active_group_refuses_reset_and_reports_assignments` |
| Every native write enforces read-only before I/O | `admin::all_admin_writes_enforce_read_only` |
| ACL grant/revoke and official CLI oracle, alice describe success/denial | `admin::secure_acls_and_scram` |
| SCRAM create, second connection authenticates, delete, fresh auth fails | `admin::secure_acls_and_scram` |
| Malformed SCRAM input cannot leak passwords in errors | `admin::scram_parse_errors_never_echo_secrets` |
| Reset preview buffer then exact confirmation and actual committed offset | `kafka-admin-it-reset-preview-apply` |
| Secure ACL and SCRAM table modes | `kafka-admin-it-secure-list-modes` |
| Secret input clearing and traffic-log suppression | `kafka-admin-scram-suppresses-traffic-log` |
| UI selection validation and wrong-confirmation refusal | `kafka-admin-selection-validation`, `kafka-admin-reset-confirmation-before-write` |

Rust tests skip when their required env vars are unset. Secure test requires
KAFKA_EL_TEST_SECURE_BOOTSTRAP, KAFKA_EL_TEST_KAFKA_HOME and
KAFKA_EL_TEST_ADMIN_CONFIG. The provided local secure broker uses PLAIN
admin/admin-secret. ERT secure mode test needs KAFKA_EL_TEST_SECURE_BOOTSTRAP.
CLI tests use the supplied Kafka 4.4 installation, not a mock.

## Validation results

* `CARGO_BUILD_JOBS=4 cargo clippy --workspace --all-targets -- -D warnings`: pass.
* `cargo test --workspace` with plain/secure brokers and CLI env: 30 passed,
  including 6 new admin tests and 5 existing bridge e2e tests. No skips in admin.
* Whole lisp directory byte-compilation with byte-compile-error-on-warn: pass.
* Emacs 31.1: 19/19 ERT passed, including all 6 new admin tests.
* Emacs 30.2: 19/19 ERT passed, including all 6 new admin tests.
* Existing jsonrpc shutdown warnings appear on both versions, with no failures.

## UI entry points

`kafka-acls`, `kafka-scram-users`, `kafka-cluster-info`, and
`kafka-group-reset-offsets` take the connection name via kafka-read-connection.
ACL/SCRAM tables use + for create/upsert, D for deletion and g for refresh.
Group modes add R for reset and m for member details. Reset opens a tabulated
preview without writing. C-c C-c then asks for the exact group name.
