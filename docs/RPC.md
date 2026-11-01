# RPC.md: kafka-bridge protocol v1

Transport: JSON-RPC 2.0, `Content-Length` framed, over the bridge's
stdin/stdout (same framing as LSP, which Emacs `jsonrpc.el` speaks natively).
stdout carries only frames. Diagnostics go to stderr. Maximum frame: 64 MiB.

Typed definitions live in `crates/kafka-rpc-protocol`. This document is the
human-readable contract.

## Lifecycle

| Method | Params | Result |
|---|---|---|
| `kafka/initialize` | none | `{protocolVersion, serverInfo{name,version}, librdkafkaVersion}` |
| `kafka/shutdown` | none | `null`. Stops all streams, then the process exits. |
| `$/cancelRequest` (notification) | `{id}` | The cancelled request gets error `-32800`. |
| `kafka/capabilities` | none | `[{id, state, implemented, note}]` |

Closing stdin is equivalent to shutdown.

Graceful shutdown and stdin-close cleanup abort outstanding transaction
sessions as well as stopping streams. Disconnecting or replacing a connection
aborts its sessions and clears its Connect/Schema Registry endpoint configuration
and credentials. Configure those adapters again after reconnecting.

## Connections

| Method | Params | Notes |
|---|---|---|
| `kafka/connect` | `connectionId, bootstrapServers, readOnly?, environment?, securityProtocol?, saslMechanism?, saslUsername?, saslPassword?, sslCaLocation?, sslCertificateLocation?, sslKeyLocation?, sslKeyPassword?, timeoutMs?, properties?{}` | Verifies reachability with a metadata call. Replaces an existing connection with the same id and stops its streams. Secrets are never logged or echoed. |
| `kafka/disconnect` | `connectionId` | `{disconnected: bool}` |
| `kafka/connections` | none | list without secrets |
| `kafka/cluster/describe` | `connectionId` | brokers, origin broker, topic count |
| `kafka/broker/configs` | `connectionId, brokerId` | config entries |

## Topics

| Method | Params | Mutating |
|---|---|---|
| `kafka/topics/list` | `connectionId, includeInternal?, filter?` | |
| `kafka/topic/describe` | `connectionId, topic` | |
| `kafka/topic/configs` | `connectionId, topic` | |
| `kafka/topic/create` | `connectionId, topic, partitions, replicationFactor, configs?{}, validateOnly?` | yes |
| `kafka/topic/delete` | `connectionId, topic, confirmName` (must equal `topic`) | yes |
| `kafka/topic/alterConfig` | `connectionId, topic, configs{}, validateOnly?` → `{previous, applied}` | yes |
| `kafka/topic/addPartitions` | `connectionId, topic, totalCount, validateOnly?` | yes |
| `kafka/offsets/forTime` | `connectionId, topic, timestamp` (ms) → `[{partition, offset}]` | |

Config entries: `{name, value, source, readOnly, default, sensitive}`. `value` is
always `null` for sensitive entries.

## Groups

| Method | Params | Mutating |
|---|---|---|
| `kafka/groups/list` | `connectionId` | |
| `kafka/group/describe` | `connectionId, groupId` → summary, `membersDetail`, `offsets[{topic,partition,committed,high,lag}]`, `totalLag` | no, does not join the group |
| `kafka/group/delete` | `connectionId, groupId, confirmName` | yes |

## Records

Bytes on the wire are `null` or `{encoding: "text"|"base64", data, size}`.
Text is used only for valid UTF-8, so the client never has to guess.

Record: `{topic, partition, offset, timestamp, timestampType, key, value, headers[{key, value}]}`.

Start position: `{type: "earliest"} | {type: "latest"} | {type: "offset", offset}
| {type: "timestamp", timestamp} | {type: "tail", count}` (count is per
partition). Offsets are clamped to `[low, high]`.

| Method | Params |
|---|---|
| `kafka/produce` | `connectionId, topic, partition?, key?, value?, encoding? ("text"\|"base64", applies to key and value), headers?[{key, value, encoding?}], timestamp?` → `{topic, partition, offset}` |
| `kafka/records/fetch` | `connectionId, topic, partitions?, startPosition, limit (1..10000, default 100), timeoutMs? (max 120000), isolationLevel?` → `{records, truncated, timedOut}` |
| `kafka/consume/start` | `connectionId, topic, subscriptionId?, partitions?, startPosition, batchSize (1..10000), initialCredit (max 64), isolationLevel?` → `{subscriptionId, state: "starting"}` |
| `kafka/consume/credit` | `subscriptionId, batches` → `{available}` (capped at 64) |
| `kafka/consume/stop` | `subscriptionId` |

`subscriptionId` may be chosen by the client so it can route notifications that
arrive before the `consume/start` response. Duplicates are rejected.

`isolationLevel` is `read_uncommitted` (default) or `read_committed` for both
fetch and streaming inspection. Invalid values return -32602. Committed
inspection hides aborted transactions and does not commit consumer offsets.

### Flow control

Each `kafka/consume/records` notification consumes one credit. With zero
credit the stream thread stops polling Kafka and the bridge emits
`state: "paused"`. The client grants credit after it has rendered a batch.
Kafka's own fetch buffering is bounded by librdkafka defaults. The bridge
never drops records: if the client cannot keep up, the stream simply goes
slower. Clients that cap their display (kafka.el does) must say so explicitly
in the UI.

### Notifications

| Method | Params |
|---|---|
| `kafka/consume/records` | `{subscriptionId, seq, records[]}`. `seq` starts at 1 and has no gaps. |
| `kafka/consume/state` | `{subscriptionId, state: "active"\|"paused"\|"error"\|"cancelled", message?}` |

## Transactions and batch production

| Method | Params | Result |
|---|---|---|
| `kafka/txn/begin` | `connectionId, transactionalId?, idleTimeoutMs?` | `{txnId}`. One explicit session per connection. |
| `kafka/txn/produce` | `txnId, topic, partition?, key?, value?, encoding?, headers?, timestamp?` | `{topic, partition, offset}`. Payload rules match `kafka/produce`. |
| `kafka/txn/commit` | `txnId` | `{txnId, committed: true}` |
| `kafka/txn/abort` | `txnId` | `{txnId, committed: false}` |
| `kafka/produce/batch` | `connectionId, topic, records[{key?,value?,partition?,headers?,encoding?,timestamp?}], transactional?` | `{results[{index,ok,delivery?{topic,partition,offset},error?}], transactional, committed}` |

Begin and batch enforce read-only before producer creation. A transaction
session owns its connection and dedicated producer, so subsequent calls use
`txnId`, not `connectionId`. Session operations are serialized. Successful
commit/abort removes the session. Unknown, expired or finished ids return
-32602. Commit failure retains the session for retry or abort.
Disconnect/replacement and graceful shutdown remove matching sessions before
attempting bounded aborts. Queued operations on a removed session are rejected.
An unavailable broker can prevent cleanup from confirming abort, in which case
the broker's transaction timeout remains the fallback.

`idleTimeoutMs` defaults to 300000, range 1..86400000. Idle cleanup checks at
most every second and aborts abandoned transactions. Broker transaction timeout
is separate and remains the fallback after hard process termination. Accepted
transaction and batch writes finish even if their RPC waiter is cancelled:
cancellation is not rollback. Transactions cover production, not consumer-offset
commits.

Batches accept 1..10000 records and default to nontransactional. Headers may be
the normal producer array or a name-to-value object. Partial delivery/decoding
failure returns 1005 with the entire result object in `error.data`.
Nontransactional successes remain delivered. Any record failure in a
transactional batch aborts the batch and sets `committed: false`. A per-record
delivery report is a broker acknowledgement, not proof of transaction commit.
Commit/abort failure returns the underlying error with per-record results and
`committed: false`.

## Native admin extensions

All methods below resolve `connectionId`. Writes, validation-only config
changes and group-reset dry runs enforce read-only before network I/O.

| Method | Params | Result / guard |
|---|---|---|
| `kafka/cluster/info` | `connectionId` | `{clusterId,controller,nodes,authorizedOperations}` |
| `kafka/acls/describe` | `connectionId, filter?{resourceType?,name?,patternType?,principal?,host?,operation?,permission?}` | ACL bindings array |
| `kafka/acls/create` | `connectionId, acl{resourceType,name,patternType?,principal,host,operation,permission}` | Write |
| `kafka/acls/delete` | `connectionId, filter, confirmName` | Deleted bindings array. Confirm filter's nonempty name. |
| `kafka/configs/incrementalAlter` | `connectionId, resourceType, name, changes[{name,operation,value?}], validateOnly?` | `{applied,validateOnly}`, write guard even for validation |
| `kafka/group/offsets` | `connectionId, groupId` | `[{topic,partition,committed}]`, no join or commit |
| `kafka/group/members` | `connectionId, groupId` | `{groupId,state,groupType,coordinator,members,partitionAssignor}` |
| `kafka/group/resetOffsets` | `connectionId, groupId, topics[{topic,partitions?}], spec, dryRun, confirmName?` | `{dryRun,offsets[{topic,partition,currentCommitted,newOffset,low,high}]}`. Apply confirms groupId. |
| `kafka/group/deleteOffsets` | `connectionId, groupId, topic, confirmName` | Partition results, confirms groupId |
| `kafka/scram/describe` | `connectionId, users?[]` | `[{user,credentials[{mechanism,iterations}]}]` |
| `kafka/scram/alter` | `connectionId, change{operation:"upsert",user,mechanism,password,iterations?}` or `change{operation:"delete",user,mechanism,confirmName}` | Write. Deletion confirms user. No passwords in responses. |
| `kafka/topic/deleteRecords` | `connectionId, topic, offsets[{partition,offset}], confirmName` | `[{partition,lowWatermark}]`. Confirms topic, removes records strictly before offsets. |

ACL enum strings are case-insensitive. Resource types: `topic`, `group`,
`cluster` (alias `broker`), `transactional-id`, plus `any` for filters. Patterns:
`literal` (creation default), `prefixed`, plus `any`/`match` for filters.
Operations: `all`, `read`, `write`, `create`, `delete`, `alter`, `describe`,
`cluster-action`, `describe-configs`, `alter-configs`, `idempotent-write`.
Permissions: `allow`/`deny`. Omitted filter fields are unconstrained.
Nameless bulk deletion is refused. A wildcard resource is confirmed as `*`.

Incremental resources are `topic` or `broker`, with broker id encoded as a
nonnegative decimal string in `name`. Operations are `set`, `delete`, `append`,
`subtract`. Delete omits value, other operations require it. Values are not
returned. Empty-name cluster-wide broker defaults are unsupported. The original
`kafka/topic/alterConfig` retains its legacy merge-and-replace behavior.

Reset spec is `{type:"to-earliest"}`, `{type:"to-latest"}`,
`{type:"to-offset",offset}`, `{type:"to-datetime",timestamp}` (milliseconds),
or `{type:"shift-by",offset}`. `dryRun` is mandatory. Omitted partitions selects
all partitions of that topic. Offsets are clamped to low/high watermarks.
Shift requires an existing commit. Duplicates and unknown partitions are refused.
Reset requires zero members and state Empty, Dead or Unknown, checked again
before apply. Keep consumers stopped: preview is not a reservation and apply
recalculates offsets. Kafka offers no atomic compare-and-reset.

Members include `memberId, clientId, host, groupInstanceId, assignment`.
SCRAM mechanisms are `SCRAM-SHA-256`/`SCRAM-SHA-512`, iterations default to
4096 and must be 4096..16384. Deletion affects fresh authentication, not
already-authenticated connections.

## Wire admin extensions

| Method | Params | Result / guard |
|---|---|---|
| `kafka/reassignments/list` | `connectionId` | `[{topic,partition,replicas,addingReplicas,removingReplicas}]` |
| `kafka/reassignments/alter` | `connectionId, topic, partitions[{partition,replicas}], dryRun?, confirmName?` | `{dryRun,changes[{topic,partition,currentReplicas,targetReplicas,cancel}],results?}`. Null replicas cancels. Apply confirms topic. |
| `kafka/leaders/elect` | `connectionId, topic, partitions[int]` | `{results}`, preferred election only, write |
| `kafka/logDirs/describe` | `connectionId` | `[{brokerId,ok,directories[{path,errorCode,error,totalBytes,usableBytes,partitions[{topic,partition,size,offsetLag,isFuture}]}],error?}]` |
| `kafka/quotas/describe` | `connectionId` | `[{entity[{entityType,entityName}],values[{key,value}]}]` |
| `kafka/quotas/alter` | `connectionId, entity[{entityType,entityName}], ops[{key,value}], validateOnly?, confirmName?` | `{validateOnly,results}`, null value removes quota |
| `kafka/shareGroups/list` | `connectionId` | `[{groupId,state,type,brokerId}]`, broker-side share filter |
| `kafka/shareGroup/describe` | `connectionId, groupId` | `{groupId,state,groupEpoch,assignmentEpoch,assignor,members[{memberId,clientId,clientHost,topics,assignment}]}` |
| `kafka/shareGroup/offsets` | `connectionId, groupId` | `[{topic,partition,startOffset,leaderEpoch,ok,errorCode,error}]` |
| `kafka/shareGroup/resetOffsets` | `connectionId, groupId, confirmName, offsets?[{topic,partition,offset}], deleteTopics?[string]` | `{results}`, confirms groupId, Empty group with no members required |
| `kafka/features/describe` | `connectionId` | `{finalizedFeaturesEpoch,finalized[{name,minVersion,maxVersion}],supported[{name,minVersion,maxVersion}]}` |

Reassignment `dryRun` defaults false. Its preview is allowed on read-only
connections. Every real write checks read-only before opening a wire connection.
Quota validation also requires write permission. Share reset supplies exactly
one nonempty offsets/deleteTopics list. Its membership check is a preflight,
not an atomic lock against concurrent membership changes.

Quota entity types are `user`, `client-id`, `ip`. Null entityName selects the
default entity. Keys: `producer_byte_rate`, `consumer_byte_rate`,
`request_percentage`, `controller_mutation_rate`, `connection_creation_rate`.
Removal, except validateOnly, confirms entity names in request order joined
with `/`, substituting `<default>` for null names.

Mutation resource results include `ok, errorCode, error`. Any failed resource
returns 1005 with the whole result in `error.data`. Log-directory broker failures
remain in that broker's row. ElectionNotNeeded and NoReassignmentInProgress are
explicit broker results, not silently treated as success. Unsupported APIs return
1008. Ambiguous write transport failures are never automatically retried.

## Schema Registry

All registry methods require an existing Kafka connection. Configure is local
and does not probe the registry. Replacing/reconnecting the Kafka connection
requires configure again. Identical configure calls on the same live connection
preserve the schema-id cache. Register, config PUT, delete and produce enforce
read-only before registry I/O.

| Method | Params | Result |
|---|---|---|
| `kafka/registry/configure` | `connectionId, url, basicAuthUser?, basicAuthPassword?, bearerToken?, sslCaLocation?, sslCertificateLocation?, sslKeyLocation?, validateJsonSchema?` | `{configured:true}` |
| `kafka/registry/subjects` | `connectionId` | Subject strings |
| `kafka/registry/versions` | `connectionId, subject` | Version numbers |
| `kafka/registry/schema` | `connectionId, schemaId` or `connectionId, subject, version?` | `{id,schema,schemaType,subject?,version?,references[]}` |
| `kafka/registry/register` | `connectionId, subject, schema, schemaType?, references?[]` | `{id}` |
| `kafka/registry/compatibility` | `connectionId, subject, version?, schema, schemaType?, references?[]` | `{is_compatible:bool}` |
| `kafka/registry/config` | `connectionId, subject?, compatibility?` | Raw Confluent result. Omit compatibility to GET, supply a level to PUT. |
| `kafka/registry/delete` | `connectionId, subject, version?, permanent?, confirmName` | Deleted versions array or version. Both soft and permanent deletion confirm subject. |
| `kafka/registry/decode` | `connectionId, bytes` (base64) | `{schemaId,subject?,format,json}` |
| `kafka/registry/produce` | `connectionId, topic, subject?, value, key?, keySubject?, partition?, timestamp?, headers?[]` | `{topic,partition,offset}` |

Schema is a string, schemaType defaults to `AVRO`, and supported formats are
`AVRO`, `JSON`, `PROTOBUF`. Version accepts a number or string and defaults to
`latest` for lookup/compatibility. Delete without version deletes the subject.
References use Confluent `{name,subject,version}` objects. Permanent deletion
follows server prerequisites, typically prior soft deletion.

Produce uses the latest schema and defaults subjects to `<topic>-value` and
`<topic>-key`. A supplied JSON null is schema-encoded, not a Kafka tombstone.
Omit key for a Kafka null key. Use ordinary `kafka/produce` for tombstones.
Confluent framing and Protobuf message indexes are handled by the adapter.
JSON Schema validation is opt-in and never retrieves external resources.
Codec failures return 1006, configuration failures -32602, HTTP/transport failures
1000. Credentials and HTTP error bodies are not echoed.

## Kafka Connect and Streams observation

Connect methods require an existing Kafka connection and, except configure, a
configured REST endpoint. REST payload keys retain native spellings such as
`error_count` and `worker_id`. Configure is local. All lifecycle writes, task
restarts and offset changes enforce read-only before HTTP. Validation is a read.

| Method | Params | Result / guard |
|---|---|---|
| `kafka/connect/configure` | `connectionId, url, basicAuthUser?, basicAuthPassword?, sslCaLocation?` | `{configured:true}` |
| `kafka/connect/info` | `connectionId` | REST cluster info |
| `kafka/connect/connectors` | `connectionId` | Connector-name map with expanded info and status |
| `kafka/connect/connector` | `connectionId, connector` | `{config,status,tasks}` |
| `kafka/connect/plugins` | `connectionId` | Plugin list |
| `kafka/connect/validate` | `connectionId, class, config` | Validation result including error_count and configs |
| `kafka/connect/upsert` | `connectionId, connector, config` | Connector info, create or update |
| `kafka/connect/pause` | `connectionId, connector` | null |
| `kafka/connect/resume` | `connectionId, connector` | null |
| `kafka/connect/stop` | `connectionId, connector` | null, Connect 3.5+ |
| `kafka/connect/restart` | `connectionId, connector, includeTasks?, onlyFailed?` | REST restart result, flags default false |
| `kafka/connect/restartTask` | `connectionId, connector, task` | null |
| `kafka/connect/taskStatus` | `connectionId, connector, task` | Task status |
| `kafka/connect/topics` | `connectionId, connector` | Connector topic map |
| `kafka/connect/delete` | `connectionId, connector, confirmName` | null, confirms connector |
| `kafka/connect/offsets` | `connectionId, connector` | `{offsets:[...]}`, Connect 3.6+ |
| `kafka/connect/offsets/alter` | `connectionId, connector, confirmName, offsets:{offsets:[...]}` | REST result, confirms connector, must be stopped |
| `kafka/connect/offsets/reset` | `connectionId, connector, confirmName` | REST result, confirms connector, must be stopped |
| `kafka/streams/apps` | `connectionId` | `[{appId,state,members,totalLag,internalTopics}]`, no Connect endpoint required |

Lifecycle responses may mean asynchronous acceptance. Refresh status to observe
completion. The Connect server enforces stopped state for offset mutations.
REST failures return 1000 with `data.httpStatus`, `data.error_code`, and the REST
message. Connect configuration is stored by connection id and cleared on
disconnect/replacement. Configure again after reconnecting a profile.

Streams discovery is read-only: consumer groups matching `<app>-...-changelog`
or `-repartition` topics, plus protocol type `streams` if visible in the existing
group listing. Prefix matching is heuristic. This is observation, not a Streams
runtime or full KIP-1071 administration.

## Errors

| Code | Meaning |
|---|---|
| -32700 | parse error (bad JSON or bad frame header) |
| -32600 | invalid request |
| -32601 | unknown method |
| -32602 | invalid params, including local validation failures |
| -32603 | internal |
| -32800 | request cancelled |
| 1000 | Kafka/broker error. `data.kafkaCode` holds the librdkafka code name. |
| 1001 | unknown connection |
| 1002 | connection is read-only |
| 1003 | confirmation name mismatch |
| 1004 | unknown subscription |
| 1005 | partial failure. `data.results[{name, ok, error?}]` |
| 1006 | payload decode error (e.g. invalid base64), distinct from Kafka errors |
| 1007 | timeout |
| 1008 | unsupported by client/broker |

## Versioning

`protocolVersion` is an integer. Additive changes (new methods, new optional
fields) do not bump it. Removing or changing the meaning of a field does.
