# RPC.md: kafka-bridge protocol v1

Transport: JSON-RPC 2.0, `Content-Length` framed, over the bridge's
stdin/stdout (same framing as LSP, which Emacs `jsonrpc.el` speaks natively).
stdout carries only frames. Diagnostics go to stderr. Maximum frame: 64 MiB.

Typed definitions live in `crates/kafka-protocol`. This document is the
human-readable contract.

## Lifecycle

| Method | Params | Result |
|---|---|---|
| `kafka/initialize` | none | `{protocolVersion, serverInfo{name,version}, librdkafkaVersion}` |
| `kafka/shutdown` | none | `null`. Stops all streams, then the process exits. |
| `$/cancelRequest` (notification) | `{id}` | The cancelled request gets error `-32800`. |
| `kafka/capabilities` | none | `[{id, state, implemented, note}]` |

Closing stdin is equivalent to shutdown.

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
| `kafka/records/fetch` | `connectionId, topic, partitions?, startPosition, limit (1..10000, default 100), timeoutMs? (max 120000)` → `{records, truncated, timedOut}` |
| `kafka/consume/start` | `connectionId, topic, subscriptionId?, partitions?, startPosition, batchSize (1..10000), initialCredit (max 64)` → `{subscriptionId, state: "starting"}` |
| `kafka/consume/credit` | `subscriptionId, batches` → `{available}` (capped at 64) |
| `kafka/consume/stop` | `subscriptionId` |

`subscriptionId` may be chosen by the client so it can route notifications that
arrive before the `consume/start` response. Duplicates are rejected.

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
