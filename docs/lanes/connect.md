# Connect lane: phase 07

## Implementation

`kafka-connect-rest` is an independent reqwest 0.13 adapter with native-tls-vendored,
JSON responses, HTTP basic authentication, custom PEM CA support, a 30 second
request deadline, encoded URL path segments, and redirects disabled. Transport
errors omit URLs and credentials. REST errors retain HTTP status, error_code,
and message. Credentials have no Debug implementation.

The bridge stores clients in a module-level map indexed by connectionId. Configure
is local and does not make an HTTP request. Every operation first resolves the
Kafka connection. Every mutation calls Cluster::guard_write before HTTP, including
pause, resume, stop, connector/task restart and offset changes. Delete and offset
changes additionally require exact confirmName. Validation is an inspection call,
not a mutation. The service enforces the stopped-state precondition for offsets.

`kafka-connectors` provides name/type/state/running-failed task count/worker columns,
editable connector JSON, C-c C-c validation before upsert, validation error display,
status/tasks detail, pause/resume/stop/restart/delete confirmations, task restart,
offset inspection and plugin inspection. `+` creates a connector configuration.
Profiles use `:connect (:url ... :basic-auth-user ... :ssl-ca-location ...)`.
Passwords come from auth-source keyed by URL host and basic-auth-user.

`kafka-streams-apps` discovers consumer groups with matching `<app>-...-changelog`
or `-repartition` topics, and protocolType `streams` if the existing core group
listing exposes it. It calls list_groups, list_topics and describe_group without
modifying core. No Streams runtime is added to the application.

## RPC.md additions

All Connect methods except configure require a configured Connect endpoint.
All methods require an existing Kafka connection. REST payload keys retain their
native spelling, for example error_count and worker_id.

| Method | Params | Result |
|---|---|---|
| `kafka/connect/configure` | `connectionId, url, basicAuthUser?, basicAuthPassword?, sslCaLocation?` | `{configured: true}`, no HTTP |
| `kafka/connect/info` | `connectionId` | REST cluster info |
| `kafka/connect/connectors` | `connectionId` | connector-name map with expanded info and status |
| `kafka/connect/connector` | `connectionId, connector` | `{config, status, tasks}` |
| `kafka/connect/plugins` | `connectionId` | plugin list |
| `kafka/connect/validate` | `connectionId, class, config` | REST validation result including error_count and configs |
| `kafka/connect/upsert` | `connectionId, connector, config` | REST connector info, guarded mutation |
| `kafka/connect/pause` | `connectionId, connector` | null, guarded mutation |
| `kafka/connect/resume` | `connectionId, connector` | null, guarded mutation |
| `kafka/connect/stop` | `connectionId, connector` | null, guarded mutation, Connect 3.5+ |
| `kafka/connect/restart` | `connectionId, connector, includeTasks?, onlyFailed?` | REST result, guarded mutation |
| `kafka/connect/restartTask` | `connectionId, connector, task` | null, guarded mutation |
| `kafka/connect/taskStatus` | `connectionId, connector, task` | REST task status |
| `kafka/connect/topics` | `connectionId, connector` | REST connector topic map |
| `kafka/connect/delete` | `connectionId, connector, confirmName` | null, guarded exact-name mutation |
| `kafka/connect/offsets` | `connectionId, connector` | `{offsets: [...]}`, Connect 3.6+ |
| `kafka/connect/offsets/alter` | `connectionId, connector, confirmName, offsets: {offsets: [...]}` | REST result, stopped connector required, guarded exact-name mutation |
| `kafka/connect/offsets/reset` | `connectionId, connector, confirmName` | REST result, stopped connector required, guarded exact-name mutation |
| `kafka/streams/apps` | `connectionId` | `[{appId, state, members, totalLag, internalTopics}]` |

Connect HTTP errors map to code 1000 with `data.httpStatus` and `data.error_code`.
Read-only errors use 1002 and confirmation errors use 1003, before HTTP.

## PARITY.md changes

| Operation | State | Path | Status | Test |
|---|---|---|---|---|
| Kafka Connect | adapter | kafka-connect-rest and guarded bridge RPC | implemented | connect_lifecycle, kafka-connect-it-edit-validate-upsert |
| Streams observability | wrapper | core group and topic inspection | implemented classic groups, conditional streams group visibility | streams_wordcount_demo |

## Capability changes for coordinator

| id | state | implemented | note |
|---|---|---|---|
| connect | adapter | true | Connect REST lifecycle, plugins, validation, tasks, topics and offsets with read-only and exact-name guards |
| streams.observability | wrapper | true | Group/topic heuristic with lag and members. No Streams runtime or metrics collection. Streams protocol groups only when visible through existing core listing. |

Do not mark full KIP-1071 group administration implemented. Discovery is conditional
on the existing core listing exposing those groups.

## TESTING.md requirement rows

| Requirement | Test |
|---|---|
| Real FileStream data reaches Kafka | connect_lifecycle appends two lines and asserts fetch values |
| Connector lifecycle and REST oracle | connect_lifecycle, independent curl status check |
| All Connect mutation guards before HTTP | connect_lifecycle configures read-only URL on unreachable port 1 and checks nine methods return 1002 |
| Exact-name delete and offsets guards | connect_lifecycle checks all three methods return 1003 |
| Stopped connector offsets get/alter/reset | connect_lifecycle |
| Invalid configuration reports errors | connect_lifecycle and kafka-connect-invalid-config-never-upserts |
| Connector tasks, restart, topics, plugins | connect_lifecycle |
| Streams app observation | streams_wordcount_demo runs distribution WordCountDemo with unique application.id |
| Auth-source lookup and row rendering | kafka-connect-auth-source, kafka-connect-row-counts |
| Read-only editor never validates/upserts | kafka-connect-read-only-before-validation |
| Editor validate/upsert and populated connector list | kafka-connect-it-edit-validate-upsert |
| Live Connect and Streams buffers | kafka-connect-it-real-worker |

## Local worker

Run `scripts/connect-dev.sh start`, `status`, `env`, or `stop`.
It uses the existing Kafka 4.4.0 distribution and shared plain broker on 19092.
REST listens only on 127.0.0.1:18083. The FileStream jar is discovered under libs
or plugins and included in plugin.path. Worker group and internal topic names
start with kel-connect- and include a startup timestamp. State and logs are under
`~/.cache/kafka-el/connect-18083`. Starting waits for HTTP readiness.

The worker is deliberately left running. Stop it with `scripts/connect-dev.sh stop`.
The script never stops or restarts the broker.

## Validation and limitations

Rust workspace tests passed against the live broker and worker: 26 tests total,
including both new live integration tests. Strict workspace all-target clippy passed.
Full ERT passed on Emacs 31.1 and 30.2: 19/19 each, including six new Connect
tests and the existing suite. All Lisp files byte-compiled with warnings treated
as errors on both versions, then generated elc files were removed.
The official WordCountDemo examples jar was present and the live test detected
its application, members, lag and internal topics. Demo process is stopped by test
cleanup. Its fixed input/output topics are not deleted because they may be shared.

Commands used from this worktree:

```sh
CARGO_BUILD_JOBS=4 KAFKA_EL_TEST_CONNECT=http://127.0.0.1:18083 \
  KAFKA_EL_TEST_BOOTSTRAP=127.0.0.1:19092 \
  KAFKA_EL_TEST_KAFKA_HOME=$HOME/.cache/kafka-el/kafka_2.13-4.4.0 cargo test --workspace
CARGO_BUILD_JOBS=4 cargo clippy --workspace --all-targets -- -D warnings
KAFKA_EL_BRIDGE=$PWD/target/debug/kafka-bridge \
  KAFKA_EL_TEST_CONNECT=http://127.0.0.1:18083 KAFKA_EL_TEST_BOOTSTRAP=127.0.0.1:19092 \
  emacs -Q --batch -L lisp -L test/ert -l kafka-tests.el \
  -l kafka-connect-tests.el -f ert-run-tests-batch-and-exit
```

Basic auth and custom CA are implemented, but this lane's live worker is plain HTTP
without authentication. A secured Connect endpoint remains a follow-up live test.
No metrics runtime is provided. Topic-prefix discovery is a heuristic and can
misattribute topics when application ids overlap. It cannot identify an application
with no matching internal topics unless exposed with protocol type streams.
Core describe_group currently fetches offsets across topic partitions and can be
expensive on large clusters. A group disappearing during discovery can fail the
request. REST mutations return asynchronous acceptance, so refresh to observe
final task states. Connect availability/version errors are forwarded rather than
silently emulated. Configure credentials remain in bridge process memory until
replacement or process exit. Call configure after reconnecting a profile.

Cargo.lock was regenerated locally to compile reqwest but is not part of the lane
commit because it is coordinator-owned. Regenerate it after integrating lanes.
