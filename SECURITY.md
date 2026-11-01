# SECURITY.md

## Threat model

kafka.el runs with the developer's own Kafka credentials. The main risks are
not remote attackers. They are:

1. **Accidental damage**: deleting the wrong topic, resetting a production
   consumer group, publishing test data to production.
2. **Silent side effects**: browsing records commits offsets for an
   application consumer group, or triggers a rebalance.
3. **Credential leakage**: secrets ending up in logs, `*Messages*`, RPC event
   buffers, bug reports, or customize files.

## Controls

| Risk | Control | Where |
|---|---|---|
| Wrong-target writes | `:read-only` profiles are enforced in `kafka-core` before I/O | `Cluster::guard_write` |
| Writes to protected envs | `yes-or-no-p` with cluster and environment shown | `kafka-confirm-write` |
| Topic/group deletion | type the exact name. The bridge re-checks `confirmName`. | UI + `delete_topic`/`delete_group` |
| Config changes | current and proposed values shown; broker `validateOnly` before apply | `kafka-config-edit` |
| Topic creation | broker `validateOnly` before apply | `kafka-topic-create` |
| Offset commits while browsing | dedicated non-committing consumer | `inspection_consumer` |
| Rebalancing a live group | committed offsets fetched by native admin API, without joining | `ffi/groups.rs` |
| Offset reset | inactive-group checks, explicit preview, exact group confirmation | `reset_group_offsets` |
| Adapter credential reuse | disconnect/reconnect drops REST clients and aborts producer sessions | bridge lifecycle hooks |
| Secret leakage | `auth-source` lookup, redacted `Debug`, sensitive config values nulled, RPC event log off by default | `kafka--secret`, `connection.rs`, `kafka-rpc-log-events` |

## Credentials

Prefer `auth-source` over putting passwords in `kafka-clusters`:

```
# ~/.authinfo.gpg
machine prod login svc-user password ...
```

The lookup uses `:host` = profile name (falling back to the first bootstrap
host) and `:user` = `:sasl-username`.

Do not enable `kafka-rpc-log-events` with production credentials. Requests can
contain passwords, bearer tokens, connector configs or record contents. SCRAM
credential changes suppress the traffic logger explicitly, but other operations
rely on the default disabled event log. Native Kafka diagnostics use fixed,
sanitized auth/TLS messages instead of echoing broker text. Real debug-log tests
capture stdout and stderr and check for supplied passwords.

Registry and Connect credentials are scoped to a connection and discarded on
successful replacement or disconnect. This is ordinary process memory, not a
guarantee of secure erasure. Prefer auth-source over plaintext profile secrets.
TLS and SASL wire admin operations require explicit security fields and reject
security overrides hidden in `properties` before opening a socket.

## Transaction and fixture safety

A timed-out produce has an indeterminate delivery outcome. Do not blindly retry
it as a new logical record and assume exactly-once behavior. Transaction
cancellation is not rollback. Accepted transaction and batch writes complete
even if their RPC waiter is cancelled. Graceful shutdown/disconnect removes
unfinished sessions and attempts bounded aborts. If the broker is unavailable,
or the process is terminated hard, broker transaction expiry is the fallback.

Local fixture scripts bind to loopback and create disposable development
credentials and private keys in the cache. They are not deployment templates.
The destructive resilience suite refuses any bootstrap list other than its
owned three-node fixture on ports 49092 through 49094.

## Reporting vulnerabilities

Please report privately to the maintainers rather than filing a public issue.
