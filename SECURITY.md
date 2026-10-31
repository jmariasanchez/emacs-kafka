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
| Rebalancing a live group | group describe never subscribes | `group_offset_reader` |
| Secret leakage | `auth-source` lookup, redacted `Debug`, sensitive config values nulled, RPC event log off by default | `kafka--secret`, `connection.rs`, `kafka-rpc-log-events` |

## Credentials

Prefer `auth-source` over putting passwords in `kafka-clusters`:

```
# ~/.authinfo.gpg
machine prod login svc-user password ...
```

The lookup uses `:host` = profile name (falling back to the first bootstrap
host) and `:user` = `:sasl-username`.

Do not enable `kafka-rpc-log-events` with production credentials: outgoing
`kafka/connect` requests contain the password.

## Reporting vulnerabilities

Please report privately to the maintainers rather than filing a public issue.
