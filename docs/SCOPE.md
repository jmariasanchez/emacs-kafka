# SCOPE.md

The full feature inventory spans the 11 domains of the master plan
(148 capability targets). This file sets release boundaries. Per-operation
status lives in `docs/PARITY.md`.

| Domain | In 0.1 (this release) | Later release |
|---|---|---|
| Cluster connections | multi-cluster, profiles, env labels, read-only, auth-source, SASL/TLS settings, timeouts, status | 0.6: SASL/TLS CI matrix, managed presets, OAuth refresh, diagnostics buffer |
| Topics and partitions | list/filter, describe (leaders/replicas/ISR/watermarks), configs view + edit, create (validated), delete (exact name), add partitions, offsets for time, broker configs | 0.4: delete records. 0.6: reassignment, log dirs, features (gaps) |
| Producer | key/value/headers/partition/timestamp, binary, tombstones, idempotent, delivery report, re-produce from record | 0.2: transactions, batch/file produce, templates |
| Consumer | bounded fetch from earliest/latest/offset/timestamp/tail, live tail with flow control, never commits | 0.3: isolation level, per-partition pause/resume, opt-in group consumption, search across ranges |
| Groups | list, describe members, committed offsets, lag, delete | 0.4: offset reset with preview. 0.8: share and streams groups |
| Serialization | raw bytes, UTF-8 text, JSON pretty-print, hexdump | 0.5: Schema Registry, Avro, Protobuf, JSON Schema |
| Security/governance | read-only, confirmations, secret redaction | 0.6: ACLs, quotas, SCRAM users |
| Connect/ecosystem | none | 0.7 |
| Observability | capabilities view | 0.7: metrics, monitor mode |
| Emacs UX | tabulated-list views for every domain above, producer buffer, record detail | 0.9: transient menus, bookmarks, diff, events buffer |
| Testing/distribution | unit, e2e vs real broker, ERT UI tests, CI config | 0.9: fault injection, multi-broker. 1.0: binaries, MELPA |

An operation is listed as done only when its semantics and failure handling
are tested.
