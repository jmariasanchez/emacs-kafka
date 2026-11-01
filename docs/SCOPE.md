# Scope

This pre-release implements the main workflows below. `PARITY.md` is the
per-operation implementation audit, `COMPATIBILITY.md` distinguishes live
verification from configured support, and `ROADMAP.md` lists remaining work.

| Domain | Implemented | Still outside scope |
|---|---|---|
| Connections | profiles, multi-cluster, read-only, auth-source, SASL/TLS, actionable errors and bounded verification | OAuth refresh, Kerberos, managed-service presets |
| Topics/partitions | metadata, watermarks, configs, create/delete, partition count, delete records, reassignment, preferred election, log dirs, features | broad storage migration orchestration |
| Producer | binary records, headers, tombstones, idempotence, sessions, transactions, batches, region/file and re-produce | transactional consumer offsets, templates |
| Consumer | manual noncommitting inspection, bounded fetch, live credit flow, isolation, bounded search | group subscription, per-partition pause/resume, share delivery/ack runtime |
| Groups | members, efficient committed offsets and lag, deletion, previewed inactive-group resets, share administration | full Streams group administration |
| Serialization | text, JSON, hexdump, Registry REST, Avro/JSON Schema/Protobuf framing and codecs | external schema dependency resolution |
| Governance | guarded writes, exact confirmations, ACLs, quotas, SCRAM users, incremental configuration | delegation tokens |
| Ecosystem | Connect REST lifecycle, validation, plugins, task/offset/topic views and changes, Streams group/topic inspection | Streams execution, connector deployment packaging |
| Observability | metadata, lag, Streams heuristics and capability audit | metrics/statistics dashboards and continuous monitoring |
| Emacs | explorers, producer, detail, transient menus, bookmarks, saved queries, bounded search and diff | dedicated events dashboard |
| Verification | real secure brokers, leader failure/restart/pause/outage, REST fixtures, load probes, macOS/Linux and multiple Emacs versions | hosted Actions execution, asymmetric proxy partitions, sustained load, managed-service matrix |
| Distribution | local source builds and CI configuration | release binaries, signing, MELPA and release automation |

A method existing does not imply that every broker variant, authentication
provider or hosted service is verified. Fixture-backed tests are opt-in and
must be enabled explicitly. The capability RPC keeps remaining gaps marked
`implemented: false` rather than presenting them as complete.
