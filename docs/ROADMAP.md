# ROADMAP.md

Phases follow the master plan. ✅ = done in this repo, with tests.

| Phase | Scope | State |
|---|---|---|
| 01 Research | Parity audit: `docs/PARITY.md`, `kafka-core::capabilities` | ✅ initial audit |
| 02 Core + connections | workspace, stdio JSON-RPC, lifecycle, profiles, auth-source, read-only, timeouts, multi-cluster, bridge crash recovery | ✅ (SASL/TLS wired but not yet CI-tested) |
| 03 Producer | key/value/headers/partition/timestamp, binary, tombstones, idempotence, delivery reports | ✅ basic. ⏳ transactions, batch produce, file payloads |
| 04 Consumer | bounded fetch from earliest/latest/offset/time/tail, live stream with credit flow control, no commits | ✅ inspection. ⏳ isolation level, opt-in group consumption, pause/resume per partition |
| 05 Admin + groups | topics CRUD, configs, partitions, broker configs, groups list/describe/lag/delete | ✅ partial. ⏳ offset reset, ACLs, quotas, reassignment (gaps, see PARITY) |
| 06 Serialization | Schema Registry, Avro, Protobuf, JSON Schema | ⏳ |
| 07 Connect, share groups, observability | Connect REST, KIP-932, metrics | ⏳ (share groups blocked upstream) |
| 08 Emacs workflows | transient menus, bookmarks, record diff, search, saved queries | ⏳ |
| 09 Resilience | multi-broker fault injection, TLS/SASL matrix, load tests | ⏳ |
| 10 Release | binary packaging per platform, MELPA, docs | ⏳ |

## Next concrete steps

1. CI matrix: SASL/SCRAM and TLS brokers, Kafka 3.9 and 4.x.
2. Offset reset for Empty groups, with preview of affected partitions.
3. Transactional produce.
4. `transient` menu over existing commands.
5. Bind ACL and incremental-alter-config APIs (upstream PR to rust-rdkafka, or
   interim FFI).
6. Replace per-partition OffsetFetch in group describe with
   ListConsumerGroupOffsets once bound.
