# Roadmap

Updated November 1, 2026. Detailed scope and remaining limits are in
`PARITY.md`. Tests and fixture commands are in `TESTING.md`.

| Phase | Scope | State |
|---|---|---|
| 01 Research | parity audit and machine-readable capabilities | Implemented and refreshed |
| 02 Core + connections | JSON-RPC, lifecycle, profiles, auth-source, read-only, security | Implemented. PLAIN/SCRAM, TLS/mTLS and failures live tested. OAuth/Kerberos remain unverified. |
| 03 Producer | binary-safe records, idempotence, transactions, batch/region/file | Implemented and live tested |
| 04 Consumer | bounded inspection and live flow control, isolation | Implemented. Real group subscription and partition pause/resume remain future work. |
| 05 Admin + groups | configs, ACLs, quotas, offsets, reassignment, log dirs, features, SCRAM | Implemented through safe FFI and native wire APIs. Upstream bindings remain desirable. |
| 06 Serialization | Registry, Avro/JSON Schema/Protobuf codecs | Implemented and tested against real registry fixtures. External schema references remain future work. |
| 07 Connect, share groups, observability | Connect REST, share administration, Streams inspection | Implemented. No share consumer or Streams runtime. Metrics collection remains future work. |
| 08 Emacs workflows | transient menus, bookmarks, record diff/search, saved queries | Implemented with ERT coverage |
| 09 Resilience | security matrix, leader loss, restart, outage, unresponsive broker, load probes | Implemented local tests. Asymmetric network partition, certificate rotation and hosted-service matrix remain open. |
| 10 Release | platform packaging, MELPA, signed binaries, release automation | Not implemented |

## Next concrete steps

1. Publish to a user-selected Git remote and run the actual GitHub Actions
   hosted matrix. Local CI linting/container runs do not substitute for that.
2. Move safe native admin bindings upstream to rust-rdkafka and remove the
   temporary FFI as upstream releases become available.
3. Resolve referenced Avro/Protobuf schemas and exercise managed registry and
   secured Connect endpoints.
4. Add OAuth token refresh, Kerberos integration and certificate rotation tests.
5. Add a real network fault proxy test, sustained load testing, metrics, and
   performance regression baselines.
6. Package binaries per OS/architecture, finalize dependency/license review,
   and prepare a MELPA recipe and release checklist.
