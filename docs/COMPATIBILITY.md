# Compatibility

Validated locally on November 1, 2026. Local Linux runs are not hosted CI runs.

| Component | Supported / intended | Evidence |
|---|---|---|
| Kafka | KRaft, Kafka 3.x/4.x with API version negotiation | Kafka 4.4.0 single broker, secured broker and three-broker clusters exercised. Kafka 3.9.1 configured in CI, not run locally. |
| Emacs | 29.1+ | Integrated suites on macOS 30.2 and 31.1, Linux 30.1 and source snapshot 32.0.50. Original baseline also tested on 29.4, new extensions not locally retested there. |
| OS | macOS arm64/x86_64 and Linux | macOS arm64 and Debian trixie Linux arm64 in a local VM. x86_64 configured in CI, not locally exercised. |
| Rust | stable, 2021 edition, declared minimum 1.89 | 1.97.1 on macOS and 1.99.1 on Linux. Minimum follows locked dependencies, not a separate MSRV test. |
| librdkafka | 2.12.1, statically bundled through rust-rdkafka 0.39 | Local native calls and safe FFI integration tests |
| Schema Registry | Confluent-compatible REST | Karapace 6.2.0, plain HTTP, Basic auth and mTLS. Avro, JSON Schema and Protobuf codec round trips. |
| Kafka Connect | distributed Connect REST | Kafka 4.4.0 FileStream source connector, plugin validation, lifecycle, offsets, tasks and Streams demo |
| Windows / ZooKeeper mode | not targeted | Not built or tested |

## Authentication

| Mechanism | Kafka core | Native wire admin client | Live verification |
|---|---|---|---|
| PLAINTEXT | yes | yes | both |
| SSL / mTLS | yes | yes | both |
| SASL PLAIN | yes | yes | both |
| SCRAM-SHA-256/512 | yes | yes | both |
| SASL_SSL | yes | yes | both |
| OAUTHBEARER | static librdkafka properties only | unsupported, explicit error | no successful issuer integration |
| GSSAPI / Kerberos | requires optional build feature | unsupported, explicit error | no |

The wire client requires explicit security fields. Security-related entries in
`properties` are rejected before network I/O rather than silently disagreeing
with librdkafka's effective security configuration. TLS certificate and hostname
verification remain enabled. Never use the loopback fixtures' credentials or
private keys for real deployments.

Registry Basic auth and mTLS have real-server coverage. Bearer authorization is
unit-tested only. Secured Connect endpoints are implemented but not live-tested.

## Exact snapshot

The Linux snapshot was built from Emacs commit
`9008bf64c3375f63620d9d6f6dc31908b68bf0a0`, reporting `GNU Emacs 32.0.50`.
It was built without X, native compilation, tree-sitter, mailutils or sound.
The local installation is cached under `~/.cache/kafka-el/emacs-snapshot/install`.
`scripts/linux-check.sh snapshot` mounts that installation at its build prefix,
prints the source commit, byte-compiles Lisp strictly and runs the live suite.

## Managed services and protocol limits

Confluent Cloud, MSK, Aiven and Redpanda deployments are not verified. MSK IAM
requires a token provider that is not implemented. API negotiation returns an
unsupported error for unavailable broker operations. Share administration needs
Kafka 4.x support and an initialized share-state topic, exercised on the
three-node fixture. There is no share consumer runtime.

GitHub Actions has a macOS/Linux, Emacs 29.4/30.1/snapshot and Kafka 3.9.1/4.4.0
matrix plus secure/resilience and ecosystem jobs. The workflow passes local
`actionlint`, but no job has run on GitHub because this repository has no remote.
