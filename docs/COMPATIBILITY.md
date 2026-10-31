# COMPATIBILITY.md

| Component | Supported | Verified |
|---|---|---|
| Kafka brokers | KRaft clusters, Kafka 3.x and 4.x (librdkafka negotiates API versions) | 4.4.0 single-node KRaft (local) |
| ZooKeeper-mode clusters | not targeted. Basic metadata likely works, untested. | no |
| Emacs | 29.1+ with GNU ELPA `jsonrpc` ≥ 1.0.24 (Emacs 29 bundles an older jsonrpc that lacks `:events-buffer-config`; package.el installs the newer one from the `Package-Requires` header) | 31.1 locally (bundled jsonrpc 1.0.29). CI config covers 29.4 and 30.1 but has not run yet. |
| OS | macOS (arm64, x86_64), Linux | macOS arm64 |
| Windows | not yet. Stdio JSON-RPC should work; build not tested. | no |
| Rust | stable, 2021 edition (needs `Option::is_none_or`, 1.82+) | 1.97 |
| librdkafka | 2.12.1, statically bundled | yes |

## Authentication

| Mechanism | Wired | Tested |
|---|---|---|
| PLAINTEXT | yes | yes |
| SSL / mTLS | yes (vendored OpenSSL) | no |
| SASL PLAIN | yes | no |
| SASL SCRAM-SHA-256/512 | yes | no |
| SASL OAUTHBEARER | yes (static config via `:properties`) | no |
| SASL GSSAPI (Kerberos) | no. Needs the rdkafka `gssapi` feature. | no |

## Managed services

Confluent Cloud, Amazon MSK (SASL/SCRAM, TLS), Aiven and Redpanda speak the
Kafka protocol and should work with the matching security settings. MSK IAM
auth needs an OAUTHBEARER token provider, which is not implemented yet. None
of these are tested.
