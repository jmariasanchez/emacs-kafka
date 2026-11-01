# kafka.el

A native Emacs environment for Apache Kafka, powered by a reusable Rust
backend.

* `lisp/`: the Emacs UI (`kafka.el`)
* `crates/kafka-bridge`: async JSON-RPC process Emacs talks to over stdio
* `crates/kafka-core`: reusable Rust library over rust-rdkafka
* `crates/kafka-rpc-protocol`: typed RPC messages
* `crates/kafka-wire`: version-negotiated Kafka admin protocol client
* `crates/kafka-schema-registry`: Registry REST API and schema codecs
* `crates/kafka-connect-rest`: Connect REST API

Status: **0.1, pre-release.** Includes ACLs, SCRAM users, incremental configs,
quota and replica administration, safe consumer/share-group offset changes,
transactional/batch producing, Schema Registry codecs, Kafka Connect, and
transient menus. Native admin calls supplement rust-rdkafka without requiring
a fork. See `docs/PARITY.md` for precise scope and remaining limits.

## Install

Requirements: Emacs 29.1+, Rust 1.85+, a C toolchain and `cmake`/`make`
(librdkafka and OpenSSL are built from source and statically linked).

```sh
cargo build --release -p kafka-bridge
```

```elisp
(add-to-list 'load-path "/path/to/kafka.el/lisp")
(require 'kafka)
(setq kafka-bridge-program "/path/to/kafka.el/target/release/kafka-bridge")
(setq kafka-clusters
      '(("local" :bootstrap "127.0.0.1:9092")
        ("prod"  :bootstrap "b1.example:9093,b2.example:9093"
                 :environment "production" :read-only t
                 :security-protocol "SASL_SSL" :sasl-mechanism "SCRAM-SHA-512"
                 :sasl-username "me")))   ; password via auth-source
```

`M-x kafka`. Press `?` in Kafka buffers for context-sensitive transient menus.
Optional adapters use profile keys `:schema-registry (:url "http://localhost:18081")`
and `:connect (:url "http://localhost:18083")`. Open them with
`M-x kafka-schema-registry` and `M-x kafka-connect`. See [RPC](docs/RPC.md).

## Keys

| Buffer | Keys |
|---|---|
| Clusters | `RET` topics, `G` groups, `b` brokers, `c` connect, `d` disconnect |
| Topics | `RET` describe, `r` records, `p` produce, `C` configs, `+` create, `D` delete, `/` filter, `i` internal |
| Topic | `RET` records of partition, `r` all records, `p` produce, `C` configs, `+` add partitions |
| Config | `e` edit (topics), `d` hide defaults |
| Records | `RET` show, `l` live tail, `k` stop, `s` seek (earliest/latest/offset/time/tail), `/` filter, `w` copy value, `P` re-produce, `g` refetch |
| Producer | `C-c C-c` send, `C-c C-s` send and keep, `C-c C-k` discard |
| Groups | `RET` lag detail, `D` delete. In detail, `RET` browses from the committed offset. |

Browsing records **never commits offsets** and never joins your application's
consumer groups.

## Development

```sh
scripts/kafka-dev.sh start     # local single-node KRaft broker on :19092 (needs Java 17+)
scripts/test-all.sh            # cargo tests + e2e + ERT against that broker
scripts/linux-check.sh         # isolated Linux build, Emacs 30.1 and live tests
```

See [TESTING](docs/TESTING.md) for opt-in TLS/SASL, multi-broker, Registry,
and Connect fixtures. `.cargo/config.toml` linker settings are macOS-only.

Docs: [VISION](docs/VISION.md) · [SCOPE](docs/SCOPE.md) · [ARCHITECTURE](docs/ARCHITECTURE.md) · [RPC](docs/RPC.md) ·
[PARITY](docs/PARITY.md) · [ROADMAP](docs/ROADMAP.md) · [COMPATIBILITY](docs/COMPATIBILITY.md) · [TESTING](docs/TESTING.md) · [SECURITY](SECURITY.md)

## License

GPL-3.0-or-later.
