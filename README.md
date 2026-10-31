# kafka.el

A native Emacs environment for Apache Kafka, powered by a reusable Rust
backend.

* `lisp/`: the Emacs UI (`kafka.el`)
* `crates/kafka-bridge`: async JSON-RPC process Emacs talks to over stdio
* `crates/kafka-core`: reusable Rust library over rust-rdkafka
* `crates/kafka-protocol`: typed RPC messages

Status: **0.1, pre-release.** Connection, metadata exploration, topic admin,
record browsing/live tail, producing, and consumer group lag work and are
tested against a real Kafka 4.4 KRaft broker. See `docs/PARITY.md` for
exactly what is and isn't supported.

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

`M-x kafka`

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
```

Docs: [ARCHITECTURE](docs/ARCHITECTURE.md) · [RPC](docs/RPC.md) ·
[PARITY](docs/PARITY.md) · [ROADMAP](docs/ROADMAP.md) · [SECURITY](SECURITY.md)

## License

GPL-3.0-or-later.
