# VISION.md

## Mission

A developer can connect to a Kafka cluster, explore topics, inspect and
publish records, debug consumer groups, investigate lag, inspect security
settings, and perform authorized administrative actions without leaving Emacs.

## Audience

* Application developers who produce to and consume from Kafka daily.
* Platform/SRE engineers debugging lag, partitions, configs and groups.
* Emacs users who prefer keyboard-driven, buffer-based tools to web consoles.

## Product shape

Of the three things people call a "complete Kafka client", this project is a
**Kafka developer environment** (uses existing clients through a rich
interface), with ecosystem integrations (Schema Registry, Connect, Streams
observability) where useful. It is powered by `kafka-core`, a Rust library
that is reusable outside Emacs.

## Success criteria

Taken from section 14 of the master plan. Progress is tracked in
`docs/PARITY.md` and `docs/ROADMAP.md`.

* Targeted producer, consumer, group and admin operations are implemented,
  tested against real brokers, and documented.
* Modern features (share groups) are supported where dependency and broker
  versions allow, and reported accurately where they do not.
* Every major workflow has an Emacs interface.
* Binary, text and schema-encoded payloads can be inspected and published safely.
* Auth, authorization, network failures and reconnects behave predictably.
* The Rust libraries are independently reusable.
* A new user can install without modifying source.

## Non-goals

* Implementing the Kafka wire protocol from scratch.
* A replacement for the Java Kafka Streams runtime.
* ZooKeeper-mode clusters. KRaft is the target.
* A graphical widget framework inside Emacs.
* Being a production consumer or a long-running data pipeline.
