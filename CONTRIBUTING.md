# Contributing

* `scripts/test-all.sh` must pass (starts a local KRaft broker; needs Java 17+).
* `cargo clippy --workspace --all-targets -- -D warnings` must be clean.
* Elisp must byte-compile with `byte-compile-error-on-warn`.
* New operations: add a row to `docs/PARITY.md` and an entry in
  `kafka-core/src/capabilities.rs`, plus an e2e test against a real broker.
* Mutating operations must respect `read_only` in `kafka-core` and document
  their confirmation and partial-failure behavior in `docs/RPC.md`.
* Never log or echo credentials.
