## Summary

Describe the change and link any related issue.

## Validation

List the commands run and their results. Note any suites not run.

- [ ] `scripts/test-all.sh`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] Elisp byte-compiles with `byte-compile-error-on-warn`
- [ ] New operations include capability, parity documentation, and real-broker tests
- [ ] Mutations enforce read-only profiles and document confirmation / partial failures
- [ ] No credentials, tokens, or production record contents in code, logs, or fixtures

See [CONTRIBUTING.md](../CONTRIBUTING.md).
