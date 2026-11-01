# Integrated validation, November 1, 2026

## Final results

| Check | Result | Scope |
|---|---|---|
| `scripts/test-all.sh`, all service variables enabled | **84 Rust tests passed**, 0 failures, 3 ignored load probes. **46/46 ERT passed**, no skips. | macOS arm64, Rust 1.97.1, Emacs 31.1, Kafka 4.4.0, real Registry and Connect |
| All ERT suites and strict byte compilation | **46/46 passed** on each version, no skips or compile warnings | macOS Emacs 30.2 and 31.1 |
| `scripts/linux-check.sh` | Passed build, warning-free clippy, strict byte compilation, Rust suites and live-broker ERT | Debian trixie Linux arm64, Rust 1.99.1, Java 21.0.13, Emacs 30.1 |
| `scripts/linux-check.sh snapshot` | Passed the same checks | Emacs 32.0.50, source `9008bf64c3375f63620d9d6f6dc31908b68bf0a0` |
| Linux ERT on each version | **40 passed, 6 skipped, 0 failures** | Only the plain Kafka fixture runs inside these VMs. Secure admin, two Connect tests, Registry UI and two SASL tests are intentionally skipped there and exercised on macOS. |
| `cargo clippy --workspace --all-targets -- -D warnings` | Passed | Combined workspace, including new lifecycle tests |
| `actionlint .github/workflows/ci.yml` | Passed | Static workflow validation, not a hosted run |
| `bash -n scripts/*.sh`, `git diff --check` | Passed | Shell syntax and whitespace |

The optional Rust tests return early when their service variables are absent.
Therefore a green default/Linux Rust run is not evidence that its secure, REST,
share-group or destructive fault paths executed. The all-enabled macOS run did
execute those paths, including the separate PLAIN-only and fault-test brokers.

## Real-service acceptance paths

* Native admin: ACL create/describe/delete, SCRAM changes, incremental config
  preservation, efficient group offsets, reset preview/apply and guards, record
  deletion. Official Kafka CLI output is used as an independent oracle.
* Wire admin: version negotiation, quota changes, replica submission and
  cancellation, preferred election, log directories, feature levels and share
  group administration. PLAIN, both SCRAM mechanisms, TLS and mTLS are exercised.
* Security: wrong credentials, unsupported broker mechanism, expired client
  certificate, incorrect CA and hostname, ACL denial, and stdout/stderr secret
  capture with native debug enabled.
* Resilience: isolated three-node leader kill/pause/restart, full outage,
  concurrent healthy connection, recovery, and acknowledged-record integrity.
* Registry: real Karapace 6.2.0 with Basic auth and mTLS, REST lifecycle and
  Avro/JSON Schema/Protobuf round trips, including nested Protobuf indexes.
* Connect: distributed FileStream connector lifecycle, validation, tasks, topics
  and offsets, plus real WordCount Streams observability.
* Transactions: nine integration tests cover commit/abort isolation, atomic and
  partial batches, idle expiry, Drop cleanup, disconnect, replacement with a
  read-only profile, multi-connection shutdown and stdin EOF. Later committed
  sentinel records prove aborted transactions release the last stable offset,
  rather than merely hiding uncommitted records.
* Adapter lifetime: a dedicated bridge test proves disconnect and replacement
  require Registry and Connect configuration anew.

## Separately executed load probes

Command, using the dedicated cluster rather than a user cluster:

```sh
KAFKA_EL_TEST_LOAD=1 KAFKA_EL_TEST_LOAD_BOOTSTRAP=127.0.0.1:49092 \
  cargo test -p kafka-bridge --test load -- --ignored --nocapture --test-threads=1
```

All three probes passed against the integrated binary:

| Probe | Observed local result |
|---|---|
| 1 MiB produce and fetch, byte-for-byte integrity | 1.1973 s |
| 5,000-topic creation setup | 3.5819 s, excluding convergence |
| Filtered 5,000-topic metadata response | 70.089 ms, exactly 5,000 entries |
| 2,000 empty-connection-list stdio RPCs | p50 74 us, p95 165 us, p99 215 us |

These are one-machine observations, not throughput promises or regression
baselines. The probes delete their uniquely named topics afterwards.

## Problems found and fixed during integration

1. Native debug/statistics events could starve actionable authentication
   diagnostics. Verification now disables irrelevant statistics and polls a
   bounded positive event slice without relaxing the overall deadline.
2. Transaction sends previously inherited only librdkafka's delivery timeout.
   They now also have an overall async deadline. Producer abort/destruction on
   Drop is moved off the async worker thread.
3. Native wire operations could silently disagree with security settings hidden
   in librdkafka properties. They now reject such overrides before network I/O,
   and all bootstrap addresses share one connection deadline.
4. The Linux broker tried to put JVM GC logs in the read-only Kafka distribution.
   The runner now supplies a writable log directory and captures failure logs.
5. Loading all ERT files with `-l` duplicated shared test definitions. The runner
   now uses `require`, loading each test feature exactly once.
6. Reassignment cancellation became visible at the controller before broker
   metadata converged. The test now waits, within a deadline, for every
   acknowledged cancellation to restore its exact original replica assignment.

Some Emacs cleanup paths print existing jsonrpc sentinel warnings. These did
not produce failed tests or compilation warnings.

## Remaining verification limits

No Git remote was created and nothing was published. Hosted GitHub Actions has
not run. Kafka 3.9.1, x86_64, managed services, OAuth success/refresh, Kerberos,
secured Connect endpoints, certificate rotation and asymmetric network
partitions remain unverified. External schema dependency resolution, share
consumption, transactional consumer offsets and metrics remain outside the
implemented scope. See `PARITY.md`, `COMPATIBILITY.md` and `ROADMAP.md`.

Local fixtures are disposable development services. Their cached data and
certificates are retained for repeat runs, not committed to the repository.
