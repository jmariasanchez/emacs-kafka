# Phase 06: Schema Registry and serialization

## Delivered

`kafka-schema-registry` is a standalone async Confluent REST adapter. It uses
reqwest 0.13 with `json`, `query`, and `native-tls-vendored`, apache-avro 0.21
without default features, protox 0.9, prost-reflect 0.16, prost 0.14, and
jsonschema 0.33 without network/file retrieval features. The registry crate's
`cargo tree` contains neither cmake nor aws-lc. The existing Kafka core still
has its own native librdkafka build requirements.

Implemented subjects, versions, schema lookup, registration, compatibility
checks, global/per-subject compatibility config, and soft/permanent deletion.
All destructive client calls require an exact subject confirmation. The bridge
checks the cluster write guard before registry I/O for registration, config
changes, deletion, and production. Wrong confirmation is error 1003, codec
failures are 1006, configuration failures are -32602, and HTTP/transport
failures are 1000. HTTP failure messages omit server bodies and credential URLs.

Confluent framing is magic byte zero plus a big-endian four-byte schema id.
Avro, JSON Schema, and Protobuf encode/decode to/from JSON are implemented.
Protobuf decoding understands zigzag-varint message-index arrays, including
the special single zero encoding and nested messages. Compilation is in-memory
with protox, not protoc or a temporary filesystem. JSON Schema validation is
opt-in with `validateJsonSchema: true`, and never retrieves external resources.
Schemas are cached by id. Repeating identical configure calls preserves that
cache. Replacing or reconnecting a Kafka connection does not inherit its old
registry configuration. A new configure call is required.

Basic and bearer authorization, custom CA PEMs, and PEM client certificate plus
PKCS#8 private key are supported. Config/client Debug output is redacted. Basic
authentication and mutual TLS were tested against real registry servers.
Bearer header construction and redaction have a unit test, not a live OAuth
issuer integration.

## RPC.md rows

All methods take `connectionId`. Additional fields appear below.

| Method | Params | Result |
|---|---|---|
| `kafka/registry/configure` | `connectionId, url, basicAuthUser?, basicAuthPassword?, bearerToken?, sslCaLocation?, sslCertificateLocation?, sslKeyLocation?, validateJsonSchema?` | `{configured: true}`. Local configuration only, does not probe the registry. |
| `kafka/registry/subjects` | `connectionId` | Subject string array |
| `kafka/registry/versions` | `connectionId, subject` | Version number array |
| `kafka/registry/schema` | `connectionId, schemaId` or `connectionId, subject, version?` | `{id, schema, schemaType, subject?, version?, references[]}`. Version defaults to `latest`. |
| `kafka/registry/register` | `connectionId, subject, schema, schemaType?, references?[]` | `{id}`. Write guard. Schema is a string, schemaType defaults to `AVRO`. |
| `kafka/registry/compatibility` | `connectionId, subject, version?, schema, schemaType?, references?[]` | `{is_compatible: bool}` |
| `kafka/registry/config` | `connectionId, subject?, compatibility?` | Omit compatibility to GET. Supply a level to PUT, with write guard. Raw Confluent response. |
| `kafka/registry/delete` | `connectionId, subject, version?, permanent?, confirmName` | Deleted versions array or deleted version. Write guard. confirmName must equal subject for both soft and hard delete. |
| `kafka/registry/decode` | `connectionId, bytes` (base64) | `{schemaId, subject?, format, json}`. Format is AVRO, JSON, or PROTOBUF. Subject can be null for id lookup. |
| `kafka/registry/produce` | `connectionId, topic, subject?, value, key?, keySubject?, partition?, timestamp?, headers?[]` | `{topic, partition, offset}`. Write guard. Latest schema, default subjects `<topic>-value` and `<topic>-key`. Headers have the normal producer shape. |

A supplied JSON null value is schema-encoded, not a Kafka tombstone. Omit key
for a Kafka null key. A supplied key, including JSON null, is schema-encoded.
Use normal `kafka/produce` to produce tombstones.

## Lisp

```elisp
(setq kafka-clusters
      '(("local" :bootstrap "127.0.0.1:19092"
         :schema-registry (:url "http://127.0.0.1:18081"
                           :validate-json-schema t))))
```

Registry profile options mirror configure with kebab-case keys. For basic auth,
`:basic-auth-user` is in the profile, and the password comes from auth-source
using the registry URL's hostname and that user. The profile has no cached
password field. Bearer tokens can be supplied with `:bearer-token`.

`M-x kafka-schema-registry` takes the connection name. RET opens a subject's
versions, then its schema. Avro/JSON schemas use pretty `js-json-mode` buffers,
Protobuf uses fundamental mode. D performs exact-name-confirmed deletion.
A prefix argument requests permanent deletion, which requires prior soft
deletion according to server semantics. `kafka-schema-registry-register` and
`kafka-schema-registry-check` operate on the current buffer and prompt for
connection, subject, and schema type. Registration uses `kafka-confirm-write`.

The only change in `lisp/kafka-records.el` adds the customizable
`kafka-records-decoders` list and calls those functions in `kafka-records-show`.
Its default function detects magic-byte payloads when the profile has a registry,
then appends decoded JSON and the schema id. Raw record data remains visible.
Decode errors are shown inline rather than losing the raw view.

## Real fixtures

```sh
scripts/schema-registry-dev.sh start
scripts/schema-registry-dev.sh status
scripts/schema-registry-dev.sh env
KAFKA_EL_REGISTRY_AUTH=1 scripts/schema-registry-dev.sh start
KAFKA_EL_REGISTRY_AUTH=1 scripts/schema-registry-dev.sh env
KAFKA_EL_REGISTRY_TLS=1 scripts/schema-registry-dev.sh start
KAFKA_EL_REGISTRY_TLS=1 scripts/schema-registry-dev.sh env
```

Karapace 6.2.0 is installed from its upstream Git tag in
`~/.cache/kafka-el/karapace-venv`. The script caches future downloads under
`~/.cache/kafka-el/uv`. It uses `uv` Python 3.12 and the host Go compiler for
Karapace's own Protobuf implementation. On macOS it selects `/usr/bin/clang`
and the Xcode SDK to avoid the Nix clang missing-libresolv linker failure.
Karapace is not distributed on PyPI under its project name. Karapace 6.2 uses
`KARAPACE_*` environment settings, not the old positional JSON config argument.

| Fixture | Address | Auth | Cache directory |
|---|---|---|---|
| Plain | `http://127.0.0.1:18081` | none | `~/.cache/kafka-el/registry-18081` |
| Basic | `http://127.0.0.1:18082` | Public test-only `registry-user` / `registry-test-password` | `~/.cache/kafka-el/registry-18082` |
| Mutual TLS | `https://127.0.0.1:18084` | Generated CA and client certificate | `~/.cache/kafka-el/registry-18084` |

Each uses the shared plain broker at `127.0.0.1:19092`, its own schema topic,
and its own group id. Readiness waits for master election, not merely HTTP
acceptance. TLS fixtures generate seven-day test certificates and renew expired
ones on start. Port 18083 belongs to the Connect lane and is not used here.
All three fixtures are left running. Stop them with:

```sh
scripts/schema-registry-dev.sh stop
KAFKA_EL_REGISTRY_AUTH=1 scripts/schema-registry-dev.sh stop
KAFKA_EL_REGISTRY_TLS=1 scripts/schema-registry-dev.sh stop
```

## PARITY.md and capability changes for coordinator

| Operation | State | Status | Test |
|---|---|---|---|
| Schema Registry REST administration | adapter | Implemented, including guarded deletion and config writes | `registry_roundtrip_and_guards` |
| Avro/JSON Schema/Protobuf codecs | adapter | Implemented for self-contained schemas, optional offline JSON validation | `registry_json_and_protobuf_live`, codec units |
| Registry basic auth and mutual TLS | adapter | Implemented and live-tested | `registry_basic_auth_live`, `registry_mutual_tls_live` |
| Registry Emacs browser and record detail decoding | wrapper | Implemented | `kafka-registry-live-browser-and-record-detail` |

Capability row: id `schemaRegistry`, state `adapter`, implemented `true`, note
`Confluent REST, Avro/JSON/Protobuf, basic/bearer auth and mTLS. External schema references remain unsupported.`

## TESTING.md requirement-to-test rows

| Requirement | Test |
|---|---|
| Confluent magic byte and big-endian id validation | `wire_validation` |
| Avro JSON roundtrip | `avro_roundtrip`, `registry_roundtrip_and_guards` |
| JSON Schema roundtrip and optional validation | `json_roundtrip`, `json_validation_is_offline`, `registry_json_and_protobuf_live` |
| Protobuf dynamic descriptors and nested indexes | `protobuf_roundtrip`, `protobuf_nested_and_invalid_indexes`, `registry_json_and_protobuf_live` |
| Schema id cache and confirmation before network | `cache_and_confirmation_before_network` |
| Credential redaction and header construction | `auth_and_redaction`, `bearer_auth_and_config_validation` |
| HTTP basic auth allow/deny | `registry_basic_auth_live`, curl oracle |
| CA trust and required client certificate | `registry_mutual_tls_live` |
| Register, produce encoded key/value with headers, fetch, decode | `registry_roundtrip_and_guards` |
| Incompatible Avro change, config get/set, subjects/versions/id lookup, soft/hard delete | `registry_roundtrip_and_guards` |
| Version soft/hard deletion | `registry_json_and_protobuf_live` |
| Schema-encoded null versus Kafka tombstone | `registry_null_value_live` |
| Registry settings do not survive connection replacement | `registry_roundtrip_and_guards` |
| Read-only and exact confirmation before mutation | `registry_roundtrip_and_guards`, `kafka-registry-read-only-register-before-network` |
| auth-source uses registry URL hostname | `kafka-registry-auth-source-host` |
| Non-wire record detail performs no registry request | `kafka-registry-non-wire-detail-no-network` |
| Real subjects/versions/schema buffers, register/check/delete, records-show decoding | `kafka-registry-live-browser-and-record-detail` |

Live Rust tests skip without `KAFKA_EL_TEST_BOOTSTRAP` and their corresponding
`KAFKA_EL_TEST_REGISTRY`, `KAFKA_EL_TEST_REGISTRY_AUTH`, or
`KAFKA_EL_TEST_REGISTRY_TLS` environment variable. TLS also requires
`KAFKA_EL_TEST_REGISTRY_CERTS`. ERT live tests require the plain registry variable.
The Rust Avro workflow calls curl directly for an independent REST oracle.

## Limitations

* External registry schema references are preserved by REST operations but
  rejected by codecs. Self-contained schemas and Protobuf well-known Google
  types are supported. No arbitrary file imports or HTTP schema retrieval.
* Protobuf encoding selects the first top-level message. Decoding supports
  indexed and nested messages. An encoding-time message selector is future work.
* Bearer auth is static-token transport, not token acquisition or refresh.
* TLS keys must be unencrypted PEM PKCS#8. No key-password or PKCS#12 option.
* Registry requests have a fixed 30-second timeout. Codec schema compilation is
  not separately cached. The schema-id cache is per configured client.
* Permanent deletion is intentionally separate from soft deletion, never an
  automatic two-step destructive operation. Deleted subjects are not shown by
  the current browser, so use the RPC to hard-delete an already hidden subject.
* Cargo.lock is generated locally by these new dependencies but is not committed
  in this lane. The coordinator should regenerate the shared lockfile after
  integrating all dependency changes. No root manifests or shared docs changed.
