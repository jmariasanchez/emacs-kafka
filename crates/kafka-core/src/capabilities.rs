//! Machine-readable capability classification (mirrors docs/PARITY.md).

use serde::Serialize;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum State {
    /// Direct support in a vetted dependency.
    Existing,
    /// Composed from existing dependency APIs.
    Wrapper,
    /// Separate supported API or library (e.g. HTTP).
    Adapter,
    /// Needs implementation here or upstream.
    Gap,
    /// Not available for the selected Kafka/client version.
    Unsupported,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Capability {
    pub id: &'static str,
    pub state: State,
    pub implemented: bool,
    pub note: &'static str,
}

const fn cap(id: &'static str, state: State, implemented: bool, note: &'static str) -> Capability {
    Capability {
        id,
        state,
        implemented,
        note,
    }
}

use State::*;

pub const CAPABILITIES: &[Capability] = &[
    cap("cluster.metadata", Existing, true, "Client::fetch_metadata"),
    cap("cluster.multiConnection", Wrapper, true, "bridge connection registry"),
    cap("cluster.readOnly", Wrapper, true, "enforced in kafka-core before I/O"),
    cap("cluster.saslTls", Existing, true, "PLAIN, SCRAM-256/512, SASL_SSL and mTLS live tested; OAuth unverified, GSSAPI needs feature"),
    cap("broker.configs", Existing, true, "AdminClient::describe_configs"),
    cap("topic.list", Existing, true, "metadata"),
    cap("topic.describe", Wrapper, true, "metadata + watermarks"),
    cap("topic.create", Existing, true, "AdminClient::create_topics"),
    cap("topic.delete", Existing, true, "exact-name confirmation"),
    cap("topic.configs", Existing, true, "describe_configs"),
    cap("topic.alterConfig", Wrapper, true, "IncrementalAlterConfigs; unrelated overrides preserved atomically"),
    cap("topic.incrementalAlterConfig", Wrapper, true, "RAII FFI: set/delete/append/subtract, topic and broker resources"),
    cap("topic.addPartitions", Existing, true, "create_partitions"),
    cap("topic.deleteRecords", Existing, true, "AdminClient::delete_records, exact topic confirmation"),
    cap("partition.reassignment", Wrapper, true, "negotiated Kafka wire client, preview/submit/cancel"),
    cap("offsets.forTime", Existing, true, "offsets_for_times"),
    cap("logDirs.describe", Wrapper, true, "native wire DescribeLogDirs per broker"),
    cap("produce.basic", Existing, true, "FutureProducer, idempotent"),
    cap("produce.transactions", Existing, true, "dedicated transactional producer sessions, idle expiry and lifecycle abort"),
    cap("produce.batch", Wrapper, true, "bounded per-record outcomes, optional atomic transaction, region/file commands"),
    cap("consume.fetch", Wrapper, true, "manual assign, no commit"),
    cap("consume.stream", Wrapper, true, "credit-based flow control"),
    cap("consume.isolation", Existing, true, "read_committed/read_uncommitted for bounded fetch and live streams"),
    cap("groups.list", Existing, true, "fetch_group_list"),
    cap("groups.describe", Wrapper, true, "group list + efficient ListConsumerGroupOffsets FFI, no group join"),
    cap("groups.offsets", Wrapper, true, "RAII FFI ListConsumerGroupOffsets without scanning all topic partitions"),
    cap("groups.delete", Existing, true, "delete_groups"),
    cap("groups.resetOffsets", Wrapper, true, "AlterConsumerGroupOffsets FFI, Empty-group checks, preview and exact confirmation"),
    cap("groups.share", Wrapper, true, "wire share list/describe/offset reset/delete, Kafka 4.x API negotiation; no share consumer runtime"),
    cap("acls", Wrapper, true, "RAII FFI Describe/Create/DeleteAcls, exact-name deletion guard"),
    cap("quotas", Wrapper, true, "native wire Describe/AlterClientQuotas, validation and guarded removals"),
    cap("scram.users", Wrapper, true, "RAII FFI Describe/AlterUserScramCredentials; passwords never returned"),
    cap("features.describe", Wrapper, true, "negotiated ApiVersions finalized feature levels"),
    cap("cluster.info", Wrapper, true, "RAII FFI DescribeCluster: id, controller, nodes and authorized operations"),
    cap("leaders.elect", Wrapper, true, "native wire preferred leader election"),
    cap("schemaRegistry", Adapter, true, "REST subjects/schemas/config, Avro/JSON/Protobuf codecs; external references not yet decoded"),
    cap("connect", Adapter, true, "Connect REST lifecycle, validation, plugins, tasks, topics and offsets"),
    cap("streams.observability", Wrapper, true, "group/topic heuristic, lag and members; no Streams runtime or metrics"),
    cap("ui.workflows", Wrapper, true, "transient menus, bookmarks, saved queries, bounded search and record diff"),
    cap("auth.oauthRefresh", Gap, false, "static librdkafka properties only; no token refresh provider or live issuer verification"),
    cap("auth.kerberos", Gap, false, "GSSAPI feature and test realm not configured"),
    cap("delegationTokens", Gap, false, "delegation token administration not implemented"),
    cap("consume.share", Gap, false, "share administration exists, but no share consumer or acknowledgement runtime"),
    cap("consume.groupSubscription", Gap, false, "inspection is manual assignment only"),
    cap("produce.transactionalOffsets", Gap, false, "producing transactions do not commit consumer offsets"),
    cap("schemaRegistry.references", Gap, false, "reference metadata retained, external codec dependency resolution not implemented"),
    cap("metrics", Gap, false, "no metrics collection or statistics dashboards"),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_unique() {
        let mut ids: Vec<_> = CAPABILITIES.iter().map(|c| c.id).collect();
        ids.sort();
        let n = ids.len();
        ids.dedup();
        assert_eq!(n, ids.len());
    }

    #[test]
    fn gaps_not_marked_implemented() {
        assert!(CAPABILITIES
            .iter()
            .filter(|c| c.state == State::Gap)
            .all(|c| !c.implemented));
    }
}
