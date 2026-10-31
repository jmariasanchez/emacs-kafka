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
    Capability { id, state, implemented, note }
}

use State::*;

pub const CAPABILITIES: &[Capability] = &[
    cap("cluster.metadata", Existing, true, "Client::fetch_metadata"),
    cap("cluster.multiConnection", Wrapper, true, "bridge connection registry"),
    cap("cluster.readOnly", Wrapper, true, "enforced in kafka-core before I/O"),
    cap("cluster.saslTls", Existing, true, "librdkafka properties; GSSAPI needs sasl feature"),
    cap("broker.configs", Existing, true, "AdminClient::describe_configs"),
    cap("topic.list", Existing, true, "metadata"),
    cap("topic.describe", Wrapper, true, "metadata + watermarks"),
    cap("topic.create", Existing, true, "AdminClient::create_topics"),
    cap("topic.delete", Existing, true, "exact-name confirmation"),
    cap("topic.configs", Existing, true, "describe_configs"),
    cap("topic.alterConfig", Wrapper, true, "non-incremental AlterConfigs; merged with current dynamic overrides"),
    cap("topic.incrementalAlterConfig", Gap, false, "not exposed by rdkafka 0.39"),
    cap("topic.addPartitions", Existing, true, "create_partitions"),
    cap("topic.deleteRecords", Existing, false, "AdminClient::delete_records"),
    cap("partition.reassignment", Gap, false, "AlterPartitionReassignments not exposed"),
    cap("offsets.forTime", Existing, true, "offsets_for_times"),
    cap("logDirs.describe", Gap, false, "DescribeLogDirs not exposed"),
    cap("produce.basic", Existing, true, "FutureProducer, idempotent"),
    cap("produce.transactions", Existing, false, "transactional producer API present"),
    cap("consume.fetch", Wrapper, true, "manual assign, no commit"),
    cap("consume.stream", Wrapper, true, "credit-based flow control"),
    cap("groups.list", Existing, true, "fetch_group_list"),
    cap("groups.describe", Wrapper, true, "group list + OffsetFetch via unsubscribed consumer"),
    cap("groups.delete", Existing, true, "delete_groups"),
    cap("groups.resetOffsets", Gap, false, "AlterConsumerGroupOffsets not exposed; commit via assigned consumer is a candidate wrapper"),
    cap("groups.share", Gap, false, "KIP-932 share groups not exposed by rdkafka 0.39"),
    cap("acls", Gap, false, "Create/Describe/DeleteAcls not exposed by rdkafka 0.39"),
    cap("quotas", Gap, false, "client quota APIs not exposed"),
    cap("scram.users", Gap, false, "AlterUserScramCredentials not exposed"),
    cap("features.describe", Gap, false, "DescribeFeatures not exposed"),
    cap("schemaRegistry", Adapter, false, "HTTP REST"),
    cap("connect", Adapter, false, "Kafka Connect REST"),
    cap("streams.observability", Adapter, false, "via group + metrics only; no Streams runtime"),
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
        assert!(CAPABILITIES.iter().filter(|c| c.state == State::Gap).all(|c| !c.implemented));
    }
}
