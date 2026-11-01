//! Thin, safe wrappers over librdkafka admin APIs that rust-rdkafka 0.39
//! does not bind (ACLs, IncrementalAlterConfigs, ListConsumerGroupOffsets,
//! AlterConsumerGroupOffsets, DescribeCluster, user SCRAM). Remove each
//! function once upstream rust-rdkafka exposes it. Owned by the admin-ffi lane.
