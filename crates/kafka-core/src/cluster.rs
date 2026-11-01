//! A live connection to one cluster and its metadata/admin/group operations.
//!
//! librdkafka metadata, watermark, and group calls are blocking, so every such
//! call runs on `tokio::task::spawn_blocking`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigSource, NewPartitions, NewTopic,
    ResourceSpecifier, TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::FutureProducer;
use rdkafka::{Offset, TopicPartitionList};
use serde::Serialize;

use crate::connection::{is_secret_key, ConnectionConfig};
use crate::errors::{check_all, CoreError, Result, ResourceResult};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterInfo {
    pub connection_id: String,
    pub bootstrap_servers: String,
    pub read_only: bool,
    pub environment: Option<String>,
    pub orig_broker_id: i32,
    pub orig_broker_name: String,
    pub brokers: Vec<BrokerInfo>,
    pub topic_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicSummary {
    pub name: String,
    pub partitions: usize,
    pub replication_factor: usize,
    pub internal: bool,
    pub under_replicated: usize,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionInfo {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub low: i64,
    pub high: i64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicDetail {
    pub name: String,
    pub partitions: Vec<PartitionInfo>,
    pub total_messages: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigEntryInfo {
    pub name: String,
    pub value: Option<String>,
    pub source: String,
    pub read_only: bool,
    pub default: bool,
    pub sensitive: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupSummary {
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    pub protocol: String,
    pub members: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberInfo {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    pub committed: Option<i64>,
    pub high: i64,
    pub lag: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDetail {
    #[serde(flatten)]
    pub summary: GroupSummary,
    pub members_detail: Vec<MemberInfo>,
    pub offsets: Vec<GroupOffset>,
    pub total_lag: i64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionOffset {
    pub partition: i32,
    pub offset: Option<i64>,
}

/// Compute lag. Returns `None` when there is no committed offset.
pub fn compute_lag(committed: Option<i64>, high: i64) -> Option<i64> {
    committed.map(|c| (high - c).max(0))
}

fn source_name(s: &ConfigSource) -> &'static str {
    match s {
        ConfigSource::Unknown => "unknown",
        ConfigSource::DynamicTopic => "dynamic-topic",
        ConfigSource::DynamicBroker => "dynamic-broker",
        ConfigSource::DynamicDefaultBroker => "dynamic-default-broker",
        ConfigSource::StaticBroker => "static-broker",
        ConfigSource::Default => "default",
    }
}

fn offset_value(o: Offset) -> Option<i64> {
    match o {
        Offset::Offset(n) => Some(n),
        _ => None,
    }
}

pub struct Cluster {
    pub config: ConnectionConfig,
    admin: AdminClient<DefaultClientContext>,
    /// Shared metadata/watermark client. Never subscribes or commits.
    meta: Arc<BaseConsumer>,
    producer: tokio::sync::OnceCell<FutureProducer>,
}

impl Cluster {
    /// Create clients and verify reachability with a metadata request.
    pub async fn connect(config: ConnectionConfig) -> Result<(Arc<Self>, ClusterInfo)> {
        let admin = config.admin()?;
        let meta = Arc::new(config.inspection_consumer()?);
        let c = Arc::new(Self { config, admin, meta, producer: tokio::sync::OnceCell::new() });
        let info = c.describe_cluster().await?;
        Ok((c, info))
    }

    pub fn timeout(&self) -> Duration {
        self.config.timeout
    }

    /// The shared admin client (for FFI wrappers over unbound admin APIs).
    pub fn admin_client(&self) -> &AdminClient<DefaultClientContext> {
        &self.admin
    }

    pub fn admin_opts(&self, validate_only: bool) -> AdminOptions {
        AdminOptions::new()
            .request_timeout(Some(self.timeout()))
            .operation_timeout(Some(self.timeout()))
            .validate_only(validate_only)
    }

    pub fn guard_write(&self) -> Result<()> {
        if self.config.read_only {
            Err(CoreError::ReadOnly)
        } else {
            Ok(())
        }
    }

    /// Run a blocking librdkafka call on the shared metadata consumer.
    pub async fn blocking<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<BaseConsumer>, Duration) -> Result<T> + Send + 'static,
    {
        let meta = self.meta.clone();
        let t = self.timeout();
        tokio::task::spawn_blocking(move || f(meta, t))
            .await
            .map_err(|e| CoreError::Internal(e.to_string()))?
    }

    pub async fn describe_cluster(&self) -> Result<ClusterInfo> {
        let cfg = self.config.clone();
        self.blocking(move |m, t| {
            let md = m.fetch_metadata(None, t)?;
            Ok(ClusterInfo {
                connection_id: cfg.id.clone(),
                bootstrap_servers: cfg.bootstrap_servers.clone(),
                read_only: cfg.read_only,
                environment: cfg.environment.clone(),
                orig_broker_id: md.orig_broker_id(),
                orig_broker_name: md.orig_broker_name().to_string(),
                brokers: md
                    .brokers()
                    .iter()
                    .map(|b| BrokerInfo { id: b.id(), host: b.host().into(), port: b.port() })
                    .collect(),
                topic_count: md.topics().len(),
            })
        })
        .await
    }

    pub async fn list_topics(
        &self,
        include_internal: bool,
        filter: Option<String>,
    ) -> Result<Vec<TopicSummary>> {
        self.blocking(move |m, t| {
            let md = m.fetch_metadata(None, t)?;
            let needle = filter.map(|f| f.to_lowercase());
            let mut out: Vec<TopicSummary> = md
                .topics()
                .iter()
                .filter(|tp| include_internal || !tp.name().starts_with("__"))
                .filter(|tp| needle.as_ref().is_none_or(|n| tp.name().to_lowercase().contains(n)))
                .map(|tp| {
                    let parts = tp.partitions();
                    TopicSummary {
                        name: tp.name().into(),
                        partitions: parts.len(),
                        replication_factor: parts.first().map(|p| p.replicas().len()).unwrap_or(0),
                        internal: tp.name().starts_with("__"),
                        under_replicated: parts
                            .iter()
                            .filter(|p| p.isr().len() < p.replicas().len())
                            .count(),
                        error: tp.error().map(|e| format!("{e:?}")),
                    }
                })
                .collect();
            out.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(out)
        })
        .await
    }

    pub async fn describe_topic(&self, topic: String) -> Result<TopicDetail> {
        self.blocking(move |m, t| {
            let md = m.fetch_metadata(Some(&topic), t)?;
            let tp = md
                .topics()
                .iter()
                .find(|x| x.name() == topic)
                .ok_or_else(|| CoreError::NotFound(topic.clone()))?;
            if let Some(e) = tp.error() {
                return Err(CoreError::NotFound(format!("{topic}: {e:?}")));
            }
            let mut parts = Vec::new();
            let mut total = 0;
            for p in tp.partitions() {
                let (low, high) = m.fetch_watermarks(&topic, p.id(), t)?;
                total += high - low;
                parts.push(PartitionInfo {
                    id: p.id(),
                    leader: p.leader(),
                    replicas: p.replicas().to_vec(),
                    isr: p.isr().to_vec(),
                    low,
                    high,
                    error: p.error().map(|e| format!("{e:?}")),
                });
            }
            parts.sort_by_key(|p| p.id);
            Ok(TopicDetail { name: topic, partitions: parts, total_messages: total })
        })
        .await
    }

    /// Partition ids for a topic.
    pub async fn partitions(&self, topic: String) -> Result<Vec<i32>> {
        self.blocking(move |m, t| {
            let md = m.fetch_metadata(Some(&topic), t)?;
            let tp = md
                .topics()
                .iter()
                .find(|x| x.name() == topic)
                .filter(|x| x.error().is_none())
                .ok_or_else(|| CoreError::NotFound(topic.clone()))?;
            let mut ids: Vec<i32> = tp.partitions().iter().map(|p| p.id()).collect();
            ids.sort();
            Ok(ids)
        })
        .await
    }

    pub async fn watermarks(&self, topic: String, partition: i32) -> Result<(i64, i64)> {
        self.blocking(move |m, t| Ok(m.fetch_watermarks(&topic, partition, t)?)).await
    }

    async fn describe_configs(&self, spec: ResourceSpecifier<'_>) -> Result<Vec<ConfigEntryInfo>> {
        let res = self.admin.describe_configs([&spec], &self.admin_opts(false)).await?;
        let r = res
            .into_iter()
            .next()
            .ok_or_else(|| CoreError::Internal("empty describe_configs result".into()))?
            .map_err(|e| CoreError::Kafka(rdkafka::error::KafkaError::AdminOp(e)))?;
        let mut out: Vec<ConfigEntryInfo> = r
            .entries
            .iter()
            .map(|e| ConfigEntryInfo {
                name: e.name.clone(),
                value: if e.is_sensitive || is_secret_key(&e.name) { None } else { e.value.clone() },
                source: source_name(&e.source).into(),
                read_only: e.is_read_only,
                default: e.is_default,
                sensitive: e.is_sensitive,
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    pub async fn topic_configs(&self, topic: &str) -> Result<Vec<ConfigEntryInfo>> {
        self.describe_configs(ResourceSpecifier::Topic(topic)).await
    }

    pub async fn broker_configs(&self, broker: i32) -> Result<Vec<ConfigEntryInfo>> {
        self.describe_configs(ResourceSpecifier::Broker(broker)).await
    }

    pub async fn create_topic(
        &self,
        name: &str,
        partitions: i32,
        replication: i32,
        configs: &BTreeMap<String, String>,
        validate_only: bool,
    ) -> Result<Vec<ResourceResult>> {
        self.guard_write()?;
        if name.is_empty() || partitions < 1 || replication < 1 {
            return Err(CoreError::Invalid("name, partitions>=1, replication>=1 required".into()));
        }
        let mut nt = NewTopic::new(name, partitions, TopicReplication::Fixed(replication));
        for (k, v) in configs {
            nt = nt.set(k, v);
        }
        let res = self.admin.create_topics([&nt], &self.admin_opts(validate_only)).await?;
        check_all(
            res.into_iter()
                .map(|r| match r {
                    Ok(n) => ResourceResult::ok(n),
                    Err((n, e)) => ResourceResult::err(n, e),
                })
                .collect(),
        )
    }

    pub async fn delete_topic(&self, name: &str, confirm: &str) -> Result<Vec<ResourceResult>> {
        self.guard_write()?;
        if name != confirm {
            return Err(CoreError::Confirmation { expected: name.into() });
        }
        let res = self.admin.delete_topics(&[name], &self.admin_opts(false)).await?;
        check_all(
            res.into_iter()
                .map(|r| match r {
                    Ok(n) => ResourceResult::ok(n),
                    Err((n, e)) => ResourceResult::err(n, e),
                })
                .collect(),
        )
    }

    /// Returns `(previous, proposed)` entries so callers can show a diff, then
    /// applies the change unless `validate_only`.
    ///
    /// Note: the classic AlterConfigs API replaces the full set of dynamic
    /// configs, so we merge existing dynamic topic overrides first.
    pub async fn alter_topic_config(
        &self,
        topic: &str,
        changes: &BTreeMap<String, String>,
        validate_only: bool,
    ) -> Result<serde_json::Value> {
        self.guard_write()?;
        let current = self.topic_configs(topic).await?;
        let mut merged: BTreeMap<String, String> = current
            .iter()
            .filter(|e| e.source == "dynamic-topic")
            .filter_map(|e| e.value.clone().map(|v| (e.name.clone(), v)))
            .collect();
        let previous: BTreeMap<String, Option<String>> = changes
            .keys()
            .map(|k| (k.clone(), current.iter().find(|e| &e.name == k).and_then(|e| e.value.clone())))
            .collect();
        if current.iter().any(|e| e.source == "dynamic-topic" && e.sensitive) {
            return Err(CoreError::Invalid(
                "topic has sensitive dynamic configs that cannot be safely re-applied".into(),
            ));
        }
        merged.extend(changes.clone());
        let mut ac = AlterConfig::new(ResourceSpecifier::Topic(topic));
        for (k, v) in &merged {
            ac = ac.set(k, v);
        }
        let res = self.admin.alter_configs([&ac], &self.admin_opts(validate_only)).await?;
        for r in res {
            r.map_err(|(_, e)| CoreError::Kafka(rdkafka::error::KafkaError::AdminOp(e)))?;
        }
        Ok(serde_json::json!({
            "topic": topic, "validateOnly": validate_only,
            "previous": previous, "applied": changes,
        }))
    }

    pub async fn add_partitions(
        &self,
        topic: &str,
        total: usize,
        validate_only: bool,
    ) -> Result<Vec<ResourceResult>> {
        self.guard_write()?;
        let current = self.partitions(topic.to_string()).await?.len();
        if total <= current {
            return Err(CoreError::Invalid(format!(
                "partition count can only increase (current {current}, requested {total})"
            )));
        }
        let np = NewPartitions::new(topic, total);
        let res = self.admin.create_partitions([&np], &self.admin_opts(validate_only)).await?;
        check_all(
            res.into_iter()
                .map(|r| match r {
                    Ok(n) => ResourceResult::ok(n),
                    Err((n, e)) => ResourceResult::err(n, e),
                })
                .collect(),
        )
    }

    pub async fn offsets_for_time(&self, topic: String, ts: i64) -> Result<Vec<PartitionOffset>> {
        let parts = self.partitions(topic.clone()).await?;
        self.blocking(move |m, t| {
            let mut tpl = TopicPartitionList::new();
            for p in &parts {
                tpl.add_partition_offset(&topic, *p, Offset::Offset(ts))?;
            }
            let r = m.offsets_for_times(tpl, t)?;
            Ok(r.elements()
                .iter()
                .map(|e| PartitionOffset { partition: e.partition(), offset: offset_value(e.offset()) })
                .collect())
        })
        .await
    }

    pub async fn list_groups(&self) -> Result<Vec<GroupSummary>> {
        self.blocking(|m, t| {
            let gl = m.fetch_group_list(None, t)?;
            let mut out: Vec<GroupSummary> = gl
                .groups()
                .iter()
                .map(|g| GroupSummary {
                    group_id: g.name().into(),
                    state: g.state().into(),
                    protocol_type: g.protocol_type().into(),
                    protocol: g.protocol().into(),
                    members: g.members().len(),
                })
                .collect();
            out.sort_by(|a, b| a.group_id.cmp(&b.group_id));
            Ok(out)
        })
        .await
    }

    /// Describe a group and compute lag for every partition of every topic
    /// the group has committed offsets for. Read-only: the reader consumer is
    /// never subscribed, so it does not join or rebalance the group.
    pub async fn describe_group(&self, group: String) -> Result<GroupDetail> {
        let cfg = self.config.clone();
        self.blocking(move |m, t| {
            let gl = m.fetch_group_list(Some(&group), t)?;
            let g = gl
                .groups()
                .iter()
                .find(|g| g.name() == group)
                .ok_or_else(|| CoreError::NotFound(group.clone()))?;
            let summary = GroupSummary {
                group_id: g.name().into(),
                state: g.state().into(),
                protocol_type: g.protocol_type().into(),
                protocol: g.protocol().into(),
                members: g.members().len(),
            };
            let members_detail = g
                .members()
                .iter()
                .map(|mm| MemberInfo {
                    member_id: mm.id().into(),
                    client_id: mm.client_id().into(),
                    client_host: mm.client_host().into(),
                })
                .collect();

            // OffsetFetch for every partition of every non-internal topic.
            // There is no ListConsumerGroupOffsets in rust-rdkafka 0.39 (see PARITY.md).
            let md = m.fetch_metadata(None, t)?;
            let mut tpl = TopicPartitionList::new();
            for tp in md.topics().iter().filter(|tp| !tp.name().starts_with("__")) {
                for p in tp.partitions() {
                    tpl.add_partition(tp.name(), p.id());
                }
            }
            let reader = cfg.group_offset_reader(&group)?;
            let committed = reader.committed_offsets(tpl, t)?;
            let mut offsets = Vec::new();
            let mut total = 0;
            for e in committed.elements() {
                let Some(c) = offset_value(e.offset()) else { continue };
                let (_, high) = m.fetch_watermarks(e.topic(), e.partition(), t)?;
                let lag = compute_lag(Some(c), high);
                total += lag.unwrap_or(0);
                offsets.push(GroupOffset {
                    topic: e.topic().into(),
                    partition: e.partition(),
                    committed: Some(c),
                    high,
                    lag,
                });
            }
            offsets.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
            Ok(GroupDetail { summary, members_detail, offsets, total_lag: total })
        })
        .await
    }

    pub async fn delete_group(&self, group: &str, confirm: &str) -> Result<Vec<ResourceResult>> {
        self.guard_write()?;
        if group != confirm {
            return Err(CoreError::Confirmation { expected: group.into() });
        }
        let res = self.admin.delete_groups(&[group], &self.admin_opts(false)).await?;
        check_all(
            res.into_iter()
                .map(|r| match r {
                    Ok(n) => ResourceResult::ok(n),
                    Err((n, e)) => ResourceResult::err(n, e),
                })
                .collect(),
        )
    }

    pub async fn producer(&self) -> Result<&FutureProducer> {
        self.guard_write()?;
        self.producer.get_or_try_init(|| async { self.config.producer() }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_math() {
        assert_eq!(compute_lag(Some(5), 10), Some(5));
        assert_eq!(compute_lag(Some(12), 10), Some(0));
        assert_eq!(compute_lag(None, 10), None);
    }

    #[tokio::test]
    async fn read_only_blocks_writes_before_network() {
        // Unreachable broker: the guard must trip before any I/O.
        let cfg = ConnectionConfig {
            id: "t".into(),
            bootstrap_servers: "127.0.0.1:1".into(),
            read_only: true,
            timeout: Duration::from_millis(200),
            ..Default::default()
        };
        let admin = cfg.admin().unwrap();
        let meta = Arc::new(cfg.inspection_consumer().unwrap());
        let c = Cluster { config: cfg, admin, meta, producer: tokio::sync::OnceCell::new() };
        assert!(matches!(c.delete_topic("x", "x").await, Err(CoreError::ReadOnly)));
        assert!(matches!(c.producer().await, Err(CoreError::ReadOnly)));
    }
}
