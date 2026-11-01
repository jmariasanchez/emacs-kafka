//! Admin operations and routing. Mutating callers must enforce their profile's
//! write policy before opening a connection.
use crate::{check, error_name, Client};
use anyhow::{bail, Context, Result};
use kafka_core::ConnectionConfig;
use kafka_protocol::messages::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Reassignment {
    pub partition: i32,
    /// null cancels a reassignment.
    pub replicas: Option<Vec<i32>>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuotaEntity {
    pub entity_type: String,
    pub entity_name: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QuotaOp {
    pub key: String,
    /// null removes the quota.
    pub value: Option<f64>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ShareOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}
fn status(code: i16) -> Value {
    json!({"ok":code == 0, "errorCode":code, "error": if code == 0 { Value::Null } else { json!(error_name(code)) }})
}
fn ids(brokers: &[BrokerId]) -> Vec<i32> {
    brokers.iter().map(|b| b.0).collect()
}
fn timeout_ms(config: &ConnectionConfig) -> i32 {
    config.timeout.as_millis().min(i32::MAX as u128) as i32
}

pub async fn features(config: &ConnectionConfig) -> Result<Value> {
    let c = Client::bootstrap(config).await?;
    let r = c.api_versions;
    Ok(json!({"finalizedFeaturesEpoch":r.finalized_features_epoch,
        "finalized":r.finalized_features.iter().map(|f| json!({"name":f.name.as_str(),"minVersion":f.min_version_level,"maxVersion":f.max_version_level})).collect::<Vec<_>>(),
        "supported":r.supported_features.iter().map(|f| json!({"name":f.name.as_str(),"minVersion":f.min_version,"maxVersion":f.max_version})).collect::<Vec<_>>() }))
}

pub async fn reassignments(config: &ConnectionConfig) -> Result<Value> {
    let mut c = Client::bootstrap(config).await?;
    let r = c
        .request(
            &ListPartitionReassignmentsRequest::default()
                .with_timeout_ms(timeout_ms(config))
                .with_topics(None),
        )
        .await?;
    check(r.error_code)?;
    Ok(json!(r.topics.iter().flat_map(|t| t.partitions.iter().map(move |p|
        json!({"topic":t.name.as_str(),"partition":p.partition_index,"replicas":ids(&p.replicas),"addingReplicas":ids(&p.adding_replicas),"removingReplicas":ids(&p.removing_replicas)}))).collect::<Vec<_>>()))
}

pub fn validate_reassignments(partitions: &[Reassignment]) -> Result<()> {
    if partitions.is_empty() {
        bail!("partitions must not be empty")
    }
    let mut seen = std::collections::BTreeSet::new();
    for p in partitions {
        if p.partition < 0 || !seen.insert(p.partition) {
            bail!("partition indices must be nonnegative and unique")
        }
        if let Some(replicas) = &p.replicas {
            let unique: std::collections::BTreeSet<_> = replicas.iter().collect();
            if replicas.is_empty()
                || unique.len() != replicas.len()
                || replicas.iter().any(|id| *id < 0)
            {
                bail!("replicas must be nonempty, nonnegative, and unique")
            }
        }
    }
    Ok(())
}

pub async fn alter_reassignments(
    config: &ConnectionConfig,
    topic: &str,
    partitions: &[Reassignment],
    dry_run: bool,
) -> Result<Value> {
    validate_reassignments(partitions)?;
    let mut c = Client::bootstrap(config).await?;
    let metadata = c.metadata().await?;
    let t = metadata
        .topics
        .iter()
        .find(|t| t.name.as_ref().is_some_and(|n| n.as_str() == topic))
        .context("topic not found")?;
    check(t.error_code)?;
    let mut preview = Vec::new();
    for target in partitions {
        let current = t
            .partitions
            .iter()
            .find(|p| p.partition_index == target.partition)
            .context("partition not found")?;
        check(current.error_code)?;
        preview.push(json!({"topic":topic,"partition":target.partition,"currentReplicas":ids(&current.replica_nodes),"targetReplicas":target.replicas,"cancel":target.replicas.is_none()}));
    }
    if dry_run {
        return Ok(json!({"dryRun":true,"changes":preview}));
    }
    use alter_partition_reassignments_request::{ReassignablePartition, ReassignableTopic};
    let parts = partitions
        .iter()
        .map(|p| {
            ReassignablePartition::default()
                .with_partition_index(p.partition)
                .with_replicas(
                    p.replicas
                        .as_ref()
                        .map(|r| r.iter().copied().map(BrokerId).collect()),
                )
        })
        .collect();
    let mut request = AlterPartitionReassignmentsRequest::default()
        .with_timeout_ms(timeout_ms(config))
        .with_topics(vec![ReassignableTopic::default()
            .with_name(TopicName(topic.to_owned().into()))
            .with_partitions(parts)]);
    // Kafka 4.4 requires explicit permission to change the replication factor.
    if c.version::<AlterPartitionReassignmentsRequest>()? >= 1 {
        request.allow_replication_factor_change = true;
    }
    let r = c.request(&request).await?;
    check(r.error_code)?;
    let results: Vec<_> = r
        .responses
        .iter()
        .flat_map(|t| {
            t.partitions.iter().map(move |p| {
                let mut v = status(p.error_code);
                v["topic"] = json!(t.name.as_str());
                v["partition"] = json!(p.partition_index);
                v
            })
        })
        .collect();
    Ok(json!({"dryRun":false,"changes":preview,"results":results}))
}

pub async fn elect(config: &ConnectionConfig, topic: &str, partitions: Vec<i32>) -> Result<Value> {
    use elect_leaders_request::TopicPartitions;
    let mut c = Client::bootstrap(config).await?;
    let r = c
        .request(
            &ElectLeadersRequest::default()
                .with_election_type(0)
                .with_timeout_ms(timeout_ms(config))
                .with_topic_partitions(Some(vec![TopicPartitions::default()
                    .with_topic(TopicName(topic.to_owned().into()))
                    .with_partitions(partitions)])),
        )
        .await?;
    check(r.error_code)?;
    Ok(
        json!({"results":r.replica_election_results.iter().flat_map(|t| t.partition_result.iter().map(move |p| {
        let mut v = status(p.error_code); v["topic"] = json!(t.topic.as_str()); v["partition"] = json!(p.partition_id); v
    })).collect::<Vec<_>>()}),
    )
}

pub async fn log_dirs(config: &ConnectionConfig) -> Result<Value> {
    let mut bootstrap = Client::bootstrap(config).await?;
    let metadata = bootstrap.metadata().await?;
    let mut brokers = Vec::new();
    for b in metadata.brokers {
        let result = async {
            let mut c = Client::connect(config, &format!("{}:{}", b.host, b.port)).await?;
            let r = c.request(&DescribeLogDirsRequest::default().with_topics(None)).await?;
            check(r.error_code)?;
            Ok::<_, anyhow::Error>(r.results.iter().map(|d| json!({"path":d.log_dir.as_str(),"errorCode":d.error_code,"error":error_name(d.error_code),"totalBytes":d.total_bytes,"usableBytes":d.usable_bytes,
                "partitions":d.topics.iter().flat_map(|t| t.partitions.iter().map(move |p| json!({"topic":t.name.as_str(),"partition":p.partition_index,"size":p.partition_size,"offsetLag":p.offset_lag,"isFuture":p.is_future_key}))).collect::<Vec<_>>() })).collect::<Vec<_>>())
        }.await;
        match result {
            Ok(dirs) => brokers.push(json!({"brokerId":b.node_id.0,"directories":dirs,"ok":true})),
            Err(e) => brokers.push(
                json!({"brokerId":b.node_id.0,"directories":[],"ok":false,"error":e.to_string()}),
            ),
        }
    }
    Ok(json!(brokers))
}

pub fn validate_quotas(entity: &[QuotaEntity], ops: &[QuotaOp]) -> Result<()> {
    if entity.is_empty() || ops.is_empty() {
        bail!("entity and ops must not be empty")
    }
    let mut seen = std::collections::BTreeSet::new();
    for e in entity {
        if !matches!(e.entity_type.as_str(), "user" | "client-id" | "ip")
            || !seen.insert(&e.entity_type)
        {
            bail!("invalid or duplicate quota entity type")
        }
    }
    for op in ops {
        if !matches!(
            op.key.as_str(),
            "producer_byte_rate"
                | "consumer_byte_rate"
                | "request_percentage"
                | "controller_mutation_rate"
                | "connection_creation_rate"
        ) {
            bail!("unsupported quota key")
        }
        if op.value.is_some_and(|v| !v.is_finite() || v < 0.0) {
            bail!("quota must be finite and nonnegative")
        }
    }
    Ok(())
}
pub async fn quotas(config: &ConnectionConfig) -> Result<Value> {
    let mut c = Client::bootstrap(config).await?;
    let r = c.request(&DescribeClientQuotasRequest::default()).await?;
    check(r.error_code)?;
    Ok(json!(r.entries.unwrap_or_default().iter().map(|e| json!({
        "entity":e.entity.iter().map(|e| json!({"entityType":e.entity_type.as_str(),"entityName":e.entity_name.as_ref().map(|s|s.as_str())})).collect::<Vec<_>>(),
        "values":e.values.iter().map(|v| json!({"key":v.key.as_str(),"value":v.value})).collect::<Vec<_>>() })).collect::<Vec<_>>()))
}
pub async fn alter_quotas(
    config: &ConnectionConfig,
    entity: &[QuotaEntity],
    ops: &[QuotaOp],
    validate_only: bool,
) -> Result<Value> {
    validate_quotas(entity, ops)?;
    use alter_client_quotas_request::{EntityData, EntryData, OpData};
    let entity = entity
        .iter()
        .map(|e| {
            EntityData::default()
                .with_entity_type(e.entity_type.clone().into())
                .with_entity_name(e.entity_name.clone().map(Into::into))
        })
        .collect();
    let ops = ops
        .iter()
        .map(|o| {
            OpData::default()
                .with_key(o.key.clone().into())
                .with_remove(o.value.is_none())
                .with_value(o.value.unwrap_or(0.0))
        })
        .collect();
    let mut c = Client::bootstrap(config).await?;
    let r = c
        .request(
            &AlterClientQuotasRequest::default()
                .with_validate_only(validate_only)
                .with_entries(vec![EntryData::default().with_entity(entity).with_ops(ops)]),
        )
        .await?;
    Ok(
        json!({"validateOnly":validate_only,"results":r.entries.iter().map(|e| status(e.error_code)).collect::<Vec<_>>()}),
    )
}

pub async fn share_groups(config: &ConnectionConfig) -> Result<Value> {
    let mut bootstrap = Client::bootstrap(config).await?;
    let metadata = bootstrap.metadata().await?;
    let mut groups = Vec::new();
    for b in metadata.brokers {
        let mut c = Client::connect(config, &format!("{}:{}", b.host, b.port)).await?;
        if c.version::<ListGroupsRequest>()? < 5 {
            bail!("share group filtering unsupported by broker")
        }
        let r = c
            .request(&ListGroupsRequest::default().with_types_filter(vec!["share".into()]))
            .await?;
        check(r.error_code)?;
        groups.extend(r.groups.iter().map(|g| json!({"groupId":g.group_id.as_str(),"state":g.group_state.as_str(),"type":g.group_type.as_str(),"brokerId":b.node_id.0})));
    }
    groups.sort_by(|a, b| a["groupId"].as_str().cmp(&b["groupId"].as_str()));
    groups.dedup_by(|a, b| a["groupId"] == b["groupId"]);
    Ok(json!(groups))
}
async fn share_client(config: &ConnectionConfig, group: &str) -> Result<Client> {
    Client::bootstrap(config)
        .await?
        .coordinator(config, group)
        .await
}
async fn describe_share(
    c: &mut Client,
    group: &str,
) -> Result<share_group_describe_response::DescribedGroup> {
    let r = c
        .request(
            &ShareGroupDescribeRequest::default()
                .with_group_ids(vec![GroupId(group.to_owned().into())]),
        )
        .await?;
    let g = r
        .groups
        .into_iter()
        .next()
        .context("missing share group response")?;
    check(g.error_code)?;
    Ok(g)
}
pub async fn share_describe(config: &ConnectionConfig, group: &str) -> Result<Value> {
    let mut c = share_client(config, group).await?;
    let g = describe_share(&mut c, group).await?;
    Ok(
        json!({"groupId":g.group_id.as_str(),"state":g.group_state.as_str(),"groupEpoch":g.group_epoch,"assignmentEpoch":g.assignment_epoch,"assignor":g.assignor_name.as_str(),
        "members":g.members.iter().map(|m| json!({"memberId":m.member_id.as_str(),"clientId":m.client_id.as_str(),"clientHost":m.client_host.as_str(),
            "topics":m.subscribed_topic_names.iter().map(|t| t.as_str()).collect::<Vec<_>>(),
            "assignment":m.assignment.topic_partitions.iter().map(|t| json!({"topic":t.topic_name.as_str(),"partitions":t.partitions})).collect::<Vec<_>>() })).collect::<Vec<_>>() }),
    )
}
pub async fn share_offsets(config: &ConnectionConfig, group: &str) -> Result<Value> {
    use describe_share_group_offsets_request::DescribeShareGroupOffsetsRequestGroup;
    let mut c = share_client(config, group).await?;
    let r = c
        .request(
            &DescribeShareGroupOffsetsRequest::default().with_groups(vec![
                DescribeShareGroupOffsetsRequestGroup::default()
                    .with_group_id(GroupId(group.to_owned().into()))
                    .with_topics(None),
            ]),
        )
        .await?;
    let g = r.groups.first().context("missing share offsets response")?;
    check(g.error_code)?;
    Ok(json!(g
        .topics
        .iter()
        .flat_map(|t| t.partitions.iter().map(move |p| {
            let mut v = status(p.error_code);
            v["topic"] = json!(t.topic_name.as_str());
            v["partition"] = json!(p.partition_index);
            v["startOffset"] = json!(p.start_offset);
            v["leaderEpoch"] = json!(p.leader_epoch);
            v
        }))
        .collect::<Vec<_>>()))
}
pub async fn share_reset(
    config: &ConnectionConfig,
    group: &str,
    offsets: &[ShareOffset],
    delete_topics: &[String],
) -> Result<Value> {
    let mut c = share_client(config, group).await?;
    let g = describe_share(&mut c, group).await?;
    if !g.members.is_empty() || g.group_state.as_str() != "Empty" {
        bail!("share group must be Empty before resetting offsets")
    }
    if !delete_topics.is_empty() {
        use delete_share_group_offsets_request::DeleteShareGroupOffsetsRequestTopic;
        let r = c
            .request(
                &DeleteShareGroupOffsetsRequest::default()
                    .with_group_id(GroupId(group.to_owned().into()))
                    .with_topics(
                        delete_topics
                            .iter()
                            .map(|t| {
                                DeleteShareGroupOffsetsRequestTopic::default()
                                    .with_topic_name(TopicName(t.clone().into()))
                            })
                            .collect(),
                    ),
            )
            .await?;
        check(r.error_code)?;
        return Ok(
            json!({"results":r.responses.iter().map(|t| {let mut v = status(t.error_code); v["topic"] = json!(t.topic_name.as_str()); v}).collect::<Vec<_>>()}),
        );
    }
    use alter_share_group_offsets_request::{
        AlterShareGroupOffsetsRequestPartition, AlterShareGroupOffsetsRequestTopic,
    };
    let mut topics: std::collections::BTreeMap<
        String,
        Vec<AlterShareGroupOffsetsRequestPartition>,
    > = Default::default();
    for o in offsets {
        topics.entry(o.topic.clone()).or_default().push(
            AlterShareGroupOffsetsRequestPartition::default()
                .with_partition_index(o.partition)
                .with_start_offset(o.offset),
        );
    }
    let r = c
        .request(
            &AlterShareGroupOffsetsRequest::default()
                .with_group_id(GroupId(group.to_owned().into()))
                .with_topics(
                    topics
                        .into_iter()
                        .map(|(t, p)| {
                            AlterShareGroupOffsetsRequestTopic::default()
                                .with_topic_name(TopicName(t.into()))
                                .with_partitions(p)
                        })
                        .collect(),
                ),
        )
        .await?;
    check(r.error_code)?;
    Ok(
        json!({"results":r.responses.iter().flat_map(|t|t.partitions.iter().map(move |p| {let mut v = status(p.error_code); v["topic"] = json!(t.topic_name.as_str()); v["partition"] = json!(p.partition_index); v})).collect::<Vec<_>>()}),
    )
}
