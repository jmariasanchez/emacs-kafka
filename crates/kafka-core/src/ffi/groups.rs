use super::*;
use rdkafka::{Offset, TopicPartitionList};

#[derive(Clone, Deserialize, Serialize)]
pub struct OffsetSelection {
    pub topic: String,
    pub partitions: Option<Vec<i32>>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum ResetSpec {
    ToEarliest,
    ToLatest,
    ToOffset { offset: i64 },
    ToDatetime { timestamp: i64 },
    ShiftBy { offset: i64 },
}
#[derive(Deserialize, Serialize)]
struct Committed {
    topic: String,
    partition: i32,
    committed: Option<i64>,
}
unsafe fn partitions(p: *const rd_kafka_topic_partition_list_t) -> Result<Vec<Value>> {
    if p.is_null() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for e in array((*p).elems, (*p).cnt.max(0) as usize) {
        check(e.err)?;
        out.push(json!({"topic":text(e.topic),"partition":e.partition,
            "committed":if e.offset >= 0 {Some(e.offset)} else {None}}));
    }
    Ok(out)
}
unsafe fn group_offsets(p: *const *const rd_kafka_group_result_t, n: usize) -> Result<Value> {
    let mut out = Vec::new();
    for g in array(p, n) {
        error(rd_kafka_group_result_error(*g))?;
        out.extend(partitions(rd_kafka_group_result_partitions(*g))?);
    }
    Ok(json!(out))
}
impl Cluster {
    pub async fn group_members(self: &Arc<Self>, group: String) -> Result<Value> {
        self.native(rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS,false,false,move |c| unsafe {
            let group = cstring(&group)?;
            rd_kafka_DescribeConsumerGroups(c.rk,&mut group.as_ptr(),1,c.options.0,c.queue.0);
            let ev = c.wait()?;
            let r = rd_kafka_event_DescribeConsumerGroups_result(ev.0);
            if r.is_null() {return Err(CoreError::Internal("unexpected group event".into()));}
            let mut n = 0;
            let p = rd_kafka_DescribeConsumerGroups_result_groups(r,&mut n);
            let g = *array(p,n).first().ok_or_else(||CoreError::Internal("missing group result".into()))?;
            error(rd_kafka_ConsumerGroupDescription_error(g))?;
            let mut members = Vec::new();
            for i in 0..rd_kafka_ConsumerGroupDescription_member_count(g) {
                let m = rd_kafka_ConsumerGroupDescription_member(g,i);
                let assignment = rd_kafka_MemberDescription_assignment(m);
                let assigned = if assignment.is_null() {Vec::new()} else {partitions(rd_kafka_MemberAssignment_partitions(assignment))?};
                members.push(json!({"memberId":text(rd_kafka_MemberDescription_consumer_id(m)),
                    "clientId":text(rd_kafka_MemberDescription_client_id(m)),"host":text(rd_kafka_MemberDescription_host(m)),
                    "groupInstanceId":text(rd_kafka_MemberDescription_group_instance_id(m)),"assignment":assigned}));
            }
            let state = match rd_kafka_ConsumerGroupDescription_state(g) {
                rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY => "Empty",
                rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_DEAD => "Dead",
                rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_STABLE => "Stable",
                rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_PREPARING_REBALANCE => "PreparingRebalance",
                rd_kafka_consumer_group_state_t::RD_KAFKA_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE => "CompletingRebalance",
                _ => "Unknown",
            };
            let ty = match rd_kafka_ConsumerGroupDescription_type(g) {
                rd_kafka_consumer_group_type_t::RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC => "classic",
                rd_kafka_consumer_group_type_t::RD_KAFKA_CONSUMER_GROUP_TYPE_CONSUMER => "consumer", _ => "unknown",
            };
            Ok(json!({"groupId":text(rd_kafka_ConsumerGroupDescription_group_id(g)),"state":state,"groupType":ty,
                "coordinator":node(rd_kafka_ConsumerGroupDescription_coordinator(g)),"members":members,
                "partitionAssignor":text(rd_kafka_ConsumerGroupDescription_partition_assignor(g))}))
        }).await
    }
    /// Fetch all committed offsets in a single OffsetFetch, without scanning topics.
    pub async fn group_offsets(self: &Arc<Self>, group: String) -> Result<Value> {
        self.native(
            rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
            false,
            false,
            move |c| unsafe {
                let group = cstring(&group)?;
                let mut req = Owned::new(
                    rd_kafka_ListConsumerGroupOffsets_new(group.as_ptr(), ptr::null()),
                    rd_kafka_ListConsumerGroupOffsets_destroy,
                )?;
                rd_kafka_ListConsumerGroupOffsets(c.rk, &mut req.0, 1, c.options.0, c.queue.0);
                let ev = c.wait()?;
                let r = rd_kafka_event_ListConsumerGroupOffsets_result(ev.0);
                if r.is_null() {
                    return Err(CoreError::Internal("unexpected offsets event".into()));
                }
                let mut n = 0;
                let p = rd_kafka_ListConsumerGroupOffsets_result_groups(r, &mut n);
                group_offsets(p, n)
            },
        )
        .await
    }
    async fn require_empty(self: &Arc<Self>, group: &str) -> Result<()> {
        let d = self.group_members(group.into()).await?;
        let state = d["state"].as_str().unwrap_or("");
        if matches!(state, "Empty" | "Dead" | "Unknown")
            && d["members"].as_array().is_some_and(|a| a.is_empty())
        {
            Ok(())
        } else {
            Err(CoreError::Invalid(format!(
                "offset reset refused: group {group} is {state}, stop all members first"
            )))
        }
    }
    pub async fn reset_group_offsets(
        self: &Arc<Self>,
        group: String,
        topics: Vec<OffsetSelection>,
        spec: ResetSpec,
        dry_run: bool,
        confirmation: String,
    ) -> Result<Value> {
        self.guard_write()?;
        if !dry_run {
            confirm(&group, &confirmation)?;
        }
        if group.is_empty() || topics.is_empty() {
            return Err(CoreError::Invalid("groupId and topics required".into()));
        }
        self.require_empty(&group).await?;
        let committed: Vec<Committed> =
            serde_json::from_value(self.group_offsets(group.clone()).await?)
                .map_err(|e| CoreError::Internal(e.to_string()))?;
        let mut preview = Vec::new();
        let mut list = TopicPartitionList::new();
        let mut seen = std::collections::BTreeSet::new();
        for selection in topics {
            let available = self.partitions(selection.topic.clone()).await?;
            let selected = selection.partitions.unwrap_or_else(|| available.clone());
            if selected.is_empty() {
                return Err(CoreError::Invalid(
                    "partition selection cannot be empty".into(),
                ));
            }
            let times = if let ResetSpec::ToDatetime { timestamp } = &spec {
                if *timestamp < 0 {
                    return Err(CoreError::Invalid(
                        "timestamp must be nonnegative milliseconds".into(),
                    ));
                }
                Some(
                    self.offsets_for_time(selection.topic.clone(), *timestamp)
                        .await?,
                )
            } else {
                None
            };
            for p in selected {
                if !available.contains(&p) || !seen.insert((selection.topic.clone(), p)) {
                    return Err(CoreError::Invalid("unknown or duplicate partition".into()));
                }
                let current = committed
                    .iter()
                    .find(|e| e.topic == selection.topic && e.partition == p)
                    .and_then(|e| e.committed);
                let (low, high) = self.watermarks(selection.topic.clone(), p).await?;
                let requested = match spec {
                    ResetSpec::ToEarliest => low,
                    ResetSpec::ToLatest => high,
                    ResetSpec::ToOffset { offset } => {
                        if offset < 0 {
                            return Err(CoreError::Invalid("offset must be nonnegative".into()));
                        }
                        offset
                    }
                    ResetSpec::ShiftBy { offset } => current
                        .ok_or_else(|| {
                            CoreError::Invalid(
                                "shift-by requires an existing committed offset".into(),
                            )
                        })?
                        .checked_add(offset)
                        .ok_or_else(|| CoreError::Invalid("offset overflow".into()))?,
                    ResetSpec::ToDatetime { .. } => times
                        .as_ref()
                        .and_then(|v| v.iter().find(|e| e.partition == p))
                        .and_then(|e| e.offset)
                        .unwrap_or(high),
                };
                let offset = requested.clamp(low, high);
                list.add_partition_offset(&selection.topic, p, Offset::Offset(offset))?;
                preview.push(json!({"topic":selection.topic,"partition":p,"currentCommitted":current,"newOffset":offset,"low":low,"high":high}));
            }
        }
        if !dry_run {
            // Recheck after metadata/preview work. Kafka has no atomic compare-and-reset.
            self.require_empty(&group).await?;
            self.native(
                rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS,
                false,
                false,
                move |c| unsafe {
                    let group = cstring(&group)?;
                    let mut req = Owned::new(
                        rd_kafka_AlterConsumerGroupOffsets_new(group.as_ptr(), list.ptr()),
                        rd_kafka_AlterConsumerGroupOffsets_destroy,
                    )?;
                    rd_kafka_AlterConsumerGroupOffsets(c.rk, &mut req.0, 1, c.options.0, c.queue.0);
                    let ev = c.wait()?;
                    let r = rd_kafka_event_AlterConsumerGroupOffsets_result(ev.0);
                    if r.is_null() {
                        return Err(CoreError::Internal("unexpected offsets event".into()));
                    }
                    let mut n = 0;
                    let p = rd_kafka_AlterConsumerGroupOffsets_result_groups(r, &mut n);
                    group_offsets(p, n)
                },
            )
            .await?;
        }
        Ok(json!({"dryRun":dry_run,"offsets":preview}))
    }
    pub async fn delete_group_offsets(
        self: &Arc<Self>,
        group: String,
        topic: String,
        confirmation: String,
    ) -> Result<Value> {
        self.guard_write()?;
        confirm(&group, &confirmation)?;
        let parts = self.partitions(topic.clone()).await?;
        let mut list = TopicPartitionList::new();
        for p in parts {
            list.add_partition(&topic, p);
        }
        self.native(
            rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DELETECONSUMERGROUPOFFSETS,
            false,
            false,
            move |c| unsafe {
                let group = cstring(&group)?;
                let mut req = Owned::new(
                    rd_kafka_DeleteConsumerGroupOffsets_new(group.as_ptr(), list.ptr()),
                    rd_kafka_DeleteConsumerGroupOffsets_destroy,
                )?;
                rd_kafka_DeleteConsumerGroupOffsets(c.rk, &mut req.0, 1, c.options.0, c.queue.0);
                let ev = c.wait()?;
                let r = rd_kafka_event_DeleteConsumerGroupOffsets_result(ev.0);
                if r.is_null() {
                    return Err(CoreError::Internal("unexpected offsets event".into()));
                }
                let mut n = 0;
                let p = rd_kafka_DeleteConsumerGroupOffsets_result_groups(r, &mut n);
                group_offsets(p, n)
            },
        )
        .await
    }
}
