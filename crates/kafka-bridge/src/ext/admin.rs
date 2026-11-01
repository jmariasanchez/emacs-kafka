//! Additive admin RPCs. Existing topic/alterConfig and group/describe remain unchanged.
use crate::convert::core_err as convert_core_err;
use crate::dispatch::{parse, State, R};
use kafka_core::ffi::{
    Acl, ConfigChange, OffsetSelection, PartitionTarget, ResetSpec, ScramChange,
};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

fn core_err(error: kafka_core::CoreError) -> kafka_rpc_protocol::RpcError {
    // rdkafka 0.39's rdkafka_error_code() returns None for AdminOp, even
    // though that variant carries a code. Preserve it explicitly here.
    let code = match &error {
        kafka_core::CoreError::Kafka(rdkafka::error::KafkaError::AdminOp(code)) => {
            Some(format!("{code:?}"))
        }
        _ => None,
    };
    let rpc = convert_core_err(error);
    match code {
        Some(code) => rpc.with_data(serde_json::json!({"kafkaCode":code})),
        None => rpc,
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Connection {
    connection_id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Acls {
    connection_id: String,
    #[serde(default)]
    filter: Acl,
    #[serde(default)]
    confirm_name: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateAcl {
    connection_id: String,
    acl: Acl,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Configs {
    connection_id: String,
    resource_type: String,
    name: String,
    changes: Vec<ConfigChange>,
    #[serde(default)]
    validate_only: bool,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Group {
    connection_id: String,
    group_id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Reset {
    connection_id: String,
    group_id: String,
    topics: Vec<OffsetSelection>,
    spec: ResetSpec,
    dry_run: bool,
    #[serde(default)]
    confirm_name: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeleteOffsets {
    connection_id: String,
    group_id: String,
    topic: String,
    confirm_name: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Users {
    connection_id: String,
    #[serde(default)]
    users: Vec<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlterScram {
    connection_id: String,
    change: ScramChange,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeleteRecords {
    connection_id: String,
    topic: String,
    offsets: Vec<PartitionTarget>,
    confirm_name: String,
}

pub async fn dispatch(st: &Arc<State>, method: &str, params: Value) -> Option<R> {
    if !matches!(
        method,
        "kafka/cluster/info"
            | "kafka/acls/describe"
            | "kafka/acls/create"
            | "kafka/acls/delete"
            | "kafka/configs/incrementalAlter"
            | "kafka/group/offsets"
            | "kafka/group/members"
            | "kafka/group/resetOffsets"
            | "kafka/group/deleteOffsets"
            | "kafka/scram/describe"
            | "kafka/scram/alter"
            | "kafka/topic/deleteRecords"
    ) {
        return None;
    }
    Some(handle(st, method, params).await)
}
async fn handle(st: &Arc<State>, method: &str, params: Value) -> R {
    match method {
        "kafka/cluster/info" => {
            let p: Connection = parse(params)?;
            st.cluster(&p.connection_id)
                .await?
                .cluster_info()
                .await
                .map_err(core_err)
        }
        "kafka/acls/describe" => {
            let p: Acls = parse(params)?;
            st.cluster(&p.connection_id)
                .await?
                .describe_acls(p.filter)
                .await
                .map_err(core_err)
        }
        "kafka/acls/create" => {
            let p: CreateAcl = parse(params)?;
            st.cluster(&p.connection_id)
                .await?
                .create_acl(p.acl)
                .await
                .map_err(core_err)
        }
        "kafka/acls/delete" => {
            let p: Acls = parse(params)?;
            st.cluster(&p.connection_id)
                .await?
                .delete_acls(p.filter, p.confirm_name)
                .await
                .map_err(core_err)
        }
        "kafka/configs/incrementalAlter" => {
            let p: Configs = parse(params)?;
            st.cluster(&p.connection_id)
                .await?
                .incremental_alter(p.resource_type, p.name, p.changes, p.validate_only)
                .await
                .map_err(core_err)
        }
        "kafka/group/offsets" => {
            let p: Group = parse(params)?;
            st.cluster(&p.connection_id)
                .await?
                .group_offsets(p.group_id)
                .await
                .map_err(core_err)
        }
        "kafka/group/members" => {
            let p: Group = parse(params)?;
            st.cluster(&p.connection_id)
                .await?
                .group_members(p.group_id)
                .await
                .map_err(core_err)
        }
        "kafka/group/resetOffsets" => {
            let p: Reset = parse(params)?;
            st.cluster(&p.connection_id)
                .await?
                .reset_group_offsets(p.group_id, p.topics, p.spec, p.dry_run, p.confirm_name)
                .await
                .map_err(core_err)
        }
        "kafka/group/deleteOffsets" => {
            let p: DeleteOffsets = parse(params)?;
            st.cluster(&p.connection_id)
                .await?
                .delete_group_offsets(p.group_id, p.topic, p.confirm_name)
                .await
                .map_err(core_err)
        }
        "kafka/scram/describe" => {
            let p: Users = parse(params)?;
            st.cluster(&p.connection_id)
                .await?
                .scram_users(p.users)
                .await
                .map_err(core_err)
        }
        "kafka/scram/alter" => {
            // Serde errors can include the offending input. Never reflect a malformed
            // password (including one of the wrong JSON type) back to the caller.
            let p: AlterScram = serde_json::from_value(params).map_err(|_| {
                kafka_rpc_protocol::RpcError::new(
                    kafka_rpc_protocol::codes::INVALID_PARAMS,
                    "invalid SCRAM parameters",
                )
            })?;
            st.cluster(&p.connection_id)
                .await?
                .alter_scram(p.change)
                .await
                .map_err(core_err)
        }
        "kafka/topic/deleteRecords" => {
            let p: DeleteRecords = parse(params)?;
            st.cluster(&p.connection_id)
                .await?
                .delete_records_before(p.topic, p.offsets, p.confirm_name)
                .await
                .map_err(core_err)
        }
        _ => unreachable!("dispatch filters method names"),
    }
}
