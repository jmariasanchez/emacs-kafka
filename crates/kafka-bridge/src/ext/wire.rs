//! Native wire admin RPC. Guards run before opening any wire connection.
use crate::{
    convert::core_err,
    dispatch::{parse, State, R},
};
use kafka_rpc_protocol::{codes, RpcError};
use kafka_wire::admin::{self, QuotaEntity, QuotaOp, Reassignment, ShareOffset};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Connection {
    connection_id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Reassign {
    connection_id: String,
    topic: String,
    partitions: Vec<Reassignment>,
    #[serde(default)]
    dry_run: bool,
    confirm_name: Option<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Elect {
    connection_id: String,
    topic: String,
    partitions: Vec<i32>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Quotas {
    connection_id: String,
    entity: Vec<QuotaEntity>,
    ops: Vec<QuotaOp>,
    #[serde(default)]
    validate_only: bool,
    confirm_name: Option<String>,
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
    confirm_name: String,
    #[serde(default)]
    offsets: Vec<ShareOffset>,
    #[serde(default)]
    delete_topics: Vec<String>,
}
fn confirm(actual: Option<&str>, expected: &str) -> Result<(), RpcError> {
    if actual == Some(expected) {
        Ok(())
    } else {
        Err(RpcError::new(
            codes::CONFIRMATION_REQUIRED,
            "confirmName must exactly match the resource name",
        ))
    }
}
fn wire_error(error: impl std::fmt::Display) -> RpcError {
    let message = error.to_string();
    let code = if message.contains("timeout") {
        codes::TIMEOUT
    } else if message.contains("unsupported") || message.contains("no mutually supported") {
        codes::UNSUPPORTED
    } else {
        codes::KAFKA
    };
    let kafka_code = message
        .strip_prefix("Kafka ")
        .and_then(|s| s.split_once(" ("))
        .map(|(name, _)| name.to_owned());
    RpcError::new(code, message).with_data(json!({"kafkaCode":kafka_code}))
}
fn partial(value: Value) -> R {
    if value
        .get("results")
        .and_then(Value::as_array)
        .is_some_and(|r| r.iter().any(|v| v["ok"] == false))
    {
        Err(
            RpcError::new(codes::PARTIAL_FAILURE, "one or more Kafka resources failed")
                .with_data(value),
        )
    } else {
        Ok(value)
    }
}

pub async fn dispatch(st: &Arc<State>, method: &str, params: Value) -> Option<R> {
    if !matches!(
        method,
        "kafka/reassignments/list"
            | "kafka/reassignments/alter"
            | "kafka/leaders/elect"
            | "kafka/logDirs/describe"
            | "kafka/quotas/describe"
            | "kafka/quotas/alter"
            | "kafka/shareGroups/list"
            | "kafka/shareGroup/describe"
            | "kafka/shareGroup/offsets"
            | "kafka/shareGroup/resetOffsets"
            | "kafka/features/describe"
    ) {
        return None;
    }
    Some(handle(st, method, params).await)
}
async fn handle(st: &Arc<State>, method: &str, params: Value) -> R {
    match method {
        "kafka/reassignments/alter" => {
            let p: Reassign = parse(params)?;
            let c = st.cluster(&p.connection_id).await?;
            // A preview is an inspection, but every real submission is guarded.
            if !p.dry_run {
                c.guard_write().map_err(core_err)?;
                confirm(p.confirm_name.as_deref(), &p.topic)?;
            }
            admin::validate_reassignments(&p.partitions)
                .map_err(|e| RpcError::new(codes::INVALID_PARAMS, e.to_string()))?;
            partial(
                admin::alter_reassignments(&c.config, &p.topic, &p.partitions, p.dry_run)
                    .await
                    .map_err(wire_error)?,
            )
        }
        "kafka/leaders/elect" => {
            let p: Elect = parse(params)?;
            let c = st.cluster(&p.connection_id).await?;
            c.guard_write().map_err(core_err)?;
            if p.partitions.is_empty() || p.partitions.iter().any(|p| *p < 0) {
                return Err(RpcError::new(
                    codes::INVALID_PARAMS,
                    "partitions must be nonempty and nonnegative",
                ));
            }
            partial(
                admin::elect(&c.config, &p.topic, p.partitions)
                    .await
                    .map_err(wire_error)?,
            )
        }
        "kafka/quotas/alter" => {
            let p: Quotas = parse(params)?;
            let c = st.cluster(&p.connection_id).await?;
            c.guard_write().map_err(core_err)?;
            admin::validate_quotas(&p.entity, &p.ops)
                .map_err(|e| RpcError::new(codes::INVALID_PARAMS, e.to_string()))?;
            if !p.validate_only && p.ops.iter().any(|o| o.value.is_none()) {
                let name = p
                    .entity
                    .iter()
                    .map(|e| e.entity_name.as_deref().unwrap_or("<default>"))
                    .collect::<Vec<_>>()
                    .join("/");
                confirm(p.confirm_name.as_deref(), &name)?;
            }
            partial(
                admin::alter_quotas(&c.config, &p.entity, &p.ops, p.validate_only)
                    .await
                    .map_err(wire_error)?,
            )
        }
        "kafka/shareGroup/resetOffsets" => {
            let p: Reset = parse(params)?;
            let c = st.cluster(&p.connection_id).await?;
            c.guard_write().map_err(core_err)?;
            confirm(Some(&p.confirm_name), &p.group_id)?;
            if p.offsets.is_empty() == p.delete_topics.is_empty()
                || p.offsets
                    .iter()
                    .any(|o| o.partition < 0 || o.offset < 0 || o.topic.is_empty())
            {
                return Err(RpcError::new(
                    codes::INVALID_PARAMS,
                    "provide either nonnegative offsets or deleteTopics",
                ));
            }
            partial(
                admin::share_reset(&c.config, &p.group_id, &p.offsets, &p.delete_topics)
                    .await
                    .map_err(wire_error)?,
            )
        }
        "kafka/shareGroup/describe" | "kafka/shareGroup/offsets" => {
            let p: Group = parse(params)?;
            let c = st.cluster(&p.connection_id).await?;
            if method == "kafka/shareGroup/describe" {
                admin::share_describe(&c.config, &p.group_id)
                    .await
                    .map_err(wire_error)
            } else {
                admin::share_offsets(&c.config, &p.group_id)
                    .await
                    .map_err(wire_error)
            }
        }
        _ => {
            let p: Connection = parse(params)?;
            let c = st.cluster(&p.connection_id).await?;
            match method {
                "kafka/reassignments/list" => admin::reassignments(&c.config).await,
                "kafka/logDirs/describe" => admin::log_dirs(&c.config).await,
                "kafka/quotas/describe" => admin::quotas(&c.config).await,
                "kafka/shareGroups/list" => admin::share_groups(&c.config).await,
                "kafka/features/describe" => admin::features(&c.config).await,
                _ => unreachable!(),
            }
            .map_err(wire_error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_confirmation() {
        assert!(confirm(Some("topic"), "topic").is_ok());
        assert_eq!(confirm(Some("Topic"), "topic").unwrap_err().code, 1003);
        assert_eq!(confirm(None, "topic").unwrap_err().code, 1003);
    }
    #[test]
    fn partial_errors_are_not_success() {
        assert_eq!(
            partial(json!({"results":[{"ok":true},{"ok":false}]}))
                .unwrap_err()
                .code,
            1005
        );
    }
}
