//! Connect REST and Streams observation methods.
use crate::{
    convert::core_err,
    dispatch::{parse, State, R},
};
use kafka_connect_rest::Connect;
use kafka_rpc_protocol::{codes, RpcError};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};
use tokio::sync::Mutex;

static CLIENTS: LazyLock<Mutex<HashMap<String, Connect>>> = LazyLock::new(Mutex::default);
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Params {
    connection_id: String,
    connector: Option<String>,
    url: Option<String>,
    basic_auth_user: Option<String>,
    basic_auth_password: Option<String>,
    ssl_ca_location: Option<String>,
    class: Option<String>,
    config: Option<Value>,
    offsets: Option<Value>,
    task: Option<u32>,
    confirm_name: Option<String>,
    #[serde(default)]
    include_tasks: bool,
    #[serde(default)]
    only_failed: bool,
}
fn required<T>(v: Option<T>, name: &str) -> Result<T, RpcError> {
    v.ok_or_else(|| RpcError::new(codes::INVALID_PARAMS, format!("Missing {name}")))
}
fn rest_err(e: kafka_connect_rest::Error) -> RpcError {
    let mut error = RpcError::new(codes::KAFKA, e.to_string());
    error.data = Some(json!({"httpStatus":e.status,"error_code":e.error_code}));
    error
}
pub async fn dispatch(st: &Arc<State>, method: &str, params: Value) -> Option<R> {
    if !matches!(
        method,
        "kafka/streams/apps"
            | "kafka/connect/configure"
            | "kafka/connect/info"
            | "kafka/connect/connectors"
            | "kafka/connect/connector"
            | "kafka/connect/plugins"
            | "kafka/connect/validate"
            | "kafka/connect/upsert"
            | "kafka/connect/pause"
            | "kafka/connect/resume"
            | "kafka/connect/stop"
            | "kafka/connect/restart"
            | "kafka/connect/restartTask"
            | "kafka/connect/delete"
            | "kafka/connect/offsets"
            | "kafka/connect/offsets/alter"
            | "kafka/connect/offsets/reset"
            | "kafka/connect/topics"
            | "kafka/connect/taskStatus"
    ) {
        return None;
    }
    Some(run(st, method, params).await)
}
async fn run(st: &Arc<State>, method: &str, params: Value) -> R {
    let p: Params = parse(params)?;
    let cluster = st.cluster(&p.connection_id).await?;
    if method == "kafka/streams/apps" {
        let groups = cluster.list_groups().await.map_err(core_err)?;
        let topics = cluster.list_topics(true, None).await.map_err(core_err)?;
        let mut apps = Vec::new();
        for g in groups {
            let internal: Vec<_> = topics
                .iter()
                .filter(|t| {
                    t.name.starts_with(&format!("{}-", g.group_id))
                        && (t.name.ends_with("-changelog") || t.name.ends_with("-repartition"))
                })
                .map(|t| t.name.clone())
                .collect();
            if g.protocol_type == "streams"
                || (g.protocol_type == "consumer" && !internal.is_empty())
            {
                let detail = cluster
                    .describe_group(g.group_id.clone())
                    .await
                    .map_err(core_err)?;
                apps.push(json!({"appId":g.group_id,"state":g.state,"members":g.members,"totalLag":detail.total_lag,"internalTopics":internal}));
            }
        }
        return Ok(json!(apps));
    }
    if method == "kafka/connect/configure" {
        let c = Connect::new(
            &required(p.url, "url")?,
            p.basic_auth_user,
            p.basic_auth_password,
            p.ssl_ca_location.as_deref(),
        )
        .map_err(rest_err)?;
        CLIENTS.lock().await.insert(p.connection_id, c);
        return Ok(json!({"configured":true}));
    }
    if matches!(
        method,
        "kafka/connect/upsert"
            | "kafka/connect/pause"
            | "kafka/connect/resume"
            | "kafka/connect/stop"
            | "kafka/connect/restart"
            | "kafka/connect/restartTask"
            | "kafka/connect/delete"
            | "kafka/connect/offsets/alter"
            | "kafka/connect/offsets/reset"
    ) {
        cluster.guard_write().map_err(core_err)?;
    }
    if matches!(
        method,
        "kafka/connect/delete" | "kafka/connect/offsets/alter" | "kafka/connect/offsets/reset"
    ) && (p.connector.is_none() || p.confirm_name != p.connector)
    {
        return Err(RpcError::new(
            codes::CONFIRMATION_REQUIRED,
            "confirmName must equal connector",
        ));
    }
    let c = CLIENTS
        .lock()
        .await
        .get(&p.connection_id)
        .cloned()
        .ok_or_else(|| {
            RpcError::new(
                codes::INVALID_PARAMS,
                "Configure Connect for this connection first",
            )
        })?;
    let result = match method {
        "kafka/connect/info" => c.info().await,
        "kafka/connect/connectors" => c.connectors().await,
        "kafka/connect/plugins" => c.plugins().await,
        "kafka/connect/validate" => {
            c.validate(&required(p.class, "class")?, &required(p.config, "config")?)
                .await
        }
        _ => {
            let n = required(p.connector, "connector")?;
            match method {
                "kafka/connect/connector" => {
                    let config = c.config(&n).await.map_err(rest_err)?;
                    let status = c.status(&n).await.map_err(rest_err)?;
                    let tasks = c.tasks(&n).await.map_err(rest_err)?;
                    return Ok(json!({"config":config,"status":status,"tasks":tasks}));
                }
                "kafka/connect/upsert" => c.upsert(&n, &required(p.config, "config")?).await,
                "kafka/connect/pause" => c.pause(&n).await,
                "kafka/connect/resume" => c.resume(&n).await,
                "kafka/connect/stop" => c.stop(&n).await,
                "kafka/connect/restart" => c.restart(&n, p.include_tasks, p.only_failed).await,
                "kafka/connect/restartTask" => c.restart_task(&n, required(p.task, "task")?).await,
                "kafka/connect/taskStatus" => c.task_status(&n, required(p.task, "task")?).await,
                "kafka/connect/topics" => c.topics(&n).await,
                "kafka/connect/delete" => c.delete(&n).await,
                "kafka/connect/offsets" => c.offsets(&n).await,
                "kafka/connect/offsets/alter" => {
                    c.alter_offsets(&n, &required(p.offsets, "offsets")?).await
                }
                "kafka/connect/offsets/reset" => c.reset_offsets(&n).await,
                _ => unreachable!(),
            }
        }
    };
    result.map_err(rest_err)
}
