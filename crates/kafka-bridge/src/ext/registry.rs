//! Schema registry RPC adapter. Registry state is scoped by connection id.
use crate::{
    convert::{core_err, decode_in},
    dispatch::{parse, State, R},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use kafka_core::records::ProduceRequest;
use kafka_rpc_protocol::{codes, RpcError};
use kafka_schema_registry::{Config, Error, Registry};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, Weak},
};
use tokio::sync::Mutex;

struct Registration {
    cluster: Weak<kafka_core::Cluster>,
    client: Registry,
}
static REGISTRIES: OnceLock<Mutex<HashMap<String, Registration>>> = OnceLock::new();
fn registries() -> &'static Mutex<HashMap<String, Registration>> {
    REGISTRIES.get_or_init(Mutex::default)
}
fn err(e: Error) -> RpcError {
    let code = match e {
        Error::Confirmation => 1003,
        Error::Codec(_) => 1006,
        Error::Config(_) => codes::INVALID_PARAMS,
        _ => 1000,
    };
    RpcError::new(code, e.to_string())
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Configure {
    connection_id: String,
    #[serde(flatten)]
    config: Config,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Params {
    connection_id: String,
    subject: Option<String>,
    version: Option<Value>,
    schema_id: Option<u32>,
    schema: Option<String>,
    schema_type: Option<String>,
    references: Option<Vec<Value>>,
    compatibility: Option<String>,
    #[serde(default)]
    permanent: bool,
    confirm_name: Option<String>,
    bytes: Option<String>,
    topic: Option<String>,
    #[serde(default, deserialize_with = "present_value")]
    value: Option<Value>,
    #[serde(default, deserialize_with = "present_value")]
    key: Option<Value>,
    key_subject: Option<String>,
    partition: Option<i32>,
    timestamp: Option<i64>,
    #[serde(default)]
    headers: Vec<kafka_rpc_protocol::Header>,
}
fn present_value<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(d).map(Some)
}
fn required<'a>(value: &'a Option<String>, name: &str) -> Result<&'a str, RpcError> {
    value
        .as_deref()
        .ok_or_else(|| RpcError::new(codes::INVALID_PARAMS, format!("{name} is required")))
}
impl Params {
    fn schema_body(&self) -> Result<Value, RpcError> {
        Ok(
            json!({"schema":required(&self.schema,"schema")?,"schemaType":self.schema_type.as_deref().unwrap_or("AVRO"),"references":self.references.as_deref().unwrap_or(&[])}),
        )
    }
}
pub async fn dispatch(st: &Arc<State>, method: &str, params: Value) -> Option<R> {
    if !method.starts_with("kafka/registry/") {
        return None;
    }
    Some(run(st, method, params).await)
}
async fn run(st: &Arc<State>, method: &str, params: Value) -> R {
    if method == "kafka/registry/configure" {
        let p: Configure = parse(params)?;
        let cluster = st.cluster(&p.connection_id).await?;
        let mut map = registries().lock().await;
        map.retain(|_, r| r.cluster.strong_count() > 0);
        if map.get(&p.connection_id).is_some_and(|r| {
            r.cluster.ptr_eq(&Arc::downgrade(&cluster)) && r.client.matches_config(&p.config)
        }) {
            return Ok(json!({"configured":true}));
        }
        let registry = Registry::new(p.config).map_err(err)?;
        map.insert(
            p.connection_id,
            Registration {
                cluster: Arc::downgrade(&cluster),
                client: registry,
            },
        );
        return Ok(json!({"configured":true}));
    }
    let p: Params = parse(params)?;
    let cluster = st.cluster(&p.connection_id).await?;
    if matches!(
        method,
        "kafka/registry/register" | "kafka/registry/delete" | "kafka/registry/produce"
    ) || (method == "kafka/registry/config" && p.compatibility.is_some())
    {
        cluster.guard_write().map_err(core_err)?;
    }
    if method == "kafka/registry/delete"
        && p.confirm_name.as_deref() != Some(required(&p.subject, "subject")?)
    {
        return Err(RpcError::new(
            1003,
            "confirmName must exactly match subject",
        ));
    }
    let registry = registries()
        .lock()
        .await
        .get(&p.connection_id)
        .filter(|r| r.cluster.ptr_eq(&Arc::downgrade(&cluster)))
        .map(|r| r.client.clone())
        .ok_or_else(|| RpcError::new(codes::INVALID_PARAMS, "registry is not configured"))?;
    let version = match &p.version {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) if n.is_u64() => n.to_string(),
        None => "latest".into(),
        _ => {
            return Err(RpcError::new(
                codes::INVALID_PARAMS,
                "version must be a string or positive integer",
            ))
        }
    };
    match method {
        "kafka/registry/subjects" => registry.subjects().await.map_err(err),
        "kafka/registry/versions" => registry
            .versions(required(&p.subject, "subject")?)
            .await
            .map_err(err),
        "kafka/registry/schema" => {
            let s = if let Some(id) = p.schema_id {
                registry.by_id(id).await
            } else {
                registry
                    .schema(required(&p.subject, "subject")?, &version)
                    .await
            }
            .map_err(err)?;
            Ok(json!(s))
        }
        "kafka/registry/register" => registry
            .register(required(&p.subject, "subject")?, p.schema_body()?)
            .await
            .map_err(err),
        "kafka/registry/compatibility" => registry
            .compatibility(required(&p.subject, "subject")?, &version, p.schema_body()?)
            .await
            .map_err(err),
        "kafka/registry/config" => registry
            .config(p.subject.as_deref(), p.compatibility.as_deref())
            .await
            .map_err(err),
        "kafka/registry/delete" => registry
            .delete(
                required(&p.subject, "subject")?,
                p.version.as_ref().map(|_| version.as_str()),
                p.permanent,
                required(&p.confirm_name, "confirmName")?,
            )
            .await
            .map_err(err),
        "kafka/registry/decode" => {
            let bytes = STANDARD
                .decode(required(&p.bytes, "bytes")?)
                .map_err(|_| RpcError::new(1006, "invalid base64"))?;
            registry.decode(&bytes).await.map_err(err)
        }
        "kafka/registry/produce" => {
            let topic = required(&p.topic, "topic")?;
            let headers = p
                .headers
                .iter()
                .map(|h| Ok((h.key.clone(), decode_in(&h.value, h.encoding)?)))
                .collect::<Result<Vec<_>, RpcError>>()?;
            let subject = p
                .subject
                .clone()
                .unwrap_or_else(|| format!("{topic}-value"));
            let value = p
                .value
                .ok_or_else(|| RpcError::new(codes::INVALID_PARAMS, "value is required"))?;
            let value = registry.encode(&subject, value).await.map_err(err)?;
            let key = if let Some(key) = p.key {
                Some(
                    registry
                        .encode(
                            &p.key_subject.unwrap_or_else(|| format!("{topic}-key")),
                            key,
                        )
                        .await
                        .map_err(err)?,
                )
            } else {
                None
            };
            let result = cluster
                .produce(ProduceRequest {
                    topic: topic.into(),
                    partition: p.partition,
                    value: Some(value),
                    key,
                    headers,
                    timestamp: p.timestamp,
                })
                .await
                .map_err(core_err)?;
            Ok(json!(result))
        }
        _ => Err(RpcError::new(
            codes::METHOD_NOT_FOUND,
            "unknown registry method",
        )),
    }
}
