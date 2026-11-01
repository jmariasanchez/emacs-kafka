//! Transaction sessions and bounded batch publishing.
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};
use std::time::{Duration, Instant};

use kafka_core::{records::ProduceRequest, transactions::Transaction};
use kafka_rpc_protocol::{codes, ProduceParams, RpcError};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::convert::{core_err, decode_in};
use crate::dispatch::{parse, State, R};

struct Session {
    transaction: Transaction,
    touched: Instant,
}
struct Entry {
    connection: String,
    session: Arc<Mutex<Session>>,
}
static SESSIONS: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
static NEXT: AtomicU64 = AtomicU64::new(1);
fn sessions() -> &'static Mutex<HashMap<String, Entry>> {
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}
fn unique_id() -> String {
    format!(
        "kafka-el-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}
fn invalid(message: &str) -> RpcError {
    RpcError::new(codes::INVALID_PARAMS, message)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Begin {
    connection_id: String,
    transactional_id: Option<String>,
    idle_timeout_ms: Option<u64>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Txn {
    txn_id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Batch {
    connection_id: String,
    topic: String,
    records: Vec<Value>,
    #[serde(default)]
    transactional: bool,
}

fn request(
    mut value: Value,
    connection: &str,
    topic: Option<&str>,
) -> Result<ProduceRequest, RpcError> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| invalid("record must be an object"))?;
    object.insert("connectionId".into(), json!(connection));
    if let Some(topic) = topic {
        object.insert("topic".into(), json!(topic));
    }
    if let Some(headers) = object.get("headers").and_then(Value::as_object) {
        let headers = headers
            .iter()
            .map(|(key, value)| json!({"key": key, "value": value}))
            .collect::<Vec<_>>();
        object.insert("headers".into(), json!(headers));
    }
    let p: ProduceParams = parse(value)?;
    Ok(ProduceRequest {
        topic: p.topic,
        partition: p.partition,
        key: decode_in(&p.key, p.encoding)?,
        value: decode_in(&p.value, p.encoding)?,
        headers: p
            .headers
            .iter()
            .map(|h| Ok((h.key.clone(), decode_in(&h.value, h.encoding)?)))
            .collect::<Result<_, RpcError>>()?,
        timestamp: p.timestamp,
    })
}

/// Abort all sessions for one connection, or all connections on shutdown.
/// The lifecycle dispatcher should call this before removing a connection.
#[allow(dead_code)]
pub async fn abort_all(connection_id: Option<&str>) {
    let entries = {
        let mut map = sessions().lock().await;
        let ids = map
            .iter()
            .filter(|(_, entry)| connection_id.is_none_or(|id| id == entry.connection))
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| map.remove(&id))
            .collect::<Vec<_>>()
    };
    for entry in entries {
        let _ = entry.session.lock().await.transaction.finish(false).await;
    }
}

pub async fn dispatch(st: &Arc<State>, method: &str, params: Value) -> Option<R> {
    if !matches!(
        method,
        "kafka/txn/begin"
            | "kafka/txn/produce"
            | "kafka/txn/commit"
            | "kafka/txn/abort"
            | "kafka/produce/batch"
    ) {
        return None;
    }
    // Once accepted, a write completes even if its RPC waiter is cancelled.
    // In particular, cancellation must not release a session during commit.
    let st = st.clone();
    let method = method.to_string();
    Some(
        tokio::spawn(async move { handle(&st, &method, params).await })
            .await
            .unwrap_or_else(|e| Err(RpcError::new(codes::INTERNAL, e.to_string()))),
    )
}

async fn handle(st: &Arc<State>, method: &str, params: Value) -> R {
    if method == "kafka/produce/batch" {
        return batch(st, parse(params)?).await;
    }
    if method == "kafka/txn/begin" {
        let p: Begin = parse(params)?;
        let cluster = st.cluster(&p.connection_id).await?;
        cluster.guard_write().map_err(core_err)?;
        let idle = Duration::from_millis(p.idle_timeout_ms.unwrap_or(300_000));
        if idle.is_zero() || idle > Duration::from_secs(86_400) {
            return Err(invalid("idleTimeoutMs must be 1..86400000"));
        }
        let mut map = sessions().lock().await;
        if map.values().any(|s| s.connection == p.connection_id) {
            return Err(invalid("connection already has an active transaction"));
        }
        let id = unique_id();
        let transaction = cluster
            .begin_transaction(p.transactional_id.unwrap_or_else(|| id.clone()))
            .await
            .map_err(core_err)?;
        let session = Arc::new(Mutex::new(Session {
            transaction,
            touched: Instant::now(),
        }));
        map.insert(
            id.clone(),
            Entry {
                connection: p.connection_id,
                session: session.clone(),
            },
        );
        let key = id.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(idle.min(Duration::from_secs(1))).await;
                let mut session = session.lock().await;
                if !sessions().lock().await.contains_key(&key) {
                    break;
                }
                if session.touched.elapsed() >= idle {
                    sessions().lock().await.remove(&key);
                    let _ = session.transaction.finish(false).await;
                    break;
                }
            }
        });
        return Ok(json!({"txnId": id}));
    }
    let p: Txn = parse(params.clone())?;
    let session = sessions()
        .lock()
        .await
        .get(&p.txn_id)
        .map(|e| e.session.clone())
        .ok_or_else(|| invalid("unknown or expired txnId"))?;
    let mut session = session.lock().await;
    if !sessions().lock().await.contains_key(&p.txn_id) {
        return Err(invalid("unknown or expired txnId"));
    }
    session.touched = Instant::now();
    if method == "kafka/txn/produce" {
        let result = session
            .transaction
            .produce(request(params, "", None)?)
            .await;
        session.touched = Instant::now();
        Ok(json!(result.map_err(core_err)?))
    } else {
        session
            .transaction
            .finish(method == "kafka/txn/commit")
            .await
            .map_err(core_err)?;
        sessions().lock().await.remove(&p.txn_id);
        Ok(json!({"txnId": p.txn_id, "committed": method == "kafka/txn/commit"}))
    }
}

async fn batch(st: &Arc<State>, p: Batch) -> R {
    let cluster = st.cluster(&p.connection_id).await?;
    cluster.guard_write().map_err(core_err)?;
    if p.records.is_empty() || p.records.len() > 10_000 {
        return Err(invalid("records must contain 1..10000 items"));
    }
    let mut transaction = if p.transactional {
        Some(
            cluster
                .begin_transaction(unique_id())
                .await
                .map_err(core_err)?,
        )
    } else {
        None
    };
    let mut results = Vec::with_capacity(p.records.len());
    let mut failed = false;
    for (index, value) in p.records.into_iter().enumerate() {
        let result = match request(value, &p.connection_id, Some(&p.topic)) {
            Err(e) => Err(e),
            Ok(req) => match &mut transaction {
                Some(txn) => txn.produce(req).await.map_err(core_err),
                None => cluster.produce(req).await.map_err(core_err),
            },
        };
        match result {
            Ok(delivery) => results.push(json!({"index": index, "ok": true, "delivery": delivery})),
            Err(error) => {
                failed = true;
                results.push(json!({"index": index, "ok": false, "error": error}));
            }
        }
    }
    if let Some(txn) = &mut transaction {
        if let Err(e) = txn.finish(!failed).await {
            return Err(core_err(e).with_data(json!({"results": results, "committed": false})));
        }
    }
    let data = json!({"results": results, "transactional": p.transactional, "committed": p.transactional && !failed});
    if failed {
        Err(RpcError::new(codes::PARTIAL_FAILURE, "batch contains failed records").with_data(data))
    } else {
        Ok(data)
    }
}
