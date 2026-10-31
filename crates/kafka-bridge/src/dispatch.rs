//! Request dispatch, connection registry, subscriptions and cancellation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kafka_core::capabilities::CAPABILITIES;
use kafka_core::records::{ProduceRequest, StreamControl, StreamEvent};
use kafka_core::{Cluster, ConnectionConfig};
use kafka_protocol::{self as p, codes, methods as m, RpcError};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::AbortHandle;

use crate::convert::{core_err, decode_in, position, record_json};

/// Max buffered outgoing messages. When full, request handlers wait, which in
/// turn stops stream threads (they block on their own bounded channel).
pub const OUTBOX_CAPACITY: usize = 256;
/// Hard cap on initial/added stream credit, so a client cannot request an
/// unbounded backlog.
pub const MAX_CREDIT: u32 = 64;

struct Subscription {
    connection_id: String,
    credit: Arc<Semaphore>,
    cancel: Arc<AtomicBool>,
}

pub struct State {
    out: mpsc::Sender<Value>,
    clusters: Mutex<HashMap<String, Arc<Cluster>>>,
    subs: Mutex<HashMap<String, Subscription>>,
    inflight: Mutex<HashMap<String, AbortHandle>>,
    next_sub: AtomicU64,
    pub shutdown_signal: tokio::sync::Notify,
}

type R = Result<Value, RpcError>;

fn parse<T: DeserializeOwned>(v: Value) -> Result<T, RpcError> {
    serde_json::from_value(v).map_err(|e| RpcError::new(codes::INVALID_PARAMS, e.to_string()))
}

fn id_key(id: &Value) -> String {
    id.to_string()
}

impl State {
    pub fn new(out: mpsc::Sender<Value>) -> Arc<Self> {
        Arc::new(Self {
            out,
            clusters: Mutex::default(),
            subs: Mutex::default(),
            inflight: Mutex::default(),
            next_sub: AtomicU64::new(1),
            shutdown_signal: tokio::sync::Notify::new(),
        })
    }

    pub async fn send(&self, v: Value) {
        let _ = self.out.send(v).await;
    }

    async fn cluster(&self, id: &str) -> Result<Arc<Cluster>, RpcError> {
        self.clusters
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| RpcError::new(codes::NOT_CONNECTED, format!("no connection {id:?}")))
    }

    /// Entry point for every decoded message.
    pub async fn handle(self: &Arc<Self>, msg: p::Incoming) {
        let Some(method) = msg.method.clone() else {
            // Responses from the client: we never send requests, so ignore.
            return;
        };
        if msg.jsonrpc.as_deref() != Some("2.0") {
            if let Some(id) = msg.id {
                self.send(p::response_err(id, &RpcError::new(codes::INVALID_REQUEST, "jsonrpc must be \"2.0\"")))
                    .await;
            }
            return;
        }
        // Notifications.
        let Some(id) = msg.id.clone() else {
            if method == m::CANCEL {
                if let Ok(c) = parse::<p::CancelParams>(msg.params) {
                    if let Some(h) = self.inflight.lock().await.remove(&id_key(&c.id)) {
                        h.abort();
                        self.send(p::response_err(c.id, &RpcError::new(codes::REQUEST_CANCELLED, "cancelled")))
                            .await;
                    }
                }
            }
            return;
        };
        // Requests run concurrently so a slow broker never blocks others.
        let st = self.clone();
        let key = id_key(&id);
        let mut inflight = self.inflight.lock().await;
        let task = tokio::spawn({
            let st = st.clone();
            let id = id.clone();
            let key = key.clone();
            async move {
                let res = st.dispatch(&method, msg.params).await;
                // Only respond if not cancelled meanwhile.
                if st.inflight.lock().await.remove(&key).is_some() {
                    let v = match res {
                        Ok(r) => p::response_ok(id, r),
                        Err(e) => p::response_err(id, &e),
                    };
                    st.send(v).await;
                }
            }
        });
        inflight.insert(key, task.abort_handle());
    }

    async fn dispatch(self: &Arc<Self>, method: &str, params: Value) -> R {
        match method {
            m::INITIALIZE => Ok(json!({
                "protocolVersion": p::PROTOCOL_VERSION,
                "serverInfo": {"name": "kafka-bridge", "version": env!("CARGO_PKG_VERSION")},
                "librdkafkaVersion": rdkafka_version(),
            })),
            m::SHUTDOWN => {
                self.stop_all_subs(None).await;
                // Respond first, then let main exit.
                let st = self.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    st.shutdown_signal.notify_one();
                });
                Ok(Value::Null)
            }
            m::CAPABILITIES => Ok(json!(CAPABILITIES)),
            m::CONNECT => self.connect(parse(params)?).await,
            m::DISCONNECT => {
                let r: p::ConnectionRef = parse(params)?;
                self.stop_all_subs(Some(&r.connection_id)).await;
                let existed = self.clusters.lock().await.remove(&r.connection_id).is_some();
                Ok(json!({"disconnected": existed}))
            }
            m::CONNECTIONS => {
                let cs = self.clusters.lock().await;
                let mut v: Vec<Value> = cs
                    .values()
                    .map(|c| json!({
                        "connectionId": c.config.id,
                        "bootstrapServers": c.config.bootstrap_servers,
                        "readOnly": c.config.read_only,
                        "environment": c.config.environment,
                    }))
                    .collect();
                v.sort_by_key(|x| x["connectionId"].as_str().unwrap_or("").to_string());
                Ok(json!(v))
            }
            m::CLUSTER_DESCRIBE => {
                let r: p::ConnectionRef = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                Ok(json!(c.describe_cluster().await.map_err(core_err)?))
            }
            m::BROKER_CONFIGS => {
                let r: p::BrokerRef = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                Ok(json!(c.broker_configs(r.broker_id).await.map_err(core_err)?))
            }
            m::TOPICS_LIST => {
                let r: p::TopicsListParams = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                Ok(json!(c.list_topics(r.include_internal, r.filter).await.map_err(core_err)?))
            }
            m::TOPIC_DESCRIBE => {
                let r: p::TopicRef = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                Ok(json!(c.describe_topic(r.topic).await.map_err(core_err)?))
            }
            m::TOPIC_CONFIGS => {
                let r: p::TopicRef = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                Ok(json!(c.topic_configs(&r.topic).await.map_err(core_err)?))
            }
            m::TOPIC_CREATE => {
                let r: p::TopicCreateParams = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                let res = c
                    .create_topic(&r.topic, r.partitions, r.replication_factor, &r.configs, r.validate_only)
                    .await
                    .map_err(core_err)?;
                Ok(json!({"results": res, "validateOnly": r.validate_only}))
            }
            m::TOPIC_DELETE => {
                let r: p::TopicDeleteParams = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                Ok(json!({"results": c.delete_topic(&r.topic, &r.confirm_name).await.map_err(core_err)?}))
            }
            m::TOPIC_ALTER_CONFIG => {
                let r: p::TopicAlterConfigParams = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                c.alter_topic_config(&r.topic, &r.configs, r.validate_only).await.map_err(core_err)
            }
            m::TOPIC_ADD_PARTITIONS => {
                let r: p::AddPartitionsParams = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                let res = c.add_partitions(&r.topic, r.total_count, r.validate_only).await.map_err(core_err)?;
                Ok(json!({"results": res, "validateOnly": r.validate_only}))
            }
            m::OFFSETS_FOR_TIME => {
                let r: p::OffsetsForTimeParams = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                Ok(json!(c.offsets_for_time(r.topic, r.timestamp).await.map_err(core_err)?))
            }
            m::GROUPS_LIST => {
                let r: p::ConnectionRef = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                Ok(json!(c.list_groups().await.map_err(core_err)?))
            }
            m::GROUP_DESCRIBE => {
                let r: p::GroupRef = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                Ok(json!(c.describe_group(r.group_id).await.map_err(core_err)?))
            }
            m::GROUP_DELETE => {
                let r: p::GroupDeleteParams = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                Ok(json!({"results": c.delete_group(&r.group_id, &r.confirm_name).await.map_err(core_err)?}))
            }
            m::PRODUCE => self.produce(parse(params)?).await,
            m::RECORDS_FETCH => {
                let r: p::FetchParams = parse(params)?;
                let c = self.cluster(&r.connection_id).await?;
                let limit = r.limit.clamp(1, 10_000);
                let timeout = Duration::from_millis(r.timeout_ms.unwrap_or(10_000).min(120_000));
                let res = c
                    .fetch(r.topic, r.partitions, position(&r.start_position), limit, timeout)
                    .await
                    .map_err(core_err)?;
                Ok(json!({
                    "records": res.records.iter().map(record_json).collect::<Vec<_>>(),
                    "truncated": res.truncated,
                    "timedOut": res.timed_out,
                }))
            }
            m::CONSUME_START => self.consume_start(parse(params)?).await,
            m::CONSUME_CREDIT => {
                let r: p::CreditParams = parse(params)?;
                let subs = self.subs.lock().await;
                let s = subs.get(&r.subscription_id).ok_or_else(unknown_sub)?;
                let room = (MAX_CREDIT as usize).saturating_sub(s.credit.available_permits());
                s.credit.add_permits((r.batches as usize).min(room));
                Ok(json!({"available": s.credit.available_permits()}))
            }
            m::CONSUME_STOP => {
                let r: p::SubscriptionRef = parse(params)?;
                let s = self.subs.lock().await.remove(&r.subscription_id).ok_or_else(unknown_sub)?;
                s.cancel.store(true, Ordering::SeqCst);
                Ok(json!({"subscriptionId": r.subscription_id, "state": "stopping"}))
            }
            other => Err(RpcError::new(codes::METHOD_NOT_FOUND, format!("unknown method {other}"))),
        }
    }

    async fn connect(&self, r: p::ConnectParams) -> R {
        if r.connection_id.is_empty() {
            return Err(RpcError::new(codes::INVALID_PARAMS, "connectionId must not be empty"));
        }
        let cfg = ConnectionConfig {
            id: r.connection_id.clone(),
            bootstrap_servers: r.bootstrap_servers,
            read_only: r.read_only,
            environment: r.environment,
            security_protocol: r.security_protocol,
            sasl_mechanism: r.sasl_mechanism,
            sasl_username: r.sasl_username,
            sasl_password: r.sasl_password,
            ssl_ca_location: r.ssl_ca_location,
            ssl_certificate_location: r.ssl_certificate_location,
            ssl_key_location: r.ssl_key_location,
            ssl_key_password: r.ssl_key_password,
            timeout: Duration::from_millis(r.timeout_ms.unwrap_or(10_000).clamp(100, 300_000)),
            properties: r.properties,
        };
        let (cluster, info) = Cluster::connect(cfg).await.map_err(core_err)?;
        // Replacing an existing connection stops its streams first.
        self.stop_all_subs(Some(&r.connection_id)).await;
        self.clusters.lock().await.insert(r.connection_id, cluster);
        Ok(json!(info))
    }

    async fn produce(&self, r: p::ProduceParams) -> R {
        let c = self.cluster(&r.connection_id).await?;
        let mut headers = Vec::new();
        for h in &r.headers {
            headers.push((h.key.clone(), decode_in(&h.value, h.encoding)?));
        }
        let req = ProduceRequest {
            topic: r.topic,
            partition: r.partition,
            key: decode_in(&r.key, r.encoding)?,
            value: decode_in(&r.value, r.encoding)?,
            headers,
            timestamp: r.timestamp,
        };
        Ok(json!(c.produce(req).await.map_err(core_err)?))
    }

    async fn consume_start(self: &Arc<Self>, r: p::ConsumeStartParams) -> R {
        let c = self.cluster(&r.connection_id).await?;
        let sub_id = match &r.subscription_id {
            Some(s) if s.is_empty() => {
                return Err(RpcError::new(codes::INVALID_PARAMS, "subscriptionId must not be empty"))
            }
            Some(s) => {
                if self.subs.lock().await.contains_key(s) {
                    return Err(RpcError::new(codes::INVALID_PARAMS, format!("subscription {s} already exists")));
                }
                s.clone()
            }
            None => format!("sub-{}", self.next_sub.fetch_add(1, Ordering::Relaxed)),
        };
        let credit = Arc::new(Semaphore::new(r.initial_credit.min(MAX_CREDIT) as usize));
        let cancel = Arc::new(AtomicBool::new(false));
        // Small channel: the stream thread blocks here when Emacs is slow.
        let (tx, mut rx) = mpsc::channel::<StreamEvent>(2);
        c.stream(
            r.topic.clone(),
            r.partitions,
            position(&r.start_position),
            StreamControl { batch_size: r.batch_size, credit: credit.clone(), cancel: cancel.clone(), sink: tx },
        )
        .await
        .map_err(core_err)?;
        self.subs.lock().await.insert(
            sub_id.clone(),
            Subscription { connection_id: r.connection_id, credit, cancel },
        );
        let st = self.clone();
        let sid = sub_id.clone();
        tokio::spawn(async move {
            let mut seq: u64 = 0;
            while let Some(ev) = rx.recv().await {
                let v = match ev {
                    StreamEvent::Batch(b) => {
                        seq += 1;
                        p::notification(m::NOTIFY_RECORDS, json!({
                            "subscriptionId": sid, "seq": seq,
                            "records": b.iter().map(record_json).collect::<Vec<_>>(),
                        }))
                    }
                    StreamEvent::Paused => state_note(&sid, "paused", None),
                    StreamEvent::Resumed => state_note(&sid, "active", None),
                    StreamEvent::Error(e) => state_note(&sid, "error", Some(e)),
                    StreamEvent::Stopped => {
                        st.subs.lock().await.remove(&sid);
                        st.send(state_note(&sid, "cancelled", None)).await;
                        break;
                    }
                };
                st.send(v).await;
            }
        });
        self.send(state_note(&sub_id, "active", None)).await;
        Ok(json!({"subscriptionId": sub_id, "state": "starting"}))
    }

    pub async fn stop_all_subs(&self, conn: Option<&str>) {
        let mut subs = self.subs.lock().await;
        subs.retain(|_, s| {
            let hit = conn.is_none_or(|c| c == s.connection_id);
            if hit {
                s.cancel.store(true, Ordering::SeqCst);
            }
            !hit
        });
    }
}

fn unknown_sub() -> RpcError {
    RpcError::new(codes::UNKNOWN_SUBSCRIPTION, "unknown subscription")
}

fn state_note(sid: &str, state: &str, msg: Option<String>) -> Value {
    p::notification(m::NOTIFY_STATE, json!({"subscriptionId": sid, "state": state, "message": msg}))
}

fn rdkafka_version() -> String {
    rdkafka::util::get_rdkafka_version().1
}
