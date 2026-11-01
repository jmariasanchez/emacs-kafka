//! Typed JSON-RPC 2.0 contract between `kafka.el` and `kafka-bridge`.
//!
//! This crate defines *application* messages. It is not an implementation of
//! the Kafka wire protocol. See `docs/RPC.md` for the human-readable contract.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Protocol version advertised by `kafka/initialize`. Bump on breaking change.
pub const PROTOCOL_VERSION: u32 = 1;

/// Method names. Kept as constants so both the dispatcher and tests agree.
pub mod methods {
    pub const INITIALIZE: &str = "kafka/initialize";
    pub const SHUTDOWN: &str = "kafka/shutdown";
    pub const CANCEL: &str = "$/cancelRequest";
    pub const CONNECT: &str = "kafka/connect";
    pub const DISCONNECT: &str = "kafka/disconnect";
    pub const CONNECTIONS: &str = "kafka/connections";
    pub const CLUSTER_DESCRIBE: &str = "kafka/cluster/describe";
    pub const BROKER_CONFIGS: &str = "kafka/broker/configs";
    pub const TOPICS_LIST: &str = "kafka/topics/list";
    pub const TOPIC_DESCRIBE: &str = "kafka/topic/describe";
    pub const TOPIC_CONFIGS: &str = "kafka/topic/configs";
    pub const TOPIC_CREATE: &str = "kafka/topic/create";
    pub const TOPIC_DELETE: &str = "kafka/topic/delete";
    pub const TOPIC_ALTER_CONFIG: &str = "kafka/topic/alterConfig";
    pub const TOPIC_ADD_PARTITIONS: &str = "kafka/topic/addPartitions";
    pub const OFFSETS_FOR_TIME: &str = "kafka/offsets/forTime";
    pub const GROUPS_LIST: &str = "kafka/groups/list";
    pub const GROUP_DESCRIBE: &str = "kafka/group/describe";
    pub const GROUP_DELETE: &str = "kafka/group/delete";
    pub const PRODUCE: &str = "kafka/produce";
    pub const RECORDS_FETCH: &str = "kafka/records/fetch";
    pub const CONSUME_START: &str = "kafka/consume/start";
    pub const CONSUME_CREDIT: &str = "kafka/consume/credit";
    pub const CONSUME_STOP: &str = "kafka/consume/stop";
    pub const CAPABILITIES: &str = "kafka/capabilities";

    /// Server -> client notifications.
    pub const NOTIFY_RECORDS: &str = "kafka/consume/records";
    pub const NOTIFY_STATE: &str = "kafka/consume/state";
    pub const NOTIFY_LOG: &str = "kafka/log";
}

/// Error codes. JSON-RPC reserves -32768..-32000; application codes are positive.
pub mod codes {
    pub const PARSE_ERROR: i64 = -32700;
    pub const INVALID_REQUEST: i64 = -32600;
    pub const METHOD_NOT_FOUND: i64 = -32601;
    pub const INVALID_PARAMS: i64 = -32602;
    pub const INTERNAL: i64 = -32603;
    pub const REQUEST_CANCELLED: i64 = -32800;
    pub const KAFKA: i64 = 1000;
    pub const NOT_CONNECTED: i64 = 1001;
    pub const READ_ONLY: i64 = 1002;
    pub const CONFIRMATION_REQUIRED: i64 = 1003;
    pub const UNKNOWN_SUBSCRIPTION: i64 = 1004;
    pub const PARTIAL_FAILURE: i64 = 1005;
    pub const DECODE: i64 = 1006;
    pub const TIMEOUT: i64 = 1007;
    pub const UNSUPPORTED: i64 = 1008;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), data: None }
    }
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }
}

/// An incoming message: request (has id) or notification (no id).
#[derive(Debug, Clone, Deserialize)]
pub struct Incoming {
    #[serde(default)]
    pub jsonrpc: Option<String>,
    #[serde(default)]
    pub id: Option<Value>,
    pub method: Option<String>,
    #[serde(default)]
    pub params: Value,
}

pub fn response_ok(id: Value, result: Value) -> Value {
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": result})
}

pub fn response_err(id: Value, err: &RpcError) -> Value {
    serde_json::json!({"jsonrpc": "2.0", "id": id, "error": err})
}

pub fn notification(method: &str, params: Value) -> Value {
    serde_json::json!({"jsonrpc": "2.0", "method": method, "params": params})
}

// ---------------------------------------------------------------------------
// Parameter types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectParams {
    pub connection_id: String,
    pub bootstrap_servers: String,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub environment: Option<String>,
    #[serde(default)]
    pub security_protocol: Option<String>,
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    #[serde(default)]
    pub sasl_username: Option<String>,
    /// Secret. Never logged or echoed back.
    #[serde(default)]
    pub sasl_password: Option<String>,
    #[serde(default)]
    pub ssl_ca_location: Option<String>,
    #[serde(default)]
    pub ssl_certificate_location: Option<String>,
    #[serde(default)]
    pub ssl_key_location: Option<String>,
    /// Secret. Never logged or echoed back.
    #[serde(default)]
    pub ssl_key_password: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    /// Extra librdkafka properties (advanced).
    #[serde(default)]
    pub properties: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionRef {
    pub connection_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicRef {
    pub connection_id: String,
    pub topic: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicsListParams {
    pub connection_id: String,
    #[serde(default)]
    pub include_internal: bool,
    #[serde(default)]
    pub filter: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicCreateParams {
    pub connection_id: String,
    pub topic: String,
    pub partitions: i32,
    pub replication_factor: i32,
    #[serde(default)]
    pub configs: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub validate_only: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicDeleteParams {
    pub connection_id: String,
    pub topic: String,
    /// Must equal `topic` exactly. Guards against accidental deletion.
    pub confirm_name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicAlterConfigParams {
    pub connection_id: String,
    pub topic: String,
    pub configs: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub validate_only: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddPartitionsParams {
    pub connection_id: String,
    pub topic: String,
    pub total_count: usize,
    #[serde(default)]
    pub validate_only: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerRef {
    pub connection_id: String,
    pub broker_id: i32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OffsetsForTimeParams {
    pub connection_id: String,
    pub topic: String,
    /// Milliseconds since epoch.
    pub timestamp: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupRef {
    pub connection_id: String,
    pub group_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDeleteParams {
    pub connection_id: String,
    pub group_id: String,
    pub confirm_name: String,
}

/// Payload encoding on the wire. Kafka bytes are opaque, so binary-safe base64
/// is always available. `text` is accepted only if valid UTF-8.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    #[default]
    Text,
    Base64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    pub key: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub encoding: Encoding,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProduceParams {
    pub connection_id: String,
    pub topic: String,
    #[serde(default)]
    pub partition: Option<i32>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub encoding: Encoding,
    #[serde(default)]
    pub headers: Vec<Header>,
    #[serde(default)]
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum StartPosition {
    Earliest,
    #[default]
    Latest,
    Offset { offset: i64 },
    Timestamp { timestamp: i64 },
    /// N records before the end of each partition.
    Tail { count: i64 },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchParams {
    pub connection_id: String,
    pub topic: String,
    #[serde(default)]
    pub partitions: Option<Vec<i32>>,
    #[serde(default)]
    pub start_position: StartPosition,
    #[serde(default = "default_fetch_limit")]
    pub limit: usize,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

fn default_fetch_limit() -> usize {
    100
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeStartParams {
    pub connection_id: String,
    /// Optional client-chosen id, so the client can register a handler before
    /// the first notification can possibly arrive. Must be unique.
    #[serde(default)]
    pub subscription_id: Option<String>,
    pub topic: String,
    #[serde(default)]
    pub partitions: Option<Vec<i32>>,
    #[serde(default)]
    pub start_position: StartPosition,
    /// Max records per notification batch.
    #[serde(default = "default_batch")]
    pub batch_size: usize,
    /// Initial credit: number of batches the client is ready to receive.
    #[serde(default = "default_credit")]
    pub initial_credit: u32,
}

fn default_batch() -> usize {
    100
}
fn default_credit() -> u32 {
    4
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreditParams {
    pub subscription_id: String,
    #[serde(default = "one")]
    pub batches: u32,
}

fn one() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionRef {
    pub subscription_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CancelParams {
    pub id: Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_position_roundtrip() {
        let p: StartPosition = serde_json::from_str(r#"{"type":"tail","count":5}"#).unwrap();
        assert_eq!(p, StartPosition::Tail { count: 5 });
        let p: StartPosition = serde_json::from_str(r#"{"type":"latest"}"#).unwrap();
        assert_eq!(p, StartPosition::Latest);
    }

    #[test]
    fn connect_params_defaults() {
        let p: ConnectParams =
            serde_json::from_str(r#"{"connectionId":"x","bootstrapServers":"b:9092"}"#).unwrap();
        assert!(!p.read_only);
        assert!(p.sasl_password.is_none());
    }

    #[test]
    fn error_serializes_without_null_data() {
        let v = response_err(1.into(), &RpcError::new(codes::READ_ONLY, "no"));
        assert!(v["error"].get("data").is_none());
    }
}
