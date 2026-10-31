use rdkafka::error::KafkaError;
use thiserror::Error;

/// Errors produced by `kafka-core`.
///
/// Transport/broker failures (`Kafka`) are kept distinct from local validation
/// failures so callers can present them differently.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("kafka error: {0}")]
    Kafka(#[from] KafkaError),
    #[error("invalid argument: {0}")]
    Invalid(String),
    #[error("operation refused: connection is read-only")]
    ReadOnly,
    #[error("confirmation mismatch: expected exact name {expected:?}")]
    Confirmation { expected: String },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("operation partially failed: {0:?}")]
    Partial(Vec<ResourceResult>),
    #[error("timed out: {0}")]
    Timeout(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, CoreError>;

/// Per-resource outcome for multi-resource admin operations, which Kafka does
/// not execute transactionally.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ResourceResult {
    pub name: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ResourceResult {
    pub fn ok(name: impl Into<String>) -> Self {
        Self { name: name.into(), ok: true, error: None }
    }
    pub fn err(name: impl Into<String>, e: impl ToString) -> Self {
        Self { name: name.into(), ok: false, error: Some(e.to_string()) }
    }
}

/// Collapse a list of per-resource outcomes into `Ok` or `Partial`.
pub fn check_all(results: Vec<ResourceResult>) -> Result<Vec<ResourceResult>> {
    if results.iter().all(|r| r.ok) {
        Ok(results)
    } else {
        Err(CoreError::Partial(results))
    }
}
