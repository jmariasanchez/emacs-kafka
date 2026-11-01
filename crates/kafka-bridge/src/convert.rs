//! Converts between core types and wire JSON, and maps errors to RPC codes.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use kafka_core::errors::CoreError;
use kafka_core::records::{Position, RawRecord};
use kafka_rpc_protocol::{codes, Encoding, RpcError, StartPosition};
use serde_json::{json, Value};

pub fn core_err(e: CoreError) -> RpcError {
    match e {
        CoreError::Security { code, message } => RpcError::new(codes::KAFKA, message)
            .with_data(json!({"kafkaCode": code})),
        CoreError::ReadOnly => RpcError::new(codes::READ_ONLY, e.to_string()),
        CoreError::Confirmation { .. } => RpcError::new(codes::CONFIRMATION_REQUIRED, e.to_string()),
        CoreError::Invalid(_) => RpcError::new(codes::INVALID_PARAMS, e.to_string()),
        CoreError::NotFound(_) => RpcError::new(codes::KAFKA, e.to_string()),
        CoreError::Timeout(_) => RpcError::new(codes::TIMEOUT, e.to_string()),
        CoreError::Partial(ref r) => {
            let data = json!({ "results": r });
            RpcError::new(codes::PARTIAL_FAILURE, e.to_string()).with_data(data)
        }
        CoreError::Kafka(ref k) => {
            let data = json!({ "kafkaCode": k.rdkafka_error_code().map(|c| format!("{c:?}")) });
            RpcError::new(codes::KAFKA, e.to_string()).with_data(data)
        }
        CoreError::Internal(_) => RpcError::new(codes::INTERNAL, e.to_string()),
    }
}

pub fn position(p: &StartPosition) -> Position {
    match p {
        StartPosition::Earliest => Position::Earliest,
        StartPosition::Latest => Position::Latest,
        StartPosition::Offset { offset } => Position::Offset(*offset),
        StartPosition::Timestamp { timestamp } => Position::Timestamp(*timestamp),
        StartPosition::Tail { count } => Position::Tail(*count),
    }
}

/// Decode an incoming payload according to its declared encoding.
pub fn decode_in(s: &Option<String>, enc: Encoding) -> Result<Option<Vec<u8>>, RpcError> {
    match (s, enc) {
        (None, _) => Ok(None),
        (Some(s), Encoding::Text) => Ok(Some(s.as_bytes().to_vec())),
        (Some(s), Encoding::Base64) => B64
            .decode(s)
            .map(Some)
            .map_err(|e| RpcError::new(codes::DECODE, format!("invalid base64: {e}"))),
    }
}

/// Encode bytes for the wire. UTF-8 is sent as text; anything else as base64.
/// Both forms carry an explicit `encoding` tag, so the client never guesses.
pub fn bytes_out(b: &Option<Vec<u8>>) -> Value {
    match b {
        None => Value::Null,
        Some(b) => match std::str::from_utf8(b) {
            Ok(s) => json!({"encoding": "text", "data": s, "size": b.len()}),
            Err(_) => json!({"encoding": "base64", "data": B64.encode(b), "size": b.len()}),
        },
    }
}

pub fn record_json(r: &RawRecord) -> Value {
    json!({
        "topic": r.topic,
        "partition": r.partition,
        "offset": r.offset,
        "timestamp": r.timestamp,
        "timestampType": r.timestamp_type,
        "key": bytes_out(&r.key),
        "value": bytes_out(&r.value),
        "headers": r.headers.iter().map(|(k, v)| json!({"key": k, "value": bytes_out(v)})).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_safe() {
        let v = bytes_out(&Some(vec![0xff, 0x00, 0x41]));
        assert_eq!(v["encoding"], "base64");
        let back = decode_in(&Some(v["data"].as_str().unwrap().into()), Encoding::Base64).unwrap();
        assert_eq!(back, Some(vec![0xff, 0x00, 0x41]));
    }

    #[test]
    fn text_passthrough_and_null() {
        assert_eq!(bytes_out(&Some(b"hi".to_vec()))["data"], "hi");
        assert_eq!(bytes_out(&Some(vec![]))["encoding"], "text");
        assert!(bytes_out(&None).is_null());
    }

    #[test]
    fn bad_base64_is_decode_error() {
        let e = decode_in(&Some("!!".into()), Encoding::Base64).unwrap_err();
        assert_eq!(e.code, codes::DECODE);
    }

    #[test]
    fn partial_carries_results() {
        let e = core_err(CoreError::Partial(vec![kafka_core::errors::ResourceResult::err("t", "x")]));
        assert_eq!(e.code, codes::PARTIAL_FAILURE);
        assert_eq!(e.data.unwrap()["results"][0]["name"], "t");
    }
}
