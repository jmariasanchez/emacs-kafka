//! RPC methods for the producer lane (transactions, batch produce). Return
//! `None` for other methods.

use std::sync::Arc;

use serde_json::Value;

use crate::dispatch::{State, R};

pub async fn dispatch(_st: &Arc<State>, method: &str, _params: Value) -> Option<R> {
    #[allow(clippy::match_single_binding)]
    match method {
        _ => None,
    }
}
