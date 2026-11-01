//! Lane-owned RPC method groups. `dispatch.rs` tries each in turn for
//! methods it does not handle itself, so lanes never edit the same match.

use std::sync::Arc;

use serde_json::Value;

use crate::dispatch::{State, R};

pub mod admin;
pub mod connect;
pub mod registry;
pub mod txn;
pub mod wire;

pub async fn dispatch(st: &Arc<State>, method: &str, params: Value) -> Option<R> {
    if let Some(r) = admin::dispatch(st, method, params.clone()).await {
        return Some(r);
    }
    if let Some(r) = wire::dispatch(st, method, params.clone()).await {
        return Some(r);
    }
    if let Some(r) = registry::dispatch(st, method, params.clone()).await {
        return Some(r);
    }
    if let Some(r) = connect::dispatch(st, method, params.clone()).await {
        return Some(r);
    }
    txn::dispatch(st, method, params).await
}
