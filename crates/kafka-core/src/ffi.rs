//! Safe admin APIs missing from rust-rdkafka 0.39.
//!
//! Native objects never cross a thread boundary. Each request owns a private
//! queue and event, and a cloned Arc keeps the client alive even if its future
//! is cancelled. All borrowed result data is copied before the event drops.
use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::Arc;

use crate::{Cluster, CoreError, Result};
use rdkafka::error::KafkaError;
use rdkafka_sys::*;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

mod acls;
mod groups;
mod scram;
pub use acls::Acl;
pub use groups::{OffsetSelection, ResetSpec};
pub use scram::ScramChange;

struct Owned<T>(*mut T, unsafe extern "C" fn(*mut T));
impl<T> Owned<T> {
    fn new(p: *mut T, destroy: unsafe extern "C" fn(*mut T)) -> Result<Self> {
        if p.is_null() {
            Err(CoreError::Internal("native admin allocation failed".into()))
        } else {
            Ok(Self(p, destroy))
        }
    }
}
impl<T> Drop for Owned<T> {
    fn drop(&mut self) {
        unsafe { (self.1)(self.0) }
    }
}
fn cstring(s: &str) -> Result<CString> {
    CString::new(s).map_err(|_| CoreError::Invalid("NUL bytes are not allowed".into()))
}
fn confirm(name: &str, supplied: &str) -> Result<()> {
    if name == supplied {
        Ok(())
    } else {
        Err(CoreError::Confirmation {
            expected: name.into(),
        })
    }
}
fn check(code: rd_kafka_resp_err_t) -> Result<()> {
    if code == rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
        Ok(())
    } else {
        Err(CoreError::Kafka(KafkaError::AdminOp(code.into())))
    }
}
unsafe fn error(p: *const rd_kafka_error_t) -> Result<()> {
    if p.is_null() {
        Ok(())
    } else {
        check(rd_kafka_error_code(p))
    }
}
unsafe fn owned_error(p: *mut rd_kafka_error_t) -> Result<()> {
    if p.is_null() {
        return Ok(());
    }
    let guard = Owned::new(p, rd_kafka_error_destroy)?;
    error(guard.0)
}
unsafe fn text(p: *const std::os::raw::c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}
// librdkafka represents an empty array using NULL. Never create a Rust slice
// from NULL, even with length zero.
unsafe fn array<'a, T>(p: *const T, n: usize) -> &'a [T] {
    if n == 0 || p.is_null() {
        &[]
    } else {
        std::slice::from_raw_parts(p, n)
    }
}
unsafe fn node(p: *const rd_kafka_Node_t) -> Value {
    if p.is_null() {
        Value::Null
    } else {
        json!({"id":rd_kafka_Node_id(p),"host":text(rd_kafka_Node_host(p)),
            "port":rd_kafka_Node_port(p),"rack":text(rd_kafka_Node_rack(p))})
    }
}
struct Call {
    rk: *mut rd_kafka_t,
    queue: Owned<rd_kafka_queue_t>,
    options: Owned<rd_kafka_AdminOptions_t>,
    timeout: i32,
}
impl Call {
    unsafe fn new(
        c: &Cluster,
        op: rd_kafka_admin_op_t,
        validate: bool,
        auth: bool,
    ) -> Result<Self> {
        let rk = c.admin_client().inner().native_ptr();
        let queue = Owned::new(rd_kafka_queue_new(rk), rd_kafka_queue_destroy)?;
        let options = Owned::new(
            rd_kafka_AdminOptions_new(rk, op),
            rd_kafka_AdminOptions_destroy,
        )?;
        let timeout = c.timeout().as_millis().min(i32::MAX as u128 - 1000) as i32;
        let mut buf = [0; 512];
        check(rd_kafka_AdminOptions_set_request_timeout(
            options.0,
            timeout,
            buf.as_mut_ptr(),
            buf.len(),
        ))?;
        if validate {
            check(rd_kafka_AdminOptions_set_validate_only(
                options.0,
                1,
                buf.as_mut_ptr(),
                buf.len(),
            ))?;
        }
        if auth {
            owned_error(rd_kafka_AdminOptions_set_include_authorized_operations(
                options.0, 1,
            ))?;
        }
        Ok(Self {
            rk,
            queue,
            options,
            timeout,
        })
    }
    unsafe fn wait(&self) -> Result<Owned<rd_kafka_event_t>> {
        let p = rd_kafka_queue_poll(self.queue.0, self.timeout + 1000);
        if p.is_null() {
            return Err(CoreError::Timeout("admin response".into()));
        }
        let event = Owned::new(p, rd_kafka_event_destroy)?;
        check(rd_kafka_event_error(event.0))?;
        Ok(event)
    }
}
impl Cluster {
    async fn native<F>(
        self: &Arc<Self>,
        op: rd_kafka_admin_op_t,
        validate: bool,
        auth: bool,
        f: F,
    ) -> Result<Value>
    where
        F: FnOnce(&Call) -> Result<Value> + Send + 'static,
    {
        let c = self.clone();
        tokio::task::spawn_blocking(move || {
            // The client and every native argument outlive the request/event.
            let call = unsafe { Call::new(&c, op, validate, auth)? };
            f(&call)
        })
        .await
        .map_err(|e| CoreError::Internal(e.to_string()))?
    }

    pub async fn cluster_info(self: &Arc<Self>) -> Result<Value> {
        self.native(rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER, false, true, |c| unsafe {
            rd_kafka_DescribeCluster(c.rk, c.options.0, c.queue.0);
            let ev = c.wait()?;
            let r = rd_kafka_event_DescribeCluster_result(ev.0);
            if r.is_null() { return Err(CoreError::Internal("unexpected admin event".into())); }
            let mut n = 0;
            let p = rd_kafka_DescribeCluster_result_nodes(r, &mut n);
            let nodes: Vec<_> = array(p,n).iter().map(|p| node(*p)).collect();
            let p = rd_kafka_DescribeCluster_result_authorized_operations(r, &mut n);
            let ops: Vec<_> = array(p,n).iter().map(|p| text(rd_kafka_AclOperation_name(*p))).collect();
            Ok(json!({"clusterId":text(rd_kafka_DescribeCluster_result_cluster_id(r)),
                "controller":node(rd_kafka_DescribeCluster_result_controller(r)), "nodes":nodes,"authorizedOperations":ops}))
        }).await
    }

    pub async fn incremental_alter(
        self: &Arc<Self>,
        resource_type: String,
        name: String,
        changes: Vec<ConfigChange>,
        validate: bool,
    ) -> Result<Value> {
        self.guard_write()?;
        use rd_kafka_ResourceType_t::*;
        let ty = match resource_type.as_str() {
            "topic" if !name.is_empty() => RD_KAFKA_RESOURCE_TOPIC,
            "broker" if name.parse::<i32>().is_ok_and(|n| n >= 0) => RD_KAFKA_RESOURCE_BROKER,
            _ => {
                return Err(CoreError::Invalid(
                    "resourceType must be topic or broker with a valid name".into(),
                ))
            }
        };
        if changes.is_empty() {
            return Err(CoreError::Invalid("changes must not be empty".into()));
        }
        self.native(rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_INCREMENTALALTERCONFIGS, validate, false, move |c| unsafe {
            let name = cstring(&name)?;
            let mut resource = Owned::new(rd_kafka_ConfigResource_new(ty, name.as_ptr()), rd_kafka_ConfigResource_destroy)?;
            for change in changes {
                use rd_kafka_AlterConfigOpType_t::*;
                let op = match change.operation.as_str() {
                    "set" => RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET,
                    "delete" => RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE,
                    "append" => RD_KAFKA_ALTER_CONFIG_OP_TYPE_APPEND,
                    "subtract" => RD_KAFKA_ALTER_CONFIG_OP_TYPE_SUBTRACT,
                    _ => return Err(CoreError::Invalid("invalid incremental config operation".into())),
                };
                if change.name.is_empty() || (change.operation == "delete") != change.value.is_none() {
                    return Err(CoreError::Invalid("config name required, delete takes no value, other operations require value".into()));
                }
                let key = cstring(&change.name)?;
                let value = change.value.as_deref().map(cstring).transpose()?;
                owned_error(rd_kafka_ConfigResource_add_incremental_config(resource.0, key.as_ptr(), op, value.as_ref().map_or(ptr::null(), |v| v.as_ptr())))?;
            }
            rd_kafka_IncrementalAlterConfigs(c.rk, &mut resource.0, 1, c.options.0, c.queue.0);
            let ev = c.wait()?;
            let mut n = 0;
            let r = rd_kafka_event_IncrementalAlterConfigs_result(ev.0);
            if r.is_null() { return Err(CoreError::Internal("unexpected admin event".into())); }
            let p = rd_kafka_IncrementalAlterConfigs_result_resources(r, &mut n);
            for p in array(p,n) { check(rd_kafka_ConfigResource_error(*p))?; }
            Ok(json!({"applied":!validate,"validateOnly":validate}))
        }).await
    }

    pub async fn delete_records_before(
        &self,
        topic: String,
        offsets: Vec<PartitionTarget>,
        confirmation: String,
    ) -> Result<Value> {
        self.guard_write()?;
        confirm(&topic, &confirmation)?;
        if offsets.is_empty() {
            return Err(CoreError::Invalid("offsets required".into()));
        }
        let mut list = rdkafka::TopicPartitionList::new();
        for p in offsets {
            if p.partition < 0 || p.offset < 0 {
                return Err(CoreError::Invalid(
                    "partition and offset must be nonnegative".into(),
                ));
            }
            list.add_partition_offset(&topic, p.partition, rdkafka::Offset::Offset(p.offset))?;
        }
        let result = self
            .admin_client()
            .delete_records(&list, &self.admin_opts(false))
            .await?;
        let mut out = Vec::new();
        for e in result.elements() {
            e.error()?;
            out.push(json!({"partition":e.partition(),"lowWatermark":e.offset().to_raw()}));
        }
        Ok(json!(out))
    }
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigChange {
    pub name: String,
    pub operation: String,
    pub value: Option<String>,
}
#[derive(Deserialize, Serialize)]
pub struct PartitionTarget {
    pub partition: i32,
    pub offset: i64,
}
