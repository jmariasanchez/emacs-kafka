use super::*;

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Acl {
    pub resource_type: Option<String>,
    pub name: Option<String>,
    pub pattern_type: Option<String>,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: Option<String>,
    pub permission: Option<String>,
}
impl Acl {
    unsafe fn native(&self, filter: bool) -> Result<Owned<rd_kafka_AclBinding_t>> {
        use rd_kafka_AclOperation_t::*;
        use rd_kafka_AclPermissionType_t::*;
        use rd_kafka_ResourcePatternType_t::*;
        use rd_kafka_ResourceType_t::*;
        let invalid =
            || CoreError::Invalid("invalid ACL enum or missing required create field".into());
        let resource = match self
            .resource_type
            .as_deref()
            .unwrap_or("any")
            .to_lowercase()
            .as_str()
        {
            "any" if filter => RD_KAFKA_RESOURCE_ANY,
            "topic" => RD_KAFKA_RESOURCE_TOPIC,
            "group" => RD_KAFKA_RESOURCE_GROUP,
            "broker" | "cluster" => RD_KAFKA_RESOURCE_BROKER,
            "transactional-id" => RD_KAFKA_RESOURCE_TRANSACTIONAL_ID,
            _ => return Err(invalid()),
        };
        let pattern = match self
            .pattern_type
            .as_deref()
            .unwrap_or(if filter { "any" } else { "literal" })
            .to_lowercase()
            .as_str()
        {
            "any" if filter => RD_KAFKA_RESOURCE_PATTERN_ANY,
            "match" if filter => RD_KAFKA_RESOURCE_PATTERN_MATCH,
            "literal" => RD_KAFKA_RESOURCE_PATTERN_LITERAL,
            "prefixed" => RD_KAFKA_RESOURCE_PATTERN_PREFIXED,
            _ => return Err(invalid()),
        };
        let op = match self
            .operation
            .as_deref()
            .unwrap_or("any")
            .to_lowercase()
            .replace('_', "-")
            .as_str()
        {
            "any" if filter => RD_KAFKA_ACL_OPERATION_ANY,
            "all" => RD_KAFKA_ACL_OPERATION_ALL,
            "read" => RD_KAFKA_ACL_OPERATION_READ,
            "write" => RD_KAFKA_ACL_OPERATION_WRITE,
            "create" => RD_KAFKA_ACL_OPERATION_CREATE,
            "delete" => RD_KAFKA_ACL_OPERATION_DELETE,
            "alter" => RD_KAFKA_ACL_OPERATION_ALTER,
            "describe" => RD_KAFKA_ACL_OPERATION_DESCRIBE,
            "cluster-action" => RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION,
            "describe-configs" => RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS,
            "alter-configs" => RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS,
            "idempotent-write" => RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE,
            _ => return Err(invalid()),
        };
        let permission = match self
            .permission
            .as_deref()
            .unwrap_or("any")
            .to_lowercase()
            .as_str()
        {
            "any" if filter => RD_KAFKA_ACL_PERMISSION_TYPE_ANY,
            "allow" => RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW,
            "deny" => RD_KAFKA_ACL_PERMISSION_TYPE_DENY,
            _ => return Err(invalid()),
        };
        if !filter
            && [
                self.name.as_ref(),
                self.principal.as_ref(),
                self.host.as_ref(),
            ]
            .iter()
            .any(|s| s.is_none_or(|s| s.is_empty()))
        {
            return Err(invalid());
        }
        let name = self.name.as_deref().map(cstring).transpose()?;
        let principal = self.principal.as_deref().map(cstring).transpose()?;
        let host = self.host.as_deref().map(cstring).transpose()?;
        let mut buf = [0; 512];
        let make = if filter {
            rd_kafka_AclBindingFilter_new
        } else {
            rd_kafka_AclBinding_new
        };
        let p = make(
            resource,
            name.as_ref().map_or(ptr::null(), |v| v.as_ptr()),
            pattern,
            principal.as_ref().map_or(ptr::null(), |v| v.as_ptr()),
            host.as_ref().map_or(ptr::null(), |v| v.as_ptr()),
            op,
            permission,
            buf.as_mut_ptr(),
            buf.len(),
        );
        if p.is_null() {
            return Err(CoreError::Invalid(text(buf.as_ptr())));
        }
        Owned::new(p, rd_kafka_AclBinding_destroy)
    }
}
unsafe fn acl(p: *const rd_kafka_AclBinding_t) -> Result<Value> {
    error(rd_kafka_AclBinding_error(p))?;
    Ok(
        json!({"resourceType":text(rd_kafka_ResourceType_name(rd_kafka_AclBinding_restype(p))).to_lowercase(),
        "name":text(rd_kafka_AclBinding_name(p)),"patternType":text(rd_kafka_ResourcePatternType_name(rd_kafka_AclBinding_resource_pattern_type(p))).to_lowercase(),
        "principal":text(rd_kafka_AclBinding_principal(p)),"host":text(rd_kafka_AclBinding_host(p)),
        "operation":text(rd_kafka_AclOperation_name(rd_kafka_AclBinding_operation(p))).to_lowercase().replace('_',"-"),
        "permission":text(rd_kafka_AclPermissionType_name(rd_kafka_AclBinding_permission_type(p))).to_lowercase()}),
    )
}
impl Cluster {
    pub async fn describe_acls(self: &Arc<Self>, filter: Acl) -> Result<Value> {
        self.native(
            rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBEACLS,
            false,
            false,
            move |c| unsafe {
                let f = filter.native(true)?;
                rd_kafka_DescribeAcls(c.rk, f.0, c.options.0, c.queue.0);
                let ev = c.wait()?;
                let r = rd_kafka_event_DescribeAcls_result(ev.0);
                if r.is_null() {
                    return Err(CoreError::Internal("unexpected ACL event".into()));
                }
                let mut n = 0;
                let p = rd_kafka_DescribeAcls_result_acls(r, &mut n);
                Ok(json!(array(p, n)
                    .iter()
                    .map(|p| acl(*p))
                    .collect::<Result<Vec<_>>>()?))
            },
        )
        .await
    }
    pub async fn create_acl(self: &Arc<Self>, binding: Acl) -> Result<Value> {
        self.guard_write()?;
        self.native(
            rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_CREATEACLS,
            false,
            false,
            move |c| unsafe {
                let mut a = binding.native(false)?;
                rd_kafka_CreateAcls(c.rk, &mut a.0, 1, c.options.0, c.queue.0);
                let ev = c.wait()?;
                let r = rd_kafka_event_CreateAcls_result(ev.0);
                if r.is_null() {
                    return Err(CoreError::Internal("unexpected ACL event".into()));
                }
                let mut n = 0;
                let p = rd_kafka_CreateAcls_result_acls(r, &mut n);
                for p in array(p, n) {
                    error(rd_kafka_acl_result_error(*p))?;
                }
                Ok(json!({"created":true}))
            },
        )
        .await
    }
    /// Deletion requires an exact resource name in the filter and confirmation.
    /// Wildcard-resource ACLs use the literal name "*". Nameless bulk deletion
    /// is intentionally unsupported.
    pub async fn delete_acls(self: &Arc<Self>, filter: Acl, confirmation: String) -> Result<Value> {
        self.guard_write()?;
        let name = filter
            .name
            .as_deref()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| CoreError::Invalid("ACL deletion requires a resource name".into()))?;
        confirm(name, &confirmation)?;
        self.native(
            rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DELETEACLS,
            false,
            false,
            move |c| unsafe {
                let mut a = filter.native(true)?;
                rd_kafka_DeleteAcls(c.rk, &mut a.0, 1, c.options.0, c.queue.0);
                let ev = c.wait()?;
                let r = rd_kafka_event_DeleteAcls_result(ev.0);
                if r.is_null() {
                    return Err(CoreError::Internal("unexpected ACL event".into()));
                }
                let mut n = 0;
                let p = rd_kafka_DeleteAcls_result_responses(r, &mut n);
                let mut out = Vec::new();
                for p in array(p, n) {
                    error(rd_kafka_DeleteAcls_result_response_error(*p))?;
                    let mut count = 0;
                    let matches = rd_kafka_DeleteAcls_result_response_matching_acls(*p, &mut count);
                    for a in array(matches, count) {
                        out.push(acl(*a)?);
                    }
                }
                Ok(json!(out))
            },
        )
        .await
    }
}
