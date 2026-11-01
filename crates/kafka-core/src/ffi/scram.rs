use super::*;

// Intentionally no Debug or Serialize: passwords must never be echoed.
#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "camelCase")]
pub enum ScramChange {
    Upsert {
        user: String,
        mechanism: String,
        password: String,
        #[serde(default = "iterations")]
        iterations: i32,
    },
    Delete {
        user: String,
        mechanism: String,
        #[serde(rename = "confirmName")]
        confirm_name: String,
    },
}
fn iterations() -> i32 {
    4096
}
fn mechanism(s: &str) -> Result<rd_kafka_ScramMechanism_t> {
    use rd_kafka_ScramMechanism_t::*;
    match s {
        "SCRAM-SHA-256" => Ok(RD_KAFKA_SCRAM_MECHANISM_SHA_256),
        "SCRAM-SHA-512" => Ok(RD_KAFKA_SCRAM_MECHANISM_SHA_512),
        _ => Err(CoreError::Invalid(
            "mechanism must be SCRAM-SHA-256 or SCRAM-SHA-512".into(),
        )),
    }
}
impl Cluster {
    pub async fn scram_users(self: &Arc<Self>, users: Vec<String>) -> Result<Value> {
        self.native(rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBEUSERSCRAMCREDENTIALS,false,false,move |c| unsafe {
            let users: Vec<_> = users.iter().map(|s|cstring(s)).collect::<Result<_>>()?;
            let mut pointers: Vec<_> = users.iter().map(|s|s.as_ptr()).collect();
            rd_kafka_DescribeUserScramCredentials(c.rk,if pointers.is_empty() {ptr::null_mut()} else {pointers.as_mut_ptr()},pointers.len(),c.options.0,c.queue.0);
            let ev = c.wait()?;
            let r = rd_kafka_event_DescribeUserScramCredentials_result(ev.0);
            if r.is_null() { return Err(CoreError::Internal("unexpected SCRAM event".into())); }
            let mut n = 0;
            let p = rd_kafka_DescribeUserScramCredentials_result_descriptions(r,&mut n);
            let mut out = Vec::new();
            for p in array(p,n) {
                error(rd_kafka_UserScramCredentialsDescription_error(*p))?;
                let mut credentials = Vec::new();
                for i in 0..rd_kafka_UserScramCredentialsDescription_scramcredentialinfo_count(*p) {
                    let info = rd_kafka_UserScramCredentialsDescription_scramcredentialinfo(*p,i);
                    let mech = match rd_kafka_ScramCredentialInfo_mechanism(info) {
                        rd_kafka_ScramMechanism_t::RD_KAFKA_SCRAM_MECHANISM_SHA_256 => "SCRAM-SHA-256",
                        rd_kafka_ScramMechanism_t::RD_KAFKA_SCRAM_MECHANISM_SHA_512 => "SCRAM-SHA-512", _ => "unknown" };
                    credentials.push(json!({"mechanism":mech,"iterations":rd_kafka_ScramCredentialInfo_iterations(info)}));
                }
                out.push(json!({"user":text(rd_kafka_UserScramCredentialsDescription_user(*p)),"credentials":credentials}));
            }
            Ok(json!(out))
        }).await
    }
    pub async fn alter_scram(self: &Arc<Self>, change: ScramChange) -> Result<Value> {
        self.guard_write()?;
        if let ScramChange::Delete {
            user, confirm_name, ..
        } = &change
        {
            confirm(user, confirm_name)?;
        }
        self.native(
            rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_ALTERUSERSCRAMCREDENTIALS,
            false,
            false,
            move |c| unsafe {
                let p = match change {
                    ScramChange::Upsert {
                        user,
                        mechanism: mech,
                        password,
                        iterations,
                    } => {
                        if user.is_empty()
                            || password.is_empty()
                            || !(4096..=16384).contains(&iterations)
                        {
                            return Err(CoreError::Invalid(
                                "nonempty user/password and iterations 4096..16384 required".into(),
                            ));
                        }
                        let user = cstring(&user)?;
                        rd_kafka_UserScramCredentialUpsertion_new(
                            user.as_ptr(),
                            mechanism(&mech)?,
                            iterations,
                            password.as_ptr(),
                            password.len(),
                            ptr::null(),
                            0,
                        )
                    }
                    ScramChange::Delete {
                        user,
                        mechanism: mech,
                        ..
                    } => {
                        if user.is_empty() {
                            return Err(CoreError::Invalid("user required".into()));
                        }
                        let user = cstring(&user)?;
                        rd_kafka_UserScramCredentialDeletion_new(user.as_ptr(), mechanism(&mech)?)
                    }
                };
                let mut alteration = Owned::new(p, rd_kafka_UserScramCredentialAlteration_destroy)?;
                rd_kafka_AlterUserScramCredentials(
                    c.rk,
                    &mut alteration.0,
                    1,
                    c.options.0,
                    c.queue.0,
                );
                let ev = c.wait()?;
                let r = rd_kafka_event_AlterUserScramCredentials_result(ev.0);
                if r.is_null() {
                    return Err(CoreError::Internal("unexpected SCRAM event".into()));
                }
                let mut n = 0;
                let p = rd_kafka_AlterUserScramCredentials_result_responses(r, &mut n);
                for p in array(p, n) {
                    error(rd_kafka_AlterUserScramCredentials_result_response_error(*p))?;
                }
                Ok(json!({"applied":true}))
            },
        )
        .await
    }
}
