//! Connection profiles and client construction.

use std::collections::BTreeMap;
use std::time::Duration;

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::BaseConsumer;
use rdkafka::producer::FutureProducer;

use crate::errors::{CoreError, Result};

/// Everything needed to talk to one cluster. Secrets live here only in memory.
#[derive(Clone, Default)]
pub struct ConnectionConfig {
    pub id: String,
    pub bootstrap_servers: String,
    pub read_only: bool,
    pub environment: Option<String>,
    pub security_protocol: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub ssl_ca_location: Option<String>,
    pub ssl_certificate_location: Option<String>,
    pub ssl_key_location: Option<String>,
    pub ssl_key_password: Option<String>,
    pub timeout: Duration,
    pub properties: BTreeMap<String, String>,
}

impl std::fmt::Debug for ConnectionConfig {
    // Hand-written so secrets can never leak through `{:?}`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionConfig")
            .field("id", &self.id)
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("read_only", &self.read_only)
            .field("security_protocol", &self.security_protocol)
            .field("sasl_mechanism", &self.sasl_mechanism)
            .field("sasl_username", &self.sasl_username)
            .field("sasl_password", &self.sasl_password.as_ref().map(|_| "<redacted>"))
            .field("ssl_key_password", &self.ssl_key_password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Property keys whose values must never be echoed.
pub fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    k.contains("password") || k.contains("secret") || k.contains("sasl.jaas") || k.contains("token")
}

impl ConnectionConfig {
    pub fn base_client_config(&self) -> Result<ClientConfig> {
        if self.bootstrap_servers.trim().is_empty() {
            return Err(CoreError::Invalid("bootstrap servers must not be empty".into()));
        }
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", &self.bootstrap_servers);
        c.set("client.id", format!("kafka.el-{}", self.id));
        let ms = self.timeout.as_millis().to_string();
        c.set("socket.timeout.ms", &ms);
        let opt = |c: &mut ClientConfig, k: &str, v: &Option<String>| {
            if let Some(v) = v {
                c.set(k, v);
            }
        };
        opt(&mut c, "security.protocol", &self.security_protocol);
        opt(&mut c, "sasl.mechanism", &self.sasl_mechanism);
        opt(&mut c, "sasl.username", &self.sasl_username);
        opt(&mut c, "sasl.password", &self.sasl_password);
        opt(&mut c, "ssl.ca.location", &self.ssl_ca_location);
        opt(&mut c, "ssl.certificate.location", &self.ssl_certificate_location);
        opt(&mut c, "ssl.key.location", &self.ssl_key_location);
        opt(&mut c, "ssl.key.password", &self.ssl_key_password);
        for (k, v) in &self.properties {
            c.set(k, v);
        }
        Ok(c)
    }

    /// A consumer that never joins a group and never commits.
    ///
    /// `group.id` is required by librdkafka even for manual assignment; we use
    /// a unique throwaway id and disable all commit paths.
    pub fn inspection_consumer(&self) -> Result<BaseConsumer> {
        let mut c = self.base_client_config()?;
        c.set("group.id", format!("kafka.el-inspect-{}", unique_suffix()));
        c.set("enable.auto.commit", "false");
        c.set("enable.auto.offset.store", "false");
        c.set("enable.partition.eof", "true");
        c.set("auto.offset.reset", "earliest");
        Ok(c.create()?)
    }

    /// A consumer bound to an existing group id, used only for OffsetFetch.
    /// It is never subscribed, so it never joins the group.
    pub fn group_offset_reader(&self, group: &str) -> Result<BaseConsumer> {
        let mut c = self.base_client_config()?;
        c.set("group.id", group);
        c.set("enable.auto.commit", "false");
        c.set("enable.auto.offset.store", "false");
        Ok(c.create()?)
    }

    pub fn admin(&self) -> Result<AdminClient<DefaultClientContext>> {
        Ok(self.base_client_config()?.create()?)
    }

    pub fn producer(&self) -> Result<FutureProducer> {
        let mut c = self.base_client_config()?;
        c.set("enable.idempotence", "true");
        c.set("message.timeout.ms", self.timeout.as_millis().max(1000).to_string());
        Ok(c.create()?)
    }
}

fn unique_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}-{}-{}", std::process::id(), t, N.fetch_add(1, Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_secrets() {
        let c = ConnectionConfig {
            sasl_password: Some("hunter2".into()),
            ssl_key_password: Some("pw".into()),
            ..Default::default()
        };
        let s = format!("{c:?}");
        assert!(!s.contains("hunter2"));
        assert!(!s.contains("\"pw\""));
    }

    #[test]
    fn empty_bootstrap_rejected() {
        assert!(ConnectionConfig::default().base_client_config().is_err());
    }

    #[test]
    fn secret_keys() {
        assert!(is_secret_key("sasl.password"));
        assert!(is_secret_key("sasl.oauthbearer.client.secret"));
        assert!(!is_secret_key("retention.ms"));
    }
}
