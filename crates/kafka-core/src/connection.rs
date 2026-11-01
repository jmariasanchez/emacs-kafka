//! Connection profiles and client construction.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::admin::AdminClient;
use rdkafka::client::{ClientContext, DefaultClientContext};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaError;
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
    /// Poll connection diagnostics while establishing metadata. Only fixed,
    /// actionable messages cross the API boundary, never broker text or secrets.
    pub(crate) async fn verify(&self) -> Result<()> {
        let cfg = self.clone();
        tokio::task::spawn_blocking(move || {
            cfg.check_certificate_validity()?;
            let diagnostic = Arc::new(Mutex::new(None));
            let context = ConnectContext(diagnostic.clone());
            let mut config = cfg.base_client_config()?;
            config.set("group.id", format!("kafka.el-verify-{}", unique_suffix()));
            let consumer: BaseConsumer<ConnectContext> = config.create_with_context(context).map_err(|error| {
                ConnectContext(diagnostic.clone()).record(&error.to_string());
                match diagnostic.lock().unwrap().take() {
                    Some((code, message)) => CoreError::Security { code, message },
                    None => CoreError::Kafka(error),
                }
            })?;
            let deadline = Instant::now() + cfg.timeout;
            loop {
                let left = deadline.saturating_duration_since(Instant::now());
                let result = consumer.fetch_metadata(None, left.min(Duration::from_millis(100)));
                consumer.poll(Duration::ZERO);
                if result.is_ok() {
                    return Ok(());
                }
                if let Some((code, message)) = diagnostic.lock().unwrap().take() {
                    return Err(CoreError::Security { code, message });
                }
                if Instant::now() >= deadline {
                    return result.map(|_| ()).map_err(CoreError::from);
                }
            }
        })
        .await
        .map_err(|e| CoreError::Internal(e.to_string()))?
    }

    fn check_certificate_validity(&self) -> Result<()> {
        let Some(path) = self.properties.get("ssl.certificate.location").or(self.ssl_certificate_location.as_ref()) else {
            return Ok(());
        };
        let pem = std::fs::read(path).map_err(|_| CoreError::Security {
            code: "SSL", message: "cannot read client certificate: check sslCertificateLocation and file permissions",
        })?;
        let certificate = openssl::x509::X509::from_pem(&pem).map_err(|_| CoreError::Security {
            code: "SSL", message: "invalid client certificate: sslCertificateLocation must contain a PEM certificate",
        })?;
        let now = openssl::asn1::Asn1Time::days_from_now(0)
            .map_err(|_| CoreError::Internal("cannot read current certificate validation time".into()))?;
        if certificate.not_after() < now {
            return Err(CoreError::Security { code: "SSL", message: "certificate expired: renew the client certificate" });
        }
        if certificate.not_before() > now {
            return Err(CoreError::Security { code: "SSL", message: "certificate not yet valid: check the system clock and client certificate" });
        }
        Ok(())
    }

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

type Diagnostic = Arc<Mutex<Option<(&'static str, &'static str)>>>;
struct ConnectContext(Diagnostic);
impl ConnectContext {
    fn record(&self, reason: &str) {
        let reason = reason.to_ascii_lowercase();
        let diagnostic = if reason.contains("certificate verify failed") {
            Some(("SSL", "certificate verify failed: check sslCaLocation and the broker hostname"))
        } else if reason.contains("certificate expired") {
            Some(("SSL", "certificate expired: renew the client certificate"))
        } else if reason.contains("ssl handshake failed") || reason.contains("alert certificate") {
            Some(("SSL", "TLS handshake failed: check the client certificate and private key, including certificate validity"))
        } else if reason.contains("unsupported sasl mechanism")
            || reason.contains("supported mechanisms")
            || reason.contains("no provider for sasl mechanism")
        {
            Some(("UnsupportedSaslMechanism", "SASL mechanism not enabled: choose a mechanism supported by the broker and this client build"))
        } else if reason.contains("authentication failed") || reason.contains("sasl authentication error") {
            Some(("SaslAuthenticationFailed", "authentication failed: check SASL username, password and mechanism"))
        } else {
            None
        };
        if let Some(diagnostic) = diagnostic {
            *self.0.lock().unwrap() = Some(diagnostic);
        }
    }
}
impl ClientContext for ConnectContext {
    fn log(&self, _: RDKafkaLogLevel, _: &str, message: &str) {
        self.record(message);
    }
    fn error(&self, _: KafkaError, reason: &str) {
        self.record(reason);
    }
}
impl ConsumerContext for ConnectContext {}

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
    fn connection_diagnostics_never_echo_broker_text() {
        let diagnostic = Arc::new(Mutex::new(None));
        let context = ConnectContext(diagnostic.clone());
        context.record("SASL authentication failed: secret-sentinel");
        let (code, message) = diagnostic.lock().unwrap().take().unwrap();
        assert_eq!(code, "SaslAuthenticationFailed");
        assert!(!message.contains("secret-sentinel"));
        context.record("SASL mechanism handshake failed: broker's supported mechanisms: PLAIN");
        assert_eq!(diagnostic.lock().unwrap().take().unwrap().0, "UnsupportedSaslMechanism");
        context.record("SSL handshake failed: certificate verify failed");
        assert!(diagnostic.lock().unwrap().take().unwrap().1.contains("sslCaLocation"));
    }

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
