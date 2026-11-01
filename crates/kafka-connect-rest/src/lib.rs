//! Small Kafka Connect REST adapter. Credentials are deliberately not Debug.
use reqwest::{Client, Method, Url};
use serde_json::Value;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
#[error("Connect HTTP {status} (error_code {error_code}): {message}")]
pub struct Error {
    pub status: u16,
    pub error_code: i64,
    pub message: String,
}
impl Error {
    fn local(message: impl Into<String>) -> Self {
        Self {
            status: 0,
            error_code: 0,
            message: message.into(),
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct Connect {
    client: Client,
    base: Url,
    auth: Option<(String, Option<String>)>,
}
impl Connect {
    pub fn new(
        url: &str,
        user: Option<String>,
        password: Option<String>,
        ca: Option<&str>,
    ) -> Result<Self> {
        let base = Url::parse(url).map_err(|_| Error::local("Invalid Connect URL"))?;
        if !matches!(base.scheme(), "http" | "https")
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(Error::local(
                "Use an HTTP(S) URL without credentials, query or fragment",
            ));
        }
        let mut builder = Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(path) = ca {
            let pem =
                std::fs::read(path).map_err(|_| Error::local("Cannot read Connect CA file"))?;
            let cert = reqwest::Certificate::from_pem(&pem)
                .map_err(|_| Error::local("Invalid Connect CA PEM"))?;
            builder = builder.add_root_certificate(cert);
        }
        Ok(Self {
            client: builder
                .build()
                .map_err(|_| Error::local("Cannot build HTTP client"))?,
            base,
            auth: user.map(|u| (u, password)),
        })
    }
    async fn request(
        &self,
        method: Method,
        segments: &[&str],
        query: &[(&str, &str)],
        body: Option<&Value>,
    ) -> Result<Value> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| Error::local("Invalid base URL"))?
            .pop_if_empty()
            .extend(segments);
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query.iter().copied());
        }
        let mut req = self.client.request(method, url);
        if let Some((u, p)) = &self.auth {
            req = req.basic_auth(u, p.as_ref());
        }
        if let Some(body) = body {
            req = req.json(body);
        }
        let response = req.send().await.map_err(|e| {
            Error::local(if e.is_timeout() {
                "Connect request timed out"
            } else {
                "Connect transport failure (check URL, authentication and TLS)"
            })
        })?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|_| Error::local("Cannot read Connect response"))?;
        let value: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        if !status.is_success() {
            return Err(Error {
                status: status.as_u16(),
                error_code: value["error_code"]
                    .as_i64()
                    .unwrap_or(status.as_u16() as i64),
                message: value["message"]
                    .as_str()
                    .unwrap_or("Non-JSON Connect error response")
                    .to_owned(),
            });
        }
        if !bytes.is_empty() && value.is_null() {
            return Err(Error::local("Invalid JSON Connect response"));
        }
        Ok(value)
    }
    pub async fn info(&self) -> Result<Value> {
        self.request(Method::GET, &[], &[], None).await
    }
    pub async fn connectors(&self) -> Result<Value> {
        self.request(
            Method::GET,
            &["connectors"],
            &[("expand", "status"), ("expand", "info")],
            None,
        )
        .await
    }
    pub async fn plugins(&self) -> Result<Value> {
        self.request(Method::GET, &["connector-plugins"], &[], None)
            .await
    }
    pub async fn validate(&self, class: &str, config: &Value) -> Result<Value> {
        self.request(
            Method::PUT,
            &["connector-plugins", class, "config", "validate"],
            &[],
            Some(config),
        )
        .await
    }
    pub async fn config(&self, n: &str) -> Result<Value> {
        self.get(n, "config").await
    }
    pub async fn status(&self, n: &str) -> Result<Value> {
        self.get(n, "status").await
    }
    pub async fn tasks(&self, n: &str) -> Result<Value> {
        self.get(n, "tasks").await
    }
    pub async fn topics(&self, n: &str) -> Result<Value> {
        self.get(n, "topics").await
    }
    pub async fn offsets(&self, n: &str) -> Result<Value> {
        self.get(n, "offsets").await
    }
    async fn get(&self, n: &str, part: &str) -> Result<Value> {
        self.request(Method::GET, &["connectors", n, part], &[], None)
            .await
    }
    pub async fn upsert(&self, n: &str, config: &Value) -> Result<Value> {
        self.request(Method::PUT, &["connectors", n, "config"], &[], Some(config))
            .await
    }
    pub async fn pause(&self, n: &str) -> Result<Value> {
        self.action(n, "pause").await
    }
    pub async fn resume(&self, n: &str) -> Result<Value> {
        self.action(n, "resume").await
    }
    pub async fn stop(&self, n: &str) -> Result<Value> {
        self.action(n, "stop").await
    }
    async fn action(&self, n: &str, action: &str) -> Result<Value> {
        self.request(Method::PUT, &["connectors", n, action], &[], None)
            .await
    }
    pub async fn restart(&self, n: &str, tasks: bool, failed: bool) -> Result<Value> {
        self.request(
            Method::POST,
            &["connectors", n, "restart"],
            &[
                ("includeTasks", if tasks { "true" } else { "false" }),
                ("onlyFailed", if failed { "true" } else { "false" }),
            ],
            None,
        )
        .await
    }
    pub async fn task_status(&self, n: &str, task: u32) -> Result<Value> {
        self.request(
            Method::GET,
            &["connectors", n, "tasks", &task.to_string(), "status"],
            &[],
            None,
        )
        .await
    }
    pub async fn restart_task(&self, n: &str, task: u32) -> Result<Value> {
        self.request(
            Method::POST,
            &["connectors", n, "tasks", &task.to_string(), "restart"],
            &[],
            None,
        )
        .await
    }
    pub async fn delete(&self, n: &str) -> Result<Value> {
        self.request(Method::DELETE, &["connectors", n], &[], None)
            .await
    }
    pub async fn alter_offsets(&self, n: &str, offsets: &Value) -> Result<Value> {
        self.request(
            Method::PATCH,
            &["connectors", n, "offsets"],
            &[],
            Some(offsets),
        )
        .await
    }
    pub async fn reset_offsets(&self, n: &str) -> Result<Value> {
        self.request(Method::DELETE, &["connectors", n, "offsets"], &[], None)
            .await
    }
}
