//! Confluent-compatible registry client. Credentials and transport errors are redacted.
use reqwest::{Client, Method, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::HashMap, fmt, sync::Arc, time::Duration};
use tokio::sync::RwLock;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid registry configuration: {0}")]
    Config(&'static str),
    #[error("registry transport failed")]
    Transport,
    #[error("registry returned HTTP {0}")]
    Http(u16),
    #[error("invalid schema or payload: {0}")]
    Codec(String),
    #[error("exact subject confirmation required")]
    Confirmation,
}
pub type Result<T> = std::result::Result<T, Error>;
fn codec(e: impl fmt::Display) -> Error {
    Error::Codec(e.to_string())
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub url: String,
    pub basic_auth_user: Option<String>,
    pub basic_auth_password: Option<String>,
    pub bearer_token: Option<String>,
    pub ssl_ca_location: Option<String>,
    pub ssl_certificate_location: Option<String>,
    pub ssl_key_location: Option<String>,
}
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RegistryConfig { credentials: [REDACTED] }")
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Schema {
    #[serde(default)]
    pub id: u32,
    pub schema: String,
    #[serde(default = "avro")]
    pub schema_type: String,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub version: Option<u32>,
    #[serde(default)]
    pub references: Vec<Value>,
}
fn avro() -> String {
    "AVRO".into()
}
#[derive(Clone)]
pub struct Registry {
    client: Client,
    base: Url,
    config: Config,
    cache: Arc<RwLock<HashMap<u32, Schema>>>,
}
impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Registry { credentials: [REDACTED] }")
    }
}
impl Registry {
    pub fn new(config: Config) -> Result<Self> {
        let base = Url::parse(&config.url).map_err(|_| Error::Config("invalid URL"))?;
        if !matches!(base.scheme(), "http" | "https")
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(Error::Config(
                "use HTTP(S) URL without credentials, query or fragment",
            ));
        }
        if config.bearer_token.is_some() && config.basic_auth_user.is_some() {
            return Err(Error::Config("choose basic or bearer authentication"));
        }
        let mut builder = Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(path) = &config.ssl_ca_location {
            let pem = std::fs::read(path).map_err(|_| Error::Config("cannot read CA"))?;
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(&pem).map_err(|_| Error::Config("invalid CA"))?,
            );
        }
        match (&config.ssl_certificate_location, &config.ssl_key_location) {
            (Some(cert), Some(key)) => {
                let cert =
                    std::fs::read(cert).map_err(|_| Error::Config("cannot read certificate"))?;
                let key =
                    std::fs::read(key).map_err(|_| Error::Config("cannot read private key"))?;
                builder = builder.identity(
                    reqwest::Identity::from_pkcs8_pem(&cert, &key)
                        .map_err(|_| Error::Config("invalid client identity"))?,
                );
            }
            (None, None) => (),
            _ => return Err(Error::Config("client certificate and key must be paired")),
        }
        Ok(Self {
            client: builder.build().map_err(|_| Error::Transport)?,
            base,
            config,
            cache: Arc::default(),
        })
    }
    fn request(&self, method: Method, segments: &[&str]) -> Result<reqwest::RequestBuilder> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| Error::Config("invalid base URL"))?
            .pop_if_empty()
            .extend(segments);
        let mut req = self.client.request(method, url);
        if let Some(user) = &self.config.basic_auth_user {
            req = req.basic_auth(user, self.config.basic_auth_password.as_ref());
        }
        if let Some(token) = &self.config.bearer_token {
            req = req.bearer_auth(token);
        }
        Ok(req)
    }
    pub async fn api(
        &self,
        method: Method,
        segments: &[&str],
        body: Option<Value>,
        permanent: bool,
    ) -> Result<Value> {
        let mut req = self.request(method, segments)?;
        if permanent {
            req = req.query(&[("permanent", "true")]);
        }
        if let Some(body) = body {
            req = req.json(&body);
        }
        let response = req.send().await.map_err(|_| Error::Transport)?;
        if !response.status().is_success() {
            return Err(Error::Http(response.status().as_u16()));
        }
        response
            .json()
            .await
            .map_err(|_| Error::Codec("invalid registry JSON response".into()))
    }
    pub async fn subjects(&self) -> Result<Value> {
        self.api(Method::GET, &["subjects"], None, false).await
    }
    pub async fn versions(&self, subject: &str) -> Result<Value> {
        self.api(Method::GET, &["subjects", subject, "versions"], None, false)
            .await
    }
    pub async fn by_id(&self, id: u32) -> Result<Schema> {
        if let Some(schema) = self.cache.read().await.get(&id) {
            return Ok(schema.clone());
        }
        let mut schema: Schema = serde_json::from_value(
            self.api(
                Method::GET,
                &["schemas", "ids", &id.to_string()],
                None,
                false,
            )
            .await?,
        )
        .map_err(codec)?;
        schema.id = id;
        self.cache.write().await.insert(id, schema.clone());
        Ok(schema)
    }
    pub async fn schema(&self, subject: &str, version: &str) -> Result<Schema> {
        serde_json::from_value(
            self.api(
                Method::GET,
                &["subjects", subject, "versions", version],
                None,
                false,
            )
            .await?,
        )
        .map_err(codec)
    }
    pub async fn register(&self, subject: &str, schema: Value) -> Result<Value> {
        self.api(
            Method::POST,
            &["subjects", subject, "versions"],
            Some(schema),
            false,
        )
        .await
    }
    pub async fn compatibility(
        &self,
        subject: &str,
        version: &str,
        schema: Value,
    ) -> Result<Value> {
        self.api(
            Method::POST,
            &["compatibility", "subjects", subject, "versions", version],
            Some(schema),
            false,
        )
        .await
    }
    pub async fn config(&self, subject: Option<&str>, level: Option<&str>) -> Result<Value> {
        let mut path = vec!["config"];
        if let Some(s) = subject {
            path.push(s);
        }
        self.api(
            if level.is_some() {
                Method::PUT
            } else {
                Method::GET
            },
            &path,
            level.map(|l| json!({"compatibility":l})),
            false,
        )
        .await
    }
    pub async fn delete(
        &self,
        subject: &str,
        version: Option<&str>,
        permanent: bool,
        confirm: &str,
    ) -> Result<Value> {
        if confirm != subject {
            return Err(Error::Confirmation);
        }
        let mut path = vec!["subjects", subject];
        if let Some(v) = version {
            path.extend(["versions", v]);
        }
        let result = self.api(Method::DELETE, &path, None, permanent).await;
        if result.is_ok() {
            self.cache.write().await.clear();
        }
        result
    }
    pub async fn decode(&self, bytes: &[u8]) -> Result<Value> {
        let (id, payload) = split_wire(bytes)?;
        let schema = self.by_id(id).await?;
        let value = decode_payload(&schema, payload)?;
        Ok(json!({"schemaId":id,"subject":schema.subject,"format":schema.schema_type,"json":value}))
    }
    pub async fn encode(&self, subject: &str, value: Value) -> Result<Vec<u8>> {
        let schema = self.schema(subject, "latest").await?;
        let payload = encode_payload(&schema, value)?;
        Ok(wire(schema.id, &payload))
    }
}
pub fn split_wire(bytes: &[u8]) -> Result<(u32, &[u8])> {
    if bytes.len() < 5 || bytes[0] != 0 {
        return Err(codec("expected magic byte 0 and four-byte schema id"));
    }
    Ok((
        u32::from_be_bytes(bytes[1..5].try_into().unwrap()),
        &bytes[5..],
    ))
}
pub fn wire(id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0];
    out.extend(id.to_be_bytes());
    out.extend(payload);
    out
}
pub fn decode_payload(schema: &Schema, payload: &[u8]) -> Result<Value> {
    if !schema.references.is_empty() {
        return Err(codec("external schema references are not yet supported"));
    }
    match schema.schema_type.as_str() {
        "AVRO" => {
            let parsed = apache_avro::Schema::parse_str(&schema.schema).map_err(codec)?;
            let mut input = payload;
            let datum = apache_avro::from_avro_datum(&parsed, &mut input, None).map_err(codec)?;
            if !input.is_empty() {
                return Err(codec("trailing Avro payload"));
            }
            apache_avro::from_value(&datum).map_err(codec)
        }
        "JSON" => serde_json::from_slice(payload).map_err(codec),
        "PROTOBUF" => protobuf_decode(schema, payload),
        _ => Err(codec("unsupported schema type")),
    }
}
pub fn encode_payload(schema: &Schema, value: Value) -> Result<Vec<u8>> {
    if !schema.references.is_empty() {
        return Err(codec("external schema references are not yet supported"));
    }
    match schema.schema_type.as_str() {
        "AVRO" => {
            let parsed = apache_avro::Schema::parse_str(&schema.schema).map_err(codec)?;
            let datum = apache_avro::to_value(value)
                .map_err(codec)?
                .resolve(&parsed)
                .map_err(codec)?;
            apache_avro::to_avro_datum(&parsed, datum).map_err(codec)
        }
        "JSON" => serde_json::to_vec(&value).map_err(codec),
        "PROTOBUF" => protobuf_encode(schema, value),
        _ => Err(codec("unsupported schema type")),
    }
}

fn protobuf_pool(schema: &Schema) -> Result<prost_reflect::DescriptorPool> {
    let file = protox::file::File::from_source("registry.proto", &schema.schema).map_err(codec)?;
    let mut resolver = protox::file::ChainFileResolver::new();
    // Well-known Google types remain available, without external protoc.
    resolver.add(protox::file::GoogleFileResolver::new());
    struct Source(Option<protox::file::File>);
    impl protox::file::FileResolver for Source {
        fn open_file(&self, name: &str) -> std::result::Result<protox::file::File, protox::Error> {
            if name == "registry.proto" {
                Ok(self.0.as_ref().unwrap().clone())
            } else {
                Err(protox::Error::file_not_found(name))
            }
        }
    }
    resolver.add(Source(Some(file)));
    let mut compiler = protox::Compiler::with_file_resolver(resolver);
    compiler
        .include_imports(true)
        .open_file("registry.proto")
        .map_err(codec)?;
    prost_reflect::DescriptorPool::from_file_descriptor_set(compiler.file_descriptor_set())
        .map_err(codec)
}
fn read_index(input: &mut &[u8]) -> Result<usize> {
    let mut n = 0u32;
    for shift in (0..35).step_by(7) {
        let byte = *input
            .first()
            .ok_or_else(|| codec("truncated protobuf index"))?;
        *input = &input[1..];
        if shift == 28 && byte > 15 {
            return Err(codec("protobuf index overflow"));
        }
        n |= u32::from(byte & 127) << shift;
        if byte & 128 == 0 {
            if n & 1 != 0 {
                return Err(codec("negative protobuf index"));
            }
            return Ok((n >> 1) as usize);
        }
    }
    Err(codec("invalid protobuf index"))
}
fn protobuf_decode(schema: &Schema, mut payload: &[u8]) -> Result<Value> {
    let count = read_index(&mut payload)?;
    if count > 100 {
        return Err(codec("protobuf index nesting exceeds 100"));
    }
    let pool = protobuf_pool(schema)?;
    let file = pool
        .get_file_by_name("registry.proto")
        .ok_or_else(|| codec("missing protobuf file"))?;
    let first = if count == 0 {
        0
    } else {
        read_index(&mut payload)?
    };
    let mut desc = file
        .messages()
        .nth(first)
        .ok_or_else(|| codec("invalid protobuf message index"))?;
    for _ in 1..count {
        let child = desc
            .child_messages()
            .nth(read_index(&mut payload)?)
            .ok_or_else(|| codec("invalid nested message index"))?;
        desc = child;
    }
    let message = prost_reflect::DynamicMessage::decode(desc, payload).map_err(codec)?;
    serde_json::to_value(message).map_err(codec)
}
fn protobuf_encode(schema: &Schema, value: Value) -> Result<Vec<u8>> {
    use prost::Message;
    let pool = protobuf_pool(schema)?;
    let desc = pool
        .get_file_by_name("registry.proto")
        .and_then(|f| f.messages().next())
        .ok_or_else(|| codec("no protobuf message"))?;
    let message = prost_reflect::DynamicMessage::deserialize(desc, value).map_err(codec)?;
    let mut out = vec![0];
    message.encode(&mut out).map_err(codec)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample(kind: &str, source: &str) -> Schema {
        Schema {
            id: 42,
            schema: source.into(),
            schema_type: kind.into(),
            subject: None,
            version: None,
            references: vec![],
        }
    }
    #[test]
    fn wire_validation() {
        assert!(split_wire(&[0, 1]).is_err());
        assert!(split_wire(&[1, 0, 0, 0, 1]).is_err());
        assert_eq!(
            split_wire(&wire(0x12345678, b"abc")).unwrap(),
            (0x12345678, b"abc".as_slice())
        );
    }
    #[test]
    fn avro_roundtrip() {
        let s = sample(
            "AVRO",
            r#"{"type":"record","name":"Item","fields":[{"name":"name","type":"string"},{"name":"count","type":"long"}]}"#,
        );
        let v = json!({"name":"hello","count":42});
        assert_eq!(
            decode_payload(&s, &encode_payload(&s, v.clone()).unwrap()).unwrap(),
            v
        );
    }
    #[test]
    fn protobuf_roundtrip() {
        let s = sample(
            "PROTOBUF",
            "syntax = \"proto3\"; message Item { string name = 1; int32 count = 2; }",
        );
        let v = json!({"name":"hello","count":42});
        assert_eq!(
            decode_payload(&s, &encode_payload(&s, v.clone()).unwrap()).unwrap(),
            v
        );
    }
    #[test]
    fn json_roundtrip() {
        let s = sample("JSON", "{}");
        let v = json!({"ok":true});
        assert_eq!(
            decode_payload(&s, &encode_payload(&s, v.clone()).unwrap()).unwrap(),
            v
        );
    }
    #[test]
    fn auth_and_redaction() {
        let config = Config {
            url: "http://localhost:18081".into(),
            basic_auth_user: Some("user".into()),
            basic_auth_password: Some("secret".into()),
            ..Default::default()
        };
        assert!(!format!("{config:?}").contains("secret"));
        let r = Registry::new(config).unwrap();
        let req = r
            .request(Method::GET, &["subjects", "a/b"])
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(req.headers()["authorization"], "Basic dXNlcjpzZWNyZXQ=");
        assert!(req.url().path().ends_with("a%2Fb"));
        assert!(!format!("{r:?}").contains("secret"));
    }
}
