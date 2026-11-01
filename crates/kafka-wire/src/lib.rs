//! A bounded, sequential async Kafka connection. No credentials implement Debug.
pub mod admin;
mod scram;

use anyhow::{anyhow, bail, Context, Result};
use bytes::{Bytes, BytesMut};
use kafka_core::ConnectionConfig;
use kafka_protocol::{
    error::ParseResponseErrorCode,
    messages::*,
    protocol::{Decodable, Encodable, HeaderVersion, Message},
};
use openssl::{
    pkey::PKey,
    ssl::{SslConnector, SslFiletype, SslMethod},
};
use std::{collections::BTreeMap, pin::Pin, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

pub use kafka_protocol;

pub fn error_name(code: i16) -> String {
    code.err()
        .map(|e| format!("{e:?}"))
        .unwrap_or_else(|| "None".into())
}
pub fn check(code: i16) -> Result<()> {
    if code == 0 {
        Ok(())
    } else {
        bail!("Kafka {} ({code})", error_name(code))
    }
}

// kafka-protocol's Request trait requires the broker feature. Keep the client
// dependency client-only and associate only the messages we actually use.
pub trait ClientRequest: Message + Encodable + HeaderVersion {
    const KEY: i16;
    type Response: Decodable + HeaderVersion;
}
macro_rules! requests {
    ($($request:ident => $response:ident, $key:expr;)*) => {$(
        impl ClientRequest for $request {
            const KEY: i16 = $key;
            type Response = $response;
        }
    )*};
}
requests! {
    ApiVersionsRequest => ApiVersionsResponse, 18;
    MetadataRequest => MetadataResponse, 3;
    SaslHandshakeRequest => SaslHandshakeResponse, 17;
    SaslAuthenticateRequest => SaslAuthenticateResponse, 36;
    FindCoordinatorRequest => FindCoordinatorResponse, 10;
    ListPartitionReassignmentsRequest => ListPartitionReassignmentsResponse, 46;
    AlterPartitionReassignmentsRequest => AlterPartitionReassignmentsResponse, 45;
    ElectLeadersRequest => ElectLeadersResponse, 43;
    DescribeLogDirsRequest => DescribeLogDirsResponse, 35;
    DescribeClientQuotasRequest => DescribeClientQuotasResponse, 48;
    AlterClientQuotasRequest => AlterClientQuotasResponse, 49;
    ListGroupsRequest => ListGroupsResponse, 16;
    ShareGroupDescribeRequest => ShareGroupDescribeResponse, 77;
    DescribeShareGroupOffsetsRequest => DescribeShareGroupOffsetsResponse, 90;
    AlterShareGroupOffsetsRequest => AlterShareGroupOffsetsResponse, 91;
    DeleteShareGroupOffsetsRequest => DeleteShareGroupOffsetsResponse, 92;
}
trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}

pub struct Client {
    socket: Box<dyn Socket>,
    versions: BTreeMap<i16, (i16, i16)>,
    correlation: i32,
    timeout: Duration,
    poisoned: bool,
    pub api_versions: ApiVersionsResponse,
}

fn frame_length(length: i32) -> Result<usize> {
    if !(4..=64 * 1024 * 1024).contains(&length) {
        bail!("invalid Kafka response frame length {length}")
    }
    Ok(length as usize)
}

impl Client {
    pub async fn bootstrap(config: &ConnectionConfig) -> Result<Self> {
        let mut last = anyhow!("no bootstrap servers");
        for address in config
            .bootstrap_servers
            .split(',')
            .map(str::trim)
            .filter(|a| !a.is_empty())
        {
            match Self::connect(config, address).await {
                Ok(client) => return Ok(client),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    pub async fn connect(config: &ConnectionConfig, address: &str) -> Result<Self> {
        timeout(config.timeout, Self::connect_inner(config, address))
            .await
            .map_err(|_| anyhow!("Kafka connection timeout"))?
    }

    async fn connect_inner(config: &ConnectionConfig, address: &str) -> Result<Self> {
        let protocol = config.security_protocol.as_deref().unwrap_or("PLAINTEXT");
        if !matches!(
            protocol,
            "PLAINTEXT" | "SSL" | "SASL_PLAINTEXT" | "SASL_SSL"
        ) {
            bail!("unsupported security protocol")
        }
        let tcp = TcpStream::connect(address)
            .await
            .context("Kafka TCP connection")?;
        tcp.set_nodelay(true)?;
        let socket: Box<dyn Socket> = if protocol.ends_with("SSL") {
            let mut builder = SslConnector::builder(SslMethod::tls_client())?;
            if let Some(path) = &config.ssl_ca_location {
                builder.set_ca_file(path)?;
            }
            if let Some(path) = &config.ssl_certificate_location {
                builder.set_certificate_chain_file(path)?;
            }
            if let Some(path) = &config.ssl_key_location {
                if let Some(password) = &config.ssl_key_password {
                    let pem = std::fs::read(path)?;
                    let key = PKey::private_key_from_pem_passphrase(&pem, password.as_bytes())
                        .map_err(|_| anyhow!("unable to decrypt TLS private key"))?;
                    builder.set_private_key(&key)?;
                } else {
                    builder.set_private_key_file(path, SslFiletype::PEM)?;
                }
                builder.check_private_key()?;
            }
            let host = address
                .rsplit_once(':')
                .context("bootstrap address must contain port")?
                .0
                .trim_matches(['[', ']']);
            let ssl = builder.build().configure()?.into_ssl(host)?;
            let mut stream = tokio_openssl::SslStream::new(ssl, tcp)?;
            Pin::new(&mut stream)
                .connect()
                .await
                .context("Kafka TLS handshake")?;
            Box::new(stream)
        } else {
            Box::new(tcp)
        };
        let mut client = Self {
            socket,
            versions: BTreeMap::new(),
            correlation: 0,
            timeout: config.timeout,
            poisoned: false,
            api_versions: ApiVersionsResponse::default(),
        };
        // v0 is understood by all brokers supporting ApiVersions. Negotiate the
        // richer response next, instead of guessing a modern broker version.
        let first = client.exchange(&ApiVersionsRequest::default(), 0).await?;
        check(first.error_code)?;
        client.versions = first
            .api_keys
            .iter()
            .map(|k| (k.api_key, (k.min_version, k.max_version)))
            .collect();
        let request = ApiVersionsRequest::default()
            .with_client_software_name("kafka.el".into())
            .with_client_software_version("0.1".into());
        client.api_versions = client.request(&request).await?;
        check(client.api_versions.error_code)?;
        if protocol.starts_with("SASL") {
            client.authenticate(config).await?;
        }
        Ok(client)
    }

    pub fn version<R: ClientRequest>(&self) -> Result<i16> {
        let (min, max) = self
            .versions
            .get(&R::KEY)
            .copied()
            .context("API unsupported by broker")?;
        let highest = max.min(R::VERSIONS.max);
        if highest < min.max(R::VERSIONS.min) {
            bail!("no mutually supported version for API {}", R::KEY)
        }
        Ok(highest)
    }

    pub async fn request<R: ClientRequest>(&mut self, request: &R) -> Result<R::Response> {
        let version = self.version::<R>()?;
        self.exchange(request, version).await
    }

    async fn exchange<R: ClientRequest>(
        &mut self,
        request: &R,
        version: i16,
    ) -> Result<R::Response> {
        if self.poisoned {
            bail!("connection unusable after interrupted request")
        }
        self.poisoned = true;
        let result = timeout(self.timeout, async {
            self.correlation = self.correlation.wrapping_add(1);
            let mut body = BytesMut::new();
            RequestHeader::default()
                .with_request_api_key(R::KEY)
                .with_request_api_version(version)
                .with_correlation_id(self.correlation)
                .with_client_id(Some("kafka.el".into()))
                .encode(&mut body, R::header_version(version))?;
            request.encode(&mut body, version)?;
            let length = i32::try_from(body.len()).context("request too large")?;
            self.socket.write_i32(length).await?;
            self.socket.write_all(&body).await?;
            self.socket.flush().await?;
            let length = frame_length(self.socket.read_i32().await?)?;
            let mut response = vec![0; length];
            self.socket.read_exact(&mut response).await?;
            let mut response = Bytes::from(response);
            let header =
                ResponseHeader::decode(&mut response, R::Response::header_version(version))?;
            if header.correlation_id != self.correlation {
                bail!("Kafka correlation id mismatch")
            }
            let response = R::Response::decode(&mut response, version)?;
            Ok(response)
        })
        .await
        .map_err(|_| anyhow!("Kafka request timeout"))?;
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }

    async fn auth_step(&mut self, data: Vec<u8>) -> Result<Vec<u8>> {
        let response = self
            .request(&SaslAuthenticateRequest::default().with_auth_bytes(data.into()))
            .await?;
        // Server messages may contain identity data. Report the code only.
        check(response.error_code)?;
        Ok(response.auth_bytes.to_vec())
    }

    async fn authenticate(&mut self, config: &ConnectionConfig) -> Result<()> {
        let mechanism = config.sasl_mechanism.as_deref().unwrap_or("PLAIN");
        if !matches!(mechanism, "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512") {
            bail!("unsupported SASL mechanism")
        }
        let username = config
            .sasl_username
            .as_deref()
            .context("SASL username required")?;
        let password = config
            .sasl_password
            .as_deref()
            .context("SASL password required")?;
        let handshake = self
            .request(&SaslHandshakeRequest::default().with_mechanism(mechanism.to_owned().into()))
            .await?;
        check(handshake.error_code)?;
        if mechanism == "PLAIN" {
            if username.contains('\0') || password.contains('\0') {
                bail!("invalid SASL PLAIN credentials")
            }
            self.auth_step(format!("\0{username}\0{password}").into_bytes())
                .await?;
        } else {
            let mut nonce = [0; 24];
            openssl::rand::rand_bytes(&mut nonce)?;
            use base64::Engine;
            let nonce = base64::engine::general_purpose::STANDARD.encode(nonce);
            let first = scram::first(username, &nonce)?;
            let server = self.auth_step(format!("n,,{first}").into_bytes()).await?;
            let server = std::str::from_utf8(&server).context("invalid SCRAM challenge")?;
            let (final_message, signature) =
                scram::respond(mechanism, password, &nonce, &first, server)?;
            let final_response = self.auth_step(final_message.into_bytes()).await?;
            scram::verify(&final_response, &signature)?;
        }
        Ok(())
    }

    pub async fn metadata(&mut self) -> Result<MetadataResponse> {
        self.request(
            &MetadataRequest::default()
                .with_topics(None)
                .with_allow_auto_topic_creation(false),
        )
        .await
    }

    pub async fn coordinator(&mut self, config: &ConnectionConfig, group: &str) -> Result<Self> {
        let version = self.version::<FindCoordinatorRequest>()?;
        let mut request = FindCoordinatorRequest::default().with_key_type(0);
        if version >= 4 {
            request.coordinator_keys = vec![group.to_owned().into()];
        } else {
            request.key = group.to_owned().into();
        }
        let response = self.request(&request).await?;
        let address = if version >= 4 {
            let c = response
                .coordinators
                .first()
                .context("missing coordinator")?;
            check(c.error_code)?;
            format!("{}:{}", c.host, c.port)
        } else {
            check(response.error_code)?;
            format!("{}:{}", response.host, response.port)
        };
        Self::connect(config, &address).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn framing_bounds() {
        for n in [i32::MIN, -1, 0, 3, 64 * 1024 * 1024 + 1] {
            assert!(frame_length(n).is_err());
        }
        assert_eq!(frame_length(4).unwrap(), 4);
    }
    #[test]
    fn errors_are_named() {
        assert_eq!(error_name(0), "None");
        assert_eq!(error_name(29), "TopicAuthorizationFailed");
        assert!(error_name(32000).contains("32000"));
    }
}
