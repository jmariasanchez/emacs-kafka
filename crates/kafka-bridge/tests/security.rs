//! Real SASL/TLS and ACL regression matrix. Enable with scripts/kafka-secure.sh env.
mod common;
use common::*;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

fn sasl(address: &str, mechanism: &str, user: &str) -> Value {
    json!({"connectionId":user,"bootstrapServers":address,"securityProtocol":"SASL_PLAINTEXT",
        "saslMechanism":mechanism,"saslUsername":user,"saslPassword":format!("{user}-secret"),"timeoutMs":3000})
}
fn bounded(b: &mut Bridge, params: Value) -> Value {
    let start = Instant::now();
    let id = b.send("kafka/connect", params);
    let r = b.wait_for(|v| v["id"] == id, 5);
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "connect exceeded timeout budget: {r}"
    );
    r
}
fn failure(b: &mut Bridge, params: Value, code: &str, message: &str) {
    let r = bounded(b, params);
    eprintln!("expected {code}/{message}: {r}");
    assert_eq!(r["error"]["code"], 1000, "{r}");
    assert_eq!(r["error"]["data"]["kafkaCode"], code, "{r}");
    assert!(
        r["error"]["message"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains(message),
        "{r}"
    );
}

#[test]
fn broker_disabled_mechanism() {
    let Some(address) = env("KAFKA_EL_TEST_SECURE_PLAIN_ONLY") else {
        return;
    };
    let mut b = Bridge::spawn();
    b.ok("kafka/connect", sasl(&address, "PLAIN", "admin"));
    failure(
        &mut b,
        sasl(&address, "SCRAM-SHA-256", "admin"),
        "UnsupportedSaslMechanism",
        "mechanism not enabled",
    );
}
#[test]
fn sasl_matrix_and_acl() {
    let Some(address) = env("KAFKA_EL_TEST_SECURE_SASL") else {
        return;
    };
    let mut b = Bridge::spawn();
    for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] {
        for user in ["admin", "alice"] {
            let r = bounded(&mut b, sasl(&address, mechanism, user));
            assert!(r.get("error").is_none(), "{mechanism}/{user}: {r}");
        }
    }
    b.ok("kafka/connect", sasl(&address, "PLAIN", "admin"));
    let topic = uniq("security");
    b.ok(
        "kafka/topic/create",
        json!({"connectionId":"admin","topic":topic,"partitions":1,"replicationFactor":1}),
    );
    b.ok(
        "kafka/produce",
        json!({"connectionId":"admin","topic":topic,"value":"secure-roundtrip"}),
    );
    let r = b.ok("kafka/records/fetch", json!({"connectionId":"admin","topic":topic,"startPosition":{"type":"earliest"},"limit":1,"timeoutMs":3000}));
    assert_eq!(r["records"][0]["value"]["data"], "secure-roundtrip");
    if let (Some(home), Some(properties)) = (
        env("KAFKA_EL_TEST_KAFKA_HOME"),
        env("KAFKA_EL_TEST_SECURE_ADMIN_PROPS"),
    ) {
        let oracle = std::process::Command::new(format!("{home}/bin/kafka-console-consumer.sh"))
            .args([
                "--bootstrap-server",
                &address,
                "--command-config",
                &properties,
                "--topic",
                &topic,
                "--from-beginning",
                "--max-messages",
                "1",
                "--timeout-ms",
                "5000",
            ])
            .output()
            .unwrap();
        assert!(
            oracle.status.success(),
            "{}",
            String::from_utf8_lossy(&oracle.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&oracle.stdout).trim(),
            "secure-roundtrip"
        );
    }
    for method in ["kafka/topic/describe", "kafka/produce"] {
        let start = Instant::now();
        let r = b.call(
            method,
            json!({"connectionId":"alice","topic":topic,"value":"denied"}),
        );
        assert!(start.elapsed() < Duration::from_secs(5), "{r}");
        assert_eq!(r["error"]["code"], 1000, "{r}");
        assert_eq!(
            r["error"]["data"]["kafkaCode"], "TopicAuthorizationFailed",
            "{r}"
        );
    }
    // Kafka filters ListGroups to authorized groups rather than denying an empty catalog.
    let start = Instant::now();
    let groups = b.call("kafka/groups/list", json!({"connectionId":"alice"}));
    assert!(start.elapsed() < Duration::from_secs(5), "{groups}");
    assert!(
        groups["result"].as_array().is_some_and(|a| a.is_empty())
            || (groups["error"]["code"] == 1000
                && matches!(
                    groups["error"]["data"]["kafkaCode"].as_str(),
                    Some("GroupAuthorizationFailed" | "ClusterAuthorizationFailed")
                )),
        "{groups}"
    );
    b.ok(
        "kafka/topic/delete",
        json!({"connectionId":"admin","topic":topic,"confirmName":topic}),
    );
    for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] {
        let mut p = sasl(&address, mechanism, "admin");
        p["saslPassword"] = json!("sentinel-password-never-log-12345");
        failure(
            &mut b,
            p,
            "SaslAuthenticationFailed",
            "authentication failed",
        );
    }
    let r = bounded(&mut b, sasl(&address, "NOT-A-MECHANISM", "admin"));
    assert_eq!(r["error"]["code"], 1000, "{r}");
    assert_eq!(
        r["error"]["data"]["kafkaCode"], "UnsupportedSaslMechanism",
        "{r}"
    );
    assert!(
        r["error"]["message"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("mechanism"),
        "{r}"
    );
    let r = bounded(&mut b, sasl(&address, "GSSAPI", "admin"));
    assert_eq!(r["error"]["code"], 1000, "{r}");
    assert_eq!(
        r["error"]["data"]["kafkaCode"], "UnsupportedSaslMechanism",
        "{r}"
    );
    assert!(
        r["error"]["message"]
            .as_str()
            .unwrap()
            .contains("mechanism"),
        "{r}"
    );
    assert!(!b
        .ok("kafka/connections", json!({}))
        .to_string()
        .contains("secret"));
}

#[test]
fn passwords_never_reach_stdout_or_stderr_even_with_debug() {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::process::{Command, Stdio};
    let Some(address) = env("KAFKA_EL_TEST_SECURE_SASL") else {
        return;
    };
    const SECRET: &str = "sentinel-password-never-log-12345";
    for debug in [false, true] {
        let mut child = Command::new(env!("CARGO_BIN_EXE_kafka-bridge"))
            .env("RUST_LOG", "trace")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        let stderr = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes).unwrap();
            bytes
        });
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut output = BufReader::new(stdout);
            loop {
                let mut header = String::new();
                if output.read_line(&mut header).unwrap() == 0 {
                    break;
                }
                let len: usize = header
                    .trim()
                    .strip_prefix("Content-Length: ")
                    .expect("stdout must contain only frames")
                    .parse()
                    .unwrap();
                let mut blank = String::new();
                output.read_line(&mut blank).unwrap();
                assert_eq!(blank, "\r\n");
                let mut bytes = vec![0; len];
                output.read_exact(&mut bytes).unwrap();
                assert!(!String::from_utf8_lossy(&bytes).contains(SECRET));
                tx.send(serde_json::from_slice::<Value>(&bytes).unwrap())
                    .unwrap();
            }
        });
        let mut p = sasl(&address, "PLAIN", "admin");
        p["saslPassword"] = json!(SECRET);
        if debug {
            p["properties"] = json!({"debug":"all"});
        }
        for (id, method, params) in [
            (1, "kafka/connect", p),
            (2, "kafka/connections", json!({})),
            (3, "kafka/shutdown", json!({})),
        ] {
            let bytes = serde_json::to_vec(
                &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
            )
            .unwrap();
            write!(input, "Content-Length: {}\r\n\r\n", bytes.len()).unwrap();
            input.write_all(&bytes).unwrap();
            input.flush().unwrap();
            let reply = rx.recv_timeout(Duration::from_secs(5));
            if reply.is_err() {
                let _ = child.kill();
            }
            let reply = reply.expect("secret probe must finish promptly");
            assert_eq!(reply["id"], id);
            if id == 1 {
                assert_eq!(
                    reply["error"]["data"]["kafkaCode"],
                    "SaslAuthenticationFailed"
                );
            }
        }
        drop(input);
        assert!(child.wait().unwrap().success());
        reader.join().unwrap();
        assert!(!String::from_utf8_lossy(&stderr.join().unwrap()).contains(SECRET));
    }
}
#[test]
fn tls_matrix() {
    let (Some(address), Some(certs)) = (
        env("KAFKA_EL_TEST_SECURE_SSL"),
        env("KAFKA_EL_TEST_SECURE_CERTS"),
    ) else {
        return;
    };
    let mut b = Bridge::spawn();
    let p = json!({"connectionId":"tls","bootstrapServers":address,"securityProtocol":"SSL","sslCaLocation":format!("{certs}/ca.pem"),"sslCertificateLocation":format!("{certs}/client.pem"),"sslKeyLocation":format!("{certs}/client.key"),"timeoutMs":3000});
    for host in [address.clone(), address.replace("127.0.0.1", "localhost")] {
        let mut p = p.clone();
        p["bootstrapServers"] = json!(host);
        let r = bounded(&mut b, p);
        assert!(r.get("error").is_none(), "{r}");
    }
    let mut missing = p.clone();
    missing
        .as_object_mut()
        .unwrap()
        .remove("sslCertificateLocation");
    missing.as_object_mut().unwrap().remove("sslKeyLocation");
    failure(&mut b, missing, "SSL", "handshake");
    let mut expired = p.clone();
    expired["sslCertificateLocation"] = json!(format!("{certs}/expired.pem"));
    expired["sslKeyLocation"] = json!(format!("{certs}/expired.key"));
    failure(&mut b, expired, "SSL", "certificate expired");
    let mut rogue = p;
    rogue["sslCaLocation"] = json!(format!("{certs}/rogue-ca.pem"));
    failure(&mut b, rogue, "SSL", "certificate verify failed");
    if let Some(address) = env("KAFKA_EL_TEST_SECURE_SASL_SSL") {
        let mut p = sasl(&address, "PLAIN", "admin");
        p["securityProtocol"] = json!("SASL_SSL");
        p["sslCaLocation"] = json!(format!("{certs}/ca.pem"));
        let r = bounded(&mut b, p);
        assert!(r.get("error").is_none(), "{r}");
    }
}
