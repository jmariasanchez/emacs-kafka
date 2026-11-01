//! Real-broker wire API tests. Opt in with the documented environment variables.
mod common;
use common::*;
use serde_json::{json, Value};
use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};

struct TestConsumer(std::process::Child);
impl Drop for TestConsumer {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = Command::new("kill")
                .args(["-TERM", &self.0.id().to_string()])
                .status();
            let _ = self.0.wait();
        }
    }
}

fn connect(b: &mut Bridge, address: &str) {
    b.ok(
        "kafka/connect",
        json!({"connectionId":"wire","bootstrapServers":address,"timeoutMs":10000}),
    );
}
fn cli(home: &str, tool: &str, args: &[&str]) -> String {
    let output = Command::new(format!("{home}/bin/{tool}.sh"))
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{tool}: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}
fn poll(mut condition: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(45);
    while !condition() {
        assert!(Instant::now() < end, "broker state did not converge");
        std::thread::sleep(Duration::from_millis(200));
    }
}
fn create(b: &mut Bridge, topic: &str) {
    b.ok(
        "kafka/topic/create",
        json!({"connectionId":"wire","topic":topic,"partitions":1,"replicationFactor":1}),
    );
}
fn produce(b: &mut Bridge, topic: &str) {
    for n in 0..5 {
        b.ok(
            "kafka/produce",
            json!({"connectionId":"wire","topic":topic,"value":format!("wire-record-{n}")}),
        );
    }
}
fn delete(b: &mut Bridge, topic: &str) {
    b.ok(
        "kafka/topic/delete",
        json!({"connectionId":"wire","topic":topic,"confirmName":topic}),
    );
}

#[test]
fn features_quotas_and_guards() {
    let Some(address) = bootstrap() else { return };
    let mut b = Bridge::spawn();
    connect(&mut b, &address);
    let features = b.ok("kafka/features/describe", json!({"connectionId":"wire"}));
    assert!(features["finalized"]
        .as_array()
        .unwrap()
        .iter()
        .any(|f| f["name"] == "metadata.version"));
    b.ok("kafka/shareGroups/list", json!({"connectionId":"wire"}));
    b.ok("kafka/reassignments/list", json!({"connectionId":"wire"}));
    let client = uniq("wire-quota");
    let entity = json!([{"entityType":"client-id","entityName":client}]);
    let mut change = json!({"connectionId":"wire","entity":entity,"ops":[{"key":"producer_byte_rate","value":1234567.0}],"validateOnly":true});
    b.ok("kafka/quotas/alter", change.clone());
    let rows = b.ok("kafka/quotas/describe", json!({"connectionId":"wire"}));
    assert!(!rows
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["entity"] == entity));
    change["validateOnly"] = json!(false);
    b.ok("kafka/quotas/alter", change.clone());
    poll(|| {
        b.ok("kafka/quotas/describe", json!({"connectionId":"wire"}))
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["entity"] == entity && e["values"][0]["value"] == 1234567.0)
    });
    if let Some(home) = env("KAFKA_EL_TEST_KAFKA_HOME") {
        let output = cli(
            &home,
            "kafka-configs",
            &[
                "--bootstrap-server",
                &address,
                "--describe",
                "--entity-type",
                "clients",
                "--entity-name",
                &client,
            ],
        );
        assert!(output.contains("producer_byte_rate=1234567"), "{output}");
    }
    change["ops"][0]["value"] = Value::Null;
    assert_eq!(
        b.call("kafka/quotas/alter", change.clone())["error"]["code"],
        1003
    );
    change["confirmName"] = json!(client);
    b.ok("kafka/quotas/alter", change);
    poll(|| {
        !b.ok("kafka/quotas/describe", json!({"connectionId":"wire"}))
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["entity"] == entity)
    });
    b.ok(
        "kafka/connect",
        json!({"connectionId":"ro","bootstrapServers":address,"readOnly":true}),
    );
    for (method, params) in [
        (
            "kafka/reassignments/alter",
            json!({"topic":"missing","partitions":[{"partition":0,"replicas":[1]}],"confirmName":"missing"}),
        ),
        (
            "kafka/leaders/elect",
            json!({"topic":"missing","partitions":[0]}),
        ),
        (
            "kafka/quotas/alter",
            json!({"entity":entity,"ops":[{"key":"producer_byte_rate","value":1000}]}),
        ),
        (
            "kafka/shareGroup/resetOffsets",
            json!({"groupId":"missing","confirmName":"missing","offsets":[{"topic":"x","partition":0,"offset":0}]}),
        ),
    ] {
        let mut p = params;
        p["connectionId"] = json!("ro");
        assert_eq!(b.call(method, p)["error"]["code"], 1002, "{method}");
    }
}

#[test]
fn reassign_elect_log_dirs_and_records() {
    let (Some(address), Some(home)) = (
        env("KAFKA_EL_TEST_CLUSTER_BOOTSTRAP"),
        env("KAFKA_EL_TEST_KAFKA_HOME"),
    ) else {
        return;
    };
    let mut b = Bridge::spawn();
    connect(&mut b, &address);
    let topic = uniq("wire-reassign");
    cli(
        &home,
        "kafka-topics",
        &[
            "--bootstrap-server",
            &address,
            "--create",
            "--topic",
            &topic,
            "--replica-assignment",
            "1",
        ],
    );
    produce(&mut b, &topic);
    let mut change = json!({"connectionId":"wire","topic":topic,"partitions":[{"partition":0,"replicas":[2,3]}],"dryRun":true});
    let preview = b.ok("kafka/reassignments/alter", change.clone());
    assert_eq!(preview["changes"][0]["currentReplicas"], json!([1]));
    assert_eq!(preview["changes"][0]["targetReplicas"], json!([2, 3]));
    change["dryRun"] = json!(false);
    assert_eq!(
        b.call("kafka/reassignments/alter", change.clone())["error"]["code"],
        1003
    );
    change["confirmName"] = json!(topic);
    b.ok("kafka/reassignments/alter", change);
    poll(|| {
        let active = b.ok("kafka/reassignments/list", json!({"connectionId":"wire"}));
        let response = b.call(
            "kafka/topic/describe",
            json!({"connectionId":"wire","topic":topic}),
        );
        let detail = &response["result"];
        !active
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["topic"] == topic)
            && detail["partitions"][0]["replicas"] == json!([2, 3])
    });
    let output = cli(
        &home,
        "kafka-topics",
        &[
            "--bootstrap-server",
            &address,
            "--describe",
            "--topic",
            &topic,
        ],
    );
    assert!(output.contains("Replicas: 2,3"), "{output}");
    let path = std::env::temp_dir().join(format!("{topic}.json"));
    std::fs::write(
        &path,
        json!({"version":1,"partitions":[{"topic":topic,"partition":0,"replicas":[2,3]}]})
            .to_string(),
    )
    .unwrap();
    let output = cli(
        &home,
        "kafka-reassign-partitions",
        &[
            "--bootstrap-server",
            &address,
            "--verify",
            "--reassignment-json-file",
            path.to_str().unwrap(),
        ],
    );
    std::fs::remove_file(path).unwrap();
    assert!(output.contains("completed"), "{output}");
    let election = b.call(
        "kafka/leaders/elect",
        json!({"connectionId":"wire","topic":topic,"partitions":[0]}),
    );
    // ElectionNotNeeded (84) is a valid broker result for an already preferred leader.
    assert!(
        election.get("error").is_none()
            || election["error"]["data"]["results"][0]["errorCode"] == 84,
        "{election}"
    );
    let dirs = b.ok("kafka/logDirs/describe", json!({"connectionId":"wire"}));
    assert_eq!(dirs.as_array().unwrap().len(), 3);
    let mut replicas = 0;
    for broker in dirs.as_array().unwrap() {
        assert_eq!(broker["ok"], true, "{broker}");
        for dir in broker["directories"].as_array().unwrap() {
            for p in dir["partitions"].as_array().unwrap() {
                if p["topic"] == topic {
                    assert!(p["size"].as_i64().unwrap() > 0);
                    replicas += 1;
                }
            }
        }
    }
    assert!(replicas >= 2);
    let records = b.ok("kafka/records/fetch",json!({"connectionId":"wire","topic":topic,"startPosition":{"type":"earliest"},"limit":10,"timeoutMs":3000}));
    assert_eq!(records["records"].as_array().unwrap().len(), 5);
    // Cancel of a finished reassignment is an explicit named partial error.
    let cancel = b.call("kafka/reassignments/alter",json!({"connectionId":"wire","topic":topic,"confirmName":topic,"partitions":[{"partition":0,"replicas":null}]}));
    assert_eq!(
        cancel["error"]["data"]["results"][0]["errorCode"], 85,
        "{cancel}"
    );
    delete(&mut b, &topic);
}

#[test]
fn sasl_ssl_plain_and_scram512() {
    let Some(address) = env("KAFKA_EL_TEST_SECURE_BOOTSTRAP") else {
        return;
    };
    let ca = env("KAFKA_EL_TEST_SSL_CA").expect("KAFKA_EL_TEST_SSL_CA required");
    let mut b = Bridge::spawn();
    for (server, protocol, mechanism) in [
        (address.as_str(), "SASL_PLAINTEXT", "SCRAM-SHA-512"),
        ("127.0.0.1:29094", "SASL_SSL", "PLAIN"),
    ] {
        b.ok("kafka/connect",json!({"connectionId":"wire","bootstrapServers":server,"securityProtocol":protocol,"saslMechanism":mechanism,"saslUsername":"admin","saslPassword":"admin-secret","sslCaLocation":ca,"timeoutMs":10000}));
        assert!(b
            .ok("kafka/quotas/describe", json!({"connectionId":"wire"}))
            .is_array());
    }
}

#[tokio::test]
async fn wire_mtls_scram256_and_auth_failures() {
    let Some(address) = env("KAFKA_EL_TEST_SECURE_BOOTSTRAP") else {
        return;
    };
    let ca = env("KAFKA_EL_TEST_SSL_CA").expect("KAFKA_EL_TEST_SSL_CA required");
    let mut config = kafka_core::ConnectionConfig {
        bootstrap_servers: address,
        security_protocol: Some("SASL_PLAINTEXT".into()),
        sasl_mechanism: Some("SCRAM-SHA-256".into()),
        sasl_username: Some("admin".into()),
        sasl_password: Some("admin-secret".into()),
        timeout: Duration::from_secs(5),
        ..Default::default()
    };
    assert!(kafka_wire::admin::quotas(&config).await.unwrap().is_array());
    config.sasl_password = Some("not-the-password".into());
    let error = kafka_wire::admin::quotas(&config)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("SaslAuthenticationFailed"), "{error}");
    assert!(!error.contains("not-the-password"));
    let dir = std::path::Path::new(&ca).parent().unwrap();
    config.bootstrap_servers = "127.0.0.1:29093".into();
    config.security_protocol = Some("SSL".into());
    config.ssl_ca_location = Some(ca.clone());
    config.ssl_certificate_location = Some(dir.join("client.pem").to_str().unwrap().into());
    config.ssl_key_location = Some(dir.join("client.key").to_str().unwrap().into());
    assert!(kafka_wire::admin::quotas(&config).await.unwrap().is_array());
    config.ssl_ca_location = Some(dir.join("rogue-ca.pem").to_str().unwrap().into());
    assert!(kafka_wire::admin::quotas(&config).await.is_err());
}

#[test]
fn cancel_active_reassignment() {
    let (Some(address), Some(home)) = (
        env("KAFKA_EL_TEST_CLUSTER_BOOTSTRAP"),
        env("KAFKA_EL_TEST_KAFKA_HOME"),
    ) else {
        return;
    };
    let mut b = Bridge::spawn();
    connect(&mut b, &address);
    let topic = uniq("wire-cancel");
    let assignment = ["1"; 32].join(",");
    cli(
        &home,
        "kafka-topics",
        &[
            "--bootstrap-server",
            &address,
            "--create",
            "--topic",
            &topic,
            "--replica-assignment",
            &assignment,
        ],
    );
    let payload = "c".repeat(256 * 1024);
    for partition in 0..32 {
        b.ok(
            "kafka/produce",
            json!({"connectionId":"wire","topic":topic,"partition":partition,"value":payload}),
        );
    }
    let parts: Vec<_> = (0..32)
        .map(|p| json!({"partition":p,"replicas":[2,3]}))
        .collect();
    b.ok(
        "kafka/reassignments/alter",
        json!({"connectionId":"wire","topic":topic,"confirmName":topic,"partitions":parts}),
    );
    let cancel: Vec<_> = (0..32)
        .map(|p| json!({"partition":p,"replicas":null}))
        .collect();
    let response = b.call(
        "kafka/reassignments/alter",
        json!({"connectionId":"wire","topic":topic,"confirmName":topic,"partitions":cancel}),
    );
    let results = response.get("result").unwrap_or(&response["error"]["data"])["results"]
        .as_array()
        .unwrap();
    let cancelled: Vec<_> = results
        .iter()
        .filter(|p| p["ok"] == true)
        .map(|p| p["partition"].as_i64().unwrap())
        .collect();
    assert!(
        !cancelled.is_empty(),
        "all reassignments finished before cancellation: {response}"
    );
    poll(|| {
        !b.ok("kafka/reassignments/list", json!({"connectionId":"wire"}))
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["topic"] == topic)
    });
    // The controller can finish cancellation before broker metadata converges.
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let detail = b.call(
            "kafka/topic/describe",
            json!({"connectionId":"wire","topic":topic}),
        );
        if detail["result"]["partitions"]
            .as_array()
            .is_some_and(|partitions| {
                cancelled.iter().all(|id| {
                    partitions
                        .iter()
                        .any(|p| p["id"] == *id && p["replicas"] == json!([1]))
                })
            })
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cancelled partitions {cancelled:?} did not restore replicas [1]: {detail}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    delete(&mut b, &topic);
}

#[test]
fn share_group_describe_offsets_reset() {
    let (Some(address), Some(home)) = (
        env("KAFKA_EL_TEST_SHARE_BOOTSTRAP"),
        env("KAFKA_EL_TEST_KAFKA_HOME"),
    ) else {
        return;
    };
    let mut b = Bridge::spawn();
    connect(&mut b, &address);
    let topic = uniq("wire-share");
    let group = uniq("wire-share-group");
    cli(
        &home,
        "kafka-configs",
        &[
            "--bootstrap-server",
            &address,
            "--alter",
            "--entity-type",
            "groups",
            "--entity-name",
            &group,
            "--add-config",
            "share.auto.offset.reset=earliest",
        ],
    );
    create(&mut b, &topic);
    produce(&mut b, &topic);
    let mut consumer = TestConsumer(
        Command::new(format!("{home}/bin/kafka-console-share-consumer.sh"))
            .args([
                "--bootstrap-server",
                &address,
                "--topic",
                &topic,
                "--group",
                &group,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    poll(|| {
        b.ok("kafka/shareGroups/list", json!({"connectionId":"wire"}))
            .as_array()
            .unwrap()
            .iter()
            .any(|g| g["groupId"] == group)
    });
    let detail = b.ok(
        "kafka/shareGroup/describe",
        json!({"connectionId":"wire","groupId":group}),
    );
    assert!(!detail["members"].as_array().unwrap().is_empty());
    produce(&mut b, &topic);
    poll(|| {
        !b.ok(
            "kafka/shareGroup/offsets",
            json!({"connectionId":"wire","groupId":group}),
        )
        .as_array()
        .unwrap()
        .is_empty()
    });
    let offsets = b.ok(
        "kafka/shareGroup/offsets",
        json!({"connectionId":"wire","groupId":group}),
    );
    assert!(
        offsets
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["topic"] == topic),
        "{offsets}"
    );
    let output = cli(
        &home,
        "kafka-share-groups",
        &[
            "--bootstrap-server",
            &address,
            "--describe",
            "--group",
            &group,
        ],
    );
    assert!(output.contains(&topic), "{output}");
    let reset = json!({"connectionId":"wire","groupId":group,"confirmName":group,"offsets":[{"topic":topic,"partition":0,"offset":0}]});
    assert!(b
        .call("kafka/shareGroup/resetOffsets", reset.clone())
        .get("error")
        .is_some());
    // SIGTERM lets the CLI leave the group instead of waiting for session expiry.
    Command::new("kill")
        .args(["-TERM", &consumer.0.id().to_string()])
        .status()
        .unwrap();
    consumer.0.wait().unwrap();
    poll(|| {
        b.ok(
            "kafka/shareGroup/describe",
            json!({"connectionId":"wire","groupId":group}),
        )["state"]
            == "Empty"
    });
    b.ok("kafka/shareGroup/resetOffsets", reset);
    let offsets = b.ok(
        "kafka/shareGroup/offsets",
        json!({"connectionId":"wire","groupId":group}),
    );
    assert_eq!(offsets[0]["startOffset"], 0);
    b.ok(
        "kafka/shareGroup/resetOffsets",
        json!({"connectionId":"wire","groupId":group,"confirmName":group,"deleteTopics":[topic]}),
    );
    delete(&mut b, &topic);
}
