//! Destructive tests only for the explicitly owned :49092 cluster.
mod common;
use common::*;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::process::Command;
use std::time::{Duration, Instant};

struct ClusterGuard {
    script: String,
}
impl ClusterGuard {
    fn run(&self, action: &str, node: Option<i64>) {
        let mut cmd = Command::new(&self.script);
        cmd.env("KAFKA_CLUSTER_PORT", "49092").arg(action);
        if let Some(node) = node {
            cmd.arg(node.to_string());
        }
        assert!(cmd.status().unwrap().success(), "cluster {action} failed");
    }
}
impl Drop for ClusterGuard {
    fn drop(&mut self) {
        // Resume paused nodes even when an assertion fails, then restore quorum.
        for node in 1..=3 {
            let _ = Command::new(&self.script)
                .env("KAFKA_CLUSTER_PORT", "49092")
                .args(["resume", &node.to_string()])
                .status();
        }
        let _ = Command::new(&self.script)
            .env("KAFKA_CLUSTER_PORT", "49092")
            .arg("start")
            .status();
    }
}
fn create(b: &mut Bridge, topic: &str, rf: i32) -> i64 {
    b.ok(
        "kafka/topic/create",
        json!({"connectionId":"cluster","topic":topic,"partitions":1,"replicationFactor":rf}),
    );
    b.ok(
        "kafka/produce",
        json!({"connectionId":"cluster","topic":topic,"partition":0,"value":"warm"}),
    );
    b.ok(
        "kafka/topic/describe",
        json!({"connectionId":"cluster","topic":topic}),
    )["partitions"][0]["leader"]
        .as_i64()
        .unwrap()
}
fn bounded(b: &mut Bridge, method: &str, params: Value, secs: u64) -> Value {
    let start = Instant::now();
    eprintln!("checking {method} response deadline: {secs}s");
    let id = b.send(method, params);
    let response = b.wait_for(|v| v["id"] == id, secs);
    eprintln!("{method} completed in {:?}", start.elapsed());
    assert!(
        start.elapsed() < Duration::from_secs(secs),
        "{method}: {response}"
    );
    response
}
#[test]
fn leader_failover_outage_restart_and_pause() {
    let (Some(address), Some(script)) = (
        env("KAFKA_EL_TEST_RESILIENCE_CLUSTER"),
        env("KAFKA_EL_TEST_RESILIENCE_SCRIPT"),
    ) else {
        return;
    };
    assert_eq!(
        address, "127.0.0.1:49092,127.0.0.1:49093,127.0.0.1:49094",
        "never kill the shared cluster"
    );
    let guard = ClusterGuard { script };
    let mut b = Bridge::spawn();
    b.ok(
        "kafka/connect",
        json!({"connectionId":"cluster","bootstrapServers":address,"timeoutMs":20000}),
    );
    b.ok(
        "kafka/connect",
        json!({"connectionId":"short","bootstrapServers":address,"timeoutMs":1500}),
    );
    let topic = uniq("failover");
    let leader = create(&mut b, &topic, 3);
    b.ok("kafka/consume/start", json!({"connectionId":"cluster","topic":topic,"subscriptionId":"live","startPosition":{"type":"earliest"},"batchSize":10,"initialCredit":64}));
    let first = b.wait_for(|v| v["method"] == "kafka/consume/records", 10);
    assert_eq!(first["params"]["records"][0]["offset"], 0);
    let script = guard.script.clone();
    let killer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        assert!(Command::new(script)
            .env("KAFKA_CLUSTER_PORT", "49092")
            .args(["kill", &leader.to_string()])
            .status()
            .unwrap()
            .success());
    });
    let mut ids = Vec::new();
    for n in 0..100 {
        ids.push(b.send("kafka/produce", json!({"connectionId":"cluster","topic":topic,"partition":0,"value":format!("record-{n}")})));
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut acked = BTreeSet::new();
    for id in ids {
        let r = b.wait_for(|v| v["id"] == id, 25);
        assert!(r.get("error").is_none(), "produce during failover: {r}");
        assert!(acked.insert(r["result"]["offset"].as_i64().unwrap()));
    }
    let mut received = BTreeSet::new();
    let mut values = BTreeSet::new();
    while received.len() < acked.len() {
        let r = b.wait_for(|v| v["method"] == "kafka/consume/records", 25);
        for record in r["params"]["records"].as_array().unwrap() {
            assert!(
                received.insert(record["offset"].as_i64().unwrap()),
                "duplicate offset: {record}"
            );
            assert!(
                values.insert(record["value"]["data"].as_str().unwrap().to_owned()),
                "duplicate value: {record}"
            );
        }
    }
    assert_eq!(
        received, acked,
        "every acknowledged record arrived exactly once"
    );
    killer.join().unwrap();
    b.ok("kafka/consume/stop", json!({"subscriptionId":"live"}));
    b.ok(
        "kafka/connect",
        json!({"connectionId":"dead-bootstrap","bootstrapServers":address,"timeoutMs":3000}),
    );
    guard.run("start-node", Some(leader));
    let snapshot = b.ok("kafka/records/fetch", json!({"connectionId":"cluster","topic":topic,"startPosition":{"type":"earliest"},"limit":200,"timeoutMs":10000}));
    assert_eq!(
        snapshot["records"].as_array().unwrap().len(),
        101,
        "idempotent producer must not append duplicate retries"
    );
    eprintln!("RF3 leader failover: 100/100 acknowledged records received exactly once");

    let single = uniq("pause");
    let single_leader = create(&mut b, &single, 1);
    b.ok(
        "kafka/produce",
        json!({"connectionId":"short","topic":single,"partition":0,"value":"warm-short-producer"}),
    );
    guard.run("pause", Some(single_leader));
    let r = bounded(
        &mut b,
        "kafka/produce",
        json!({"connectionId":"short","topic":single,"partition":0,"value":"paused"}),
        4,
    );
    assert_eq!(r["error"]["code"], 1000, "{r}");
    assert!(
        r["error"]["data"]["kafkaCode"]
            .as_str()
            .unwrap()
            .contains("TimedOut"),
        "{r}"
    );
    let r = bounded(
        &mut b,
        "kafka/records/fetch",
        json!({"connectionId":"cluster","topic":single,"startPosition":{"type":"earliest"},"timeoutMs":500}),
        2,
    );
    assert!(r.get("error").is_some() || r.get("result").is_some(), "{r}");
    guard.run("resume", Some(single_leader));
    let started = Instant::now();
    let fetch = b.send("kafka/records/fetch", json!({"connectionId":"cluster","topic":single,"startPosition":{"type":"earliest"},"timeoutMs":500}));
    let mut restart = Command::new(&guard.script)
        .env("KAFKA_CLUSTER_PORT", "49092")
        .args(["kill", &single_leader.to_string()])
        .spawn()
        .unwrap();
    let r = b.wait_for(|v| v["id"] == fetch, 2);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "fetch exceeded its end-to-end deadline"
    );
    assert!(r.get("error").is_some() || r.get("result").is_some());
    assert!(restart.wait().unwrap().success());
    guard.run("start-node", Some(single_leader));

    let plain = bootstrap().expect("plain broker required for concurrent isolation check");
    b.ok(
        "kafka/connect",
        json!({"connectionId":"plain","bootstrapServers":plain,"timeoutMs":1500}),
    );
    for node in 1..=3 {
        guard.run("kill", Some(node));
    }
    let started = Instant::now();
    let pending = b.send("kafka/topics/list", json!({"connectionId":"short"}));
    let r = bounded(
        &mut b,
        "kafka/topics/list",
        json!({"connectionId":"plain"}),
        2,
    );
    assert!(
        r.get("error").is_none(),
        "healthy connection blocked by outage: {r}"
    );
    let r = b.wait_for(|v| v["id"] == pending, 3);
    assert!(started.elapsed() < Duration::from_secs(3));
    assert_eq!(r["error"]["code"], 1000, "{r}");
    assert!(r["error"]["data"]["kafkaCode"].is_string(), "{r}");
    let r = bounded(
        &mut b,
        "kafka/produce",
        json!({"connectionId":"short","topic":single,"partition":0,"value":"leader-down"}),
        4,
    );
    assert_eq!(r["error"]["code"], 1000, "{r}");
    assert_eq!(r["error"]["data"]["kafkaCode"], "MessageTimedOut", "{r}");
    guard.run("start", None);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let r = b.call(
            "kafka/topic/describe",
            json!({"connectionId":"short","topic":topic}),
        );
        if r.get("error").is_none() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "existing connection did not recover: {r}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    eprintln!(
        "all-broker outage recovered without reconnect, healthy connection remained responsive"
    );
    for topic in [topic, single] {
        b.ok(
            "kafka/topic/delete",
            json!({"connectionId":"cluster","topic":topic,"confirmName":topic}),
        );
    }
}
