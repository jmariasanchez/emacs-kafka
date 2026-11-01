//! Real bridge transaction, isolation and batch acceptance tests.
mod common;
use common::*;
use serde_json::{json, Value};

fn setup(bootstrap: &str) -> (Bridge, String) {
    let mut bridge = Bridge::spawn();
    bridge.ok(
        "kafka/connect",
        json!({"connectionId":"rw", "bootstrapServers":bootstrap, "timeoutMs":15000}),
    );
    static NEXT_TOPIC: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let topic = uniq(&format!(
        "kel-txn-{}-{}",
        std::process::id(),
        NEXT_TOPIC.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    bridge.ok(
        "kafka/topic/create",
        json!({"connectionId":"rw", "topic":topic, "partitions":1, "replicationFactor":1}),
    );
    (bridge, topic)
}
fn fetch(bridge: &mut Bridge, topic: &str, isolation: &str) -> Value {
    bridge.ok("kafka/records/fetch", json!({"connectionId":"rw", "topic":topic,
        "startPosition":{"type":"earliest"}, "isolationLevel":isolation, "limit":20, "timeoutMs":5000}))["records"].clone()
}
fn begin(bridge: &mut Bridge) -> Value {
    bridge.ok("kafka/txn/begin", json!({"connectionId":"rw"}))["txnId"].clone()
}
fn cleanup(bridge: &mut Bridge, topic: &str) {
    bridge.ok(
        "kafka/topic/delete",
        json!({"connectionId":"rw", "topic":topic,"confirmName":topic}),
    );
}

#[test]
fn transaction_abort_commit_isolation_and_cli_oracle() {
    let Some(bootstrap) = bootstrap() else { return };
    let (mut bridge, topic) = setup(&bootstrap);
    let id = begin(&mut bridge);
    for i in 0..3 {
        bridge.ok(
            "kafka/txn/produce",
            json!({"txnId":id,"topic":topic,"value":format!("aborted-{i}")}),
        );
    }
    bridge.ok("kafka/txn/abort", json!({"txnId":id}));
    assert_eq!(
        fetch(&mut bridge, &topic, "read_committed")
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        fetch(&mut bridge, &topic, "read_uncommitted")
            .as_array()
            .unwrap()
            .len(),
        3
    );
    let default_fetch = bridge.ok(
        "kafka/records/fetch",
        json!({
            "connectionId":"rw", "topic":topic, "startPosition":{"type":"earliest"},
            "limit":20, "timeoutMs":5000
        }),
    );
    assert_eq!(default_fetch["records"].as_array().unwrap().len(), 3);
    let id = begin(&mut bridge);
    for i in 0..2 {
        bridge.ok("kafka/txn/produce", json!({"txnId":id,"topic":topic,"key":"key", "value":format!("committed-{i}"), "headers":[{"key":"source","value":"ert"}]}));
    }
    bridge.ok("kafka/txn/commit", json!({"txnId":id}));
    let records = fetch(&mut bridge, &topic, "read_committed");
    assert_eq!(records.as_array().unwrap().len(), 2);
    assert_eq!(records[0]["value"]["data"], "committed-0");
    assert_eq!(records[1]["value"]["data"], "committed-1");
    assert_eq!(records[0]["headers"][0]["value"]["data"], "ert");
    bridge.ok("kafka/consume/start", json!({"connectionId":"rw","topic":topic,"subscriptionId":"committed-stream",
        "startPosition":{"type":"earliest"},"isolationLevel":"read_committed","batchSize":10,"initialCredit":1}));
    let batch = bridge.wait_for(|v| v["method"] == "kafka/consume/records", 20);
    assert_eq!(batch["params"]["records"].as_array().unwrap().len(), 2);
    assert_eq!(
        batch["params"]["records"][0]["value"]["data"],
        "committed-0"
    );
    assert_eq!(
        batch["params"]["records"][1]["value"]["data"],
        "committed-1"
    );
    bridge.ok(
        "kafka/consume/stop",
        json!({"subscriptionId":"committed-stream"}),
    );
    if let Some(home) = env("KAFKA_EL_TEST_KAFKA_HOME") {
        let output = std::process::Command::new(format!("{home}/bin/kafka-console-consumer.sh"))
            .args([
                "--bootstrap-server",
                &bootstrap,
                "--topic",
                &topic,
                "--from-beginning",
                "--isolation-level",
                "read_committed",
                "--max-messages",
                "2",
                "--timeout-ms",
                "10000",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .collect::<Vec<_>>(),
            ["committed-0", "committed-1"]
        );
    }
    cleanup(&mut bridge, &topic);
}

#[test]
fn batch_partial_failure_and_atomic_abort() {
    let Some(bootstrap) = bootstrap() else { return };
    let (mut bridge, topic) = setup(&bootstrap);
    let result = bridge.call("kafka/produce/batch", json!({"connectionId":"rw","topic":topic,
        "records":[{"key":"k","value":"good", "headers":{"source":"batch"}}, {"value":"!!","encoding":"base64"}, {"value":"last"}]}));
    assert_eq!(result["error"]["code"], 1005);
    let results = &result["error"]["data"]["results"];
    assert_eq!(results[0]["ok"], true);
    assert_eq!(results[1]["error"]["code"], 1006);
    assert_eq!(results[2]["ok"], true);
    assert_eq!(
        fetch(&mut bridge, &topic, "read_committed")
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let aborted = bridge.call(
        "kafka/produce/batch",
        json!({"connectionId":"rw","topic":topic,"transactional":true,
        "records":[{"value":"invisible"},{"encoding":"base64","value":"!!"}]}),
    );
    assert_eq!(aborted["error"]["code"], 1005);
    assert_eq!(aborted["error"]["data"]["committed"], false);
    assert_eq!(
        fetch(&mut bridge, &topic, "read_committed")
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let committed = bridge.ok(
        "kafka/produce/batch",
        json!({"connectionId":"rw","topic":topic,"transactional":true,
        "records":[{"value":"atomic-1"},{"value":"atomic-2"}]}),
    );
    assert_eq!(committed["committed"], true);
    assert_eq!(
        fetch(&mut bridge, &topic, "read_committed")
            .as_array()
            .unwrap()
            .len(),
        4
    );
    for records in [vec![], vec![json!({"value":"x"}); 10_001]] {
        let error = bridge.call(
            "kafka/produce/batch",
            json!({"connectionId":"rw","topic":topic,"records":records}),
        );
        assert_eq!(error["error"]["code"], -32602);
    }
    cleanup(&mut bridge, &topic);
}

#[test]
fn idle_timeout_aborts_and_session_ids_are_one_shot() {
    let Some(bootstrap) = bootstrap() else { return };
    let (mut bridge, topic) = setup(&bootstrap);
    let id = bridge.ok(
        "kafka/txn/begin",
        json!({"connectionId":"rw","idleTimeoutMs":1000}),
    )["txnId"]
        .clone();
    bridge.ok(
        "kafka/txn/produce",
        json!({"txnId":id,"topic":topic,"value":"expires"}),
    );
    assert_eq!(
        bridge.call("kafka/txn/begin", json!({"connectionId":"rw"}))["error"]["code"],
        -32602
    );
    std::thread::sleep(std::time::Duration::from_millis(2500));
    assert_eq!(
        bridge.call("kafka/txn/commit", json!({"txnId":id}))["error"]["code"],
        -32602
    );
    assert_eq!(
        fetch(&mut bridge, &topic, "read_committed")
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        fetch(&mut bridge, &topic, "read_uncommitted")
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let id = begin(&mut bridge);
    bridge.ok("kafka/txn/abort", json!({"txnId":id}));
    assert_eq!(
        bridge.call(
            "kafka/txn/produce",
            json!({"txnId":id,"topic":topic,"value":"late"})
        )["error"]["code"],
        -32602
    );
    cleanup(&mut bridge, &topic);
}

#[test]
fn readonly_and_invalid_isolation_are_rejected() {
    let Some(bootstrap) = bootstrap() else { return };
    let (mut bridge, topic) = setup(&bootstrap);
    bridge.ok(
        "kafka/connect",
        json!({"connectionId":"ro","bootstrapServers":bootstrap,"readOnly":true}),
    );
    assert_eq!(
        bridge.call("kafka/txn/begin", json!({"connectionId":"ro"}))["error"]["code"],
        1002
    );
    assert_eq!(
        bridge.call(
            "kafka/produce/batch",
            json!({"connectionId":"ro","topic":topic,"records":[{"value":"no"}]})
        )["error"]["code"],
        1002
    );
    for method in ["kafka/records/fetch", "kafka/consume/start"] {
        assert_eq!(
            bridge.call(
                method,
                json!({"connectionId":"rw","topic":topic,"isolationLevel":"dirty"})
            )["error"]["code"],
            -32602
        );
    }
    cleanup(&mut bridge, &topic);
}

#[tokio::test]
async fn unfinished_core_transaction_aborts_on_drop() {
    let Some(bootstrap) = bootstrap() else { return };
    let (mut bridge, topic) = setup(&bootstrap);
    let (cluster, _) = kafka_core::Cluster::connect(kafka_core::ConnectionConfig {
        id: "drop-test".into(),
        bootstrap_servers: bootstrap,
        timeout: std::time::Duration::from_secs(15),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut transaction = cluster.begin_transaction(uniq("kel-drop")).await.unwrap();
    transaction
        .produce(kafka_core::records::ProduceRequest {
            topic: topic.clone(),
            partition: Some(0),
            key: None,
            value: Some(b"drop-aborted".to_vec()),
            headers: vec![],
            timestamp: None,
        })
        .await
        .unwrap();
    let dropped = std::time::Instant::now();
    drop(transaction);
    assert!(dropped.elapsed() < std::time::Duration::from_millis(250));
    bridge.ok(
        "kafka/produce",
        json!({"connectionId":"rw", "topic":topic, "partition":0, "value":"after-drop"}),
    );
    // A later committed record remains hidden until the earlier transaction
    // is actually aborted. Seeing it proves cleanup, not just invisibility.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let records = fetch(&mut bridge, &topic, "read_committed");
        if !records.as_array().unwrap().is_empty() {
            assert_eq!(records.as_array().unwrap().len(), 1);
            assert_eq!(records[0]["value"]["data"], "after-drop");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "background abort did not release the last stable offset"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        fetch(&mut bridge, &topic, "read_uncommitted")
            .as_array()
            .unwrap()
            .len(),
        2
    );
    cleanup(&mut bridge, &topic);
}

fn lifecycle_case(test: impl FnOnce(&mut Bridge, &mut Bridge, &str, &str)) {
    let Some(bootstrap) = bootstrap() else { return };
    let (mut subject, topic) = setup(&bootstrap);
    let mut observer = Bridge::spawn();
    observer.ok(
        "kafka/connect",
        json!({"connectionId":"rw", "bootstrapServers":bootstrap}),
    );
    // Clean up the unique test topic even when an intentional regression check fails.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        test(&mut subject, &mut observer, &topic, &bootstrap);
    }));
    cleanup(&mut observer, &topic);
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

fn unfinished(bridge: &mut Bridge, connection: &str, topic: &str, value: &str) -> Value {
    let txn = bridge.ok(
        "kafka/txn/begin",
        json!({
            "connectionId":connection, "idleTimeoutMs":300000
        }),
    )["txnId"]
        .clone();
    bridge.ok(
        "kafka/txn/produce",
        json!({
            "txnId":txn, "topic":topic, "partition":0, "value":value
        }),
    );
    txn
}

fn assert_lifecycle_abort(observer: &mut Bridge, topic: &str, abandoned: &[&str]) {
    observer.ok(
        "kafka/produce",
        json!({
            "connectionId":"rw", "topic":topic, "partition":0, "value":"after-lifecycle"
        }),
    );
    // An unclosed transaction also hides its records. A later sentinel becomes
    // visible only after every preceding transaction releases the stable offset.
    // Five seconds is well below the 300-second bridge idle cleanup fallback.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let result = observer.ok(
            "kafka/records/fetch",
            json!({
                "connectionId":"rw", "topic":topic, "startPosition":{"type":"earliest"},
                "isolationLevel":"read_committed", "limit":20, "timeoutMs":500
            }),
        );
        let records = result["records"].as_array().unwrap();
        if !records.is_empty() {
            assert_eq!(
                records.len(),
                1,
                "lifecycle cleanup committed abandoned records: {result}"
            );
            assert_eq!(records[0]["value"]["data"], "after-lifecycle");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "lifecycle cleanup left an open transaction blocking the sentinel"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    let records = fetch(observer, topic, "read_uncommitted");
    let values = records
        .as_array()
        .unwrap()
        .iter()
        .map(|record| record["value"]["data"].as_str().unwrap())
        .collect::<Vec<_>>();
    let mut expected = abandoned.to_vec();
    expected.push("after-lifecycle");
    assert_eq!(
        values, expected,
        "aborted records must remain visible to uncommitted readers"
    );
}

fn assert_clean_exit(bridge: &mut Bridge) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    loop {
        if let Some(status) = bridge.child.try_wait().unwrap() {
            assert!(status.success(), "bridge failed to exit cleanly: {status}");
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "bridge did not exit after lifecycle cleanup"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

#[test]
fn lifecycle_disconnect_aborts_and_invalidates_session() {
    lifecycle_case(|subject, observer, topic, bootstrap| {
        let txn = unfinished(subject, "rw", topic, "abandoned-disconnect");
        let disconnected = subject.ok("kafka/disconnect", json!({"connectionId":"rw"}));
        assert_eq!(disconnected["disconnected"], true);
        assert_eq!(
            subject.call("kafka/txn/commit", json!({"txnId":txn}))["error"]["code"],
            -32602
        );
        assert_eq!(
            subject.call(
                "kafka/txn/produce",
                json!({
                    "txnId":txn, "topic":topic, "value":"stale"
                })
            )["error"]["code"],
            -32602
        );
        assert_lifecycle_abort(observer, topic, &["abandoned-disconnect"]);
        subject.ok(
            "kafka/connect",
            json!({"connectionId":"rw", "bootstrapServers":bootstrap}),
        );
        let fresh = begin(subject);
        assert_ne!(fresh, txn);
        subject.ok("kafka/txn/abort", json!({"txnId":fresh}));
    });
}

#[test]
fn lifecycle_replacement_aborts_old_writable_session() {
    lifecycle_case(|subject, observer, topic, bootstrap| {
        let txn = unfinished(subject, "rw", topic, "abandoned-replacement");
        subject.ok(
            "kafka/connect",
            json!({
                "connectionId":"rw", "bootstrapServers":bootstrap, "readOnly":true
            }),
        );
        assert_eq!(
            subject.call("kafka/txn/commit", json!({"txnId":txn}))["error"]["code"],
            -32602
        );
        assert_eq!(
            subject.call(
                "kafka/txn/produce",
                json!({
                    "txnId":txn, "topic":topic, "value":"stale"
                })
            )["error"]["code"],
            -32602
        );
        assert_eq!(
            subject.call("kafka/txn/begin", json!({"connectionId":"rw"}))["error"]["code"],
            1002
        );
        assert_lifecycle_abort(observer, topic, &["abandoned-replacement"]);
    });
}

#[test]
fn lifecycle_shutdown_aborts_all_connections_before_exit() {
    lifecycle_case(|subject, observer, topic, bootstrap| {
        unfinished(subject, "rw", topic, "abandoned-first");
        subject.ok(
            "kafka/connect",
            json!({"connectionId":"other", "bootstrapServers":bootstrap}),
        );
        unfinished(subject, "other", topic, "abandoned-second");
        assert!(subject.ok("kafka/shutdown", json!({})).is_null());
        assert_clean_exit(subject);
        assert_lifecycle_abort(observer, topic, &["abandoned-first", "abandoned-second"]);
    });
}

#[test]
fn lifecycle_stdin_eof_aborts_before_exit() {
    lifecycle_case(|subject, observer, topic, _bootstrap| {
        unfinished(subject, "rw", topic, "abandoned-eof");
        // Replace the harness's nonoptional stdin with a closed dummy pipe,
        // dropping the real bridge stdin without sending a shutdown request.
        let mut dummy = std::process::Command::new("true")
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let replacement = dummy.stdin.take().unwrap();
        assert!(dummy.wait().unwrap().success());
        drop(std::mem::replace(&mut subject.stdin, replacement));
        assert_clean_exit(subject);
        assert_lifecycle_abort(observer, topic, &["abandoned-eof"]);
    });
}
