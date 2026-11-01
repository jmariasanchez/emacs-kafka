//! Opt-in load probes: KAFKA_EL_TEST_LOAD=1 cargo test -p kafka-bridge --test load -- --ignored --nocapture.
mod common;
use common::*;
use serde_json::json;
use std::time::{Duration, Instant};

fn enabled() -> bool {
    env("KAFKA_EL_TEST_LOAD").as_deref() == Some("1")
}
#[test]
#[ignore = "opt-in load probe, set KAFKA_EL_TEST_LOAD=1"]
fn rpc_overhead_2000() {
    if !enabled() {
        return;
    }
    let mut b = Bridge::spawn();
    b.ok("kafka/initialize", json!({}));
    let mut samples = Vec::new();
    for _ in 0..2000 {
        let start = Instant::now();
        assert!(b
            .ok("kafka/connections", json!({}))
            .as_array()
            .unwrap()
            .is_empty());
        samples.push(start.elapsed().as_micros());
    }
    samples.sort_unstable();
    eprintln!(
        "kafka/connections 2000 stdio RPCs: p50={}us p95={}us p99={}us",
        samples[999], samples[1899], samples[1979]
    );
}
#[test]
#[ignore = "opt-in large record probe, set KAFKA_EL_TEST_LOAD=1"]
fn one_mib_round_trip() {
    if !enabled() {
        return;
    }
    let address = env("KAFKA_EL_TEST_LOAD_BOOTSTRAP").expect("dedicated load broker required");
    let mut b = Bridge::spawn();
    b.ok(
        "kafka/connect",
        json!({"connectionId":"load","bootstrapServers":address,"timeoutMs":10000,
        "properties":{"message.max.bytes":"2097152","fetch.message.max.bytes":"2097152"}}),
    );
    let topic = uniq("load-mib");
    b.ok("kafka/topic/create", json!({"connectionId":"load","topic":topic,"partitions":1,"replicationFactor":1,"configs":{"max.message.bytes":"2097152"}}));
    let value = "x".repeat(1024 * 1024);
    let start = Instant::now();
    b.ok(
        "kafka/produce",
        json!({"connectionId":"load","topic":topic,"value":value}),
    );
    let r = b.ok("kafka/records/fetch", json!({"connectionId":"load","topic":topic,"startPosition":{"type":"earliest"},"limit":1,"timeoutMs":10000}));
    assert_eq!(r["records"][0]["value"]["data"], value);
    assert_eq!(r["records"][0]["value"]["size"], 1024 * 1024);
    eprintln!("1 MiB produce+fetch round trip: {:?}", start.elapsed());
    b.ok(
        "kafka/topic/delete",
        json!({"connectionId":"load","topic":topic,"confirmName":topic}),
    );
}
#[test]
#[ignore = "creates 5000 topics on a dedicated broker, set KAFKA_EL_TEST_LOAD=1"]
fn topics_catalog_5000() {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::ClientConfig;
    if !enabled() {
        return;
    }
    let address = env("KAFKA_EL_TEST_LOAD_BOOTSTRAP").expect("dedicated load broker required");
    assert!(
        address.contains(":49092"),
        "large catalog must not affect shared brokers"
    );
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &address)
        .create()
        .unwrap();
    let prefix = uniq("catalog");
    let names: Vec<String> = (0..5000).map(|n| format!("{prefix}-{n}")).collect();
    let opts = AdminOptions::new()
        .operation_timeout(Some(Duration::from_secs(60)))
        .request_timeout(Some(Duration::from_secs(90)));
    let setup = Instant::now();
    for chunk in names.chunks(100) {
        let topics: Vec<_> = chunk
            .iter()
            .map(|n| NewTopic::new(n, 1, TopicReplication::Fixed(1)))
            .collect();
        let result = runtime
            .block_on(admin.create_topics(&topics, &opts))
            .unwrap();
        assert!(result.iter().all(Result::is_ok), "{result:?}");
    }
    eprintln!("5000-topic setup: {:?}", setup.elapsed());
    let mut b = Bridge::spawn();
    b.ok(
        "kafka/connect",
        json!({"connectionId":"catalog","bootstrapServers":address,"timeoutMs":30000}),
    );
    // CreateTopics acknowledges controller persistence before every broker has
    // applied the metadata image. Measure the settled catalog, not propagation.
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let catalog = b.ok(
            "kafka/topics/list",
            json!({"connectionId":"catalog","filter":prefix}),
        );
        if catalog.as_array().unwrap().len() == 5000 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "catalog metadata did not converge"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    let start = Instant::now();
    let result = b.ok(
        "kafka/topics/list",
        json!({"connectionId":"catalog","filter":prefix}),
    );
    let elapsed = start.elapsed();
    let count = result.as_array().unwrap().len();
    // Clean up even if the catalog completeness assertion below fails.
    for chunk in names.chunks(100) {
        let topics: Vec<_> = chunk.iter().map(String::as_str).collect();
        let result = runtime
            .block_on(admin.delete_topics(&topics, &opts))
            .unwrap();
        assert!(result.iter().all(Result::is_ok), "{result:?}");
    }
    assert_eq!(count, 5000);
    eprintln!("5000 topics/list: {elapsed:?}");
}
