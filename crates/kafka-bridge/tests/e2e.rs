//! End-to-end tests: spawn the real `kafka-bridge` binary and talk JSON-RPC
//! over stdio against a live broker.
//!
//! Skipped unless `KAFKA_EL_TEST_BOOTSTRAP` is set, e.g.
//!   scripts/kafka-dev.sh start
//!   KAFKA_EL_TEST_BOOTSTRAP=127.0.0.1:19092 cargo test -p kafka-bridge --test e2e

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

struct Bridge {
    child: Child,
    stdin: ChildStdin,
    rx: mpsc::Receiver<Value>,
    next: i64,
    backlog: Vec<Value>,
}

impl Bridge {
    fn spawn() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_kafka-bridge"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut r = BufReader::new(stdout);
            loop {
                let mut len = 0usize;
                loop {
                    let mut l = String::new();
                    if r.read_line(&mut l).unwrap_or(0) == 0 {
                        return;
                    }
                    let l = l.trim();
                    if l.is_empty() {
                        break;
                    }
                    if let Some(v) = l.strip_prefix("Content-Length:") {
                        len = v.trim().parse().unwrap();
                    }
                }
                let mut buf = vec![0; len];
                r.read_exact(&mut buf).unwrap();
                if tx.send(serde_json::from_slice(&buf).unwrap()).is_err() {
                    return;
                }
            }
        });
        Self { child, stdin, rx, next: 1, backlog: Vec::new() }
    }

    fn raw(&mut self, v: &Value) {
        let b = serde_json::to_vec(v).unwrap();
        write!(self.stdin, "Content-Length: {}\r\n\r\n", b.len()).unwrap();
        self.stdin.write_all(&b).unwrap();
        self.stdin.flush().unwrap();
    }

    fn send(&mut self, method: &str, params: Value) -> i64 {
        let id = self.next;
        self.next += 1;
        self.raw(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}));
        id
    }

    fn wait_for(&mut self, pred: impl Fn(&Value) -> bool, secs: u64) -> Value {
        if let Some(i) = self.backlog.iter().position(&pred) {
            return self.backlog.remove(i);
        }
        let end = Instant::now() + Duration::from_secs(secs);
        loop {
            let left = end.saturating_duration_since(Instant::now());
            let v = self.rx.recv_timeout(left).expect("timed out waiting for message");
            if pred(&v) {
                return v;
            }
            self.backlog.push(v);
        }
    }

    fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.send(method, params);
        self.wait_for(|v| v["id"] == json!(id), 60)
    }

    fn ok(&mut self, method: &str, params: Value) -> Value {
        let r = self.call(method, params);
        assert!(r.get("error").is_none(), "{method} failed: {r}");
        r["result"].clone()
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

fn bootstrap() -> Option<String> {
    std::env::var("KAFKA_EL_TEST_BOOTSTRAP").ok().filter(|s| !s.is_empty())
}

fn uniq(p: &str) -> String {
    format!("{p}-{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos())
}

#[test]
fn protocol_errors_without_broker() {
    let mut b = Bridge::spawn();
    let r = b.ok("kafka/initialize", json!({}));
    assert_eq!(r["protocolVersion"], 1);
    let r = b.call("nope", json!({}));
    assert_eq!(r["error"]["code"], -32601);
    let r = b.call("kafka/topics/list", json!({"connectionId": "missing"}));
    assert_eq!(r["error"]["code"], 1001);
    let r = b.call("kafka/topics/list", json!({}));
    assert_eq!(r["error"]["code"], -32602);
    // Malformed JSON body still gets a parse error, and the bridge survives.
    b.stdin.write_all(b"Content-Length: 3\r\n\r\n{x}").unwrap();
    b.stdin.flush().unwrap();
    let r = b.wait_for(|v| v["error"]["code"] == -32700, 5);
    assert!(r["id"].is_null());
    b.ok("kafka/initialize", json!({}));
    // Clean shutdown exits the process.
    b.ok("kafka/shutdown", json!({}));
    let start = Instant::now();
    while b.child.try_wait().unwrap().is_none() {
        assert!(start.elapsed() < Duration::from_secs(5), "bridge did not exit");
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn unreachable_broker_fails_fast() {
    let mut b = Bridge::spawn();
    let r = b.call(
        "kafka/connect",
        json!({"connectionId":"x","bootstrapServers":"127.0.0.1:1","timeoutMs":500}),
    );
    assert_eq!(r["error"]["code"], 1000, "{r}");
}

#[test]
fn full_workflow() {
    let Some(bs) = bootstrap() else {
        eprintln!("skipping: KAFKA_EL_TEST_BOOTSTRAP not set");
        return;
    };
    let mut b = Bridge::spawn();
    let info = b.ok("kafka/connect", json!({"connectionId":"rw","bootstrapServers":bs}));
    assert!(!info["brokers"].as_array().unwrap().is_empty());
    b.ok("kafka/connect", json!({"connectionId":"ro","bootstrapServers":bs,"readOnly":true}));
    assert_eq!(b.ok("kafka/connections", json!({})).as_array().unwrap().len(), 2);

    let topic = uniq("kel-test");
    // Validate-only creates nothing.
    b.ok("kafka/topic/create", json!({"connectionId":"rw","topic":topic,"partitions":3,"replicationFactor":1,"validateOnly":true}));
    let r = b.call("kafka/topic/describe", json!({"connectionId":"rw","topic":topic}));
    assert!(r.get("error").is_some(), "validateOnly must not create: {r}");

    // Read-only connection refuses writes.
    let r = b.call("kafka/topic/create", json!({"connectionId":"ro","topic":topic,"partitions":1,"replicationFactor":1}));
    assert_eq!(r["error"]["code"], 1002);

    b.ok("kafka/topic/create", json!({"connectionId":"rw","topic":topic,"partitions":3,"replicationFactor":1,"configs":{"retention.ms":"3600000"}}));
    // Metadata can lag topic creation briefly.
    let mut found = false;
    for _ in 0..50 {
        let ts = b.ok("kafka/topics/list", json!({"connectionId":"ro","filter":topic}));
        if ts.as_array().unwrap().iter().any(|t| t["name"] == topic.as_str() && t["partitions"] == 3) {
            found = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(found);

    // Configs: retention we set is visible; alter merges, does not wipe it.
    let cfg = b.ok("kafka/topic/configs", json!({"connectionId":"rw","topic":topic}));
    let get = |cfg: &Value, k: &str| cfg.as_array().unwrap().iter().find(|e| e["name"] == k).unwrap()["value"].clone();
    assert_eq!(get(&cfg, "retention.ms"), "3600000");
    let alt = b.ok("kafka/topic/alterConfig", json!({"connectionId":"rw","topic":topic,"configs":{"cleanup.policy":"compact,delete"}}));
    assert_eq!(alt["previous"]["cleanup.policy"], "delete");
    let mut cfg = Value::Null;
    for _ in 0..50 {
        cfg = b.ok("kafka/topic/configs", json!({"connectionId":"rw","topic":topic}));
        if get(&cfg, "cleanup.policy") == "compact,delete" {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(get(&cfg, "cleanup.policy"), "compact,delete");
    assert_eq!(get(&cfg, "retention.ms"), "3600000", "alter must preserve existing overrides");

    // Partitions only grow.
    let r = b.call("kafka/topic/addPartitions", json!({"connectionId":"rw","topic":topic,"totalCount":2}));
    assert_eq!(r["error"]["code"], -32602);
    b.ok("kafka/topic/addPartitions", json!({"connectionId":"rw","topic":topic,"totalCount":4}));

    // Produce text + binary with headers to partition 0.
    for i in 0..5 {
        let d = b.ok("kafka/produce", json!({"connectionId":"rw","topic":topic,"partition":0,"key":format!("k{i}"),"value":format!("{{\"n\":{i}}}"),"headers":[{"key":"h","value":"v"}]}));
        assert_eq!(d["partition"], 0);
        assert_eq!(d["offset"], i);
    }
    b.ok("kafka/produce", json!({"connectionId":"rw","topic":topic,"partition":1,"key":"Ymlu","value":"/wBB","encoding":"base64"}));
    b.ok("kafka/produce", json!({"connectionId":"rw","topic":topic,"partition":1,"key":"tomb","value":null}));
    // Compacted topics reject null keys: the broker error must surface as a Kafka error.
    let r = b.call("kafka/produce", json!({"connectionId":"rw","topic":topic,"partition":3,"value":"nokey"}));
    assert_eq!(r["error"]["code"], 1000);
    assert_eq!(r["error"]["data"]["kafkaCode"], "InvalidRecord");
    let r = b.call("kafka/produce", json!({"connectionId":"ro","topic":topic,"value":"x"}));
    assert_eq!(r["error"]["code"], 1002);

    let desc = b.ok("kafka/topic/describe", json!({"connectionId":"ro","topic":topic}));
    assert_eq!(desc["totalMessages"], 7);
    assert_eq!(desc["partitions"][0]["high"], 5);

    // Fetch earliest: all 7; tail 2 on p0: offsets 3,4.
    let f = b.ok("kafka/records/fetch", json!({"connectionId":"ro","topic":topic,"startPosition":{"type":"earliest"},"limit":100}));
    let recs = f["records"].as_array().unwrap();
    assert_eq!(recs.len(), 7, "{f}");
    assert_eq!(f["truncated"], false);
    let bin = recs.iter().find(|r| r["partition"] == 1 && r["offset"] == 0).unwrap();
    assert_eq!(bin["value"]["encoding"], "base64");
    assert_eq!(bin["value"]["data"], "/wBB");
    let tomb = recs.iter().find(|r| r["partition"] == 1 && r["offset"] == 1).unwrap();
    assert!(tomb["value"].is_null());
    assert_eq!(tomb["key"]["data"], "tomb");
    let first = recs.iter().find(|r| r["partition"] == 0 && r["offset"] == 0).unwrap();
    assert_eq!(first["key"]["data"], "k0");
    assert_eq!(first["headers"][0]["key"], "h");
    assert_eq!(first["headers"][0]["value"]["data"], "v");

    let f = b.ok("kafka/records/fetch", json!({"connectionId":"ro","topic":topic,"partitions":[0],"startPosition":{"type":"tail","count":2}}));
    let offs: Vec<i64> = f["records"].as_array().unwrap().iter().map(|r| r["offset"].as_i64().unwrap()).collect();
    assert_eq!(offs, vec![3, 4]);
    let f = b.ok("kafka/records/fetch", json!({"connectionId":"ro","topic":topic,"startPosition":{"type":"earliest"},"limit":3}));
    assert_eq!(f["records"].as_array().unwrap().len(), 3);
    assert_eq!(f["truncated"], true);
    let f = b.ok("kafka/records/fetch", json!({"connectionId":"ro","topic":topic,"startPosition":{"type":"latest"},"timeoutMs":2000}));
    assert!(f["records"].as_array().unwrap().is_empty());
    let r = b.call("kafka/records/fetch", json!({"connectionId":"ro","topic":topic,"partitions":[99]}));
    assert_eq!(r["error"]["code"], -32602);

    // Offsets for time: timestamp 0 -> beginning.
    let o = b.ok("kafka/offsets/forTime", json!({"connectionId":"ro","topic":topic,"timestamp":0}));
    let p0 = o.as_array().unwrap().iter().find(|e| e["partition"] == 0).unwrap();
    assert_eq!(p0["offset"], 0);

    // Live stream with credit flow control.
    let s = b.ok("kafka/consume/start", json!({"connectionId":"ro","topic":topic,"startPosition":{"type":"earliest"},"batchSize":2,"initialCredit":1}));
    let sid = s["subscriptionId"].as_str().unwrap().to_string();
    let n1 = b.wait_for(|v| v["method"] == "kafka/consume/records" && v["params"]["subscriptionId"] == sid.as_str(), 20);
    assert!(n1["params"]["records"].as_array().unwrap().len() <= 2);
    // Credit exhausted -> paused, no more records until credit.
    b.wait_for(|v| v["method"] == "kafka/consume/state" && v["params"]["state"] == "paused", 20);
    b.ok("kafka/consume/credit", json!({"subscriptionId":sid,"batches":10}));
    let mut seen = n1["params"]["records"].as_array().unwrap().len();
    while seen < 7 {
        let n = b.wait_for(|v| v["method"] == "kafka/consume/records" && v["params"]["subscriptionId"] == sid.as_str(), 20);
        seen += n["params"]["records"].as_array().unwrap().len();
    }
    assert_eq!(seen, 7);
    // New records produced while streaming are delivered.
    b.ok("kafka/produce", json!({"connectionId":"rw","topic":topic,"partition":2,"key":"l","value":"live"}));
    let n = b.wait_for(|v| v["method"] == "kafka/consume/records" && v["params"]["subscriptionId"] == sid.as_str(), 20);
    assert_eq!(n["params"]["records"][0]["value"]["data"], "live");
    b.ok("kafka/consume/stop", json!({"subscriptionId":sid}));
    b.wait_for(|v| v["method"] == "kafka/consume/state" && v["params"]["state"] == "cancelled", 10);
    let r = b.call("kafka/consume/credit", json!({"subscriptionId":sid}));
    assert_eq!(r["error"]["code"], 1004);

    // Inspection never created a visible consumer group with offsets.
    let groups = b.ok("kafka/groups/list", json!({"connectionId":"ro"}));
    for g in groups.as_array().unwrap() {
        let gid = g["groupId"].as_str().unwrap();
        if gid.starts_with("kafka.el-inspect") {
            let d = b.ok("kafka/group/describe", json!({"connectionId":"ro","groupId":gid}));
            assert!(d["offsets"].as_array().unwrap().is_empty(), "inspection group committed offsets: {d}");
        }
    }

    // Delete requires exact confirmation.
    let r = b.call("kafka/topic/delete", json!({"connectionId":"rw","topic":topic,"confirmName":"wrong"}));
    assert_eq!(r["error"]["code"], 1003);
    b.ok("kafka/topic/delete", json!({"connectionId":"rw","topic":topic,"confirmName":topic}));
    b.ok("kafka/disconnect", json!({"connectionId":"rw"}));
    let r = b.call("kafka/topics/list", json!({"connectionId":"rw"}));
    assert_eq!(r["error"]["code"], 1001);
}

#[test]
fn stdin_close_mid_stream_exits_and_leaks_nothing() {
    let Some(bs) = bootstrap() else { return };
    let mut b = Bridge::spawn();
    b.ok("kafka/connect", json!({"connectionId":"c","bootstrapServers":bs}));
    let topic = uniq("kel-leak");
    b.ok("kafka/topic/create", json!({"connectionId":"c","topic":topic,"partitions":2,"replicationFactor":1}));
    for i in 0..20 {
        b.ok("kafka/produce", json!({"connectionId":"c","topic":topic,"key":"k","value":i.to_string()}));
    }
    // Three concurrent streams, one deliberately starved of credit.
    for credit in [0, 1, 4] {
        b.ok("kafka/consume/start", json!({"connectionId":"c","topic":topic,
            "startPosition":{"type":"earliest"},"batchSize":3,"initialCredit":credit}));
    }
    b.wait_for(|v| v["method"] == "kafka/consume/records", 20);
    // Emacs dies: stdin closes without a shutdown request.
    drop(std::mem::replace(&mut b.stdin, {
        // Replace with a dummy pipe we immediately drop, closing the real stdin.
        let mut tmp = Command::new("true").stdin(Stdio::piped()).spawn().unwrap();
        let s = tmp.stdin.take().unwrap();
        let _ = tmp.wait();
        s
    }));
    let start = Instant::now();
    let status = loop {
        if let Some(s) = b.child.try_wait().unwrap() {
            break s;
        }
        assert!(start.elapsed() < Duration::from_secs(10), "bridge kept running after stdin closed");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success(), "exit status {status:?}");
    // No inspection consumer survives as a group member on the broker.
    let mut b2 = Bridge::spawn();
    b2.ok("kafka/connect", json!({"connectionId":"c","bootstrapServers":bs}));
    std::thread::sleep(Duration::from_secs(1));
    for g in b2.ok("kafka/groups/list", json!({"connectionId":"c"})).as_array().unwrap() {
        if g["groupId"].as_str().unwrap().starts_with("kafka.el-inspect") {
            assert_eq!(g["members"], 0, "leaked member: {g}");
        }
    }
    b2.ok("kafka/topic/delete", json!({"connectionId":"c","topic":topic,"confirmName":topic}));
}

#[test]
fn group_lag_is_reported() {
    let Some(bs) = bootstrap() else { return };
    let kafka_home = std::env::var("KAFKA_EL_TEST_KAFKA_HOME").ok();
    let Some(home) = kafka_home else {
        eprintln!("skipping: KAFKA_EL_TEST_KAFKA_HOME not set");
        return;
    };
    let mut b = Bridge::spawn();
    b.ok("kafka/connect", json!({"connectionId":"c","bootstrapServers":bs}));
    let topic = uniq("kel-lag");
    let group = uniq("kel-group");
    b.ok("kafka/topic/create", json!({"connectionId":"c","topic":topic,"partitions":1,"replicationFactor":1}));
    for i in 0..10 {
        b.ok("kafka/produce", json!({"connectionId":"c","topic":topic,"partition":0,"value":i.to_string()}));
    }
    // Use the official CLI to commit offset 4 for the group (independent oracle).
    let st = Command::new(format!("{home}/bin/kafka-consumer-groups.sh"))
        .args(["--bootstrap-server", &bs, "--group", &group, "--topic", &format!("{topic}:0"),
               "--reset-offsets", "--to-offset", "4", "--execute"])
        .stdout(Stdio::null())
        .status()
        .unwrap();
    assert!(st.success());
    let d = b.ok("kafka/group/describe", json!({"connectionId":"c","groupId":group}));
    let o = d["offsets"].as_array().unwrap().iter().find(|o| o["topic"] == topic.as_str()).unwrap().clone();
    assert_eq!(o["committed"], 4);
    assert_eq!(o["high"], 10);
    assert_eq!(o["lag"], 6);
    // Describing did not change the committed offset.
    let d2 = b.ok("kafka/group/describe", json!({"connectionId":"c","groupId":group}));
    assert_eq!(d2["totalLag"], d["totalLag"]);
    let r = b.call("kafka/group/delete", json!({"connectionId":"c","groupId":group,"confirmName":"x"}));
    assert_eq!(r["error"]["code"], 1003);
    b.ok("kafka/group/delete", json!({"connectionId":"c","groupId":group,"confirmName":group}));
    b.ok("kafka/topic/delete", json!({"connectionId":"c","topic":topic,"confirmName":topic}));
}
