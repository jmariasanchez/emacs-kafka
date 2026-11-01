mod common;
use common::*;
use serde_json::{json, Value};
use std::{io::Write, process::Command, thread::sleep, time::Duration};

#[test]
fn streams_wordcount_demo() {
    let (Some(home), Some(bootstrap)) = (env("KAFKA_EL_TEST_KAFKA_HOME"), bootstrap()) else {
        return;
    };
    if !std::fs::read_dir(format!("{home}/libs")).unwrap().any(|e| {
        e.unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("kafka-streams-examples-")
    }) {
        eprintln!("Streams demo skipped: distribution has no examples jar");
        return;
    }
    let mut b = Bridge::spawn();
    b.ok(
        "kafka/connect",
        json!({"connectionId":"s","bootstrapServers":bootstrap}),
    );
    // The official demo fixes its input/output names. Do not delete shared topics.
    for topic in ["streams-plaintext-input", "streams-wordcount-output"] {
        let output = Command::new(format!("{home}/bin/kafka-topics.sh"))
            .args([
                "--bootstrap-server",
                &bootstrap,
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
    }
    let app = uniq("kel-connect-streams");
    let dir = std::env::temp_dir().join(&app);
    std::fs::create_dir_all(&dir).unwrap();
    let config = dir.join("streams.properties");
    std::fs::write(&config, format!("bootstrap.servers={bootstrap}\napplication.id={app}\nstate.dir={}\nreplication.factor=1\ncommit.interval.ms=100\n", dir.display())).unwrap();
    struct Demo(std::process::Child);
    impl Drop for Demo {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let log = std::fs::File::create(dir.join("demo.log")).unwrap();
    let _demo = Demo(
        Command::new(format!("{home}/bin/kafka-run-class.sh"))
            .arg("org.apache.kafka.streams.examples.wordcount.WordCountDemo")
            .arg(config)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap(),
    );
    b.ok("kafka/produce", json!({"connectionId":"s","topic":"streams-plaintext-input","value":"connect lane streams test"}));
    for _ in 0..90 {
        let apps = b.ok("kafka/streams/apps", json!({"connectionId":"s"}));
        if let Some(found) = apps.as_array().unwrap().iter().find(|a| a["appId"] == app) {
            assert!(found["members"].as_u64().unwrap() > 0);
            assert!(found["totalLag"].is_number());
            assert!(!found["internalTopics"].as_array().unwrap().is_empty());
            return;
        }
        sleep(Duration::from_secs(1));
    }
    panic!("Streams app not detected, see {}/demo.log", dir.display());
}

fn state(b: &mut Bridge, n: &str, expected: &str) {
    for _ in 0..60 {
        let d = b.call(
            "kafka/connect/connector",
            json!({"connectionId":"c","connector":n}),
        );
        if d["result"]["status"]["connector"]["state"] == expected {
            return;
        }
        sleep(Duration::from_millis(500));
    }
    panic!("connector did not become {expected}");
}
#[test]
fn connect_lifecycle() {
    let (Some(url), Some(bootstrap)) = (env("KAFKA_EL_TEST_CONNECT"), bootstrap()) else {
        return;
    };
    let mut b = Bridge::spawn();
    for (id, ro) in [("c", false), ("ro", true)] {
        b.ok(
            "kafka/connect",
            json!({"connectionId":id,"bootstrapServers":bootstrap,"readOnly":ro}),
        );
        b.ok(
            "kafka/connect/configure",
            json!({"connectionId":id,"url":if ro { "http://127.0.0.1:1" } else { &url }}),
        );
    }
    for action in [
        "upsert",
        "pause",
        "resume",
        "stop",
        "restart",
        "restartTask",
        "delete",
        "offsets/alter",
        "offsets/reset",
    ] {
        let r = b.call(
            &format!("kafka/connect/{action}"),
            json!({"connectionId":"ro","connector":"no-network"}),
        );
        assert_eq!(r["error"]["code"], 1002, "{action}: {r}");
    }
    assert!(b.ok("kafka/connect/info", json!({"connectionId":"c"}))["version"].is_string());
    let class = "org.apache.kafka.connect.file.FileStreamSourceConnector";
    assert!(b
        .ok("kafka/connect/plugins", json!({"connectionId":"c"}))
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["class"] == class));
    let invalid = b.ok("kafka/connect/validate", json!({"connectionId":"c","class":class,"config":{"connector.class":class,"tasks.max":"bad"}}));
    assert!(invalid["error_count"].as_u64().unwrap() > 0);
    let name = uniq("kel-connect-source");
    let topic = uniq("kel-connect-data");
    let path = std::env::temp_dir().join(format!("{name}.txt"));
    std::fs::write(&path, "first\n").unwrap();
    b.ok(
        "kafka/topic/create",
        json!({"connectionId":"c","topic":topic,"partitions":1,"replicationFactor":1}),
    );
    let config =
        json!({"name":name,"connector.class":class,"tasks.max":"1","file":path,"topic":topic});
    b.ok(
        "kafka/connect/upsert",
        json!({"connectionId":"c","connector":name,"config":config}),
    );
    state(&mut b, &name, "RUNNING");
    writeln!(
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap(),
        "second"
    )
    .unwrap();
    let mut records = Value::Null;
    for _ in 0..60 {
        records = b.ok("kafka/records/fetch", json!({"connectionId":"c","topic":topic,"startPosition":{"type":"earliest"},"limit":10,"timeoutMs":2000}));
        if records["records"].as_array().unwrap().len() >= 2 {
            break;
        }
        sleep(Duration::from_millis(500));
    }
    let values: Vec<_> = records["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["value"]["data"].as_str().unwrap())
        .collect();
    assert_eq!(values, ["first", "second"]);
    let oracle = Command::new("curl")
        .args(["-fsS", &format!("{url}/connectors/{name}/status")])
        .output()
        .unwrap();
    assert!(oracle.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&oracle.stdout).unwrap()["connector"]["state"],
        "RUNNING"
    );
    assert!(b
        .ok("kafka/connect/connectors", json!({"connectionId":"c"}))
        .get(&name)
        .is_some());
    b.ok(
        "kafka/connect/topics",
        json!({"connectionId":"c","connector":name}),
    );
    b.ok(
        "kafka/connect/taskStatus",
        json!({"connectionId":"c","connector":name,"task":0}),
    );
    b.ok(
        "kafka/connect/restartTask",
        json!({"connectionId":"c","connector":name,"task":0}),
    );
    b.ok(
        "kafka/connect/restart",
        json!({"connectionId":"c","connector":name,"includeTasks":true,"onlyFailed":true}),
    );
    b.ok(
        "kafka/connect/pause",
        json!({"connectionId":"c","connector":name}),
    );
    state(&mut b, &name, "PAUSED");
    b.ok(
        "kafka/connect/resume",
        json!({"connectionId":"c","connector":name}),
    );
    state(&mut b, &name, "RUNNING");
    b.ok(
        "kafka/connect/stop",
        json!({"connectionId":"c","connector":name}),
    );
    state(&mut b, &name, "STOPPED");
    let offsets = b.ok(
        "kafka/connect/offsets",
        json!({"connectionId":"c","connector":name}),
    );
    assert!(!offsets["offsets"].as_array().unwrap().is_empty());
    for action in ["delete", "offsets/alter", "offsets/reset"] {
        assert_eq!(
            b.call(
                &format!("kafka/connect/{action}"),
                json!({"connectionId":"c","connector":name,"confirmName":"wrong"})
            )["error"]["code"],
            1003
        );
    }
    b.ok(
        "kafka/connect/offsets/alter",
        json!({"connectionId":"c","connector":name,"confirmName":name,"offsets":offsets}),
    );
    b.ok(
        "kafka/connect/offsets/reset",
        json!({"connectionId":"c","connector":name,"confirmName":name}),
    );
    assert!(b.ok(
        "kafka/connect/offsets",
        json!({"connectionId":"c","connector":name})
    )["offsets"]
        .as_array()
        .unwrap()
        .is_empty());
    b.ok(
        "kafka/connect/delete",
        json!({"connectionId":"c","connector":name,"confirmName":name}),
    );
    let err = b.call(
        "kafka/connect/connector",
        json!({"connectionId":"c","connector":name}),
    );
    assert_eq!(err["error"]["data"]["error_code"], 404);
    b.ok(
        "kafka/topic/delete",
        json!({"connectionId":"c","topic":topic,"confirmName":topic}),
    );
    std::fs::remove_file(path).unwrap();
}
