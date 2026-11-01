mod common;
use common::*;
use serde_json::{json, Value};
use std::process::Command;

fn connect(b: &mut Bridge, bs: &str) {
    b.ok(
        "kafka/connect",
        json!({"connectionId":"c","bootstrapServers":bs}),
    );
}
fn topic(b: &mut Bridge, name: &str) {
    b.ok(
        "kafka/topic/create",
        json!({"connectionId":"c","topic":name,"partitions":2,"replicationFactor":1}),
    );
}
fn cli(home: &str, tool: &str, args: &[&str]) -> String {
    let out = Command::new(format!("{home}/bin/{tool}.sh"))
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}
#[test]
fn native_configs_cluster_and_records() {
    let Some(bs) = bootstrap() else { return };
    let mut b = Bridge::spawn();
    connect(&mut b, &bs);
    let t = uniq("admin-config");
    topic(&mut b, &t);
    let info = b.ok("kafka/cluster/info", json!({"connectionId":"c"}));
    assert!(!info["clusterId"].as_str().unwrap().is_empty());
    assert!(!info["nodes"].as_array().unwrap().is_empty());
    for (key, value) in [("retention.ms", "3600000"), ("segment.ms", "1800000")] {
        b.ok("kafka/configs/incrementalAlter",json!({"connectionId":"c","resourceType":"topic","name":t,"changes":[{"name":key,"operation":"set","value":value}]}));
        let mut visible = false;
        for _ in 0..50 {
            let configs = b.ok("kafka/topic/configs", json!({"connectionId":"c","topic":t}));
            if configs
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e["name"] == key && e["value"] == value)
            {
                visible = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        assert!(visible, "config update did not propagate");
    }
    b.ok("kafka/configs/incrementalAlter",json!({"connectionId":"c","resourceType":"topic","name":t,"validateOnly":true,"changes":[{"name":"retention.ms","operation":"set","value":"7200000"}]}));
    let configs = b.ok("kafka/topic/configs", json!({"connectionId":"c","topic":t}));
    assert_eq!(
        configs
            .as_array()
            .unwrap()
            .iter()
            .find(|x| x["name"] == "retention.ms")
            .unwrap()["value"],
        "3600000"
    );
    assert_eq!(
        configs
            .as_array()
            .unwrap()
            .iter()
            .find(|x| x["name"] == "segment.ms")
            .unwrap()["value"],
        "1800000"
    );
    b.ok("kafka/configs/incrementalAlter",json!({"connectionId":"c","resourceType":"topic","name":t,"changes":[{"name":"retention.ms","operation":"delete"}]}));
    for op in ["append", "subtract"] {
        b.ok("kafka/configs/incrementalAlter",json!({"connectionId":"c","resourceType":"topic","name":t,"changes":[{"name":"cleanup.policy","operation":op,"value":"compact"}]}));
    }
    b.ok("kafka/configs/incrementalAlter",json!({"connectionId":"c","resourceType":"broker","name":info["nodes"][0]["id"].to_string(),"validateOnly":true,"changes":[{"name":"log.retention.ms","operation":"set","value":"3600000"}]}));
    for i in 0..5 {
        b.ok(
            "kafka/produce",
            json!({"connectionId":"c","topic":t,"partition":0,"value":i.to_string()}),
        );
    }
    assert_eq!(b.call("kafka/topic/deleteRecords",json!({"connectionId":"c","topic":t,"offsets":[{"partition":0,"offset":3}],"confirmName":"wrong"}))["error"]["code"],1003);
    b.ok("kafka/topic/deleteRecords",json!({"connectionId":"c","topic":t,"offsets":[{"partition":0,"offset":3}],"confirmName":t}));
    let d = b.ok(
        "kafka/topic/describe",
        json!({"connectionId":"c","topic":t}),
    );
    assert_eq!(d["partitions"][0]["low"], 3);
    b.ok(
        "kafka/topic/delete",
        json!({"connectionId":"c","topic":t,"confirmName":t}),
    );
}
#[test]
fn offsets_reset_and_cli_oracle() {
    let (Some(bs), Some(home)) = (bootstrap(), env("KAFKA_EL_TEST_KAFKA_HOME")) else {
        return;
    };
    let mut b = Bridge::spawn();
    connect(&mut b, &bs);
    let t = uniq("admin-offsets");
    let g = uniq("admin-group");
    topic(&mut b, &t);
    for i in 0..10 {
        b.ok(
            "kafka/produce",
            json!({"connectionId":"c","topic":t,"partition":0,"value":i.to_string()}),
        );
    }
    let fresh = b.ok("kafka/group/resetOffsets",json!({"connectionId":"c","groupId":g,"topics":[{"topic":t,"partitions":[0]}],"spec":{"type":"to-earliest"},"dryRun":true}));
    assert_eq!(fresh["offsets"].as_array().unwrap().len(), 1);
    cli(
        &home,
        "kafka-consumer-groups",
        &[
            "--bootstrap-server",
            &bs,
            "--group",
            &g,
            "--topic",
            &format!("{t}:0"),
            "--reset-offsets",
            "--to-offset",
            "4",
            "--execute",
        ],
    );
    let o = b.ok(
        "kafka/group/offsets",
        json!({"connectionId":"c","groupId":g}),
    );
    assert_eq!(o[0]["committed"], 4);
    let m = b.ok(
        "kafka/group/members",
        json!({"connectionId":"c","groupId":g}),
    );
    assert_eq!(m["state"], "Empty");
    assert!(m["members"].as_array().unwrap().is_empty());
    let mut p = json!({"connectionId":"c","groupId":g,"topics":[{"topic":t,"partitions":[0]}],"spec":{"type":"shift-by","offset":2},"dryRun":true});
    let preview = b.ok("kafka/group/resetOffsets", p.clone());
    assert_eq!(preview["offsets"][0]["newOffset"], 6);
    assert_eq!(
        b.ok(
            "kafka/group/offsets",
            json!({"connectionId":"c","groupId":g})
        )[0]["committed"],
        4
    );
    p["dryRun"] = json!(false);
    p["confirmName"] = json!("wrong");
    assert_eq!(
        b.call("kafka/group/resetOffsets", p.clone())["error"]["code"],
        1003
    );
    p["confirmName"] = json!(g);
    b.ok("kafka/group/resetOffsets", p.clone());
    let oracle = cli(
        &home,
        "kafka-consumer-groups",
        &["--bootstrap-server", &bs, "--group", &g, "--describe"],
    );
    let row = oracle.lines().find(|l| l.contains(&t)).unwrap();
    let cells: Vec<_> = row.split_whitespace().collect();
    assert_eq!(cells[3], "6", "{oracle}");
    for (spec, expected) in [
        (json!({"type":"to-earliest"}), 0),
        (json!({"type":"to-latest"}), 10),
        (json!({"type":"to-offset","offset":3}), 3),
        (json!({"type":"to-datetime","timestamp":1}), 0),
    ] {
        p["spec"] = spec;
        p["dryRun"] = json!(true);
        assert_eq!(
            b.ok("kafka/group/resetOffsets", p.clone())["offsets"][0]["newOffset"],
            expected
        );
    }
    b.ok(
        "kafka/group/deleteOffsets",
        json!({"connectionId":"c","groupId":g,"topic":t,"confirmName":g}),
    );
    assert!(b
        .ok(
            "kafka/group/offsets",
            json!({"connectionId":"c","groupId":g})
        )
        .as_array()
        .unwrap()
        .is_empty());
    b.ok(
        "kafka/group/delete",
        json!({"connectionId":"c","groupId":g,"confirmName":g}),
    );
    b.ok(
        "kafka/topic/delete",
        json!({"connectionId":"c","topic":t,"confirmName":t}),
    );
}
fn secure_connect(
    b: &mut Bridge,
    bs: &str,
    id: &str,
    user: &str,
    password: &str,
    mechanism: &str,
) -> Value {
    b.call("kafka/connect",json!({"connectionId":id,"bootstrapServers":bs,"securityProtocol":"SASL_PLAINTEXT","saslMechanism":mechanism,"saslUsername":user,"saslPassword":password,"timeoutMs":5000}))
}
#[test]
fn secure_acls_and_scram() {
    let (Some(bs), Some(home), Some(config)) = (
        env("KAFKA_EL_TEST_SECURE_BOOTSTRAP"),
        env("KAFKA_EL_TEST_KAFKA_HOME"),
        env("KAFKA_EL_TEST_ADMIN_CONFIG"),
    ) else {
        return;
    };
    let mut b = Bridge::spawn();
    assert!(
        secure_connect(&mut b, &bs, "c", "admin", "admin-secret", "PLAIN")
            .get("error")
            .is_none()
    );
    let t = uniq("admin-acl");
    topic(&mut b, &t);
    let acl = json!({"resourceType":"topic","name":t,"patternType":"literal","principal":"User:alice","host":"*","operation":"read","permission":"allow"});
    b.ok("kafka/acls/create", json!({"connectionId":"c","acl":acl}));
    let mut visible = false;
    for _ in 0..50 {
        let a = b.ok(
            "kafka/acls/describe",
            json!({"connectionId":"c","filter":{"name":t}}),
        );
        if a.as_array().unwrap().len() == 1 {
            visible = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(visible, "created ACL did not propagate");
    let oracle = cli(
        &home,
        "kafka-acls",
        &[
            "--bootstrap-server",
            &bs,
            "--command-config",
            &config,
            "--list",
            "--topic",
            &t,
        ],
    );
    assert!(oracle.contains("User:alice"));
    assert!(oracle.contains("READ"));
    assert!(
        secure_connect(&mut b, &bs, "alice", "alice", "alice-secret", "PLAIN")
            .get("error")
            .is_none()
    );
    b.ok(
        "kafka/topic/describe",
        json!({"connectionId":"alice","topic":t}),
    );
    let deleted = b.ok(
        "kafka/acls/delete",
        json!({"connectionId":"c","filter":acl,"confirmName":t}),
    );
    assert_eq!(deleted.as_array().unwrap().len(), 1);
    // Reconnect to discard cached metadata after ACL revocation.
    b.ok("kafka/disconnect", json!({"connectionId":"alice"}));
    assert!(
        secure_connect(&mut b, &bs, "alice", "alice", "alice-secret", "PLAIN")
            .get("error")
            .is_none()
    );
    assert!(b
        .call(
            "kafka/topic/describe",
            json!({"connectionId":"alice","topic":t})
        )
        .get("error")
        .is_some());
    let user = uniq("admin-scram");
    let denied = b.call("kafka/configs/incrementalAlter",json!({"connectionId":"c","resourceType":"topic","name":t,"changes":[{"name":"kafka.el.invalid.config","operation":"set","value":"x"}]}));
    assert_eq!(denied["error"]["code"], 1000);
    assert!(denied["error"]["data"]["kafkaCode"].is_string());
    let txn = uniq("admin-txn-acl");
    b.ok("kafka/acls/create",json!({"connectionId":"c","acl":{"resourceType":"transactional-id","name":txn,"principal":"User:alice","host":"*","operation":"write","permission":"allow"}}));
    let mut returned = Value::Null;
    for _ in 0..50 {
        let entries = b.ok(
            "kafka/acls/describe",
            json!({"connectionId":"c","filter":{"name":txn}}),
        );
        if let Some(entry) = entries.as_array().unwrap().first() {
            returned = entry.clone();
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert_eq!(returned["resourceType"], "transactional-id");
    b.ok(
        "kafka/acls/delete",
        json!({"connectionId":"c","filter":returned,"confirmName":txn}),
    );
    let password = "unique-test-password-123";
    b.ok("kafka/scram/alter",json!({"connectionId":"c","change":{"operation":"upsert","user":user,"mechanism":"SCRAM-SHA-256","password":password}}));
    let users = b.ok(
        "kafka/scram/describe",
        json!({"connectionId":"c","users":[user]}),
    );
    assert_eq!(users[0]["credentials"][0]["mechanism"], "SCRAM-SHA-256");
    assert!(!users.to_string().contains(password));
    assert!(
        secure_connect(&mut b, &bs, "scram", &user, password, "SCRAM-SHA-256")
            .get("error")
            .is_none()
    );
    b.ok("kafka/scram/alter",json!({"connectionId":"c","change":{"operation":"delete","user":user,"mechanism":"SCRAM-SHA-256","confirmName":user}}));
    b.ok("kafka/disconnect", json!({"connectionId":"scram"}));
    assert!(
        secure_connect(&mut b, &bs, "scram", &user, password, "SCRAM-SHA-256")
            .get("error")
            .is_some()
    );
    b.ok(
        "kafka/topic/delete",
        json!({"connectionId":"c","topic":t,"confirmName":t}),
    );
}
#[test]
fn scram_parse_errors_never_echo_secrets() {
    let mut b = Bridge::spawn();
    let r=b.call("kafka/scram/alter",json!({"connectionId":"c","change":{"operation":"upsert","user":"x","mechanism":"SCRAM-SHA-256","password":{"secret":"DO-NOT-ECHO"}}}));
    assert_eq!(r["error"]["code"], -32602);
    assert!(!r.to_string().contains("DO-NOT-ECHO"));
}

#[test]
fn active_group_refuses_reset_and_reports_assignments() {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use std::time::{Duration, Instant};
    let Some(bs) = bootstrap() else { return };
    let mut b = Bridge::spawn();
    connect(&mut b, &bs);
    let t = uniq("admin-active");
    let g = uniq("admin-active-group");
    topic(&mut b, &t);
    let consumer: BaseConsumer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &bs)
        .set("group.id", &g)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .unwrap();
    consumer.subscribe(&[&t]).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while consumer.assignment().unwrap().count() == 0 && Instant::now() < deadline {
        consumer.poll(Duration::from_millis(100));
    }
    assert_eq!(consumer.assignment().unwrap().count(), 2);
    let m = b.ok(
        "kafka/group/members",
        json!({"connectionId":"c","groupId":g}),
    );
    assert_eq!(m["state"], "Stable");
    assert_eq!(m["members"][0]["assignment"].as_array().unwrap().len(), 2);
    let r = b.call("kafka/group/resetOffsets",json!({"connectionId":"c","groupId":g,"topics":[{"topic":t}],"spec":{"type":"to-earliest"},"dryRun":false,"confirmName":g}));
    assert_eq!(r["error"]["code"], -32602);
    assert!(r["error"]["message"]
        .as_str()
        .unwrap()
        .contains("stop all members"));
    drop(consumer);
    b.ok(
        "kafka/group/delete",
        json!({"connectionId":"c","groupId":g,"confirmName":g}),
    );
    b.ok(
        "kafka/topic/delete",
        json!({"connectionId":"c","topic":t,"confirmName":t}),
    );
}

#[test]
fn all_admin_writes_enforce_read_only() {
    let Some(bs) = bootstrap() else { return };
    let mut b = Bridge::spawn();
    b.ok(
        "kafka/connect",
        json!({"connectionId":"ro","bootstrapServers":bs,"readOnly":true}),
    );
    for (method, mut p) in [
        ("kafka/acls/create", json!({"acl":{}})),
        ("kafka/acls/delete", json!({"filter":{}})),
        (
            "kafka/configs/incrementalAlter",
            json!({"resourceType":"topic","name":"absent","changes":[]}),
        ),
        (
            "kafka/group/resetOffsets",
            json!({"groupId":"absent","topics":[],"spec":{"type":"to-earliest"},"dryRun":true}),
        ),
        (
            "kafka/group/deleteOffsets",
            json!({"groupId":"absent","topic":"absent","confirmName":"wrong"}),
        ),
        (
            "kafka/topic/deleteRecords",
            json!({"topic":"absent","offsets":[],"confirmName":"wrong"}),
        ),
        (
            "kafka/scram/alter",
            json!({"change":{"operation":"delete","user":"absent","mechanism":"SCRAM-SHA-256","confirmName":"wrong"}}),
        ),
    ] {
        p["connectionId"] = json!("ro");
        assert_eq!(b.call(method, p)["error"]["code"], 1002, "{method}");
    }
}
