mod common;
use base64::{engine::general_purpose::STANDARD, Engine};
use common::*;
use serde_json::{json, Value};

#[test]
fn registry_roundtrip_and_guards() {
    let (Some(bootstrap), Some(url)) = (bootstrap(), env("KAFKA_EL_TEST_REGISTRY")) else {
        return;
    };
    let mut b = Bridge::spawn();
    for (id, ro) in [("rw", false), ("ro", true)] {
        b.ok(
            "kafka/connect",
            json!({"connectionId":id,"bootstrapServers":bootstrap,"readOnly":ro}),
        );
        b.ok(
            "kafka/registry/configure",
            json!({"connectionId":id,"url":url}),
        );
    }
    let topic = uniq("registry");
    let subject = format!("{topic}-value");
    b.ok(
        "kafka/topic/create",
        json!({"connectionId":"rw","topic":topic,"partitions":1,"replicationFactor":1}),
    );
    let schema = r#"{"type":"record","name":"Item","fields":[{"name":"name","type":"string"},{"name":"count","type":"long"}]}"#;
    let registered = b.ok(
        "kafka/registry/register",
        json!({"connectionId":"rw","subject":subject,"schema":schema}),
    );
    let id = registered["id"].as_u64().unwrap();
    let oracle = std::process::Command::new("curl")
        .args(["-fsS", &format!("{url}/subjects/{subject}/versions/latest")])
        .output()
        .unwrap();
    assert!(oracle.status.success());
    let oracle: Value = serde_json::from_slice(&oracle.stdout).unwrap();
    assert_eq!(oracle["id"], id);
    assert_eq!(
        b.ok(
            "kafka/registry/versions",
            json!({"connectionId":"rw","subject":subject})
        ),
        json!([1])
    );
    assert!(b
        .ok("kafka/registry/subjects", json!({"connectionId":"rw"}))
        .as_array()
        .unwrap()
        .contains(&json!(subject)));
    assert_eq!(
        b.ok(
            "kafka/registry/schema",
            json!({"connectionId":"rw","schemaId":id})
        )["schema"],
        oracle["schema"]
    );
    b.ok(
        "kafka/registry/config",
        json!({"connectionId":"rw","subject":subject,"compatibility":"BACKWARD"}),
    );
    assert_eq!(
        b.ok(
            "kafka/registry/config",
            json!({"connectionId":"rw","subject":subject})
        )["compatibilityLevel"],
        "BACKWARD"
    );
    let incompatible = r#"{"type":"record","name":"Item","fields":[{"name":"name","type":"string"},{"name":"count","type":"string"}]}"#;
    assert_eq!(
        b.ok(
            "kafka/registry/compatibility",
            json!({"connectionId":"rw","subject":subject,"schema":incompatible})
        )["is_compatible"],
        false
    );
    let value = json!({"name":"hello","count":42});
    b.ok(
        "kafka/registry/produce",
        json!({"connectionId":"rw","topic":topic,"value":value}),
    );
    let fetched = b.ok(
        "kafka/records/fetch",
        json!({"connectionId":"rw","topic":topic,"startPosition":{"type":"earliest"},"limit":10}),
    );
    let record = &fetched["records"][0]["value"];
    let bytes = if record["encoding"] == "base64" {
        record["data"].as_str().unwrap().to_string()
    } else {
        STANDARD.encode(record["data"].as_str().unwrap().as_bytes())
    };
    let decoded = b.ok(
        "kafka/registry/decode",
        json!({"connectionId":"rw","bytes":bytes}),
    );
    assert_eq!(decoded["schemaId"], id);
    assert_eq!(decoded["json"], value);
    for (method, extra) in [
        ("register", json!({"schema":schema})),
        ("delete", json!({"confirmName":subject})),
        ("config", json!({"compatibility":"NONE"})),
        ("produce", json!({"topic":topic,"value":value})),
    ] {
        let mut p = json!({"connectionId":"ro","subject":subject});
        p.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert_eq!(
            b.call(&format!("kafka/registry/{method}"), p)["error"]["code"],
            1002
        );
    }
    assert_eq!(
        b.call(
            "kafka/registry/delete",
            json!({"connectionId":"rw","subject":subject,"confirmName":"wrong","permanent":true})
        )["error"]["code"],
        1003
    );
    assert_eq!(
        b.call(
            "kafka/registry/decode",
            json!({"connectionId":"rw","bytes":"AA=="})
        )["error"]["code"],
        1006
    );
    for permanent in [false, true] {
        b.ok("kafka/registry/delete",json!({"connectionId":"rw","subject":subject,"confirmName":subject,"permanent":permanent}));
    }
    b.ok(
        "kafka/topic/delete",
        json!({"connectionId":"rw","topic":topic,"confirmName":topic}),
    );
}

#[test]
fn registry_json_and_protobuf_live() {
    let (Some(bootstrap), Some(url)) = (bootstrap(), env("KAFKA_EL_TEST_REGISTRY")) else {
        return;
    };
    let mut b = Bridge::spawn();
    b.ok(
        "kafka/connect",
        json!({"connectionId":"rw","bootstrapServers":bootstrap}),
    );
    b.ok(
        "kafka/registry/configure",
        json!({"connectionId":"rw","url":url}),
    );
    for (kind, source) in [
        (
            "JSON",
            r#"{"type":"object","properties":{"name":{"type":"string"},"count":{"type":"integer"}}}"#,
        ),
        (
            "PROTOBUF",
            "syntax = \"proto3\"; message Item { string name = 1; int32 count = 2; }",
        ),
    ] {
        let topic = uniq("registry-codec");
        let subject = format!("{topic}-value");
        b.ok(
            "kafka/topic/create",
            json!({"connectionId":"rw","topic":topic,"partitions":1,"replicationFactor":1}),
        );
        b.ok(
            "kafka/registry/register",
            json!({"connectionId":"rw","subject":subject,"schemaType":kind,"schema":source}),
        );
        let value = json!({"name":"live","count":9});
        b.ok(
            "kafka/registry/produce",
            json!({"connectionId":"rw","topic":topic,"value":value}),
        );
        let records = b.ok("kafka/records/fetch",json!({"connectionId":"rw","topic":topic,"startPosition":{"type":"earliest"},"limit":10}));
        let record = &records["records"][0]["value"];
        let bytes = if record["encoding"] == "base64" {
            record["data"].as_str().unwrap().to_owned()
        } else {
            STANDARD.encode(record["data"].as_str().unwrap().as_bytes())
        };
        assert_eq!(
            b.ok(
                "kafka/registry/decode",
                json!({"connectionId":"rw","bytes":bytes})
            )["json"],
            value
        );
        b.ok(
            "kafka/registry/delete",
            json!({"connectionId":"rw","subject":subject,"confirmName":subject,"version":1}),
        );
        b.ok("kafka/registry/delete",json!({"connectionId":"rw","subject":subject,"confirmName":subject,"version":1,"permanent":true}));
        b.ok(
            "kafka/topic/delete",
            json!({"connectionId":"rw","topic":topic,"confirmName":topic}),
        );
    }
}
