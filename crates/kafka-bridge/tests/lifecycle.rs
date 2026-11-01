//! Cross-adapter lifetime checks against the actual stdio bridge.
mod common;
use common::*;
use serde_json::json;

#[test]
fn reconnect_and_disconnect_discard_adapter_configuration() {
    let Some(bootstrap) = bootstrap() else { return };
    let mut bridge = Bridge::spawn();
    for disconnect in [false, true] {
        bridge.ok(
            "kafka/connect",
            json!({"connectionId":"lifecycle","bootstrapServers":bootstrap}),
        );
        // Configure is local only. No Registry or Connect service is needed.
        for method in ["kafka/registry/configure", "kafka/connect/configure"] {
            bridge.ok(
                method,
                json!({"connectionId":"lifecycle","url":"http://127.0.0.1:1"}),
            );
        }
        if disconnect {
            bridge.ok("kafka/disconnect", json!({"connectionId":"lifecycle"}));
        }
        bridge.ok(
            "kafka/connect",
            json!({"connectionId":"lifecycle","bootstrapServers":bootstrap}),
        );
        for method in ["kafka/registry/subjects", "kafka/connect/info"] {
            let result = bridge.call(method, json!({"connectionId":"lifecycle"}));
            assert_eq!(result["error"]["code"], -32602, "{method}: {result}");
            assert!(
                result["error"]["message"]
                    .as_str()
                    .unwrap()
                    .to_lowercase()
                    .contains("configur"),
                "{result}"
            );
        }
    }
    bridge.ok("kafka/shutdown", json!({}));
}
