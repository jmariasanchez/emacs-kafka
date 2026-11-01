//! kafka-bridge: JSON-RPC 2.0 over stdio between Emacs and Kafka.
//!
//! stdout carries only protocol frames. Diagnostics go to stderr.

mod convert;
mod dispatch;
mod ext;
mod transport;

use kafka_rpc_protocol::{codes, response_err, Incoming, RpcError};
use serde_json::Value;
use tokio::io::BufReader;
use tokio::sync::mpsc;

use transport::{read_frame, write_frame, FrameError};

#[tokio::main]
async fn main() {
    if std::env::args().any(|a| a == "--version") {
        println!("kafka-bridge {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    let (tx, mut rx) = mpsc::channel::<Value>(dispatch::OUTBOX_CAPACITY);
    let writer = tokio::spawn(async move {
        let mut out = tokio::io::stdout();
        while let Some(v) = rx.recv().await {
            let body = serde_json::to_vec(&v).expect("serializable");
            if write_frame(&mut out, &body).await.is_err() {
                break; // Emacs went away.
            }
        }
    });

    let state = dispatch::State::new(tx);
    let mut input = BufReader::new(tokio::io::stdin());
    loop {
        let frame = tokio::select! {
            f = read_frame(&mut input) => f,
            _ = state.shutdown_signal.notified() => break,
        };
        match frame {
            Ok(body) => match serde_json::from_slice::<Incoming>(&body) {
                Ok(msg) => state.handle(msg).await,
                Err(e) => {
                    let id = serde_json::from_slice::<Value>(&body)
                        .ok()
                        .and_then(|v| v.get("id").cloned())
                        .unwrap_or(Value::Null);
                    state.send(response_err(id, &RpcError::new(codes::PARSE_ERROR, e.to_string()))).await;
                }
            },
            Err(FrameError::BadHeader(h)) => {
                eprintln!("kafka-bridge: bad frame header: {h}");
                state
                    .send(response_err(Value::Null, &RpcError::new(codes::PARSE_ERROR, format!("bad header: {h}"))))
                    .await;
            }
            Err(FrameError::TooLarge(n)) => {
                eprintln!("kafka-bridge: frame too large ({n} bytes); exiting");
                break;
            }
            Err(FrameError::Io(e)) => {
                eprintln!("kafka-bridge: input error: {e}; exiting");
                break;
            }
            Err(FrameError::Eof) => break,
        }
    }
    // Stdin closed or shutdown: stop streams so consumers close cleanly.
    state.stop_all_subs(None).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    drop(state);
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), writer).await;
    // Tokio's stdin reader lives on a blocking thread that never returns, so
    // exit explicitly instead of waiting for runtime teardown.
    std::process::exit(0);
}
