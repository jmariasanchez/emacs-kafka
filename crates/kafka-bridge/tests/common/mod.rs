//! Shared e2e harness: spawn the real `kafka-bridge` binary and talk
//! JSON-RPC over stdio. Include with `mod common;` from a test file.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

pub struct Bridge {
    pub child: Child,
    pub stdin: ChildStdin,
    rx: mpsc::Receiver<Value>,
    next: i64,
    backlog: Vec<Value>,
}

impl Bridge {
    pub fn spawn() -> Self {
        Self::spawn_env(&[])
    }

    pub fn spawn_env(vars: &[(&str, &str)]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_kafka-bridge"))
            .envs(vars.iter().copied())
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
        Self {
            child,
            stdin,
            rx,
            next: 1,
            backlog: Vec::new(),
        }
    }

    pub fn raw(&mut self, v: &Value) {
        let b = serde_json::to_vec(v).unwrap();
        write!(self.stdin, "Content-Length: {}\r\n\r\n", b.len()).unwrap();
        self.stdin.write_all(&b).unwrap();
        self.stdin.flush().unwrap();
    }

    pub fn send(&mut self, method: &str, params: Value) -> i64 {
        let id = self.next;
        self.next += 1;
        self.raw(&json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}));
        id
    }

    pub fn wait_for(&mut self, pred: impl Fn(&Value) -> bool, secs: u64) -> Value {
        if let Some(i) = self.backlog.iter().position(&pred) {
            return self.backlog.remove(i);
        }
        let end = Instant::now() + Duration::from_secs(secs);
        loop {
            let left = end.saturating_duration_since(Instant::now());
            let v = self
                .rx
                .recv_timeout(left)
                .expect("timed out waiting for message");
            if pred(&v) {
                return v;
            }
            self.backlog.push(v);
        }
    }

    pub fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.send(method, params);
        self.wait_for(|v| v["id"] == json!(id), 60)
    }

    pub fn ok(&mut self, method: &str, params: Value) -> Value {
        let r = self.call(method, params);
        assert!(r.get("error").is_none(), "{method} failed: {r}");
        r["result"].clone()
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn bootstrap() -> Option<String> {
    env("KAFKA_EL_TEST_BOOTSTRAP")
}

/// Non-empty environment variable.
pub fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|s| !s.is_empty())
}

/// Spawn the bridge with extra environment variables.
pub fn spawn_with_env(vars: &[(&str, &str)]) -> Bridge {
    Bridge::spawn_env(vars)
}

pub fn uniq(p: &str) -> String {
    format!(
        "{p}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
