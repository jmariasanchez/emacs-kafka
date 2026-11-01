//! Producing and reading records.
//!
//! Reading always uses a dedicated inspection consumer with manual assignment
//! and no commits, so browsing a topic can never move an application group's
//! offsets.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Header, Headers, Message, OwnedHeaders, Timestamp};
use rdkafka::producer::FutureRecord;
use rdkafka::{Offset, TopicPartitionList};
use serde::Serialize;
use tokio::sync::{mpsc, Semaphore};

use crate::cluster::Cluster;
use crate::errors::{CoreError, Result};

/// A record as raw bytes. Decoding is the caller's concern.
#[derive(Debug, Clone)]
pub struct RawRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub timestamp_type: &'static str,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

/// Where to begin reading, resolved per partition.
#[derive(Debug, Clone, PartialEq)]
pub enum Position {
    Earliest,
    Latest,
    Offset(i64),
    Timestamp(i64),
    Tail(i64),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryReport {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

pub struct ProduceRequest {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
    pub timestamp: Option<i64>,
}

fn to_raw<M: Message>(m: &M) -> RawRecord {
    let (timestamp, timestamp_type) = match m.timestamp() {
        Timestamp::NotAvailable => (None, "none"),
        Timestamp::CreateTime(t) => (Some(t), "create"),
        Timestamp::LogAppendTime(t) => (Some(t), "logAppend"),
    };
    let headers = m
        .headers()
        .map(|h| h.iter().map(|x| (x.key.to_string(), x.value.map(|v| v.to_vec()))).collect())
        .unwrap_or_default();
    RawRecord {
        topic: m.topic().to_string(),
        partition: m.partition(),
        offset: m.offset(),
        timestamp,
        timestamp_type,
        key: m.key().map(|k| k.to_vec()),
        value: m.payload().map(|v| v.to_vec()),
        headers,
    }
}

/// Pure resolution of a tail start, exposed for testing.
pub fn tail_start(low: i64, high: i64, count: i64) -> i64 {
    (high - count.max(0)).max(low)
}

/// Build the assignment for the requested position. Returns the assignment and
/// the set of partitions that already have nothing to read (empty range).
fn resolve_assignment(
    m: &BaseConsumer,
    topic: &str,
    parts: &[i32],
    pos: &Position,
    t: Duration,
) -> Result<(TopicPartitionList, BTreeMap<i32, i64>)> {
    let mut tpl = TopicPartitionList::new();
    let mut ends = BTreeMap::new();
    let ts_offsets = if let Position::Timestamp(ts) = pos {
        let mut q = TopicPartitionList::new();
        for p in parts {
            q.add_partition_offset(topic, *p, Offset::Offset(*ts))?;
        }
        Some(m.offsets_for_times(q, t)?)
    } else {
        None
    };
    for p in parts {
        let (low, high) = m.fetch_watermarks(topic, *p, t)?;
        ends.insert(*p, high);
        let off = match pos {
            Position::Earliest => Offset::Offset(low),
            Position::Latest => Offset::Offset(high),
            Position::Offset(o) => Offset::Offset((*o).clamp(low, high)),
            Position::Tail(n) => Offset::Offset(tail_start(low, high, *n)),
            Position::Timestamp(_) => ts_offsets
                .as_ref()
                .and_then(|r| r.find_partition(topic, *p))
                .map(|e| match e.offset() {
                    Offset::Offset(o) => Offset::Offset(o),
                    _ => Offset::Offset(high),
                })
                .unwrap_or(Offset::Offset(high)),
        };
        tpl.add_partition_offset(topic, *p, off)?;
    }
    Ok((tpl, ends))
}

impl Cluster {
    fn inspection_config(&self, isolation: Option<String>) -> Result<crate::ConnectionConfig> {
        let isolation = isolation.as_deref().unwrap_or("read_uncommitted");
        if !matches!(isolation, "read_committed" | "read_uncommitted") {
            return Err(CoreError::Invalid("isolationLevel must be read_committed or read_uncommitted".into()));
        }
        let mut cfg = self.config.clone();
        cfg.properties.insert("isolation.level".into(), isolation.into());
        Ok(cfg)
    }

    pub async fn produce(&self, req: ProduceRequest) -> Result<DeliveryReport> {
        let producer = self.producer().await?;
        let mut headers = OwnedHeaders::new();
        for (k, v) in &req.headers {
            headers = headers.insert(Header { key: k, value: v.as_deref() });
        }
        let mut rec: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(&req.topic).headers(headers);
        if let Some(k) = &req.key {
            rec = rec.key(k.as_slice());
        }
        if let Some(v) = &req.value {
            rec = rec.payload(v.as_slice());
        }
        if let Some(p) = req.partition {
            rec = rec.partition(p);
        }
        if let Some(ts) = req.timestamp {
            rec = rec.timestamp(ts);
        }
        match producer.send(rec, self.config.timeout).await {
            Ok(d) => Ok(DeliveryReport {
                topic: req.topic.clone(),
                partition: d.partition,
                offset: d.offset,
            }),
            Err((e, _)) => Err(CoreError::Kafka(e)),
        }
    }

    async fn resolve_parts(&self, topic: &str, parts: Option<Vec<i32>>) -> Result<Vec<i32>> {
        let all = self.partitions(topic.to_string()).await?;
        match parts {
            None => Ok(all),
            Some(req) => {
                if let Some(bad) = req.iter().find(|p| !all.contains(p)) {
                    return Err(CoreError::Invalid(format!("topic {topic} has no partition {bad}")));
                }
                Ok(req)
            }
        }
    }

    /// Bounded one-shot read. Stops at `limit` records, when every partition
    /// reaches its end offset as of the start of the call, or at `timeout`.
    pub async fn fetch(
        &self,
        topic: String,
        parts: Option<Vec<i32>>,
        pos: Position,
        limit: usize,
        timeout: Duration,
        isolation_level: Option<String>,
    ) -> Result<FetchResult> {
        let cfg = self.inspection_config(isolation_level)?;
        let parts = self.resolve_parts(&topic, parts).await?;
        tokio::task::spawn_blocking(move || {
            let c = cfg.inspection_consumer()?;
            let t = cfg.timeout;
            let (tpl, ends) = resolve_assignment(&c, &topic, &parts, &pos, t)?;
            let mut done: HashSet<i32> = HashSet::new();
            for e in tpl.elements() {
                if let Offset::Offset(o) = e.offset() {
                    if o >= ends[&e.partition()] {
                        done.insert(e.partition());
                    }
                }
            }
            c.assign(&tpl)?;
            let deadline = Instant::now() + timeout;
            let mut records = Vec::new();
            let mut timed_out = false;
            while records.len() < limit && done.len() < parts.len() {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    timed_out = true;
                    break;
                }
                match c.poll(left.min(Duration::from_millis(200))) {
                    None => {}
                    Some(Ok(m)) => {
                        if m.offset() + 1 >= ends[&m.partition()] {
                            done.insert(m.partition());
                        }
                        if m.offset() < ends[&m.partition()] {
                            records.push(to_raw(&m));
                        }
                    }
                    Some(Err(KafkaError::PartitionEOF(p))) => {
                        done.insert(p);
                    }
                    Some(Err(e)) => return Err(CoreError::Kafka(e)),
                }
            }
            records.sort_by_key(|r| (r.timestamp, r.partition, r.offset));
            let truncated = records.len() >= limit && done.len() < parts.len();
            Ok(FetchResult { records, truncated, timed_out })
        })
        .await
        .map_err(|e| CoreError::Internal(e.to_string()))?
    }

    /// Start a live stream. Batches are delivered on `sink`; one permit from
    /// `ctl.credit` is consumed per batch, providing back-pressure from the UI.
    pub async fn stream(
        self: &Arc<Self>,
        topic: String,
        parts: Option<Vec<i32>>,
        pos: Position,
        ctl: StreamControl,
        isolation_level: Option<String>,
    ) -> Result<()> {
        let StreamControl { batch_size, credit, cancel, sink } = ctl;
        let cfg = self.inspection_config(isolation_level)?;
        let parts = self.resolve_parts(&topic, parts).await?;
        let c = cfg.inspection_consumer()?;
        let (tpl, _) = {
            let topic = topic.clone();
            let parts = parts.clone();
            let t = cfg.timeout;
            let c2 = cfg.inspection_consumer()?;
            tokio::task::spawn_blocking(move || resolve_assignment(&c2, &topic, &parts, &pos, t))
                .await
                .map_err(|e| CoreError::Internal(e.to_string()))??
        };
        c.assign(&tpl)?;
        let batch_size = batch_size.clamp(1, 10_000);
        std::thread::Builder::new()
            .name(format!("kafka-stream-{topic}"))
            .spawn(move || stream_loop(c, batch_size, credit, cancel, sink))
            .map_err(|e| CoreError::Internal(e.to_string()))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct FetchResult {
    pub records: Vec<RawRecord>,
    /// More records were available than `limit`.
    pub truncated: bool,
    pub timed_out: bool,
}

/// Flow-control handles for a live stream.
pub struct StreamControl {
    pub batch_size: usize,
    /// One permit per batch the consumer is allowed to emit.
    pub credit: Arc<Semaphore>,
    /// Set to stop the stream thread.
    pub cancel: Arc<AtomicBool>,
    pub sink: mpsc::Sender<StreamEvent>,
}

#[derive(Debug)]
pub enum StreamEvent {
    Batch(Vec<RawRecord>),
    /// Credit is exhausted and the stream is paused waiting for the client.
    Paused,
    Resumed,
    Error(String),
    Stopped,
}

fn stream_loop(
    c: BaseConsumer,
    batch_size: usize,
    credit: Arc<Semaphore>,
    cancel: Arc<AtomicBool>,
    sink: mpsc::Sender<StreamEvent>,
) {
    let mut paused = false;
    'outer: while !cancel.load(Ordering::Relaxed) {
        // Wait for credit without blocking cancellation.
        let permit = loop {
            if cancel.load(Ordering::Relaxed) {
                break 'outer;
            }
            match credit.try_acquire() {
                Ok(p) => break p,
                Err(_) => {
                    if !paused {
                        paused = true;
                        if sink.blocking_send(StreamEvent::Paused).is_err() {
                            break 'outer;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        };
        if paused {
            paused = false;
            if sink.blocking_send(StreamEvent::Resumed).is_err() {
                break;
            }
        }
        permit.forget();
        let mut batch = Vec::new();
        let start = Instant::now();
        // Fill a batch, flushing at least every 250ms so the UI stays live.
        while batch.len() < batch_size && start.elapsed() < Duration::from_millis(250) {
            if cancel.load(Ordering::Relaxed) {
                break 'outer;
            }
            match c.poll(Duration::from_millis(50)) {
                None | Some(Err(KafkaError::PartitionEOF(_))) => {
                    if !batch.is_empty() {
                        break;
                    }
                }
                Some(Ok(m)) => batch.push(to_raw(&m)),
                Some(Err(e)) => {
                    let _ = sink.blocking_send(StreamEvent::Error(e.to_string()));
                }
            }
        }
        if batch.is_empty() {
            // Nothing read: return the credit.
            credit.add_permits(1);
            continue;
        }
        if sink.blocking_send(StreamEvent::Batch(batch)).is_err() {
            break;
        }
    }
    let _ = sink.blocking_send(StreamEvent::Stopped);
    // `c` drops here: librdkafka closes the consumer. No commits were ever made.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_math() {
        assert_eq!(tail_start(0, 100, 10), 90);
        assert_eq!(tail_start(95, 100, 10), 95);
        assert_eq!(tail_start(0, 0, 10), 0);
        assert_eq!(tail_start(0, 10, -3), 10);
    }
}
