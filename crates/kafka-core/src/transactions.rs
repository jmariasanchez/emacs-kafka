//! Dedicated transactional producers. Inspection never commits consumer offsets.
use std::time::Duration;

use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

use crate::records::{DeliveryReport, ProduceRequest};
use crate::{Cluster, CoreError, Result};

/// One active transaction. Dropping an unfinished session schedules a bounded
/// abort and producer destruction on a background OS thread. Use `finish(false)`
/// when the caller needs to wait for the abort to complete.
pub struct Transaction {
    producer: Option<FutureProducer>,
    timeout: Duration,
    active: bool,
}

impl Cluster {
    pub async fn begin_transaction(&self, transactional_id: String) -> Result<Transaction> {
        self.guard_write()?;
        if transactional_id.trim().is_empty() {
            return Err(CoreError::Invalid(
                "transactionalId must not be empty".into(),
            ));
        }
        let config = self.config.clone();
        tokio::task::spawn_blocking(move || {
            let mut cfg = config.base_client_config()?;
            cfg.set("transactional.id", transactional_id);
            cfg.set("enable.idempotence", "true");
            cfg.set(
                "message.timeout.ms",
                config.timeout.as_millis().max(1000).to_string(),
            );
            let producer: FutureProducer = cfg.create()?;
            producer.init_transactions(config.timeout)?;
            producer.begin_transaction()?;
            Ok(Transaction {
                producer: Some(producer),
                timeout: config.timeout,
                active: true,
            })
        })
        .await
        .map_err(|e| CoreError::Internal(e.to_string()))?
    }
}

impl Transaction {
    fn check_active(&self) -> Result<()> {
        if self.active {
            Ok(())
        } else {
            Err(CoreError::Invalid("transaction is closed".into()))
        }
    }

    pub async fn produce(&mut self, req: ProduceRequest) -> Result<DeliveryReport> {
        self.check_active()?;
        let mut headers = OwnedHeaders::new();
        for (key, value) in &req.headers {
            headers = headers.insert(Header {
                key,
                value: value.as_deref(),
            });
        }
        let mut record: FutureRecord<'_, [u8], [u8]> =
            FutureRecord::to(&req.topic).headers(headers);
        if let Some(key) = &req.key {
            record = record.key(key);
        }
        if let Some(value) = &req.value {
            record = record.payload(value);
        }
        if let Some(partition) = req.partition {
            record = record.partition(partition);
        }
        if let Some(timestamp) = req.timestamp {
            record = record.timestamp(timestamp);
        }
        let producer = self
            .producer
            .as_ref()
            .expect("transaction owns its producer");
        let delivery = tokio::time::timeout(self.timeout, producer.send(record, self.timeout))
            .await
            .map_err(|_| {
                CoreError::Kafka(KafkaError::MessageProduction(
                    RDKafkaErrorCode::MessageTimedOut,
                ))
            })?
            .map_err(|(e, _)| CoreError::Kafka(e))?;
        Ok(DeliveryReport {
            topic: req.topic,
            partition: delivery.partition,
            offset: delivery.offset,
        })
    }

    pub async fn finish(&mut self, commit: bool) -> Result<()> {
        self.check_active()?;
        let producer = self
            .producer
            .as_ref()
            .expect("transaction owns its producer")
            .clone();
        let timeout = self.timeout;
        tokio::task::spawn_blocking(move || {
            if commit {
                producer.commit_transaction(timeout)
            } else {
                producer.abort_transaction(timeout)
            }
        })
        .await
        .map_err(|e| CoreError::Internal(e.to_string()))??;
        self.active = false;
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if let Some(producer) = self.producer.take() {
            let timeout = self.timeout;
            let active = self.active;
            // Move the last producer handle too: destroying librdkafka's
            // producer can block even after the abort itself has completed.
            let _ = std::thread::Builder::new()
                .name("kafka-transaction-cleanup".into())
                .spawn(move || {
                    if active {
                        let _ = producer.abort_transaction(timeout);
                    }
                    drop(producer);
                });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn delivery_deadline_bounds_an_unresponsive_endpoint() {
        // Keep a real TCP endpoint listening without answering Kafka requests.
        // Transaction setup is bypassed only to isolate delivery timeout logic.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let producer = rdkafka::ClientConfig::new()
            .set(
                "bootstrap.servers",
                listener.local_addr().unwrap().to_string(),
            )
            .set("message.timeout.ms", "60000")
            .set("socket.timeout.ms", "60000")
            .set("log_level", "0")
            .create::<FutureProducer>()
            .unwrap();
        let mut transaction = Transaction {
            producer: Some(producer),
            timeout: Duration::from_millis(50),
            active: true,
        };
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            transaction.produce(ProduceRequest {
                topic: "unresponsive".into(),
                partition: None,
                key: None,
                value: Some(b"deadline".to_vec()),
                headers: vec![],
                timestamp: None,
            }),
        )
        .await
        .expect("delivery must not wait for the 60-second librdkafka timeout");
        assert!(matches!(
            result,
            Err(CoreError::Kafka(KafkaError::MessageProduction(
                RDKafkaErrorCode::MessageTimedOut
            )))
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        transaction.active = false;
        drop(transaction);
    }
}
