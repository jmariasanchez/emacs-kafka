//! Dedicated transactional producers. Inspection never commits consumer offsets.
use std::time::Duration;

use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

use crate::records::{DeliveryReport, ProduceRequest};
use crate::{Cluster, CoreError, Result};

/// One active transaction. Dropping an unfinished session attempts a bounded abort.
pub struct Transaction {
    producer: FutureProducer,
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
                producer,
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
        let delivery = self
            .producer
            .send(record, self.timeout)
            .await
            .map_err(|(e, _)| CoreError::Kafka(e))?;
        Ok(DeliveryReport {
            topic: req.topic,
            partition: delivery.partition,
            offset: delivery.offset,
        })
    }

    pub async fn finish(&mut self, commit: bool) -> Result<()> {
        self.check_active()?;
        let producer = self.producer.clone();
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
        if self.active {
            let _ = self.producer.abort_transaction(self.timeout);
        }
    }
}
