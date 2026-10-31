//! `kafka-core`: reusable, editor-agnostic Kafka operations built on
//! `rust-rdkafka`. Has no knowledge of Emacs or JSON-RPC.

pub mod capabilities;
pub mod cluster;
pub mod connection;
pub mod errors;
pub mod records;

pub use cluster::Cluster;
pub use connection::ConnectionConfig;
pub use errors::{CoreError, Result};
