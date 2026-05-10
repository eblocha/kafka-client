//! A Kafka client implementation built on tokio.
//!
//! ## Features
//!
//! - Multiplexed, async IO
//! - Client-side load balancing
//! - Connection retry with exponential backoff
//! - Generic over the IO channel
//! - Custom partitioning strategies

mod backoff;
mod broker;
mod cancel;
mod conn;
mod network;
mod proto;
mod selector;
mod util;

pub mod admin;
pub mod common;
pub mod config;
pub mod consumer;
pub mod error;
pub mod producer;

pub mod connect {
    //! Low-level connection primitives
    pub use crate::conn::channel::KafkaChannel;
    pub use crate::conn::connect::{Connect, Tcp};
}
