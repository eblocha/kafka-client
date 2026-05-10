//! Orchestrator for managing connections to brokers in the cluster.
//!
//! The selector performs bootstrap, maintains the current cluster state, performs metadata refreshes, and assigns
//! partitions to broker tasks.
//!
//! ## Bootstrap
//!
//! Bootstrapping is performed when the selector is created. There is no way to get a selector without performing the
//! bootstrap step.
//!
//! To create a selector, you must provide the bootstrap servers, the kafka configuration, and a factory to define the
//! type of tasks the selector will manage.
//!
//! For example, an admin client would provide a factory that creates network tasks, which simply forward messages to
//! the broker, and do not maintain any partition state. A consumer client would provide a factory that creates consumer
//! tasks, which fetch data from the partitions it is assigned.

mod cluster;
mod cluster_state;
mod metadata;
mod task;

pub use cluster_state::*;
pub use task::*;
