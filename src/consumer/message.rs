//! selector maintains a vec of subscriptions
//! - on metadata refresh, it will construct the request based on the subscription state
//!
//! Is this generic? Maybe producer does the same thing? I.e. producer subscribes to a topic when it wants to produce to
//! it.
//!
//! to subscribe:
//! - topic-based: register subscription with selector
//! - selector allows us to wait to resolve the topic data
//!     - needed for producer, since we need the partition queue
//!     - don't need to for consumer?
//!
//! to unsubscribe:
//! - remove the topic partition Senders from the cluster
//! - need a way to wait for consumer to flush existing requests afterwards
//! - for example: imagine a REST API which subscribes or unsubscribes to a topic. This wants to know that no more
//!   messages will be consumed for the topic before responding to the REST call.
//!     - feels like a condvar-like thing: REST endpoint handler can't continue until consumer has processed messages
//!
//! task-side:
//! - get a list of topic partitions whose recievers are not closed
//! - for partitions which do not have offsets stored, fetch offsets
//!     - wait?
//! - for partitions with offsets, send fetch request
//!     - wait?
//! - if any offsets are invalid, remove the partition offset state and loop
//!
//! Maybe the _channel_ is the generic bit, not the message?
//! - consumer could use watch channels (might be too slow)
//! - admin would use unit
//! - producer would use current setup

use tokio::sync::oneshot;

enum ConsumerTaskMessage {
    Subscribe(oneshot::Sender<()>),
}
