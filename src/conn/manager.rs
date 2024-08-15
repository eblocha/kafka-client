use std::{collections::HashMap, sync::Arc};

use futures::channel::oneshot;
use tokio::sync::mpsc;

use super::PreparedConnection;

struct ConnectionManagerBackgroundTask {
    requests: mpsc::Receiver<(
        Option<String>,
        oneshot::Sender<Option<Arc<PreparedConnection>>>,
    )>,
    connections: HashMap<String, Arc<PreparedConnection>>,
}

// recv request for connection
// if bound for specific broker, choose that broker for connection

// otherwise: https://github.com/apache/kafka/blob/0f7cd4dcdeb2c705c01743927e36b66b06010f20/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java#L709
// chose a random number 0..nodes, this is where we start iteration (and wrap around)
// for each node:
// if the node is connected and isn't due for metadata refresh , we have at least one "ready".
// if it also has no in-flight requests, select it as the best node
// otherwise, choose the node that meets the above with the lowest in-flight requests

// if no nodes meet the above, get any connecting node (kafka chooses the last one?)

// if no connecting node, then find nodes that:
// - is disconnected
// - has not tried to connect within the backoff period
// and choose the one that has been the longest since last connection attempt

// if no nodes left after the above, error

impl ConnectionManagerBackgroundTask {
    async fn run(mut self) {
        while let Some((node, sender)) = self.requests.recv().await {
            match node {
                Some(node) => todo!(),
                None => todo!(),
            }
        };
    }
}

pub struct ConnectionManager {}
