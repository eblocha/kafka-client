use kafka_protocol::{
    messages::{
        FindCoordinatorRequest, FindCoordinatorResponse, JoinGroupRequest, JoinGroupResponse,
        OffsetFetchRequest, SyncGroupRequest, SyncGroupResponse,
        offset_fetch_request::OffsetFetchRequestGroup,
    },
    protocol::StrBytes,
};

use crate::{
    conn::{KafkaChannelError, broker::task::BrokerTaskHandle, selector::SelectorTaskHandle},
    connect::Connect,
    consumer::{handle::ConsumerTaskHandle, task::ConsumerTask},
    error::KafkaError,
    proto::ver::with_max_version,
};

#[derive(Debug, Clone)]
pub struct FindCoordinatorState {
    group_id: StrBytes,
    member_id: StrBytes,
}

impl FindCoordinatorState {
    async fn advance<Conn: Connect + Send + 'static>(
        self,
        selector: &SelectorTaskHandle<ConsumerTask<Conn>, ConsumerTaskHandle>,
    ) -> Result<JoinGroupState, (KafkaError, Self)> {
        let Some(conn) = selector
            .cluster
            .load()
            .brokers
            .get_best_connection()
            .map(|conn| conn.handle.clone())
        else {
            return Err((KafkaError::Channel(KafkaChannelError::Closed), self));
        };

        let key = self.group_id.clone();

        let response = match conn
            .send(with_max_version(move |ver| {
                let mut req = FindCoordinatorRequest::default().with_key_type(0);

                if ver > 3 {
                    req.coordinator_keys.push(key);
                } else {
                    req.key = key;
                }

                Some(req)
            }))
            .await
        {
            Ok(response) => response,
            Err(e) => return Err((e, self)),
        };

        if response.error_code != 0 {
            return Err((KafkaError::ErrorCode(response.error_code.into()), self));
        }

        Ok(JoinGroupState {
            find_coordinator_state: self,
            find_coordinator_response: response,
        })
    }
}

#[derive(Debug, Clone)]
pub struct JoinGroupState {
    find_coordinator_state: FindCoordinatorState,
    find_coordinator_response: FindCoordinatorResponse,
}

impl JoinGroupState {
    async fn advance<Conn: Connect + Send + 'static>(
        mut self,
        selector: &SelectorTaskHandle<ConsumerTask<Conn>, ConsumerTaskHandle>,
    ) -> Result<SyncGroupState, (KafkaError, Self)> {
        let conn = match self.get_coordinator_connection(selector) {
            Ok(conn) => conn,
            Err(e) => return Err((e, self)),
        };

        let req = JoinGroupRequest::default()
            .with_group_id(self.find_coordinator_state.group_id.clone().into())
            .with_member_id(self.find_coordinator_state.member_id.clone());

        let response = match conn.send(req).await {
            Ok(response) => response,
            Err(e) => return Err((e, self)),
        };

        if response.error_code != 0 {
            return Err((KafkaError::ErrorCode(response.error_code.into()), self));
        }

        self.find_coordinator_state.member_id = response.member_id.clone();

        Ok(SyncGroupState {
            join_group_state: self,
            join_group_response: response,
        })
    }

    /// Get a connection to the group coordinator
    fn get_coordinator_connection<Conn: Connect + Send + 'static>(
        &self,
        selector: &SelectorTaskHandle<ConsumerTask<Conn>, ConsumerTaskHandle>,
    ) -> Result<ConsumerTaskHandle, KafkaError> {
        let coordinator_id = self
            .find_coordinator_response
            .coordinators
            .first()
            .map(|c| c.node_id)
            .unwrap_or(self.find_coordinator_response.node_id);

        selector
            .cluster
            .load()
            .brokers
            .get_connection_to(coordinator_id.0)
            .map(|conn| conn.handle.clone())
            .ok_or(KafkaError::Channel(KafkaChannelError::Closed))
    }
}

#[derive(Debug, Clone)]
pub struct SyncGroupState {
    join_group_state: JoinGroupState,
    join_group_response: JoinGroupResponse,
}

impl SyncGroupState {
    async fn advance<Conn: Connect + Send + 'static>(
        self,
        selector: &SelectorTaskHandle<ConsumerTask<Conn>, ConsumerTaskHandle>,
    ) -> Result<GetOffsetsState, (KafkaError, Self)> {
        let conn = match self.get_coordinator_connection(selector) {
            Ok(conn) => conn,
            Err(e) => return Err((e, self)),
        };

        let mut req = SyncGroupRequest::default()
            .with_group_id(
                self.join_group_state
                    .find_coordinator_state
                    .group_id
                    .clone()
                    .into(),
            )
            .with_generation_id(self.join_group_response.generation_id)
            .with_member_id(self.join_group_response.member_id.clone());

        if self.join_group_response.leader == self.join_group_response.member_id
            && !self.join_group_response.skip_assignment
        {
            // TODO We are leader. Assign partitions.
            req.assignments = Vec::new();
        }

        let response = match conn.send(req).await {
            Ok(response) => response,
            Err(e) => return Err((e, self)),
        };

        if response.error_code != 0 {
            return Err((KafkaError::ErrorCode(response.error_code.into()), self));
        }

        Ok(GetOffsetsState {
            sync_group_state: self,
            sync_group_response: response,
        })
    }

    /// Get a connection to the group coordinator
    #[inline]
    fn get_coordinator_connection<Conn: Connect + Send + 'static>(
        &self,
        selector: &SelectorTaskHandle<ConsumerTask<Conn>, ConsumerTaskHandle>,
    ) -> Result<ConsumerTaskHandle, KafkaError> {
        self.join_group_state.get_coordinator_connection(selector)
    }
}

#[derive(Debug, Clone)]
pub struct GetOffsetsState {
    sync_group_state: SyncGroupState,
    sync_group_response: SyncGroupResponse,
}

impl GetOffsetsState {
    async fn advance<Conn: Connect + Send + 'static>(
        self,
        selector: &SelectorTaskHandle<ConsumerTask<Conn>, ConsumerTaskHandle>,
    ) -> Result<(), (KafkaError, Self)> {
        let conn = match self.get_coordinator_connection(selector) {
            Ok(conn) => conn,
            Err(e) => return Err((e, self)),
        };

        let mut req = OffsetFetchRequest::default();
        let group_id = self
            .sync_group_state
            .join_group_state
            .find_coordinator_state
            .group_id
            .clone();

        let response = match conn
            .send(with_max_version(|ver| {
                if ver <= 7 {
                    req.group_id = group_id.into();
                } else {
                    let req_group =
                        OffsetFetchRequestGroup::default().with_group_id(group_id.into());

                    // if ver >= 9 {
                    //     req_group.member_id = Some(self.prev.join_group_response.member_id.clone());
                    //     // TODO where to get member_epoch?
                    //     req_group.member_epoch =
                    // }

                    req.groups.push(req_group);

                    // TODO add topics
                }

                Some(req)
            }))
            .await
        {
            Ok(response) => response,
            Err(e) => return Err((e, self)),
        };

        if response.error_code != 0 {
            return Err((KafkaError::ErrorCode(response.error_code.into()), self));
        }

        Ok(())
    }

    /// Get a connection to the group coordinator
    #[inline]
    fn get_coordinator_connection<Conn: Connect + Send + 'static>(
        &self,
        selector: &SelectorTaskHandle<ConsumerTask<Conn>, ConsumerTaskHandle>,
    ) -> Result<ConsumerTaskHandle, KafkaError> {
        self.sync_group_state.get_coordinator_connection(selector)
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ConsumerState {
    FindCoordinator(FindCoordinatorState),
    JoinGroup(JoinGroupState),
    SyncGroup(SyncGroupState),
    GetOffsets(GetOffsetsState),
    Fetch,
}

impl ConsumerState {
    pub async fn advance<Conn: Connect + Send + 'static>(
        self,
        selector: &SelectorTaskHandle<ConsumerTask<Conn>, ConsumerTaskHandle>,
    ) -> Result<Self, (KafkaError, Self)> {
        match self {
            ConsumerState::FindCoordinator(state) => state
                .advance(selector)
                .await
                .map(ConsumerState::JoinGroup)
                .map_err(|(e, state)| (e, ConsumerState::FindCoordinator(state))),
            ConsumerState::JoinGroup(state) => state
                .advance(selector)
                .await
                .map(ConsumerState::SyncGroup)
                .map_err(|(e, state)| (e, ConsumerState::JoinGroup(state))),
            ConsumerState::SyncGroup(state) => state
                .advance(selector)
                .await
                .map(ConsumerState::GetOffsets)
                .map_err(|(e, state)| (e, ConsumerState::SyncGroup(state))),
            ConsumerState::GetOffsets(_state) => todo!(),
            ConsumerState::Fetch => todo!(),
        }
    }
}
