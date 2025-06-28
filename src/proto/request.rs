use std::io;

use derive_more::derive::From;
use kafka_protocol::protocol::VersionRange;
use paste::paste;

use crate::{
    conn::{DecodableResponse, Sendable},
    proto::ver::{max_intersecting_version, FromVersionRange, GetApiKey, Versionable},
};

macro_rules! requests {
    ($($name:ident),* $(,)?) => {
        use ::kafka_protocol::protocol::{Message, Encodable};

        paste! {
            /// Enumeration of all possible Kafka message types. See https://kafka.apache.org/protocol#protocol_messages
            ///
            /// This is used internally to represent a generic, encodable message type.
            #[non_exhaustive]
            #[derive(Debug, Clone, From)]
            pub enum KafkaRequest {
                $($name(#[from] ::kafka_protocol::messages::[<$name Request>]),)*
            }

            impl KafkaRequest {
                /// Encode the request into a byte buffer given an API version.
                pub fn encode<B: ::kafka_protocol::protocol::buf::ByteBufMut>(&self, buf: &mut B, version: i16) -> anyhow::Result<()> {
                    match self {
                        $(Self::$name(req) => req.encode(buf, version),)*
                    }
                }

                /// Get the api key associated with this request type.
                pub fn as_api_key(&self) -> ::kafka_protocol::messages::ApiKey {
                    match self {
                        $(Self::$name(_) => ::kafka_protocol::messages::ApiKey::$name,)*
                    }
                }
            }

            impl Versionable for KafkaRequest {

                fn versions(&self) -> ::kafka_protocol::protocol::VersionRange {
                    match self {
                        $(Self::$name(_) => ::kafka_protocol::messages::[<$name Request>]::VERSIONS,)*
                    }
                }
            }

            $(
                impl FromVersionRange for ::kafka_protocol::messages::[<$name Request>] {
                    type Req = Self;

                    fn from_version_range(self, range: ::kafka_protocol::protocol::VersionRange) -> Option<(Self::Req, i16)> {
                        let ver = max_intersecting_version(&::kafka_protocol::messages::[<$name Request>]::VERSIONS, &range)?;
                        Some((self, ver))
                    }
                }
            )*

            $(
                impl GetApiKey for ::kafka_protocol::messages::[<$name Request>] {
                    fn key(&self) -> i16 {
                        ::kafka_protocol::messages::ApiKey::$name as i16
                    }
                }
            )*

            impl Sendable for KafkaRequest {
                type Response = ::kafka_protocol::messages::ResponseKind;

                fn decode(response: DecodableResponse) -> Result<Self::Response, io::Error> {
                    match response.record.api_key {
                        $(::kafka_protocol::messages::ApiKey::$name => Ok(::kafka_protocol::messages::ResponseKind::$name(::kafka_protocol::messages::[<$name Request>]::decode(response)?)),)*
                    }
                }
            }
        }
    };
}

requests!(
    AddOffsetsToTxn,
    AddPartitionsToTxn,
    AddRaftVoter,
    AllocateProducerIds,
    AlterClientQuotas,
    AlterConfigs,
    AlterPartitionReassignments,
    AlterPartition,
    AlterReplicaLogDirs,
    AlterUserScramCredentials,
    ApiVersions,
    AssignReplicasToDirs,
    BeginQuorumEpoch,
    BrokerHeartbeat,
    BrokerRegistration,
    ConsumerGroupDescribe,
    ConsumerGroupHeartbeat,
    ControlledShutdown,
    ControllerRegistration,
    CreateAcls,
    CreateDelegationToken,
    CreatePartitions,
    CreateTopics,
    DeleteAcls,
    DeleteGroups,
    DeleteRecords,
    DeleteTopics,
    DescribeAcls,
    DescribeClientQuotas,
    DescribeCluster,
    DescribeConfigs,
    DescribeDelegationToken,
    DescribeGroups,
    DescribeLogDirs,
    DescribeProducers,
    DescribeQuorum,
    DescribeTopicPartitions,
    DescribeTransactions,
    DescribeUserScramCredentials,
    ElectLeaders,
    EndQuorumEpoch,
    EndTxn,
    Envelope,
    ExpireDelegationToken,
    Fetch,
    FetchSnapshot,
    FindCoordinator,
    GetTelemetrySubscriptions,
    Heartbeat,
    IncrementalAlterConfigs,
    InitProducerId,
    JoinGroup,
    LeaderAndIsr,
    LeaveGroup,
    ListClientMetricsResources,
    ListGroups,
    ListOffsets,
    ListPartitionReassignments,
    ListTransactions,
    Metadata,
    OffsetCommit,
    OffsetDelete,
    OffsetFetch,
    OffsetForLeaderEpoch,
    Produce,
    PushTelemetry,
    RemoveRaftVoter,
    RenewDelegationToken,
    SaslAuthenticate,
    SaslHandshake,
    StopReplica,
    SyncGroup,
    TxnOffsetCommit,
    UnregisterBroker,
    UpdateFeatures,
    UpdateMetadata,
    UpdateRaftVoter,
    Vote,
    WriteTxnMarkers,
);

impl GetApiKey for KafkaRequest {
    fn key(&self) -> i16 {
        self.as_api_key() as i16
    }
}

impl FromVersionRange for KafkaRequest {
    type Req = Self;

    fn from_version_range(self, range: VersionRange) -> Option<(Self::Req, i16)> {
        let ver = max_intersecting_version(&self.versions(), &range)?;
        Some((self, ver))
    }
}
