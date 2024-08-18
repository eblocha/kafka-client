use derive_more::derive::From;
use kafka_protocol::{
    messages::{
        AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, AllocateProducerIdsRequest,
        AlterClientQuotasRequest, AlterConfigsRequest, AlterPartitionReassignmentsRequest,
        AlterPartitionRequest, AlterReplicaLogDirsRequest, AlterUserScramCredentialsRequest,
        ApiKey, ApiVersionsRequest, AssignReplicasToDirsRequest, BeginQuorumEpochRequest,
        BrokerHeartbeatRequest, BrokerRegistrationRequest, ConsumerGroupHeartbeatRequest,
        ControlledShutdownRequest, ControllerRegistrationRequest, CreateAclsRequest,
        CreateDelegationTokenRequest, CreatePartitionsRequest, CreateTopicsRequest,
        DeleteAclsRequest, DeleteGroupsRequest, DeleteRecordsRequest, DeleteTopicsRequest,
        DescribeAclsRequest, DescribeClientQuotasRequest, DescribeClusterRequest,
        DescribeConfigsRequest, DescribeDelegationTokenRequest, DescribeGroupsRequest,
        DescribeLogDirsRequest, DescribeProducersRequest, DescribeQuorumRequest,
        DescribeTransactionsRequest, DescribeUserScramCredentialsRequest, ElectLeadersRequest,
        EndQuorumEpochRequest, EndTxnRequest, EnvelopeRequest, ExpireDelegationTokenRequest,
        FetchRequest, FetchSnapshotRequest, FindCoordinatorRequest,
        GetTelemetrySubscriptionsRequest, HeartbeatRequest, IncrementalAlterConfigsRequest,
        InitProducerIdRequest, JoinGroupRequest, LeaderAndIsrRequest, LeaveGroupRequest,
        ListClientMetricsResourcesRequest, ListGroupsRequest, ListOffsetsRequest,
        ListPartitionReassignmentsRequest, ListTransactionsRequest, MetadataRequest,
        OffsetCommitRequest, OffsetDeleteRequest, OffsetFetchRequest, OffsetForLeaderEpochRequest,
        ProduceRequest, PushTelemetryRequest, RenewDelegationTokenRequest, SaslAuthenticateRequest,
        SaslHandshakeRequest, StopReplicaRequest, SyncGroupRequest, TxnOffsetCommitRequest,
        UnregisterBrokerRequest, UpdateFeaturesRequest, UpdateMetadataRequest, VoteRequest,
        WriteTxnMarkersRequest,
    },
    protocol::{buf::ByteBufMut, Encodable, Message},
};
use paste::paste;

use crate::conn::Versionable;

macro_rules! requests {
    ($($name:ident),* $(,)?) => {
        paste! {
            #[non_exhaustive]
            #[derive(Debug, Clone, From)]
            pub enum KafkaRequest {
                $($name(#[from] [<$name Request>]),)*
            }

            impl KafkaRequest {
                pub fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> anyhow::Result<()> {
                    match self {
                        $(Self::$name(req) => req.encode(buf, version),)*
                    }
                }

                pub fn as_api_key(&self) -> ApiKey {
                    match self {
                        $(Self::$name(_) => ApiKey::[<$name Key>],)*
                    }
                }
            }

            impl Versionable for KafkaRequest {
                fn key(&self) -> i16 {
                    self.as_api_key() as i16
                }

                fn versions(&self) -> ::kafka_protocol::protocol::VersionRange {
                    match self {
                        $(Self::$name(_) => [<$name Request>]::VERSIONS,)*
                    }
                }
            }
        }
    };
}

requests!(
    AddOffsetsToTxn,
    AddPartitionsToTxn,
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
    RenewDelegationToken,
    SaslAuthenticate,
    SaslHandshake,
    StopReplica,
    SyncGroup,
    TxnOffsetCommit,
    UnregisterBroker,
    UpdateFeatures,
    UpdateMetadata,
    Vote,
    WriteTxnMarkers,
);
