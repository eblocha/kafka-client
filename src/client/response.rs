use derive_more::derive::From;
use kafka_protocol::{
    messages::{
        AddOffsetsToTxnResponse, AddPartitionsToTxnResponse, AllocateProducerIdsResponse,
        AlterClientQuotasResponse, AlterConfigsResponse, AlterPartitionReassignmentsResponse,
        AlterPartitionResponse, AlterReplicaLogDirsResponse, AlterUserScramCredentialsResponse,
        ApiKey, ApiVersionsResponse, AssignReplicasToDirsResponse, BeginQuorumEpochResponse,
        BrokerHeartbeatResponse, BrokerRegistrationResponse, ConsumerGroupHeartbeatResponse,
        ControlledShutdownResponse, ControllerRegistrationResponse, CreateAclsResponse,
        CreateDelegationTokenResponse, CreatePartitionsResponse, CreateTopicsResponse,
        DeleteAclsResponse, DeleteGroupsResponse, DeleteRecordsResponse, DeleteTopicsResponse,
        DescribeAclsResponse, DescribeClientQuotasResponse, DescribeClusterResponse,
        DescribeConfigsResponse, DescribeDelegationTokenResponse, DescribeGroupsResponse,
        DescribeLogDirsResponse, DescribeProducersResponse, DescribeQuorumResponse,
        DescribeTransactionsResponse, DescribeUserScramCredentialsResponse, ElectLeadersResponse,
        EndQuorumEpochResponse, EndTxnResponse, EnvelopeResponse, ExpireDelegationTokenResponse,
        FetchResponse, FetchSnapshotResponse, FindCoordinatorResponse,
        GetTelemetrySubscriptionsResponse, HeartbeatResponse, IncrementalAlterConfigsResponse,
        InitProducerIdResponse, JoinGroupResponse, LeaderAndIsrResponse, LeaveGroupResponse,
        ListClientMetricsResourcesResponse, ListGroupsResponse, ListOffsetsResponse,
        ListPartitionReassignmentsResponse, ListTransactionsResponse, MetadataResponse,
        OffsetCommitResponse, OffsetDeleteResponse, OffsetFetchResponse,
        OffsetForLeaderEpochResponse, ProduceResponse, PushTelemetryResponse,
        RenewDelegationTokenResponse, SaslAuthenticateResponse, SaslHandshakeResponse,
        StopReplicaResponse, SyncGroupResponse, TxnOffsetCommitResponse, UnregisterBrokerResponse,
        UpdateFeaturesResponse, UpdateMetadataResponse, VoteResponse, WriteTxnMarkersResponse,
    },
    protocol::{buf::ByteBuf, Decodable},
};

#[non_exhaustive]
#[allow(unused)]
#[derive(Debug, Clone, From)]
pub enum KafkaResponse {
    AddOffsetsToTxn(#[from] AddOffsetsToTxnResponse),
    AddPartitionsToTxn(#[from] AddPartitionsToTxnResponse),
    AllocateProducerIds(#[from] AllocateProducerIdsResponse),
    AlterClientQuotas(#[from] AlterClientQuotasResponse),
    AlterConfigs(#[from] AlterConfigsResponse),
    AlterPartitionReassignments(#[from] AlterPartitionReassignmentsResponse),
    AlterPartition(#[from] AlterPartitionResponse),
    AlterReplicaLogDirs(#[from] AlterReplicaLogDirsResponse),
    AlterUserScramCredentials(#[from] AlterUserScramCredentialsResponse),
    ApiVersions(#[from] ApiVersionsResponse),
    AssignReplicasToDirs(#[from] AssignReplicasToDirsResponse),
    BeginQuorumEpoch(#[from] BeginQuorumEpochResponse),
    BrokerHeartbeat(#[from] BrokerHeartbeatResponse),
    BrokerRegistration(#[from] BrokerRegistrationResponse),
    ConsumerGroupHeartbeat(#[from] ConsumerGroupHeartbeatResponse),
    ControlledShutdown(#[from] ControlledShutdownResponse),
    ControllerRegistration(#[from] ControllerRegistrationResponse),
    CreateAcls(#[from] CreateAclsResponse),
    CreateDelegationToken(#[from] CreateDelegationTokenResponse),
    CreatePartitions(#[from] CreatePartitionsResponse),
    CreateTopics(#[from] CreateTopicsResponse),
    DeleteAcls(#[from] DeleteAclsResponse),
    DeleteGroups(#[from] DeleteGroupsResponse),
    DeleteRecords(#[from] DeleteRecordsResponse),
    DeleteTopics(#[from] DeleteTopicsResponse),
    DescribeAcls(#[from] DescribeAclsResponse),
    DescribeClientQuotas(#[from] DescribeClientQuotasResponse),
    DescribeCluster(#[from] DescribeClusterResponse),
    DescribeConfigs(#[from] DescribeConfigsResponse),
    DescribeDelegationToken(#[from] DescribeDelegationTokenResponse),
    DescribeGroups(#[from] DescribeGroupsResponse),
    DescribeLogDirs(#[from] DescribeLogDirsResponse),
    DescribeProducers(#[from] DescribeProducersResponse),
    DescribeQuorum(#[from] DescribeQuorumResponse),
    DescribeTransactions(#[from] DescribeTransactionsResponse),
    DescribeUserScramCredentials(#[from] DescribeUserScramCredentialsResponse),
    ElectLeaders(#[from] ElectLeadersResponse),
    EndQuorumEpoch(#[from] EndQuorumEpochResponse),
    EndTxn(#[from] EndTxnResponse),
    Envelope(#[from] EnvelopeResponse),
    ExpireDelegationToken(#[from] ExpireDelegationTokenResponse),
    Fetch(#[from] FetchResponse),
    FetchSnapshot(#[from] FetchSnapshotResponse),
    FindCoordinator(#[from] FindCoordinatorResponse),
    GetTelemetrySubscriptions(#[from] GetTelemetrySubscriptionsResponse),
    Heartbeat(#[from] HeartbeatResponse),
    IncrementalAlterConfigs(#[from] IncrementalAlterConfigsResponse),
    InitProducerId(#[from] InitProducerIdResponse),
    JoinGroup(#[from] JoinGroupResponse),
    LeaderAndIsr(#[from] LeaderAndIsrResponse),
    LeaveGroup(#[from] LeaveGroupResponse),
    ListClientMetricsResources(#[from] ListClientMetricsResourcesResponse),
    ListGroups(#[from] ListGroupsResponse),
    ListOffsets(#[from] ListOffsetsResponse),
    ListPartitionReassignments(#[from] ListPartitionReassignmentsResponse),
    ListTransactions(#[from] ListTransactionsResponse),
    Metadata(#[from] MetadataResponse),
    OffsetCommit(#[from] OffsetCommitResponse),
    OffsetDelete(#[from] OffsetDeleteResponse),
    OffsetFetch(#[from] OffsetFetchResponse),
    OffsetForLeaderEpoch(#[from] OffsetForLeaderEpochResponse),
    Produce(#[from] ProduceResponse),
    PushTelemetry(#[from] PushTelemetryResponse),
    RenewDelegationToken(#[from] RenewDelegationTokenResponse),
    SaslAuthenticate(#[from] SaslAuthenticateResponse),
    SaslHandshake(#[from] SaslHandshakeResponse),
    StopReplica(#[from] StopReplicaResponse),
    SyncGroup(#[from] SyncGroupResponse),
    TxnOffsetCommit(#[from] TxnOffsetCommitResponse),
    UnregisterBroker(#[from] UnregisterBrokerResponse),
    UpdateFeatures(#[from] UpdateFeaturesResponse),
    UpdateMetadata(#[from] UpdateMetadataResponse),
    Vote(#[from] VoteResponse),
    WriteTxnMarkers(#[from] WriteTxnMarkersResponse),
}

impl KafkaResponse {
    pub fn decode<B: ByteBuf>(buf: &mut B, version: i16, api_key: ApiKey) -> anyhow::Result<Self> {
        match api_key {
            ApiKey::ProduceKey => ProduceResponse::decode(buf, version).map(Self::Produce),
            ApiKey::FetchKey => FetchResponse::decode(buf, version).map(Self::Fetch),
            ApiKey::ListOffsetsKey => {
                ListOffsetsResponse::decode(buf, version).map(Self::ListOffsets)
            }
            ApiKey::MetadataKey => MetadataResponse::decode(buf, version).map(Self::Metadata),
            ApiKey::LeaderAndIsrKey => {
                LeaderAndIsrResponse::decode(buf, version).map(Self::LeaderAndIsr)
            }
            ApiKey::StopReplicaKey => {
                StopReplicaResponse::decode(buf, version).map(Self::StopReplica)
            }
            ApiKey::UpdateMetadataKey => {
                UpdateMetadataResponse::decode(buf, version).map(Self::UpdateMetadata)
            }
            ApiKey::ControlledShutdownKey => {
                ControlledShutdownResponse::decode(buf, version).map(Self::ControlledShutdown)
            }
            ApiKey::OffsetCommitKey => {
                OffsetCommitResponse::decode(buf, version).map(Self::OffsetCommit)
            }
            ApiKey::OffsetFetchKey => {
                OffsetFetchResponse::decode(buf, version).map(Self::OffsetFetch)
            }
            ApiKey::FindCoordinatorKey => {
                FindCoordinatorResponse::decode(buf, version).map(Self::FindCoordinator)
            }
            ApiKey::JoinGroupKey => JoinGroupResponse::decode(buf, version).map(Self::JoinGroup),
            ApiKey::HeartbeatKey => HeartbeatResponse::decode(buf, version).map(Self::Heartbeat),
            ApiKey::LeaveGroupKey => LeaveGroupResponse::decode(buf, version).map(Self::LeaveGroup),
            ApiKey::SyncGroupKey => SyncGroupResponse::decode(buf, version).map(Self::SyncGroup),
            ApiKey::DescribeGroupsKey => {
                DescribeGroupsResponse::decode(buf, version).map(Self::DescribeGroups)
            }
            ApiKey::ListGroupsKey => ListGroupsResponse::decode(buf, version).map(Self::ListGroups),
            ApiKey::SaslHandshakeKey => {
                SaslHandshakeResponse::decode(buf, version).map(Self::SaslHandshake)
            }
            ApiKey::ApiVersionsKey => {
                ApiVersionsResponse::decode(buf, version).map(Self::ApiVersions)
            }
            ApiKey::CreateTopicsKey => {
                CreateTopicsResponse::decode(buf, version).map(Self::CreateTopics)
            }
            ApiKey::DeleteTopicsKey => {
                DeleteTopicsResponse::decode(buf, version).map(Self::DeleteTopics)
            }
            ApiKey::DeleteRecordsKey => {
                DeleteRecordsResponse::decode(buf, version).map(Self::DeleteRecords)
            }
            ApiKey::InitProducerIdKey => {
                InitProducerIdResponse::decode(buf, version).map(Self::InitProducerId)
            }
            ApiKey::OffsetForLeaderEpochKey => {
                OffsetForLeaderEpochResponse::decode(buf, version).map(Self::OffsetForLeaderEpoch)
            }
            ApiKey::AddPartitionsToTxnKey => {
                AddPartitionsToTxnResponse::decode(buf, version).map(Self::AddPartitionsToTxn)
            }
            ApiKey::AddOffsetsToTxnKey => {
                AddOffsetsToTxnResponse::decode(buf, version).map(Self::AddOffsetsToTxn)
            }
            ApiKey::EndTxnKey => EndTxnResponse::decode(buf, version).map(Self::EndTxn),
            ApiKey::WriteTxnMarkersKey => {
                WriteTxnMarkersResponse::decode(buf, version).map(Self::WriteTxnMarkers)
            }
            ApiKey::TxnOffsetCommitKey => {
                TxnOffsetCommitResponse::decode(buf, version).map(Self::TxnOffsetCommit)
            }
            ApiKey::DescribeAclsKey => {
                DescribeAclsResponse::decode(buf, version).map(Self::DescribeAcls)
            }
            ApiKey::CreateAclsKey => CreateAclsResponse::decode(buf, version).map(Self::CreateAcls),
            ApiKey::DeleteAclsKey => DeleteAclsResponse::decode(buf, version).map(Self::DeleteAcls),
            ApiKey::DescribeConfigsKey => {
                DescribeConfigsResponse::decode(buf, version).map(Self::DescribeConfigs)
            }
            ApiKey::AlterConfigsKey => {
                AlterConfigsResponse::decode(buf, version).map(Self::AlterConfigs)
            }
            ApiKey::AlterReplicaLogDirsKey => {
                AlterReplicaLogDirsResponse::decode(buf, version).map(Self::AlterReplicaLogDirs)
            }
            ApiKey::DescribeLogDirsKey => {
                DescribeLogDirsResponse::decode(buf, version).map(Self::DescribeLogDirs)
            }
            ApiKey::SaslAuthenticateKey => {
                SaslAuthenticateResponse::decode(buf, version).map(Self::SaslAuthenticate)
            }
            ApiKey::CreatePartitionsKey => {
                CreatePartitionsResponse::decode(buf, version).map(Self::CreatePartitions)
            }
            ApiKey::CreateDelegationTokenKey => {
                CreateDelegationTokenResponse::decode(buf, version).map(Self::CreateDelegationToken)
            }
            ApiKey::RenewDelegationTokenKey => {
                RenewDelegationTokenResponse::decode(buf, version).map(Self::RenewDelegationToken)
            }
            ApiKey::ExpireDelegationTokenKey => {
                ExpireDelegationTokenResponse::decode(buf, version).map(Self::ExpireDelegationToken)
            }
            ApiKey::DescribeDelegationTokenKey => {
                DescribeDelegationTokenResponse::decode(buf, version)
                    .map(Self::DescribeDelegationToken)
            }
            ApiKey::DeleteGroupsKey => {
                DeleteGroupsResponse::decode(buf, version).map(Self::DeleteGroups)
            }
            ApiKey::ElectLeadersKey => {
                ElectLeadersResponse::decode(buf, version).map(Self::ElectLeaders)
            }
            ApiKey::IncrementalAlterConfigsKey => {
                IncrementalAlterConfigsResponse::decode(buf, version)
                    .map(Self::IncrementalAlterConfigs)
            }
            ApiKey::AlterPartitionReassignmentsKey => {
                AlterPartitionReassignmentsResponse::decode(buf, version)
                    .map(Self::AlterPartitionReassignments)
            }
            ApiKey::ListPartitionReassignmentsKey => {
                ListPartitionReassignmentsResponse::decode(buf, version)
                    .map(Self::ListPartitionReassignments)
            }
            ApiKey::OffsetDeleteKey => {
                OffsetDeleteResponse::decode(buf, version).map(Self::OffsetDelete)
            }
            ApiKey::DescribeClientQuotasKey => {
                DescribeClientQuotasResponse::decode(buf, version).map(Self::DescribeClientQuotas)
            }
            ApiKey::AlterClientQuotasKey => {
                AlterClientQuotasResponse::decode(buf, version).map(Self::AlterClientQuotas)
            }
            ApiKey::DescribeUserScramCredentialsKey => {
                DescribeUserScramCredentialsResponse::decode(buf, version)
                    .map(Self::DescribeUserScramCredentials)
            }
            ApiKey::AlterUserScramCredentialsKey => {
                AlterUserScramCredentialsResponse::decode(buf, version)
                    .map(Self::AlterUserScramCredentials)
            }
            ApiKey::VoteKey => VoteResponse::decode(buf, version).map(Self::Vote),
            ApiKey::BeginQuorumEpochKey => {
                BeginQuorumEpochResponse::decode(buf, version).map(Self::BeginQuorumEpoch)
            }
            ApiKey::EndQuorumEpochKey => {
                EndQuorumEpochResponse::decode(buf, version).map(Self::EndQuorumEpoch)
            }
            ApiKey::DescribeQuorumKey => {
                DescribeQuorumResponse::decode(buf, version).map(Self::DescribeQuorum)
            }
            ApiKey::AlterPartitionKey => {
                AlterPartitionResponse::decode(buf, version).map(Self::AlterPartition)
            }
            ApiKey::UpdateFeaturesKey => {
                UpdateFeaturesResponse::decode(buf, version).map(Self::UpdateFeatures)
            }
            ApiKey::EnvelopeKey => EnvelopeResponse::decode(buf, version).map(Self::Envelope),
            ApiKey::FetchSnapshotKey => {
                FetchSnapshotResponse::decode(buf, version).map(Self::FetchSnapshot)
            }
            ApiKey::DescribeClusterKey => {
                DescribeClusterResponse::decode(buf, version).map(Self::DescribeCluster)
            }
            ApiKey::DescribeProducersKey => {
                DescribeProducersResponse::decode(buf, version).map(Self::DescribeProducers)
            }
            ApiKey::BrokerRegistrationKey => {
                BrokerRegistrationResponse::decode(buf, version).map(Self::BrokerRegistration)
            }
            ApiKey::BrokerHeartbeatKey => {
                BrokerHeartbeatResponse::decode(buf, version).map(Self::BrokerHeartbeat)
            }
            ApiKey::UnregisterBrokerKey => {
                UnregisterBrokerResponse::decode(buf, version).map(Self::UnregisterBroker)
            }
            ApiKey::DescribeTransactionsKey => {
                DescribeTransactionsResponse::decode(buf, version).map(Self::DescribeTransactions)
            }
            ApiKey::ListTransactionsKey => {
                ListTransactionsResponse::decode(buf, version).map(Self::ListTransactions)
            }
            ApiKey::AllocateProducerIdsKey => {
                AllocateProducerIdsResponse::decode(buf, version).map(Self::AllocateProducerIds)
            }
            ApiKey::ConsumerGroupHeartbeatKey => {
                ConsumerGroupHeartbeatResponse::decode(buf, version)
                    .map(Self::ConsumerGroupHeartbeat)
            }
            ApiKey::ControllerRegistrationKey => {
                ControllerRegistrationResponse::decode(buf, version)
                    .map(Self::ControllerRegistration)
            }
            ApiKey::GetTelemetrySubscriptionsKey => {
                GetTelemetrySubscriptionsResponse::decode(buf, version)
                    .map(Self::GetTelemetrySubscriptions)
            }
            ApiKey::PushTelemetryKey => {
                PushTelemetryResponse::decode(buf, version).map(Self::PushTelemetry)
            }
            ApiKey::AssignReplicasToDirsKey => {
                AssignReplicasToDirsResponse::decode(buf, version).map(Self::AssignReplicasToDirs)
            }
            ApiKey::ListClientMetricsResourcesKey => {
                ListClientMetricsResourcesResponse::decode(buf, version)
                    .map(Self::ListClientMetricsResources)
            }
        }
    }
}
