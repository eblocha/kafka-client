use std::io;

use bytes::BytesMut;
use kafka_protocol::{
    messages::{
        AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
        AddPartitionsToTxnResponse, AllocateProducerIdsRequest, AllocateProducerIdsResponse,
        AlterClientQuotasRequest, AlterClientQuotasResponse, AlterConfigsRequest,
        AlterConfigsResponse, AlterPartitionReassignmentsRequest,
        AlterPartitionReassignmentsResponse, AlterPartitionRequest, AlterPartitionResponse,
        AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse, AlterUserScramCredentialsRequest,
        AlterUserScramCredentialsResponse, ApiKey, ApiVersionsRequest, ApiVersionsResponse,
        AssignReplicasToDirsRequest, AssignReplicasToDirsResponse, BeginQuorumEpochRequest,
        BeginQuorumEpochResponse, BrokerHeartbeatRequest, BrokerHeartbeatResponse,
        BrokerRegistrationRequest, BrokerRegistrationResponse, ConsumerGroupHeartbeatRequest,
        ConsumerGroupHeartbeatResponse, ControlledShutdownRequest, ControlledShutdownResponse,
        ControllerRegistrationRequest, ControllerRegistrationResponse, CreateAclsRequest,
        CreateAclsResponse, CreateDelegationTokenRequest, CreateDelegationTokenResponse,
        CreatePartitionsRequest, CreatePartitionsResponse, CreateTopicsRequest,
        CreateTopicsResponse, DeleteAclsRequest, DeleteAclsResponse, DeleteGroupsRequest,
        DeleteGroupsResponse, DeleteRecordsRequest, DeleteRecordsResponse, DeleteTopicsRequest,
        DeleteTopicsResponse, DescribeAclsRequest, DescribeAclsResponse,
        DescribeClientQuotasRequest, DescribeClientQuotasResponse, DescribeClusterRequest,
        DescribeClusterResponse, DescribeConfigsRequest, DescribeConfigsResponse,
        DescribeDelegationTokenRequest, DescribeDelegationTokenResponse, DescribeGroupsRequest,
        DescribeGroupsResponse, DescribeLogDirsRequest, DescribeLogDirsResponse,
        DescribeProducersRequest, DescribeProducersResponse, DescribeQuorumRequest,
        DescribeQuorumResponse, DescribeTransactionsRequest, DescribeTransactionsResponse,
        DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse,
        ElectLeadersRequest, ElectLeadersResponse, EndQuorumEpochRequest, EndQuorumEpochResponse,
        EndTxnRequest, EndTxnResponse, EnvelopeRequest, EnvelopeResponse,
        ExpireDelegationTokenRequest, ExpireDelegationTokenResponse, FetchRequest, FetchResponse,
        FetchSnapshotRequest, FetchSnapshotResponse, FindCoordinatorRequest,
        FindCoordinatorResponse, GetTelemetrySubscriptionsRequest,
        GetTelemetrySubscriptionsResponse, HeartbeatRequest, HeartbeatResponse,
        IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse, InitProducerIdRequest,
        InitProducerIdResponse, JoinGroupRequest, JoinGroupResponse, LeaderAndIsrRequest,
        LeaderAndIsrResponse, LeaveGroupRequest, LeaveGroupResponse,
        ListClientMetricsResourcesRequest, ListClientMetricsResourcesResponse, ListGroupsRequest,
        ListGroupsResponse, ListOffsetsRequest, ListOffsetsResponse,
        ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse,
        ListTransactionsRequest, ListTransactionsResponse, MetadataRequest, MetadataResponse,
        OffsetCommitRequest, OffsetCommitResponse, OffsetDeleteRequest, OffsetDeleteResponse,
        OffsetFetchRequest, OffsetFetchResponse, OffsetForLeaderEpochRequest,
        OffsetForLeaderEpochResponse, ProduceRequest, ProduceResponse, PushTelemetryRequest,
        PushTelemetryResponse, RenewDelegationTokenRequest, RenewDelegationTokenResponse,
        ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
        SaslHandshakeResponse, StopReplicaRequest, StopReplicaResponse, SyncGroupRequest,
        SyncGroupResponse, TxnOffsetCommitRequest, TxnOffsetCommitResponse,
        UnregisterBrokerRequest, UnregisterBrokerResponse, UpdateFeaturesRequest,
        UpdateFeaturesResponse, UpdateMetadataRequest, UpdateMetadataResponse, VoteRequest,
        VoteResponse, WriteTxnMarkersRequest, WriteTxnMarkersResponse,
    },
    protocol::Decodable,
};

use crate::conn::{request::KafkaRequest, response::KafkaResponse};

#[inline]
fn into_invalid_data(error: anyhow::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[derive(Debug, Clone)]
pub struct RequestRecord {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub response_header_version: i16,
}

pub trait Sendable: Into<KafkaRequest> {
    type Response;

    fn decode_frame(frame: BytesMut, record: RequestRecord) -> Result<Self::Response, io::Error>;
}

impl Sendable for KafkaRequest {
    type Response = KafkaResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        KafkaResponse::decode(&mut frame, record.api_version, record.api_key)
            .map_err(into_invalid_data)
    }
}

impl Sendable for AddOffsetsToTxnRequest {
    type Response = AddOffsetsToTxnResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for AddPartitionsToTxnRequest {
    type Response = AddPartitionsToTxnResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for AllocateProducerIdsRequest {
    type Response = AllocateProducerIdsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for AlterClientQuotasRequest {
    type Response = AlterClientQuotasResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for AlterConfigsRequest {
    type Response = AlterConfigsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for AlterPartitionReassignmentsRequest {
    type Response = AlterPartitionReassignmentsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for AlterPartitionRequest {
    type Response = AlterPartitionResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for AlterReplicaLogDirsRequest {
    type Response = AlterReplicaLogDirsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for AlterUserScramCredentialsRequest {
    type Response = AlterUserScramCredentialsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for ApiVersionsRequest {
    type Response = ApiVersionsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for AssignReplicasToDirsRequest {
    type Response = AssignReplicasToDirsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for BeginQuorumEpochRequest {
    type Response = BeginQuorumEpochResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for BrokerHeartbeatRequest {
    type Response = BrokerHeartbeatResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for BrokerRegistrationRequest {
    type Response = BrokerRegistrationResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for ConsumerGroupHeartbeatRequest {
    type Response = ConsumerGroupHeartbeatResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for ControlledShutdownRequest {
    type Response = ControlledShutdownResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for ControllerRegistrationRequest {
    type Response = ControllerRegistrationResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for CreateAclsRequest {
    type Response = CreateAclsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for CreateDelegationTokenRequest {
    type Response = CreateDelegationTokenResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for CreatePartitionsRequest {
    type Response = CreatePartitionsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for CreateTopicsRequest {
    type Response = CreateTopicsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DeleteAclsRequest {
    type Response = DeleteAclsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DeleteGroupsRequest {
    type Response = DeleteGroupsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DeleteRecordsRequest {
    type Response = DeleteRecordsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DeleteTopicsRequest {
    type Response = DeleteTopicsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DescribeAclsRequest {
    type Response = DescribeAclsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DescribeClientQuotasRequest {
    type Response = DescribeClientQuotasResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DescribeClusterRequest {
    type Response = DescribeClusterResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DescribeConfigsRequest {
    type Response = DescribeConfigsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DescribeDelegationTokenRequest {
    type Response = DescribeDelegationTokenResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DescribeGroupsRequest {
    type Response = DescribeGroupsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DescribeLogDirsRequest {
    type Response = DescribeLogDirsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DescribeProducersRequest {
    type Response = DescribeProducersResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DescribeQuorumRequest {
    type Response = DescribeQuorumResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DescribeTransactionsRequest {
    type Response = DescribeTransactionsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for DescribeUserScramCredentialsRequest {
    type Response = DescribeUserScramCredentialsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for ElectLeadersRequest {
    type Response = ElectLeadersResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for EndQuorumEpochRequest {
    type Response = EndQuorumEpochResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for EndTxnRequest {
    type Response = EndTxnResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for EnvelopeRequest {
    type Response = EnvelopeResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for ExpireDelegationTokenRequest {
    type Response = ExpireDelegationTokenResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for FetchRequest {
    type Response = FetchResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for FetchSnapshotRequest {
    type Response = FetchSnapshotResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for FindCoordinatorRequest {
    type Response = FindCoordinatorResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for GetTelemetrySubscriptionsRequest {
    type Response = GetTelemetrySubscriptionsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for HeartbeatRequest {
    type Response = HeartbeatResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for IncrementalAlterConfigsRequest {
    type Response = IncrementalAlterConfigsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for InitProducerIdRequest {
    type Response = InitProducerIdResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for JoinGroupRequest {
    type Response = JoinGroupResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for LeaderAndIsrRequest {
    type Response = LeaderAndIsrResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for LeaveGroupRequest {
    type Response = LeaveGroupResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for ListClientMetricsResourcesRequest {
    type Response = ListClientMetricsResourcesResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for ListGroupsRequest {
    type Response = ListGroupsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for ListOffsetsRequest {
    type Response = ListOffsetsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for ListPartitionReassignmentsRequest {
    type Response = ListPartitionReassignmentsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for ListTransactionsRequest {
    type Response = ListTransactionsResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for MetadataRequest {
    type Response = MetadataResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for OffsetCommitRequest {
    type Response = OffsetCommitResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for OffsetDeleteRequest {
    type Response = OffsetDeleteResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for OffsetFetchRequest {
    type Response = OffsetFetchResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for OffsetForLeaderEpochRequest {
    type Response = OffsetForLeaderEpochResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for ProduceRequest {
    type Response = ProduceResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for PushTelemetryRequest {
    type Response = PushTelemetryResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for RenewDelegationTokenRequest {
    type Response = RenewDelegationTokenResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for SaslAuthenticateRequest {
    type Response = SaslAuthenticateResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for SaslHandshakeRequest {
    type Response = SaslHandshakeResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for StopReplicaRequest {
    type Response = StopReplicaResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for SyncGroupRequest {
    type Response = SyncGroupResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for TxnOffsetCommitRequest {
    type Response = TxnOffsetCommitResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for UnregisterBrokerRequest {
    type Response = UnregisterBrokerResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for UpdateFeaturesRequest {
    type Response = UpdateFeaturesResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for UpdateMetadataRequest {
    type Response = UpdateMetadataResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for VoteRequest {
    type Response = VoteResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
impl Sendable for WriteTxnMarkersRequest {
    type Response = WriteTxnMarkersResponse;

    fn decode_frame(
        mut frame: BytesMut,
        record: RequestRecord,
    ) -> Result<Self::Response, io::Error> {
        ResponseHeader::decode(&mut frame, record.response_header_version)
            .map_err(into_invalid_data)?;

        Self::Response::decode(&mut frame, record.api_version).map_err(into_invalid_data)
    }
}
