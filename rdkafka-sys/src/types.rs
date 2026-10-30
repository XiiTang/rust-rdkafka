//! Aliases for types defined in the auto-generated bindings.

use std::error::Error;
use std::ffi::CStr;
use std::fmt;

use num_enum::IntoPrimitive;

use crate::bindings;
use crate::helpers;

// TYPES

/// Native rdkafka client.
pub type RDKafka = bindings::rd_kafka_t;

/// Native rdkafka configuration.
pub type RDKafkaConf = bindings::rd_kafka_conf_t;

/// Native rdkafka message.
pub type RDKafkaMessage = bindings::rd_kafka_message_t;

/// Native rdkafka topic.
pub type RDKafkaTopic = bindings::rd_kafka_topic_t;

/// Native rdkafka topic configuration.
pub type RDKafkaTopicConf = bindings::rd_kafka_topic_conf_t;

/// Native rdkafka topic partition.
pub type RDKafkaTopicPartition = bindings::rd_kafka_topic_partition_t;

/// Native rdkafka topic partition list.
pub type RDKafkaTopicPartitionList = bindings::rd_kafka_topic_partition_list_t;

/// Native rdkafka metadata container.
pub type RDKafkaMetadata = bindings::rd_kafka_metadata_t;

/// Native rdkafka topic information.
pub type RDKafkaMetadataTopic = bindings::rd_kafka_metadata_topic_t;

/// Native rdkafka partition information.
pub type RDKafkaMetadataPartition = bindings::rd_kafka_metadata_partition_t;

/// Native rdkafka broker information.
pub type RDKafkaMetadataBroker = bindings::rd_kafka_metadata_broker_t;

/// Native rdkafka consumer group metadata.
pub type RDKafkaConsumerGroupMetadata = bindings::rd_kafka_consumer_group_metadata_t;

/// Native rdkafka state.
pub type RDKafkaState = bindings::rd_kafka_s;

/// Native rdkafka list of groups.
pub type RDKafkaGroupList = bindings::rd_kafka_group_list;

/// Native rdkafka group information.
pub type RDKafkaGroupInfo = bindings::rd_kafka_group_info;

/// Native rdkafka group member information.
pub type RDKafkaGroupMemberInfo = bindings::rd_kafka_group_member_info;

/// Native rdkafka group member information.
pub type RDKafkaHeaders = bindings::rd_kafka_headers_t;

/// Native rdkafka queue.
pub type RDKafkaQueue = bindings::rd_kafka_queue_t;

/// Native rdkafka new topic object.
pub type RDKafkaNewTopic = bindings::rd_kafka_NewTopic_t;

/// Native rdkafka delete topic object.
pub type RDKafkaDeleteTopic = bindings::rd_kafka_DeleteTopic_t;

/// Native rdkafka delete group object.
pub type RDKafkaDeleteGroup = bindings::rd_kafka_DeleteGroup_t;

/// Native rdkafka new partitions object.
pub type RDKafkaNewPartitions = bindings::rd_kafka_NewPartitions_t;

/// Native rdkafka delete records object.
pub type RDKafkaDeleteRecords = bindings::rd_kafka_DeleteRecords_t;

/// Native rdkafka config resource.
pub type RDKafkaConfigResource = bindings::rd_kafka_ConfigResource_t;

/// Native rdkafka event.
pub type RDKafkaEvent = bindings::rd_kafka_event_t;

/// Native rdkafka admin options.
pub type RDKafkaAdminOptions = bindings::rd_kafka_AdminOptions_t;

/// Native rdkafka topic result.
pub type RDKafkaTopicResult = bindings::rd_kafka_topic_result_t;

/// Native rdkafka group result.
pub type RDKafkaGroupResult = bindings::rd_kafka_group_result_t;

/// Native rdkafka mock cluster.
pub type RDKafkaMockCluster = bindings::rd_kafka_mock_cluster_t;

// ENUMS

/// Client types.
pub use bindings::rd_kafka_type_t as RDKafkaType;

/// Configuration result.
pub use bindings::rd_kafka_conf_res_t as RDKafkaConfRes;

/// Response error.
pub use bindings::rd_kafka_resp_err_t as RDKafkaRespErr;

/// Admin operation.
pub use bindings::rd_kafka_admin_op_t as RDKafkaAdminOp;

/// Config resource type.
pub use bindings::rd_kafka_ResourceType_t as RDKafkaResourceType;

/// Config source.
pub use bindings::rd_kafka_ConfigSource_t as RDKafkaConfigSource;

// Errors enum

/// Native rdkafka error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[repr(i32)]
pub enum RDKafkaErrorCode {
    #[doc(hidden)]
    Begin = -200,
    /// Received message is incorrect.
    BadMessage = -199,
    /// Bad/unknown compression.
    BadCompression = -198,
    /// Broker is going away.
    BrokerDestroy = -197,
    /// Generic failure.
    Fail = -196,
    /// Broker transport failure.
    BrokerTransportFailure = -195,
    /// Critical system resource.
    CriticalSystemResource = -194,
    /// Failed to resolve broker.
    Resolve = -193,
    /// Produced message timed out.
    MessageTimedOut = -192,
    /// Reached the end of the topic+partition queue on the broker. Not really an error.
    PartitionEOF = -191,
    /// Permanent: Partition does not exist in cluster.
    UnknownPartition = -190,
    /// File or filesystem error.
    FileSystem = -189,
    /// Permanent: Topic does not exist in cluster.
    UnknownTopic = -188,
    /// All broker connections are down.
    AllBrokersDown = -187,
    /// Invalid argument, or invalid configuration.
    InvalidArgument = -186,
    /// Operation timed out.
    OperationTimedOut = -185,
    /// Queue is full.
    QueueFull = -184,
    /// ISR count < required.acks.
    ISRInsufficient = -183,
    /// Broker node update.
    NodeUpdate = -182,
    /// SSL error.
    SSL = -181,
    /// Waiting for coordinator to become available.
    WaitingForCoordinator = -180,
    /// Unknown client group.
    UnknownGroup = -179,
    /// Operation in progress.
    InProgress = -178,
    /// Previous operation in progress, wait for it to finish.
    PreviousInProgress = -177,
    /// This operation would interfere with an existing subscription.
    ExistingSubscription = -176,
    /// Assigned partitions (rebalance_cb).
    AssignPartitions = -175,
    /// Revoked partitions (rebalance_cb).
    RevokePartitions = -174,
    /// Conflicting use.
    Conflict = -173,
    /// Wrong state.
    State = -172,
    /// Unknown protocol.
    UnknownProtocol = -171,
    /// Not implemented.
    NotImplemented = -170,
    /// Authentication failure.
    Authentication = -169,
    /// No stored offset.
    NoOffset = -168,
    /// Outdated.
    Outdated = -167,
    /// Timed out in queue.
    TimedOutQueue = -166,
    /// Feature not supported by broker.
    UnsupportedFeature = -165,
    /// Awaiting cache update.
    WaitCache = -164,
    /// Operation interrupted (e.g., due to yield).
    Interrupted = -163,
    /// Key serialization error.
    KeySerialization = -162,
    /// Value serialization error.
    ValueSerialization = -161,
    /// Key deserialization error.
    KeyDeserialization = -160,
    /// Value deserialization error.
    ValueDeserialization = -159,
    /// Partial response.
    Partial = -158,
    /// Modification attempted on read-only object.
    ReadOnly = -157,
    /// No such entry or item not found.
    NoEnt = -156,
    /// Read underflow.
    Underflow = -155,
    /// Invalid type.
    InvalidType = -154,
    /// Retry operation.
    Retry = -153,
    /// Purged in queue.
    PurgeQueue = -152,
    /// Purged in flight.
    PurgeInflight = -151,
    /// Fatal error: see rd_kafka_fatal_error().
    Fatal = -150,
    /// Inconsistent state.
    Inconsistent = -149,
    /// Gap-less ordering would not be guaranteed if proceeding.
    GaplessGuarantee = -148,
    /// Maximum poll interval exceeded.
    PollExceeded = -147,
    /// Unknown broker.
    UnknownBroker = -146,
    /// Functionality not configured.
    NotConfigured = -145,
    /// Instance has been fenced.
    Fenced = -144,
    /// Application generated error.
    Application = -143,
    /// Assignment lost.
    AssignmentLost = -142,
    /// No operation performed.
    Noop = -141,
    /// No offset to automatically reset to.
    AutoOffsetReset = -140,
    /// Partition log truncation detected
    LogTruncation = -139,
    /// A different record in the batch was invalid and this message failed persisting.
    InvalidDifferentRecord = -138,
    /// Broker is going away but client isn't terminating */
    DestroyBroker = -137,
    /// `broker.population.max` reached.
    BrokerPopulation = -136,
    /// `partition.population.max` reached.
    PartitionPopulation = -135,
    #[doc(hidden)]
    End = -100,
    /// Unknown broker error.
    Unknown = -1,
    /// Success.
    NoError = 0,
    /// Offset out of range.
    OffsetOutOfRange = 1,
    /// Invalid message.
    InvalidMessage = 2,
    /// Unknown topic or partition.
    UnknownTopicOrPartition = 3,
    /// Invalid message size.
    InvalidMessageSize = 4,
    /// Leader not available.
    LeaderNotAvailable = 5,
    /// Not leader for partition.
    NotLeaderForPartition = 6,
    /// Request timed out.
    RequestTimedOut = 7,
    /// Broker not available.
    BrokerNotAvailable = 8,
    /// Replica not available.
    ReplicaNotAvailable = 9,
    /// Message size too large.
    MessageSizeTooLarge = 10,
    /// Stale controller epoch code.
    StaleControllerEpoch = 11,
    /// Offset metadata string too large.
    OffsetMetadataTooLarge = 12,
    /// Broker disconnected before response received.
    NetworkException = 13,
    /// Coordinator load in progress.
    CoordinatorLoadInProgress = 14,
    /// Coordinator not available.
    CoordinatorNotAvailable = 15,
    /// Not coordinator.
    NotCoordinator = 16,
    /// Invalid topic.
    InvalidTopic = 17,
    /// Message batch larger than configured server segment size.
    MessageBatchTooLarge = 18,
    /// Not enough in-sync replicas.
    NotEnoughReplicas = 19,
    /// Message(s) written to insufficient number of in-sync replicas.
    NotEnoughReplicasAfterAppend = 20,
    /// Invalid required acks value.
    InvalidRequiredAcks = 21,
    /// Specified group generation id is not valid.
    IllegalGeneration = 22,
    /// Inconsistent group protocol.
    InconsistentGroupProtocol = 23,
    /// Invalid group.id.
    InvalidGroupId = 24,
    /// Unknown member.
    UnknownMemberId = 25,
    /// Invalid session timeout.
    InvalidSessionTimeout = 26,
    /// Group rebalance in progress.
    RebalanceInProgress = 27,
    /// Commit offset data size is not valid.
    InvalidCommitOffsetSize = 28,
    /// Topic authorization failed.
    TopicAuthorizationFailed = 29,
    /// Group authorization failed.
    GroupAuthorizationFailed = 30,
    /// Cluster authorization failed.
    ClusterAuthorizationFailed = 31,
    /// Invalid timestamp.
    InvalidTimestamp = 32,
    /// Unsupported SASL mechanism.
    UnsupportedSASLMechanism = 33,
    /// Illegal SASL state.
    IllegalSASLState = 34,
    /// Unsupported version.
    UnsupportedVersion = 35,
    /// Topic already exists.
    TopicAlreadyExists = 36,
    /// Invalid number of partitions.
    InvalidPartitions = 37,
    /// Invalid replication factor.
    InvalidReplicationFactor = 38,
    /// Invalid replica assignment.
    InvalidReplicaAssignment = 39,
    /// Invalid config.
    InvalidConfig = 40,
    /// Not controller for cluster.
    NotController = 41,
    /// Invalid request.
    InvalidRequest = 42,
    /// Message format on broker does not support request.
    UnsupportedForMessageFormat = 43,
    /// Policy violation.
    PolicyViolation = 44,
    /// Broker received an out of order sequence number.
    OutOfOrderSequenceNumber = 45,
    /// Broker received a duplicate sequence number.
    DuplicateSequenceNumber = 46,
    /// Producer attempted an operation with an old epoch.
    InvalidProducerEpoch = 47,
    /// Producer attempted a transactional operation in an invalid state.
    InvalidTransactionalState = 48,
    /// Producer attempted to use a producer id which is currently assigned to
    /// its transactional id.
    InvalidProducerIdMapping = 49,
    /// Transaction timeout is larger than the maxi value allowed by the
    /// broker's max.transaction.timeout.ms.
    InvalidTransactionTimeout = 50,
    /// Producer attempted to update a transaction while another concurrent
    /// operation on the same transaction was ongoing.
    ConcurrentTransactions = 51,
    /// Indicates that the transaction coordinator sending a WriteTxnMarker is
    /// no longer the current coordinator for a given producer.
    TransactionCoordinatorFenced = 52,
    /// Transactional Id authorization failed.
    TransactionalIdAuthorizationFailed = 53,
    /// Security features are disabled.
    SecurityDisabled = 54,
    /// Operation not attempted.
    OperationNotAttempted = 55,
    /// Disk error when trying to access log file on the disk.
    KafkaStorageError = 56,
    /// The user-specified log directory is not found in the broker config.
    LogDirNotFound = 57,
    /// SASL Authentication failed.
    SaslAuthenticationFailed = 58,
    /// Unknown Producer Id.
    UnknownProducerId = 59,
    /// Partition reassignment is in progress.
    ReassignmentInProgress = 60,
    /// Delegation Token feature is not enabled.
    DelegationTokenAuthDisabled = 61,
    /// Delegation Token is not found on server.
    DelegationTokenNotFound = 62,
    /// Specified Principal is not valid Owner/Renewer.
    DelegationTokenOwnerMismatch = 63,
    /// Delegation Token requests are not allowed on this connection.
    DelegationTokenRequestNotAllowed = 64,
    /// Delegation Token authorization failed.
    DelegationTokenAuthorizationFailed = 65,
    /// Delegation Token is expired.
    DelegationTokenExpired = 66,
    /// Supplied principalType is not supported.
    InvalidPrincipalType = 67,
    /// The group is not empty.
    NonEmptyGroup = 68,
    /// The group id does not exist.
    GroupIdNotFound = 69,
    /// The fetch session ID was not found.
    FetchSessionIdNotFound = 70,
    /// The fetch session epoch is invalid.
    InvalidFetchSessionEpoch = 71,
    /// No matching listener.
    ListenerNotFound = 72,
    /// Topic deletion is disabled.
    TopicDeletionDisabled = 73,
    /// Leader epoch is older than broker epoch.
    FencedLeaderEpoch = 74,
    /// Leader epoch is newer than broker epoch.
    UnknownLeaderEpoch = 75,
    /// Unsupported compression type.
    UnsupportedCompressionType = 76,
    /// Broker epoch has changed.
    StaleBrokerEpoch = 77,
    /// Leader high watermark is not caught up.
    OffsetNotAvailable = 78,
    /// Group member needs a valid member ID.
    MemberIdRequired = 79,
    /// Preferred leader was not available.
    PreferredLeaderNotAvailable = 80,
    /// Consumer group has reached maximum size.
    GroupMaxSizeReached = 81,
    /// Static consumer fenced by other consumer with same group.instance.id.
    FencedInstanceId = 82,
    /// Eligible partition leaders are not available.
    EligibleLeadersNotAvailable = 83,
    /// Leader election not needed for topic partition.
    ElectionNotNeeded = 84,
    /// No partition reassignment is in progress.
    NoReassignmentInProgress = 85,
    /// Deleting offsets of a topic while the consumer group is subscribed to
    /// it.
    GroupSubscribedToTopic = 86,
    /// Broker failed to validate record.
    InvalidRecord = 87,
    /// There are unstable offsets that need to be cleared.
    UnstableOffsetCommit = 88,
    /// Throttling quota has been exceeded.
    ThrottlingQuotaExceeded = 89,
    /// There is a newer producer with the same transactional ID which fences
    /// the current one.
    ProducerFenced = 90,
    /// Request illegally referred to resource that does not exist.
    ResourceNotFound = 91,
    /// Request illegally referred to the same resource twice.
    DuplicateResource = 92,
    /// Requested credential would not meet criteria for acceptability.
    UnacceptableCredential = 93,
    /// Either the sender or recipient of a voter-only request is not one of the
    /// expected voters.
    InconsistentVoterSet = 94,
    /// Invalid update version.
    InvalidUpdateVersion = 95,
    /// Unable to update finalized features due to server error.
    FeatureUpdateFailed = 96,
    /// Request principal deserialization failed during forwarding.
    PrincipalDeserializationFailure = 97,
    /// Unknown Topic Id
    UnknownTopicId = 100,
    /// The member epoch is fenced by the group coordinator
    FencedMemberEpoch = 110,
    /// The instance ID is still used by another member in the consumer group
    UnreleasedInstanceId = 111,
    /// The assignor or its version range is not supported by the consumer group
    UnsupportedAssignor = 112,
    /// The member epoch is stale
    StaleMemberEpoch = 113,
    /// Client sent a push telemetry request with an invalid or outdated
    /// subscription ID.
    UnknownSubscriptionId = 117,
    /// Client sent a push telemetry request larger than the maximum size
    /// the broker will accept.
    TelemetryTooLarge = 118,
    /// Client metadata is stale,
    /// client should rebootstrap to obtain new metadata.
    RebootstrapRequired = 129,
    #[doc(hidden)]
    EndAll,
    /// A native code not named by this version of librdkafka.
    UnknownCode(i32),
}

impl From<RDKafkaRespErr> for RDKafkaErrorCode {
    fn from(err: RDKafkaRespErr) -> RDKafkaErrorCode {
        helpers::rd_kafka_resp_err_t_to_rdkafka_error(err)
    }
}

impl RDKafkaErrorCode {
    /// Returns the native convenience name from `rd_kafka_err2name`.
    /// Unnamed statuses still retain their integer through [`Self::raw_code`].
    pub fn name(self) -> String {
        unsafe { CStr::from_ptr(bindings::rd_kafka_err2name(RDKafkaRespErr(self.raw_code()))) }
            .to_string_lossy()
            .into_owned()
    }

    /// Returns the original native integer, including unnamed codes.
    pub fn raw_code(self) -> i32 {
        match self {
            Self::Begin => RDKafkaRespErr::RD_KAFKA_RESP_ERR__BEGIN.0,
            Self::BadMessage => RDKafkaRespErr::RD_KAFKA_RESP_ERR__BAD_MSG.0,
            Self::BadCompression => RDKafkaRespErr::RD_KAFKA_RESP_ERR__BAD_COMPRESSION.0,
            Self::BrokerDestroy => RDKafkaRespErr::RD_KAFKA_RESP_ERR__DESTROY.0,
            Self::Fail => RDKafkaRespErr::RD_KAFKA_RESP_ERR__FAIL.0,
            Self::BrokerTransportFailure => RDKafkaRespErr::RD_KAFKA_RESP_ERR__TRANSPORT.0,
            Self::CriticalSystemResource => RDKafkaRespErr::RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE.0,
            Self::Resolve => RDKafkaRespErr::RD_KAFKA_RESP_ERR__RESOLVE.0,
            Self::MessageTimedOut => RDKafkaRespErr::RD_KAFKA_RESP_ERR__MSG_TIMED_OUT.0,
            Self::PartitionEOF => RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTITION_EOF.0,
            Self::UnknownPartition => RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION.0,
            Self::FileSystem => RDKafkaRespErr::RD_KAFKA_RESP_ERR__FS.0,
            Self::UnknownTopic => RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC.0,
            Self::AllBrokersDown => RDKafkaRespErr::RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN.0,
            Self::InvalidArgument => RDKafkaRespErr::RD_KAFKA_RESP_ERR__INVALID_ARG.0,
            Self::OperationTimedOut => RDKafkaRespErr::RD_KAFKA_RESP_ERR__TIMED_OUT.0,
            Self::QueueFull => RDKafkaRespErr::RD_KAFKA_RESP_ERR__QUEUE_FULL.0,
            Self::ISRInsufficient => RDKafkaRespErr::RD_KAFKA_RESP_ERR__ISR_INSUFF.0,
            Self::NodeUpdate => RDKafkaRespErr::RD_KAFKA_RESP_ERR__NODE_UPDATE.0,
            Self::SSL => RDKafkaRespErr::RD_KAFKA_RESP_ERR__SSL.0,
            Self::WaitingForCoordinator => RDKafkaRespErr::RD_KAFKA_RESP_ERR__WAIT_COORD.0,
            Self::UnknownGroup => RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_GROUP.0,
            Self::InProgress => RDKafkaRespErr::RD_KAFKA_RESP_ERR__IN_PROGRESS.0,
            Self::PreviousInProgress => RDKafkaRespErr::RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS.0,
            Self::ExistingSubscription => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__EXISTING_SUBSCRIPTION.0
            }
            Self::AssignPartitions => RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS.0,
            Self::RevokePartitions => RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS.0,
            Self::Conflict => RDKafkaRespErr::RD_KAFKA_RESP_ERR__CONFLICT.0,
            Self::State => RDKafkaRespErr::RD_KAFKA_RESP_ERR__STATE.0,
            Self::UnknownProtocol => RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_PROTOCOL.0,
            Self::NotImplemented => RDKafkaRespErr::RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED.0,
            Self::Authentication => RDKafkaRespErr::RD_KAFKA_RESP_ERR__AUTHENTICATION.0,
            Self::NoOffset => RDKafkaRespErr::RD_KAFKA_RESP_ERR__NO_OFFSET.0,
            Self::Outdated => RDKafkaRespErr::RD_KAFKA_RESP_ERR__OUTDATED.0,
            Self::TimedOutQueue => RDKafkaRespErr::RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE.0,
            Self::UnsupportedFeature => RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE.0,
            Self::WaitCache => RDKafkaRespErr::RD_KAFKA_RESP_ERR__WAIT_CACHE.0,
            Self::Interrupted => RDKafkaRespErr::RD_KAFKA_RESP_ERR__INTR.0,
            Self::KeySerialization => RDKafkaRespErr::RD_KAFKA_RESP_ERR__KEY_SERIALIZATION.0,
            Self::ValueSerialization => RDKafkaRespErr::RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION.0,
            Self::KeyDeserialization => RDKafkaRespErr::RD_KAFKA_RESP_ERR__KEY_DESERIALIZATION.0,
            Self::ValueDeserialization => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION.0
            }
            Self::Partial => RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTIAL.0,
            Self::ReadOnly => RDKafkaRespErr::RD_KAFKA_RESP_ERR__READ_ONLY.0,
            Self::NoEnt => RDKafkaRespErr::RD_KAFKA_RESP_ERR__NOENT.0,
            Self::Underflow => RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNDERFLOW.0,
            Self::InvalidType => RDKafkaRespErr::RD_KAFKA_RESP_ERR__INVALID_TYPE.0,
            Self::Retry => RDKafkaRespErr::RD_KAFKA_RESP_ERR__RETRY.0,
            Self::PurgeQueue => RDKafkaRespErr::RD_KAFKA_RESP_ERR__PURGE_QUEUE.0,
            Self::PurgeInflight => RDKafkaRespErr::RD_KAFKA_RESP_ERR__PURGE_INFLIGHT.0,
            Self::Fatal => RDKafkaRespErr::RD_KAFKA_RESP_ERR__FATAL.0,
            Self::Inconsistent => RDKafkaRespErr::RD_KAFKA_RESP_ERR__INCONSISTENT.0,
            Self::GaplessGuarantee => RDKafkaRespErr::RD_KAFKA_RESP_ERR__GAPLESS_GUARANTEE.0,
            Self::PollExceeded => RDKafkaRespErr::RD_KAFKA_RESP_ERR__MAX_POLL_EXCEEDED.0,
            Self::UnknownBroker => RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_BROKER.0,
            Self::NotConfigured => RDKafkaRespErr::RD_KAFKA_RESP_ERR__NOT_CONFIGURED.0,
            Self::Fenced => RDKafkaRespErr::RD_KAFKA_RESP_ERR__FENCED.0,
            Self::Application => RDKafkaRespErr::RD_KAFKA_RESP_ERR__APPLICATION.0,
            Self::AssignmentLost => RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGNMENT_LOST.0,
            Self::Noop => RDKafkaRespErr::RD_KAFKA_RESP_ERR__NOOP.0,
            Self::AutoOffsetReset => RDKafkaRespErr::RD_KAFKA_RESP_ERR__AUTO_OFFSET_RESET.0,
            Self::End => RDKafkaRespErr::RD_KAFKA_RESP_ERR__END.0,
            Self::Unknown => RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN.0,
            Self::NoError => RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR.0,
            Self::OffsetOutOfRange => RDKafkaRespErr::RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE.0,
            Self::InvalidMessage => RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_MSG.0,
            Self::UnknownTopicOrPartition => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART.0
            }
            Self::InvalidMessageSize => RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE.0,
            Self::LeaderNotAvailable => RDKafkaRespErr::RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE.0,
            Self::NotLeaderForPartition => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION.0
            }
            Self::RequestTimedOut => RDKafkaRespErr::RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT.0,
            Self::BrokerNotAvailable => RDKafkaRespErr::RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE.0,
            Self::ReplicaNotAvailable => RDKafkaRespErr::RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE.0,
            Self::MessageSizeTooLarge => RDKafkaRespErr::RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE.0,
            Self::StaleControllerEpoch => RDKafkaRespErr::RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH.0,
            Self::OffsetMetadataTooLarge => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE.0
            }
            Self::NetworkException => RDKafkaRespErr::RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION.0,
            Self::CoordinatorLoadInProgress => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS.0
            }
            Self::CoordinatorNotAvailable => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE.0
            }
            Self::NotCoordinator => RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_COORDINATOR.0,
            Self::InvalidTopic => RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION.0,
            Self::MessageBatchTooLarge => RDKafkaRespErr::RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE.0,
            Self::NotEnoughReplicas => RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS.0,
            Self::NotEnoughReplicasAfterAppend => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND.0
            }
            Self::InvalidRequiredAcks => RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_REQUIRED_ACKS.0,
            Self::IllegalGeneration => RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION.0,
            Self::InconsistentGroupProtocol => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INCONSISTENT_GROUP_PROTOCOL.0
            }
            Self::InvalidGroupId => RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_GROUP_ID.0,
            Self::UnknownMemberId => RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID.0,
            Self::InvalidSessionTimeout => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_SESSION_TIMEOUT.0
            }
            Self::RebalanceInProgress => RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS.0,
            Self::InvalidCommitOffsetSize => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_COMMIT_OFFSET_SIZE.0
            }
            Self::TopicAuthorizationFailed => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED.0
            }
            Self::GroupAuthorizationFailed => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED.0
            }
            Self::ClusterAuthorizationFailed => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED.0
            }
            Self::InvalidTimestamp => RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_TIMESTAMP.0,
            Self::UnsupportedSASLMechanism => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNSUPPORTED_SASL_MECHANISM.0
            }
            Self::IllegalSASLState => RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_SASL_STATE.0,
            Self::UnsupportedVersion => RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNSUPPORTED_VERSION.0,
            Self::TopicAlreadyExists => RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS.0,
            Self::InvalidPartitions => RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_PARTITIONS.0,
            Self::InvalidReplicationFactor => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_REPLICATION_FACTOR.0
            }
            Self::InvalidReplicaAssignment => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_REPLICA_ASSIGNMENT.0
            }
            Self::InvalidConfig => RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_CONFIG.0,
            Self::NotController => RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_CONTROLLER.0,
            Self::InvalidRequest => RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_REQUEST.0,
            Self::UnsupportedForMessageFormat => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNSUPPORTED_FOR_MESSAGE_FORMAT.0
            }
            Self::PolicyViolation => RDKafkaRespErr::RD_KAFKA_RESP_ERR_POLICY_VIOLATION.0,
            Self::OutOfOrderSequenceNumber => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_OUT_OF_ORDER_SEQUENCE_NUMBER.0
            }
            Self::DuplicateSequenceNumber => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_DUPLICATE_SEQUENCE_NUMBER.0
            }
            Self::InvalidProducerEpoch => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH.0
            }
            Self::InvalidTransactionalState => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_TXN_STATE.0
            }
            Self::InvalidProducerIdMapping => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_PRODUCER_ID_MAPPING.0
            }
            Self::InvalidTransactionTimeout => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_TRANSACTION_TIMEOUT.0
            }
            Self::ConcurrentTransactions => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_CONCURRENT_TRANSACTIONS.0
            }
            Self::TransactionCoordinatorFenced => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_TRANSACTION_COORDINATOR_FENCED.0
            }
            Self::SecurityDisabled => RDKafkaRespErr::RD_KAFKA_RESP_ERR_SECURITY_DISABLED.0,
            Self::OperationNotAttempted => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_OPERATION_NOT_ATTEMPTED.0
            }
            Self::KafkaStorageError => RDKafkaRespErr::RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR.0,
            Self::LogDirNotFound => RDKafkaRespErr::RD_KAFKA_RESP_ERR_LOG_DIR_NOT_FOUND.0,
            Self::SaslAuthenticationFailed => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED.0
            }
            Self::UnknownProducerId => RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_PRODUCER_ID.0,
            Self::ReassignmentInProgress => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_REASSIGNMENT_IN_PROGRESS.0
            }
            Self::DelegationTokenAuthDisabled => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_AUTH_DISABLED.0
            }
            Self::DelegationTokenNotFound => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_NOT_FOUND.0
            }
            Self::DelegationTokenOwnerMismatch => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_OWNER_MISMATCH.0
            }
            Self::DelegationTokenRequestNotAllowed => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED.0
            }
            Self::DelegationTokenExpired => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_EXPIRED.0
            }
            Self::InvalidPrincipalType => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_PRINCIPAL_TYPE.0
            }
            Self::NonEmptyGroup => RDKafkaRespErr::RD_KAFKA_RESP_ERR_NON_EMPTY_GROUP.0,
            Self::GroupIdNotFound => RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_ID_NOT_FOUND.0,
            Self::FetchSessionIdNotFound => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_FETCH_SESSION_ID_NOT_FOUND.0
            }
            Self::InvalidFetchSessionEpoch => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_FETCH_SESSION_EPOCH.0
            }
            Self::ListenerNotFound => RDKafkaRespErr::RD_KAFKA_RESP_ERR_LISTENER_NOT_FOUND.0,
            Self::TopicDeletionDisabled => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_DELETION_DISABLED.0
            }
            Self::FencedLeaderEpoch => RDKafkaRespErr::RD_KAFKA_RESP_ERR_FENCED_LEADER_EPOCH.0,
            Self::UnknownLeaderEpoch => RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_LEADER_EPOCH.0,
            Self::UnsupportedCompressionType => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNSUPPORTED_COMPRESSION_TYPE.0
            }
            Self::StaleBrokerEpoch => RDKafkaRespErr::RD_KAFKA_RESP_ERR_STALE_BROKER_EPOCH.0,
            Self::OffsetNotAvailable => RDKafkaRespErr::RD_KAFKA_RESP_ERR_OFFSET_NOT_AVAILABLE.0,
            Self::MemberIdRequired => RDKafkaRespErr::RD_KAFKA_RESP_ERR_MEMBER_ID_REQUIRED.0,
            Self::PreferredLeaderNotAvailable => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_PREFERRED_LEADER_NOT_AVAILABLE.0
            }
            Self::GroupMaxSizeReached => RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_MAX_SIZE_REACHED.0,
            Self::FencedInstanceId => RDKafkaRespErr::RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID.0,
            Self::EligibleLeadersNotAvailable => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_ELIGIBLE_LEADERS_NOT_AVAILABLE.0
            }
            Self::ElectionNotNeeded => RDKafkaRespErr::RD_KAFKA_RESP_ERR_ELECTION_NOT_NEEDED.0,
            Self::NoReassignmentInProgress => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_REASSIGNMENT_IN_PROGRESS.0
            }
            Self::GroupSubscribedToTopic => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_SUBSCRIBED_TO_TOPIC.0
            }
            Self::InvalidRecord => RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_RECORD.0,
            Self::UnstableOffsetCommit => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNSTABLE_OFFSET_COMMIT.0
            }
            Self::ThrottlingQuotaExceeded => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_THROTTLING_QUOTA_EXCEEDED.0
            }
            Self::ProducerFenced => RDKafkaRespErr::RD_KAFKA_RESP_ERR_PRODUCER_FENCED.0,
            Self::ResourceNotFound => RDKafkaRespErr::RD_KAFKA_RESP_ERR_RESOURCE_NOT_FOUND.0,
            Self::DuplicateResource => RDKafkaRespErr::RD_KAFKA_RESP_ERR_DUPLICATE_RESOURCE.0,
            Self::UnacceptableCredential => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNACCEPTABLE_CREDENTIAL.0
            }
            Self::InconsistentVoterSet => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INCONSISTENT_VOTER_SET.0
            }
            Self::InvalidUpdateVersion => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_UPDATE_VERSION.0
            }
            Self::FeatureUpdateFailed => RDKafkaRespErr::RD_KAFKA_RESP_ERR_FEATURE_UPDATE_FAILED.0,
            Self::PrincipalDeserializationFailure => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_PRINCIPAL_DESERIALIZATION_FAILURE.0
            }
            Self::EndAll => RDKafkaRespErr::RD_KAFKA_RESP_ERR_END_ALL.0,
            Self::LogTruncation => RDKafkaRespErr::RD_KAFKA_RESP_ERR__LOG_TRUNCATION.0,
            Self::InvalidDifferentRecord => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR__INVALID_DIFFERENT_RECORD.0
            }
            Self::DestroyBroker => RDKafkaRespErr::RD_KAFKA_RESP_ERR__DESTROY_BROKER.0,
            Self::BrokerPopulation => RDKafkaRespErr::RD_KAFKA_RESP_ERR__BROKER_POPULATION.0,
            Self::PartitionPopulation => RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTITION_POPULATION.0,
            Self::UnknownTopicId => RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID.0,
            Self::FencedMemberEpoch => RDKafkaRespErr::RD_KAFKA_RESP_ERR_FENCED_MEMBER_EPOCH.0,
            Self::UnreleasedInstanceId => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNRELEASED_INSTANCE_ID.0
            }
            Self::UnsupportedAssignor => RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNSUPPORTED_ASSIGNOR.0,
            Self::StaleMemberEpoch => RDKafkaRespErr::RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH.0,
            Self::UnknownSubscriptionId => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_SUBSCRIPTION_ID.0
            }
            Self::TelemetryTooLarge => RDKafkaRespErr::RD_KAFKA_RESP_ERR_TELEMETRY_TOO_LARGE.0,
            Self::RebootstrapRequired => RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBOOTSTRAP_REQUIRED.0,
            Self::TransactionalIdAuthorizationFailed => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED.0
            }
            Self::DelegationTokenAuthorizationFailed => {
                RDKafkaRespErr::RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_AUTHORIZATION_FAILED.0
            }
            Self::UnknownCode(code) => code,
        }
    }
}

impl fmt::Display for RDKafkaErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let cstr = unsafe { bindings::rd_kafka_err2str(RDKafkaRespErr(self.raw_code())) };
        f.write_str(&unsafe { CStr::from_ptr(cstr) }.to_string_lossy())
    }
}

impl Error for RDKafkaErrorCode {}

/// Native rdkafka ApiKeys / protocol requests
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive)]
#[repr(i16)]
#[non_exhaustive]
pub enum RDKafkaApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersion = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    OffsetForLeaderEpoch = 23,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
    WriteTxnMarkers = 27,
    TxnOffsetCommit = 28,
    DescribeAcls = 29,
    CreateAcls = 30,
    DeleteAcls = 31,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    AlterReplicaLogDirs = 34,
    DescribeLogDirs = 35,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    CreateDelegationToken = 38,
    RenewDelegationToken = 39,
    ExpireDelegationToken = 40,
    DescribeDelegationToken = 41,
    DeleteGroups = 42,
    ElectLeaders = 43,
    IncrementalAlterConfigs = 44,
    AlterPartitionReassignments = 45,
    ListPartitionReassignments = 46,
    OffsetDelete = 47,
    DescribeClientQuotas = 48,
    AlterClientQuotas = 49,
    DescribeUserScramCredentials = 50,
    AlterUserScramCredentials = 51,
    Vote = 52,
    BeginQuorumEpoch = 53,
    EndQuorumEpoch = 54,
    DescribeQuorum = 55,
    AlterIsr = 56,
    UpdateFeatures = 57,
    Envelope = 58,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_error() {
        let error: RDKafkaErrorCode = RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTITION_EOF.into();
        assert_eq!("Broker: No more messages", format!("{}", error));
        for raw in [i32::MAX, i32::MIN] {
            let error = RDKafkaErrorCode::UnknownCode(raw);
            assert_eq!(error.raw_code(), raw);
            assert_eq!(error.name(), format!("ERR_{raw}?"));
            assert_eq!(error.to_string(), format!("Err-{raw}?"));
        }
    }
}
