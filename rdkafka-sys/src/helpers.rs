//! Utility functions.

use crate::types::RDKafkaErrorCode;
use crate::types::RDKafkaErrorCode::*;
use crate::types::RDKafkaRespErr;

pub fn rd_kafka_resp_err_t_to_rdkafka_error(err: RDKafkaRespErr) -> RDKafkaErrorCode {
    match err {
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__BEGIN => Begin,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__BAD_MSG => BadMessage,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__BAD_COMPRESSION => BadCompression,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__DESTROY => BrokerDestroy,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__FAIL => Fail,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__TRANSPORT => BrokerTransportFailure,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE => CriticalSystemResource,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__RESOLVE => Resolve,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__MSG_TIMED_OUT => MessageTimedOut,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTITION_EOF => PartitionEOF,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION => UnknownPartition,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__FS => FileSystem,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC => UnknownTopic,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN => AllBrokersDown,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__INVALID_ARG => InvalidArgument,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__TIMED_OUT => OperationTimedOut,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__QUEUE_FULL => QueueFull,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__ISR_INSUFF => ISRInsufficient,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__NODE_UPDATE => NodeUpdate,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__SSL => SSL,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__WAIT_COORD => WaitingForCoordinator,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_GROUP => UnknownGroup,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__IN_PROGRESS => InProgress,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__PREV_IN_PROGRESS => PreviousInProgress,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__EXISTING_SUBSCRIPTION => ExistingSubscription,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => AssignPartitions,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => RevokePartitions,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__CONFLICT => Conflict,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__STATE => State,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_PROTOCOL => UnknownProtocol,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__NOT_IMPLEMENTED => NotImplemented,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__AUTHENTICATION => Authentication,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__NO_OFFSET => NoOffset,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__OUTDATED => Outdated,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE => TimedOutQueue,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE => UnsupportedFeature,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__WAIT_CACHE => WaitCache,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__INTR => Interrupted,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__KEY_SERIALIZATION => KeySerialization,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__VALUE_SERIALIZATION => ValueSerialization,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__KEY_DESERIALIZATION => KeyDeserialization,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__VALUE_DESERIALIZATION => ValueDeserialization,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTIAL => Partial,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__READ_ONLY => ReadOnly,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__NOENT => NoEnt,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNDERFLOW => Underflow,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__INVALID_TYPE => InvalidType,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__RETRY => Retry,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__PURGE_QUEUE => PurgeQueue,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__PURGE_INFLIGHT => PurgeInflight,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__FATAL => Fatal,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__INCONSISTENT => Inconsistent,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__GAPLESS_GUARANTEE => GaplessGuarantee,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__MAX_POLL_EXCEEDED => PollExceeded,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__UNKNOWN_BROKER => UnknownBroker,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__NOT_CONFIGURED => NotConfigured,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__FENCED => Fenced,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__APPLICATION => Application,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGNMENT_LOST => AssignmentLost,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__NOOP => Noop,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__AUTO_OFFSET_RESET => AutoOffsetReset,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__END => End,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN => Unknown,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR => NoError,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE => OffsetOutOfRange,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_MSG => InvalidMessage,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART => UnknownTopicOrPartition,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE => InvalidMessageSize,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE => LeaderNotAvailable,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION => NotLeaderForPartition,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT => RequestTimedOut,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE => BrokerNotAvailable,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE => ReplicaNotAvailable,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE => MessageSizeTooLarge,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH => StaleControllerEpoch,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE => OffsetMetadataTooLarge,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION => NetworkException,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS => CoordinatorLoadInProgress,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE => CoordinatorNotAvailable,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_COORDINATOR => NotCoordinator,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_EXCEPTION => InvalidTopic,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE => MessageBatchTooLarge,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS => NotEnoughReplicas,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_ENOUGH_REPLICAS_AFTER_APPEND => {
            NotEnoughReplicasAfterAppend
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_REQUIRED_ACKS => InvalidRequiredAcks,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_GENERATION => IllegalGeneration,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INCONSISTENT_GROUP_PROTOCOL => InconsistentGroupProtocol,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_GROUP_ID => InvalidGroupId,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_MEMBER_ID => UnknownMemberId,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_SESSION_TIMEOUT => InvalidSessionTimeout,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBALANCE_IN_PROGRESS => RebalanceInProgress,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_COMMIT_OFFSET_SIZE => InvalidCommitOffsetSize,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED => TopicAuthorizationFailed,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED => GroupAuthorizationFailed,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED => {
            ClusterAuthorizationFailed
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_TIMESTAMP => InvalidTimestamp,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNSUPPORTED_SASL_MECHANISM => UnsupportedSASLMechanism,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_ILLEGAL_SASL_STATE => IllegalSASLState,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNSUPPORTED_VERSION => UnsupportedVersion,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS => TopicAlreadyExists,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_PARTITIONS => InvalidPartitions,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_REPLICATION_FACTOR => InvalidReplicationFactor,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_REPLICA_ASSIGNMENT => InvalidReplicaAssignment,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_CONFIG => InvalidConfig,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NOT_CONTROLLER => NotController,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_REQUEST => InvalidRequest,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNSUPPORTED_FOR_MESSAGE_FORMAT => {
            UnsupportedForMessageFormat
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_POLICY_VIOLATION => PolicyViolation,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_OUT_OF_ORDER_SEQUENCE_NUMBER => OutOfOrderSequenceNumber,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_DUPLICATE_SEQUENCE_NUMBER => DuplicateSequenceNumber,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH => InvalidProducerEpoch,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_TXN_STATE => InvalidTransactionalState,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_PRODUCER_ID_MAPPING => InvalidProducerIdMapping,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_TRANSACTION_TIMEOUT => InvalidTransactionTimeout,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_CONCURRENT_TRANSACTIONS => ConcurrentTransactions,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_TRANSACTION_COORDINATOR_FENCED => {
            TransactionCoordinatorFenced
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED => {
            TransactionalIdAuthorizationFailed
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_SECURITY_DISABLED => SecurityDisabled,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_OPERATION_NOT_ATTEMPTED => OperationNotAttempted,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_KAFKA_STORAGE_ERROR => KafkaStorageError,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_LOG_DIR_NOT_FOUND => LogDirNotFound,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED => SaslAuthenticationFailed,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_PRODUCER_ID => UnknownProducerId,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_REASSIGNMENT_IN_PROGRESS => ReassignmentInProgress,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_AUTH_DISABLED => {
            DelegationTokenAuthDisabled
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_NOT_FOUND => DelegationTokenNotFound,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_OWNER_MISMATCH => {
            DelegationTokenOwnerMismatch
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_REQUEST_NOT_ALLOWED => {
            DelegationTokenRequestNotAllowed
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_AUTHORIZATION_FAILED => {
            DelegationTokenAuthorizationFailed
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_DELEGATION_TOKEN_EXPIRED => DelegationTokenExpired,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_PRINCIPAL_TYPE => InvalidPrincipalType,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NON_EMPTY_GROUP => NonEmptyGroup,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_ID_NOT_FOUND => GroupIdNotFound,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_FETCH_SESSION_ID_NOT_FOUND => FetchSessionIdNotFound,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_FETCH_SESSION_EPOCH => InvalidFetchSessionEpoch,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_LISTENER_NOT_FOUND => ListenerNotFound,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_DELETION_DISABLED => TopicDeletionDisabled,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_FENCED_LEADER_EPOCH => FencedLeaderEpoch,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_LEADER_EPOCH => UnknownLeaderEpoch,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNSUPPORTED_COMPRESSION_TYPE => {
            UnsupportedCompressionType
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_STALE_BROKER_EPOCH => StaleBrokerEpoch,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_OFFSET_NOT_AVAILABLE => OffsetNotAvailable,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_MEMBER_ID_REQUIRED => MemberIdRequired,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_PREFERRED_LEADER_NOT_AVAILABLE => {
            PreferredLeaderNotAvailable
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_MAX_SIZE_REACHED => GroupMaxSizeReached,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_FENCED_INSTANCE_ID => FencedInstanceId,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_ELIGIBLE_LEADERS_NOT_AVAILABLE => {
            EligibleLeadersNotAvailable
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_ELECTION_NOT_NEEDED => ElectionNotNeeded,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_REASSIGNMENT_IN_PROGRESS => NoReassignmentInProgress,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_SUBSCRIBED_TO_TOPIC => GroupSubscribedToTopic,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_RECORD => InvalidRecord,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNSTABLE_OFFSET_COMMIT => UnstableOffsetCommit,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_THROTTLING_QUOTA_EXCEEDED => ThrottlingQuotaExceeded,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_PRODUCER_FENCED => ProducerFenced,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_RESOURCE_NOT_FOUND => ResourceNotFound,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_DUPLICATE_RESOURCE => DuplicateResource,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNACCEPTABLE_CREDENTIAL => UnacceptableCredential,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INCONSISTENT_VOTER_SET => InconsistentVoterSet,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_INVALID_UPDATE_VERSION => InvalidUpdateVersion,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_FEATURE_UPDATE_FAILED => FeatureUpdateFailed,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_PRINCIPAL_DESERIALIZATION_FAILURE => {
            PrincipalDeserializationFailure
        }
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_END_ALL => EndAll,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__LOG_TRUNCATION => LogTruncation,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__INVALID_DIFFERENT_RECORD => InvalidDifferentRecord,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__DESTROY_BROKER => DestroyBroker,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__BROKER_POPULATION => BrokerPopulation,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR__PARTITION_POPULATION => PartitionPopulation,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_ID => UnknownTopicId,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_FENCED_MEMBER_EPOCH => FencedMemberEpoch,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNRELEASED_INSTANCE_ID => UnreleasedInstanceId,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNSUPPORTED_ASSIGNOR => UnsupportedAssignor,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_STALE_MEMBER_EPOCH => StaleMemberEpoch,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_UNKNOWN_SUBSCRIPTION_ID => UnknownSubscriptionId,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_TELEMETRY_TOO_LARGE => TelemetryTooLarge,
        RDKafkaRespErr::RD_KAFKA_RESP_ERR_REBOOTSTRAP_REQUIRED => RebootstrapRequired,
        other => UnknownCode(other.0),
    }
}
