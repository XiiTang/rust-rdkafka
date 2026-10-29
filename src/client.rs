//! Common client functionality.
//!
//! In librdkafka parlance, a client is either a consumer or a producer. This
//! module's [`Client`] type provides the functionality that is common to both
//! consumers and producers.
//!
//! Typically you will not want to construct a client directly. Construct one of
//! the consumers in the [`consumer`] module or one of the producers in the
//! [`producer`] modules instead.
//!
//! [`consumer`]: crate::consumer
//! [`producer`]: crate::producer

use std::ffi::{CStr, CString};
use std::mem::ManuallyDrop;
use std::os::raw::c_char;
use std::ptr;
use std::string::ToString;
use std::sync::Arc;

use libc::c_void;
use rdkafka_sys as rdsys;
use rdkafka_sys::types::*;

use crate::admin::NativeEvent;
use crate::config::{ClientConfig, NativeClientConfig, RDKafkaLogLevel};
use crate::consumer::RebalanceProtocol;
use crate::error::{IsError, KafkaError, KafkaResult, RDKafkaError};
use crate::groups::GroupList;
use crate::log::{debug, error, info, trace, warn};
use crate::metadata::Metadata;
use crate::mocking::MockCluster;
use crate::statistics::Statistics;
use crate::util::{ErrBuf, KafkaDrop, NativePtr, Timeout};

/// One failed group-list subrequest with its broker and request context.
#[derive(Debug)]
pub struct BrokerRequestError {
    /// Addressed broker id.
    pub broker_id: i32,
    /// Addressed broker authority.
    pub peer: String,
    /// ListGroups or DescribeGroups API key.
    pub api_key: i16,
    /// Selected version, absent if construction failed before selection.
    pub api_version: Option<i16>,
    /// Native error and its own decoded reply, if any.
    pub error: KafkaError,
}
/// Completed groups and all failed subrequests, independent of overall status.
pub struct GroupListOutcome {
    /// Completely decoded groups, if a list was returned.
    pub groups: Option<GroupList>,
    /// Every callback-observed failed subrequest of this call.
    pub broker_errors: Vec<BrokerRequestError>,
    /// Unchanged overall native status.
    pub error: Option<KafkaError>,
}

/// Client-level context.
///
/// Each client (consumers and producers included) has a context object that can
/// be used to customize its behavior. Implementing `ClientContext` enables the
/// customization of methods common to all clients, while [`ProducerContext`]
/// and [`ConsumerContext`] are specific to producers and consumers. Refer to
/// the list of methods to see which callbacks can currently be overridden.
///
/// **Important**: implementations of `ClientContext` must be thread safe, as
/// they might be shared between multiple threads.
///
/// [`ConsumerContext`]: crate::consumer::ConsumerContext
/// [`ProducerContext`]: crate::producer::ProducerContext
pub trait ClientContext: Send + Sync {
    /// Optional caller-owned transport boundary for every broker connection.
    fn supplied_transport(&self) -> Option<&crate::transport::SuppliedTransport> {
        None
    }
    /// Admit a new SASL exchange for the actual broker authority. Also called
    /// for in-place reauthentication, before the provider consumes credentials.
    /// Runs synchronously on a broker thread; failures do not expose diagnostics.
    fn authorize_sasl(&self, _broker: &str) -> Result<(), crate::sasl::Error> {
        Ok(())
    }
    /// The caller-owned SASL state machine, invoked on broker threads. Every
    /// SASL client requires one: the native mechanism engines are not used.
    fn supplied_sasl(&self) -> Option<&crate::sasl::Provider> {
        None
    }

    /// Receives log lines from librdkafka.
    ///
    /// The default implementation forwards the log lines to the appropriate
    /// [`log`] crate macro. Consult the [`RDKafkaLogLevel`] documentation for
    /// details about the log level mapping.
    ///
    /// [`log`]: https://docs.rs/log
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                error!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Warning => {
                warn!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Notice => {
                info!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Info => {
                info!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
            RDKafkaLogLevel::Debug => {
                debug!(target: "librdkafka", "librdkafka: {} {}", fac, log_message)
            }
        }
    }

    /// Receives the decoded statistics of the librdkafka client. To enable, the
    /// `statistics.interval.ms` configuration parameter must be specified.
    ///
    /// The default implementation logs the statistics at the `info` log level.
    fn stats(&self, statistics: Statistics) {
        info!("Client stats: {:?}", statistics);
    }

    /// Receives the JSON-encoded statistics of the librdkafka client. To
    /// enable, the `statistics.interval.ms` configuration parameter must be
    /// specified.
    ///
    /// The default implementation calls [`ClientContext::stats`] with the
    /// decoded statistics, logging an error if the decoding fails.
    fn stats_raw(&self, statistics: &[u8]) {
        match serde_json::from_slice(statistics) {
            Ok(stats) => self.stats(stats),
            Err(e) => error!("Could not parse statistics JSON: {}", e),
        }
    }

    /// Receives global errors from the librdkafka client.
    ///
    /// The default implementation logs the error at the `error` log level.
    fn error(&self, error: KafkaError, reason: &str) {
        error!("librdkafka: {}: {}", error, reason);
    }

    // NOTE: when adding a new method, remember to add it to the
    // FutureProducerContext as well.
    // https://github.com/rust-lang/rfcs/pull/1406 will maybe help in the
    // future.
}

/// An empty [`ClientContext`] that can be used when no customizations are
/// needed.
///
/// Uses the default callback implementations provided by `ClientContext`.
#[derive(Clone, Debug, Default)]
pub struct DefaultClientContext;

impl ClientContext for DefaultClientContext {}

//
// ********** CLIENT **********
//

/// A native rdkafka-sys client. This struct shouldn't be used directly. Use
/// higher level `Client` or producers and consumers.
// TODO(benesch): this should be `pub(crate)`.
pub struct NativeClient {
    ptr: ManuallyDrop<NativePtr<RDKafka>>,
    explicit_close: bool,
}

unsafe impl KafkaDrop for RDKafka {
    const TYPE: &'static str = "client";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_destroy;
}

// The library is completely thread safe, according to the documentation.
unsafe impl Sync for NativeClient {}
unsafe impl Send for NativeClient {}

impl NativeClient {
    /// Wraps a pointer to an RDKafka object and returns a new NativeClient.
    pub(crate) unsafe fn from_ptr(ptr: *mut RDKafka) -> NativeClient {
        NativeClient {
            ptr: ManuallyDrop::new(NativePtr::from_ptr(ptr).unwrap()),
            explicit_close: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn destruction_probe(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        self.ptr.destruction_probe()
    }

    /// Returns the wrapped pointer to RDKafka.
    pub fn ptr(&self) -> *mut RDKafka {
        self.ptr.ptr()
    }

    pub(crate) fn rebalance_protocol(&self) -> RebalanceProtocol {
        let protocol = unsafe { rdsys::rd_kafka_rebalance_protocol(self.ptr()) };
        if protocol.is_null() {
            RebalanceProtocol::None
        } else {
            let protocol = unsafe { CStr::from_ptr(protocol) };
            match protocol.to_bytes() {
                b"NONE" => RebalanceProtocol::None,
                b"EAGER" => RebalanceProtocol::Eager,
                b"COOPERATIVE" => RebalanceProtocol::Cooperative,
                _ => unreachable!(),
            }
        }
    }
}

pub(crate) enum EventPollResult<T> {
    None,
    EventConsumed,
    Event(T),
}

impl<T> From<EventPollResult<T>> for Option<T> {
    fn from(val: EventPollResult<T>) -> Self {
        match val {
            EventPollResult::None | EventPollResult::EventConsumed => None,
            EventPollResult::Event(evt) => Some(evt),
        }
    }
}

unsafe extern "C" fn native_synchronous_log<C: ClientContext>(
    client: *const RDKafka,
    level: i32,
    fac: *const c_char,
    message: *const c_char,
) {
    if client.is_null() || fac.is_null() || message.is_null() {
        return;
    }
    let context = unsafe { rdsys::rd_kafka_opaque(client).cast::<C>().as_ref() };
    if let Some(context) = context {
        let fac = unsafe { CStr::from_ptr(fac) }.to_string_lossy();
        let message = unsafe { CStr::from_ptr(message) }.to_string_lossy();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            context.log(RDKafkaLogLevel::from_int(level), fac.trim(), message.trim())
        }));
    }
}

/// A low-level rdkafka client.
///
/// This type is the basis of the consumers and producers in the [`consumer`]
/// and [`producer`] modules, respectively.
///
/// Typically you do not want to construct a `Client` directly, but instead
/// construct a consumer or producer. A `Client` can be used, however, when
/// only access to cluster metadata and watermarks is required.
///
/// [`consumer`]: crate::consumer
/// [`producer`]: crate::producer
pub struct Client<C: ClientContext = DefaultClientContext> {
    native: NativeClient,
    context: Arc<C>,
}

impl<C: ClientContext> Client<C> {
    /// Creates a new `Client` given a configuration, a client type and a context.
    pub fn new(
        config: &ClientConfig,
        native_config: NativeClientConfig,
        rd_kafka_type: RDKafkaType,
        context: C,
    ) -> KafkaResult<Client<C>> {
        Self::new_context_arc(config, native_config, rd_kafka_type, Arc::new(context))
    }

    /// Creates a new `Client` given a configuration, a client type and a context.
    pub(crate) fn new_context_arc(
        config: &ClientConfig,
        native_config: NativeClientConfig,
        rd_kafka_type: RDKafkaType,
        context: Arc<C>,
    ) -> KafkaResult<Client<C>> {
        let mut err_buf = ErrBuf::new();
        unsafe {
            rdsys::rd_kafka_conf_set_opaque(
                native_config.ptr(),
                Arc::as_ptr(&context) as *mut c_void,
            )
        };
        let queue_logs = config.get("log.queue") != Some("false");
        native_config.set("log.queue", if queue_logs { "true" } else { "false" })?;
        if !queue_logs {
            unsafe {
                rdsys::rd_kafka_conf_set_log_cb(
                    native_config.ptr(),
                    Some(native_synchronous_log::<C>),
                );
            }
        }
        unsafe {
            crate::sasl::install_admission::<C>(native_config.ptr());
        }
        if context.supplied_sasl().is_some() {
            unsafe {
                crate::sasl::install::<C>(native_config.ptr());
            }
        }
        if context.supplied_transport().is_some() {
            unsafe {
                crate::transport::install::<C>(native_config.ptr());
            }
        }

        let client_ptr = unsafe {
            let native_config = ManuallyDrop::new(native_config);
            rdsys::rd_kafka_new(
                rd_kafka_type,
                native_config.ptr(),
                err_buf.as_mut_ptr(),
                err_buf.capacity(),
            )
        };
        trace!("Create new librdkafka client {:p}", client_ptr);

        if client_ptr.is_null() {
            return Err(KafkaError::ClientCreation(err_buf.to_string()));
        }

        if queue_logs {
            let ret = unsafe {
                rdsys::rd_kafka_set_log_queue(
                    client_ptr,
                    rdsys::rd_kafka_queue_get_main(client_ptr),
                )
            };
            if ret.is_error() {
                return Err(KafkaError::Global(ret.into()));
            }
        }
        unsafe { rdsys::rd_kafka_set_log_level(client_ptr, config.log_level as i32) };

        Ok(Client {
            native: unsafe {
                let mut native = NativeClient::from_ptr(client_ptr);
                native.explicit_close = context.supplied_transport().is_some();
                native
            },
            context,
        })
    }

    /// Returns a reference to the native rdkafka-sys client.
    pub fn native_client(&self) -> &NativeClient {
        &self.native
    }

    /// Returns a pointer to the native rdkafka-sys client.
    pub fn native_ptr(&self) -> *mut RDKafka {
        self.native.ptr.ptr()
    }

    /// Returns a reference to the context.
    pub fn context(&self) -> &Arc<C> {
        &self.context
    }

    pub(crate) fn poll_event<T: Into<Timeout>>(
        &self,
        queue: &NativeQueue,
        timeout: T,
    ) -> EventPollResult<NativeEvent> {
        let event = unsafe { NativeEvent::from_ptr(queue.poll(timeout)) };
        if let Some(ev) = event {
            let evtype = unsafe { rdsys::rd_kafka_event_type(ev.ptr()) };
            match evtype {
                rdsys::RD_KAFKA_EVENT_LOG => {
                    self.handle_log_event(ev.ptr());
                    return EventPollResult::EventConsumed;
                }
                rdsys::RD_KAFKA_EVENT_STATS => {
                    self.handle_stats_event(ev.ptr());
                    return EventPollResult::EventConsumed;
                }
                rdsys::RD_KAFKA_EVENT_ERROR => {
                    // rdkafka reports consumer errors via RD_KAFKA_EVENT_ERROR but producer errors gets
                    // embedded on the ack returned via RD_KAFKA_EVENT_DR. Hence we need to return this event
                    // for the consumer case in order to return the error to the user.
                    self.handle_error_event(ev.ptr());
                    return EventPollResult::Event(ev);
                }
                _ => {
                    return EventPollResult::Event(ev);
                }
            }
        }
        EventPollResult::None
    }

    fn handle_log_event(&self, event: *mut RDKafkaEvent) {
        let mut fac: *const c_char = std::ptr::null();
        let mut str_: *const c_char = std::ptr::null();
        let mut level: i32 = 0;
        let result = unsafe { rdsys::rd_kafka_event_log(event, &mut fac, &mut str_, &mut level) };
        if result == 0 {
            let fac = unsafe { CStr::from_ptr(fac).to_string_lossy() };
            let log_message = unsafe { CStr::from_ptr(str_).to_string_lossy() };
            self.context().log(
                RDKafkaLogLevel::from_int(level),
                fac.trim(),
                log_message.trim(),
            );
        }
    }

    fn handle_stats_event(&self, event: *mut RDKafkaEvent) {
        let json = unsafe { CStr::from_ptr(rdsys::rd_kafka_event_stats(event)) };
        self.context().stats_raw(json.to_bytes());
    }

    fn handle_error_event(&self, event: *mut RDKafkaEvent) {
        let rdkafka_err = unsafe { rdsys::rd_kafka_event_error(event) };
        let error = unsafe {
            KafkaError::Global(rdkafka_err.into())
                .with_reply(rdsys::rd_kafka_event_broker_reply(event))
        };
        let reason =
            unsafe { CStr::from_ptr(rdsys::rd_kafka_event_error_string(event)).to_string_lossy() };
        self.context().error(error, reason.trim());
    }

    /// Returns the metadata information for the specified topic, or for all topics in the cluster
    /// if no topic is specified.
    pub fn fetch_metadata<T: Into<Timeout>>(
        &self,
        topic: Option<&str>,
        timeout: T,
    ) -> KafkaResult<Metadata> {
        let mut metadata_ptr: *const RDKafkaMetadata = ptr::null_mut();
        let (flag, native_topic) = if let Some(topic_name) = topic {
            (0, Some(self.native_topic(topic_name)?))
        } else {
            (1, None)
        };
        trace!("Starting metadata fetch");
        let mut native_error = ptr::null_mut();
        let ret = unsafe {
            rdsys::rd_kafka_metadata_with_error(
                self.native_ptr(),
                flag,
                native_topic.map(|t| t.ptr()).unwrap_or_else(ptr::null_mut),
                &mut metadata_ptr as *mut *const RDKafkaMetadata,
                timeout.into().as_millis(),
                &mut native_error,
            )
        };
        trace!("Metadata fetch completed");
        let native_error = unsafe { RDKafkaError::from_ptr(native_error) };
        if ret.is_error() {
            return Err(KafkaError::MetadataFetch(ret.into()).with_native_error(&native_error));
        }

        Ok(unsafe { Metadata::from_ptr(metadata_ptr) })
    }

    /// Returns high and low watermark for the specified topic and partition.
    pub fn fetch_watermarks<T: Into<Timeout>>(
        &self,
        topic: &str,
        partition: i32,
        timeout: T,
    ) -> KafkaResult<(i64, i64)> {
        let mut low = -1;
        let mut high = -1;
        let topic_c = CString::new(topic.to_string())?;
        let mut native_error = ptr::null_mut();
        let ret = unsafe {
            rdsys::rd_kafka_query_watermark_offsets_with_error(
                self.native_ptr(),
                topic_c.as_ptr(),
                partition,
                &mut low as *mut i64,
                &mut high as *mut i64,
                timeout.into().as_millis(),
                &mut native_error,
            )
        };
        let native_error = unsafe { RDKafkaError::from_ptr(native_error) };
        if ret.is_error() {
            return Err(KafkaError::MetadataFetch(ret.into()).with_native_error(&native_error));
        }
        Ok((low, high))
    }

    /// Returns the cluster identifier option or None if the cluster identifier is null
    pub fn fetch_cluster_id<T: Into<Timeout>>(&self, timeout: T) -> Option<String> {
        let cluster_id =
            unsafe { rdsys::rd_kafka_clusterid(self.native_ptr(), timeout.into().as_millis()) };
        if cluster_id.is_null() {
            return None;
        }
        let buf = unsafe { CStr::from_ptr(cluster_id).to_bytes() };
        String::from_utf8(buf.to_vec()).ok()
    }

    /// Returns the group membership information for the given group. If no group is
    /// specified, all groups will be returned.
    pub fn fetch_group_list<T: Into<Timeout>>(
        &self,
        group: Option<&str>,
        timeout: T,
    ) -> KafkaResult<GroupList> {
        // Careful with group_c getting freed before time
        let group_c = CString::new(group.map_or("".to_string(), ToString::to_string))?;
        let group_c_ptr = if group.is_some() {
            group_c.as_ptr()
        } else {
            ptr::null_mut()
        };
        let mut group_list_ptr: *const RDKafkaGroupList = ptr::null_mut();
        trace!("Starting group list fetch");
        let ret = unsafe {
            rdsys::rd_kafka_list_groups(
                self.native_ptr(),
                group_c_ptr,
                &mut group_list_ptr as *mut *const RDKafkaGroupList,
                timeout.into().as_millis(),
            )
        };
        trace!("Group list fetch completed");
        if ret.is_error() {
            return Err(KafkaError::GroupListFetch(ret.into()));
        }

        Ok(unsafe { GroupList::from_ptr(group_list_ptr) })
    }

    /// Fetches group data and failed broker subrequests independently.
    pub fn fetch_group_list_with_outcome<T: Into<Timeout>>(
        &self,
        group: Option<&str>,
        timeout: T,
    ) -> KafkaResult<GroupListOutcome> {
        let group = group.map(CString::new).transpose()?;
        let mut groups = ptr::null();
        let mut error = ptr::null_mut();
        let mut broker_errors = ptr::null_mut();
        let mut count = 0;
        let status = unsafe {
            rdsys::rd_kafka_list_groups_with_error(
                self.native_ptr(),
                group.as_ref().map_or(ptr::null(), |g| g.as_ptr()),
                &mut groups,
                timeout.into().as_millis(),
                &mut error,
                &mut broker_errors,
                &mut count,
            )
        };
        let native_error = unsafe { RDKafkaError::from_ptr(error) };
        let groups = (!groups.is_null()).then(|| unsafe { GroupList::from_ptr(groups) });
        struct Errors(*mut rdsys::rd_kafka_group_list_broker_error_t, usize);
        impl Drop for Errors {
            fn drop(&mut self) {
                unsafe { rdsys::rd_kafka_group_list_broker_errors_destroy(self.0, self.1) }
            }
        }
        let errors = Errors(broker_errors, count);
        let mut broker_errors = Vec::with_capacity(count);
        for i in 0..count {
            let item = unsafe { &*errors.0.add(i) };
            let code = unsafe { rdsys::rd_kafka_error_code(item.error) };
            let error = unsafe {
                KafkaError::GroupListFetch(code.into())
                    .with_reply(rdsys::rd_kafka_error_broker_reply(item.error))
            };
            broker_errors.push(BrokerRequestError {
                broker_id: item.broker_id,
                peer: unsafe { CStr::from_ptr(item.peer) }
                    .to_str()
                    .expect("native broker authority is UTF-8")
                    .to_owned(),
                api_key: item.api_key,
                api_version: (item.api_version >= 0).then_some(item.api_version),
                error,
            });
        }
        Ok(GroupListOutcome {
            groups,
            broker_errors,
            error: status.is_error().then(|| {
                KafkaError::GroupListFetch(status.into()).with_native_error(&native_error)
            }),
        })
    }

    /// Copies the existing first fatal result, including its original reply.
    pub fn fatal_error_details(&self) -> Option<RDKafkaError> {
        let ptr = unsafe { rdsys::rd_kafka_fatal_error_copy(self.native_ptr()) };
        (!ptr.is_null()).then(|| unsafe { RDKafkaError::from_ptr(ptr) })
    }

    /// Returns the first fatal error set on this client instance, or `None` if
    /// no fatal error has occurred.
    ///
    /// This function is intended to be used with idempotent producers, where
    /// some errors must logically be considered fatal to retain consistency.
    pub fn fatal_error(&self) -> Option<(RDKafkaErrorCode, String)> {
        let mut err_buf = ErrBuf::new();
        let code = unsafe {
            rdsys::rd_kafka_fatal_error(self.native_ptr(), err_buf.as_mut_ptr(), err_buf.capacity())
        };
        if code == RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
            None
        } else {
            Some((code.into(), err_buf.to_string()))
        }
    }

    /// If this client was configured with `test.mock.num.brokers`,
    /// this will return a [`MockCluster`] instance associated with this client,
    /// otherwise `None` is returned.
    ///
    /// [`MockCluster`]: crate::mocking::MockCluster
    pub fn mock_cluster(&self) -> Option<MockCluster<'_, C>> {
        MockCluster::from_client(self)
    }

    /// Returns a NativeTopic from the current client. The NativeTopic shouldn't outlive the client
    /// it was generated from.
    pub(crate) fn native_topic(&self, topic: &str) -> KafkaResult<NativeTopic> {
        let topic_c = CString::new(topic.to_string())?;
        unsafe {
            NativeTopic::from_ptr(rdsys::rd_kafka_topic_new(
                self.native_ptr(),
                topic_c.as_ptr(),
                ptr::null_mut(),
            ))
        }
        .ok_or_else(|| KafkaError::Global(unsafe { rdsys::rd_kafka_last_error() }.into()))
    }

    /// Returns a NativeQueue from the current client. The NativeQueue shouldn't
    /// outlive the client it was generated from.
    pub(crate) fn new_native_queue(&self) -> NativeQueue {
        unsafe { NativeQueue::from_ptr(rdsys::rd_kafka_queue_new(self.native_ptr())).unwrap() }
    }

    pub(crate) fn consumer_queue(&self) -> Option<NativeQueue> {
        unsafe { NativeQueue::from_ptr(rdsys::rd_kafka_queue_get_consumer(self.native_ptr())) }
    }

    /// Returns a NativeQueue for the main librdkafka event queue from the current client.
    pub(crate) fn main_queue(&self) -> NativeQueue {
        unsafe { NativeQueue::from_ptr(rdsys::rd_kafka_queue_get_main(self.native_ptr())).unwrap() }
    }
}

pub(crate) type NativeTopic = NativePtr<RDKafkaTopic>;

unsafe impl KafkaDrop for RDKafkaTopic {
    const TYPE: &'static str = "native topic";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_topic_destroy;
}

unsafe impl Send for NativeTopic {}
unsafe impl Sync for NativeTopic {}

pub(crate) type NativeQueue = NativePtr<RDKafkaQueue>;

unsafe impl KafkaDrop for RDKafkaQueue {
    const TYPE: &'static str = "queue";
    const DROP: unsafe extern "C" fn(*mut Self) = rdsys::rd_kafka_queue_destroy;
}

// The library is completely thread safe, according to the documentation.
unsafe impl Sync for NativeQueue {}
unsafe impl Send for NativeQueue {}

impl NativeQueue {
    pub fn poll<T: Into<Timeout>>(&self, t: T) -> *mut RDKafkaEvent {
        unsafe { rdsys::rd_kafka_queue_poll(self.ptr(), t.into().as_millis()) }
    }
}

#[cfg(test)]
mod tests {
    // Just call everything to test there no panics by default, behavior
    // is tested in the integrations tests.

    use super::*;
    use crate::config::ClientConfig;

    #[test]
    fn refused_topic_creation_is_a_typed_error() {
        let config = ClientConfig::new()
            .set("partition.population.max", "1")
            .clone();
        let client = Client::new(
            &config,
            config.create_native_config().unwrap(),
            RDKafkaType::RD_KAFKA_PRODUCER,
            DefaultClientContext,
        )
        .unwrap();
        let first = client.native_topic("one").unwrap();
        assert!(matches!(
            client.native_topic("two"),
            Err(KafkaError::Global(RDKafkaErrorCode::PartitionPopulation))
        ));
        assert_eq!(
            client.fatal_error().unwrap().0,
            RDKafkaErrorCode::PartitionPopulation
        );
        drop(first);
    }

    #[test]
    fn test_client() {
        let config = ClientConfig::new();
        let native_config = config.create_native_config().unwrap();
        let client = Client::new(
            &config,
            native_config,
            RDKafkaType::RD_KAFKA_PRODUCER,
            DefaultClientContext,
        )
        .unwrap();
        assert!(!client.native_ptr().is_null());
    }
}

impl Drop for NativeClient {
    fn drop(&mut self) {
        unsafe {
            if self.explicit_close {
                rdsys::rd_kafka_destroy_flags(
                    self.ptr.ptr(),
                    rdsys::RD_KAFKA_DESTROY_F_NO_CONSUMER_CLOSE,
                );
            } else {
                ManuallyDrop::drop(&mut self.ptr);
            }
        }
    }
}
