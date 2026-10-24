//! A socket adapter for caller-owned proxy/TLS transports. No Kafka parsing.
use crate::{bindings as sys, client::ClientContext};
#[cfg(unix)]
use std::os::fd::{FromRawFd, IntoRawFd};
#[cfg(windows)]
use std::os::windows::io::{FromRawSocket, IntoRawSocket};
use std::{
    collections::BTreeMap,
    ffi::{c_char, c_int, c_void, CStr},
    io,
    net::{TcpListener, TcpStream},
    panic::{catch_unwind, AssertUnwindSafe},
    sync::Mutex,
};

/// Three separately owned permits, reserved together before any private socket
/// is created. Each argument is an opaque RAII guard supplied by the caller.
pub struct SocketPermits {
    listener: Box<dyn Send>,
    native: Box<dyn Send>,
    peer: Box<dyn Send>,
}

impl SocketPermits {
    /// The admission callback must reserve all three permits atomically, rolling
    /// back on refusal. Guards release their individual permits on drop.
    pub fn new(
        listener: impl Send + 'static,
        native: impl Send + 'static,
        peer: impl Send + 'static,
    ) -> Self {
        Self {
            listener: Box::new(listener),
            native: Box::new(native),
            peer: Box::new(peer),
        }
    }
}

/// The peer of the native engine's private socket and its lifetime permit.
/// Move this owner into the transport pump and keep it until copying ends in
/// both directions. In particular, one direction's EOF does not release it.
pub struct TransportPeer<S = TcpStream> {
    // Fields drop in declaration order: destroy the stream before its permit.
    stream: S,
    _permit: Box<dyn Send>,
}

impl<S> TransportPeer<S> {
    /// Mutably borrow the stream, for example for bidirectional copying.
    pub fn stream_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// Convert the stream (for example to a Tokio stream), carrying its permit
    /// into the new owner. A failed conversion drops the permit. The conversion
    /// must destroy the input on failure and must not leave an unguarded clone.
    pub fn try_map<T>(
        self,
        convert: impl FnOnce(S) -> io::Result<T>,
    ) -> io::Result<TransportPeer<T>> {
        let Self { stream, _permit } = self;
        let stream = convert(stream)?;
        Ok(TransportPeer { stream, _permit })
    }
}

struct SocketState {
    peer: Option<TransportPeer>,
    _native_permit: Box<dyn Send>,
}

#[derive(Default)]
struct Sockets {
    entries: BTreeMap<c_int, SocketState>,
    pending: usize,
}

/// A caller-owned transport adapter. All callbacks run synchronously on native
/// broker threads: bound their time and own/join any transport tasks they start.
pub struct SuppliedTransport {
    admit: Box<dyn Fn() -> io::Result<SocketPermits> + Send + Sync>,
    connect: Box<dyn Fn(&str, u64, TransportPeer) -> io::Result<()> + Send + Sync>,
    failed: Box<dyn Fn(io::Error) + Send + Sync>,
    sockets: Mutex<Sockets>,
    maximum: usize,
}
impl SuppliedTransport {
    /// Construct an adapter bounded by `maximum` pairs awaiting connect handoff.
    ///
    /// `admit` reserves the listener and both loopback halves together before
    /// binding the listener. The listener returns its permit at socket callback
    /// return; each half retains its own permit until destroyed. Admission of
    /// the external route, authorization and TLS remain caller policy.
    ///
    /// `connect` receives the actual logical broker host:port (including discovered
    /// leaders and coordinators), its stable native instance ID and a guarded
    /// private peer. Move that peer into the pump, using `try_map` for conversion.
    ///
    /// `failed` receives each socket/setup or connect error before the FFI failure
    /// return. The original `io::Error`, including an embedded caller error, is
    /// moved without stringification. This hook remains active for every attempt,
    /// including after bootstrap; the caller owns retention and event delivery.
    /// Panics are contained at FFI and reported as a fixed local error. A panic
    /// in `failed` is contained too. Callbacks must not unwind on purpose.
    pub fn new(
        maximum: usize,
        admit: impl Fn() -> io::Result<SocketPermits> + Send + Sync + 'static,
        connect: impl Fn(&str, u64, TransportPeer) -> io::Result<()> + Send + Sync + 'static,
        failed: impl Fn(io::Error) + Send + Sync + 'static,
    ) -> Self {
        Self {
            admit: Box::new(admit),
            connect: Box::new(connect),
            failed: Box::new(failed),
            sockets: Mutex::new(Sockets::default()),
            maximum,
        }
    }

    fn socket(&self) -> io::Result<c_int> {
        self.socket_with(Self::pair)
    }

    fn socket_with(
        &self,
        pair: impl FnOnce() -> io::Result<(TcpStream, TcpStream)>,
    ) -> io::Result<c_int> {
        // No caller callback runs while the socket-state lock is held.
        let SocketPermits {
            listener: _listener,
            native: native_permit,
            peer: peer_permit,
        } = (self.admit)()?;
        let mut sockets = self
            .sockets
            .lock()
            .map_err(|_| io::Error::other("transport stopped"))?;
        if sockets.pending >= self.maximum {
            return Err(io::Error::other("transport admission limit"));
        }
        let (native, peer) = pair()?;
        #[cfg(unix)]
        let descriptor = native.into_raw_fd();
        #[cfg(windows)]
        let descriptor = {
            use std::os::windows::io::AsRawSocket;
            let descriptor = c_int::try_from(native.as_raw_socket())
                .map_err(|_| io::Error::other("socket handle exceeds native callback ABI"))?;
            let _ = native.into_raw_socket();
            descriptor
        };
        sockets.entries.insert(
            descriptor,
            SocketState {
                peer: Some(TransportPeer {
                    stream: peer,
                    _permit: peer_permit,
                }),
                _native_permit: native_permit,
            },
        );
        sockets.pending += 1;
        Ok(descriptor)
    }

    fn pair() -> io::Result<(TcpStream, TcpStream)> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        let native =
            TcpStream::connect_timeout(&listener.local_addr()?, std::time::Duration::from_secs(2))?;
        let expected = native.local_addr()?;
        let (peer, remote) = listener.accept()?;
        if remote != expected {
            return Err(io::Error::other("private socket peer mismatch"));
        }
        native.set_nodelay(true)?;
        peer.set_nodelay(true)?;
        Ok((native, peer))
    }

    fn connect_peer(&self, fd: c_int, broker: &CStr, instance: u64) -> io::Result<()> {
        let peer = {
            let mut sockets = self
                .sockets
                .lock()
                .map_err(|_| io::Error::other("transport stopped"))?;
            let peer = sockets
                .entries
                .get_mut(&fd)
                .and_then(|socket| socket.peer.take())
                .ok_or_else(|| io::Error::other("unknown private socket"))?;
            sockets.pending -= 1;
            peer
        };
        let broker = broker
            .to_str()
            .map_err(|_| io::Error::other("invalid broker name"))?;
        (self.connect)(broker, instance, peer)
    }

    fn remove(&self, fd: c_int) -> Option<SocketState> {
        // Closing must release owned state even after a setup panic poisoned it.
        let mut sockets = self.sockets.lock().unwrap_or_else(|e| e.into_inner());
        let socket = sockets.entries.remove(&fd)?;
        if socket.peer.is_some() {
            sockets.pending -= 1;
        }
        Some(socket)
    }

    fn run<T>(&self, callback: impl FnOnce() -> io::Result<T>) -> Option<T> {
        match catch_unwind(AssertUnwindSafe(callback))
            .unwrap_or_else(|_| Err(io::Error::other("transport callback panicked")))
        {
            Ok(value) => Some(value),
            Err(error) => {
                let _ = catch_unwind(AssertUnwindSafe(|| (self.failed)(error)));
                None
            }
        }
    }
}
unsafe extern "C" fn socket<C: ClientContext>(
    _: c_int,
    _: c_int,
    _: c_int,
    opaque: *mut c_void,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let adapter = (&*(opaque as *const C)).supplied_transport().unwrap();
        adapter.run(|| adapter.socket()).unwrap_or(-1)
    }))
    .unwrap_or(-1)
}
unsafe extern "C" fn connect<C: ClientContext>(
    fd: c_int,
    id: *const c_char,
    instance: u64,
    opaque: *mut c_void,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        let adapter = (&*(opaque as *const C)).supplied_transport().unwrap();
        adapter.run(|| adapter.connect_peer(fd, CStr::from_ptr(id), instance))
    }))
    .ok()
    .flatten()
    .map_or(libc::ECONNREFUSED, |()| 0)
}
unsafe extern "C" fn close<C: ClientContext>(fd: c_int, opaque: *mut c_void) -> c_int {
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        #[cfg(unix)]
        let native = TcpStream::from_raw_fd(fd);
        #[cfg(windows)]
        let native = TcpStream::from_raw_socket(fd as u64);
        let socket = (&*(opaque as *const C))
            .supplied_transport()
            .unwrap()
            .remove(fd);
        drop(native);
        drop(socket);
    }));
    0
}
extern "C" {
    fn rdkafka_install_supplied_resolver(conf: *mut sys::rd_kafka_conf_t);
    fn rd_kafka_conf_set_runtime_connect_cb(
        conf: *mut sys::rd_kafka_conf_t,
        callback: Option<unsafe extern "C" fn(c_int, *const c_char, u64, *mut c_void) -> c_int>,
    );
}
pub(crate) unsafe fn install<C: ClientContext>(conf: *mut sys::rd_kafka_conf_t) {
    unsafe {
        sys::rd_kafka_conf_set_socket_cb(conf, Some(socket::<C>));
        rd_kafka_conf_set_runtime_connect_cb(conf, Some(connect::<C>));
        sys::rd_kafka_conf_set_closesocket_cb(conf, Some(close::<C>));
        rdkafka_install_supplied_resolver(conf);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fmt,
        io::{BufReader, Read, Write},
        mem::ManuallyDrop,
        net::Shutdown,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
            Arc,
        },
        time::Duration,
    };

    #[derive(Debug)]
    struct Refused(Arc<()>);

    impl fmt::Display for Refused {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("capacity_full")
        }
    }

    impl std::error::Error for Refused {}

    struct Budget {
        maximum: usize,
        live: AtomicUsize,
        released: [AtomicUsize; 3],
        refusal: Arc<()>,
    }

    struct Permit(Arc<Budget>, usize);

    impl Drop for Permit {
        fn drop(&mut self) {
            self.0.live.fetch_sub(1, SeqCst);
            self.0.released[self.1].fetch_add(1, SeqCst);
        }
    }

    impl Budget {
        fn new(maximum: usize) -> Arc<Self> {
            Arc::new(Self {
                maximum,
                live: AtomicUsize::new(0),
                released: std::array::from_fn(|_| AtomicUsize::new(0)),
                refusal: Arc::new(()),
            })
        }

        fn admit(self: &Arc<Self>) -> io::Result<SocketPermits> {
            self.live
                .fetch_update(SeqCst, SeqCst, |live| {
                    live.checked_add(3).filter(|n| *n <= self.maximum)
                })
                .map_err(|_| io::Error::other(Refused(self.refusal.clone())))?;
            Ok(SocketPermits::new(
                Permit(self.clone(), 0),
                Permit(self.clone(), 1),
                Permit(self.clone(), 2),
            ))
        }

        fn released(&self) -> [usize; 3] {
            std::array::from_fn(|i| self.released[i].load(SeqCst))
        }
    }

    struct Context(SuppliedTransport);

    impl ClientContext for Context {
        fn supplied_transport(&self) -> Option<&SuppliedTransport> {
            Some(&self.0)
        }
    }

    impl Context {
        fn opaque(&self) -> *mut c_void {
            (self as *const Self).cast_mut().cast()
        }

        fn open(&self) -> Option<NativeSocket<'_>> {
            let fd = unsafe { socket::<Self>(0, 0, 0, self.opaque()) };
            (fd != -1).then(|| NativeSocket { context: self, fd })
        }
    }

    // Own test descriptors through the same close callback librdkafka uses.
    struct NativeSocket<'a> {
        context: &'a Context,
        fd: c_int,
    }

    impl NativeSocket<'_> {
        fn connect(&self) -> c_int {
            let broker = std::ffi::CString::new("broker.example:9092").unwrap();
            unsafe { connect::<Context>(self.fd, broker.as_ptr(), 17, self.context.opaque()) }
        }

        fn stream(&self) -> ManuallyDrop<TcpStream> {
            // Borrow this descriptor; NativeSocket remains its sole close owner.
            #[cfg(unix)]
            let stream = unsafe { TcpStream::from_raw_fd(self.fd) };
            #[cfg(windows)]
            let stream = unsafe { TcpStream::from_raw_socket(self.fd as u64) };
            let stream = ManuallyDrop::new(stream);
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream
        }
    }

    impl Drop for NativeSocket<'_> {
        fn drop(&mut self) {
            assert_eq!(
                unsafe { close::<Context>(self.fd, self.context.opaque()) },
                0
            );
        }
    }

    fn context(
        maximum: usize,
        budget: &Arc<Budget>,
        connect: impl Fn(&str, u64, TransportPeer) -> io::Result<()> + Send + Sync + 'static,
    ) -> (Context, Arc<Mutex<Vec<io::Error>>>) {
        let budget = budget.clone();
        let errors = Arc::new(Mutex::new(Vec::new()));
        let failed = errors.clone();
        (
            Context(SuppliedTransport::new(
                maximum,
                move || budget.admit(),
                connect,
                move |error| failed.lock().unwrap().push(error),
            )),
            errors,
        )
    }

    #[test]
    fn admission_refuses_before_socket_creation_or_connect_and_preserves_cause() {
        let budget = Budget::new(2);
        let (context, errors) = context(1, &budget, |_, _, _| panic!("connect before admission"));
        let created = AtomicBool::new(false);
        assert!(context
            .0
            .run(|| context.0.socket_with(|| {
                created.store(true, SeqCst);
                SuppliedTransport::pair()
            }))
            .is_none());
        assert!(!created.load(SeqCst));
        assert!(context.open().is_none()); // Also exercise the socket FFI return.
        let errors = errors.lock().unwrap();
        assert_eq!(errors.len(), 2);
        for error in errors.iter() {
            let cause = error.get_ref().unwrap().downcast_ref::<Refused>().unwrap();
            assert!(Arc::ptr_eq(&cause.0, &budget.refusal));
        }
        assert_eq!(budget.live.load(SeqCst), 0);
        assert_eq!(budget.released(), [0, 0, 0]);
        assert!(context.0.sockets.lock().unwrap().entries.is_empty());
    }

    #[test]
    fn setup_failure_returns_all_three_permits_and_the_original_os_error() {
        let budget = Budget::new(3);
        let (context, errors) =
            context(1, &budget, |_, _, _| panic!("connect after setup failure"));
        assert!(context
            .0
            .run(|| context.0.socket_with(|| {
                assert_eq!(budget.live.load(SeqCst), 3);
                let _listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
                Err(io::Error::from_raw_os_error(libc::EMFILE))
            }))
            .is_none());
        let errors = errors.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].raw_os_error(), Some(libc::EMFILE));
        assert_eq!(budget.live.load(SeqCst), 0);
        assert_eq!(budget.released(), [1, 1, 1]);
        assert!(context.0.sockets.lock().unwrap().entries.is_empty());
    }

    #[test]
    fn pending_pairs_are_bounded_but_handed_off_native_permits_stay_owned() {
        let budget = Budget::new(9);
        let peers = Arc::new(Mutex::new(Vec::new()));
        let connected = peers.clone();
        let (context, errors) = context(1, &budget, move |broker, instance, peer| {
            assert_eq!(broker, "broker.example:9092");
            assert_eq!(instance, 17);
            connected.lock().unwrap().push(peer);
            Ok(())
        });
        let first = context.open().unwrap();
        assert!(context.open().is_none());
        assert_eq!(budget.live.load(SeqCst), 2);
        assert_eq!(first.connect(), 0);
        // The maximum bounds pending handoffs, not active brokers.
        let second = context.open().unwrap();
        assert_eq!(budget.live.load(SeqCst), 4);
        drop(second); // Close before connect also destroys the pending peer.
        assert_eq!(budget.live.load(SeqCst), 2);
        drop(first);
        assert_eq!(budget.live.load(SeqCst), 1);
        peers.lock().unwrap().clear();
        assert_eq!(budget.live.load(SeqCst), 0);
        assert_eq!(errors.lock().unwrap().len(), 1);
        let sockets = context.0.sockets.lock().unwrap();
        assert!(sockets.entries.is_empty());
        assert_eq!(sockets.pending, 0);
    }

    #[test]
    fn native_close_and_one_sided_eof_keep_the_pumps_peer_permit() {
        let budget = Budget::new(3);
        let peer = Arc::new(Mutex::new(None));
        let connected = peer.clone();
        let counts = budget.clone();
        let (context, errors) = context(1, &budget, move |_, _, peer| {
            // The listener is gone: three slots suffice, with one now available
            // for the remote route's ordinary socket opener.
            assert_eq!(counts.live.load(SeqCst), 2);
            assert_eq!(counts.released(), [1, 0, 0]);
            *connected.lock().unwrap() = Some(peer);
            Ok(())
        });
        let native = context.open().unwrap();
        assert_eq!(native.connect(), 0);
        let mut peer = peer
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .try_map(|stream| Ok(BufReader::new(stream)))
            .unwrap();
        peer.stream_mut()
            .get_ref()
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        {
            let mut stream = native.stream();
            stream.shutdown(Shutdown::Write).unwrap();
            assert_eq!(peer.stream_mut().read(&mut [0]).unwrap(), 0);
            assert_eq!(budget.live.load(SeqCst), 2);
            peer.stream_mut().get_mut().write_all(b"x").unwrap();
            let mut received = [0];
            stream.read_exact(&mut received).unwrap();
            assert_eq!(received, *b"x");
        }
        drop(native);
        assert_eq!(budget.released(), [1, 1, 0]);
        assert_eq!(budget.live.load(SeqCst), 1);
        // The still-owned peer leaves only two slots, so another pair refuses.
        assert!(context.open().is_none());
        assert_eq!(errors.lock().unwrap().len(), 1);
        drop(peer);
        assert_eq!(budget.released(), [1, 1, 1]);
        assert_eq!(budget.live.load(SeqCst), 0);
        drop(context.open().unwrap());
        assert_eq!(budget.live.load(SeqCst), 0);
    }

    #[test]
    fn connect_causes_keep_their_identity_after_a_successful_handoff() {
        let budget = Budget::new(3);
        let calls = AtomicUsize::new(0);
        let refusal = Arc::new(());
        let cause = refusal.clone();
        let (context, errors) = context(1, &budget, move |_, _, _peer| {
            if calls.fetch_add(1, SeqCst) == 0 {
                Ok(())
            } else {
                Err(io::Error::other(Refused(cause.clone())))
            }
        });
        let first = context.open().unwrap();
        assert_eq!(first.connect(), 0);
        assert_eq!(budget.live.load(SeqCst), 1); // Peer dropped first.
        assert!(errors.lock().unwrap().is_empty());
        drop(first);
        let second = context.open().unwrap();
        assert_eq!(second.connect(), libc::ECONNREFUSED);
        assert_eq!(budget.live.load(SeqCst), 1);
        let errors = errors.lock().unwrap();
        assert_eq!(errors.len(), 1);
        let error = errors[0]
            .get_ref()
            .unwrap()
            .downcast_ref::<Refused>()
            .unwrap();
        assert!(Arc::ptr_eq(&error.0, &refusal));
        drop(second);
        assert_eq!(budget.live.load(SeqCst), 0);
        assert_eq!(budget.released(), [2, 2, 2]);
    }

    #[test]
    fn failed_peer_conversion_releases_only_the_peer_and_preserves_the_cause() {
        let budget = Budget::new(3);
        let cause = budget.refusal.clone();
        let (context, errors) = context(1, &budget, move |_, _, peer| {
            peer.try_map::<()>(|stream| {
                drop(stream);
                Err(io::Error::other(Refused(cause.clone())))
            })?;
            unreachable!()
        });
        let native = context.open().unwrap();
        assert_eq!(native.connect(), libc::ECONNREFUSED);
        assert_eq!(budget.released(), [1, 0, 1]);
        let errors = errors.lock().unwrap();
        assert_eq!(errors.len(), 1);
        let error = errors[0]
            .get_ref()
            .unwrap()
            .downcast_ref::<Refused>()
            .unwrap();
        assert!(Arc::ptr_eq(&error.0, &budget.refusal));
        drop(native);
        assert_eq!(budget.live.load(SeqCst), 0);
    }

    #[test]
    fn peer_stream_is_destroyed_before_its_permit() {
        struct PeerPermit(Arc<Mutex<Vec<&'static str>>>);
        impl Drop for PeerPermit {
            fn drop(&mut self) {
                self.0.lock().unwrap().push("permit");
            }
        }
        struct Stream(Option<TcpStream>, Arc<Mutex<Vec<&'static str>>>);
        impl Drop for Stream {
            fn drop(&mut self) {
                drop(self.0.take());
                self.1.lock().unwrap().push("stream");
            }
        }
        let events = Arc::new(Mutex::new(Vec::new()));
        let permit_events = events.clone();
        let stream_events = events.clone();
        let context = Context(SuppliedTransport::new(
            1,
            move || {
                Ok(SocketPermits::new(
                    (),
                    (),
                    PeerPermit(permit_events.clone()),
                ))
            },
            move |_, _, peer| {
                let peer = peer.try_map(|s| Ok(Stream(Some(s), stream_events.clone())))?;
                drop(peer);
                Ok(())
            },
            |_| panic!("unexpected failure"),
        ));
        let native = context.open().unwrap();
        assert_eq!(native.connect(), 0);
        assert_eq!(*events.lock().unwrap(), ["stream", "permit"]);
    }

    #[test]
    fn callback_panics_are_contained_and_return_owned_permits() {
        let budget = Budget::new(3);
        let admitted = budget.clone();
        let panic_in_admit = Arc::new(AtomicBool::new(true));
        let admit_panics = panic_in_admit.clone();
        let failures = Arc::new(AtomicUsize::new(0));
        let failed = failures.clone();
        let context = Context(SuppliedTransport::new(
            1,
            move || {
                let permits = admitted.admit()?;
                assert!(!admit_panics.load(SeqCst), "admission panic");
                Ok(permits)
            },
            |_, _, _peer| panic!("connect panic"),
            move |_| {
                failed.fetch_add(1, SeqCst);
                panic!("failure handler panic");
            },
        ));
        assert!(context.open().is_none());
        assert_eq!(budget.live.load(SeqCst), 0);
        assert_eq!(budget.released(), [1, 1, 1]);
        panic_in_admit.store(false, SeqCst);
        let native = context.open().unwrap();
        assert_eq!(native.connect(), libc::ECONNREFUSED);
        assert_eq!(budget.live.load(SeqCst), 1);
        drop(native);
        assert_eq!(budget.live.load(SeqCst), 0);
        assert_eq!(budget.released(), [2, 2, 2]);
        assert_eq!(failures.load(SeqCst), 2);
    }
}
