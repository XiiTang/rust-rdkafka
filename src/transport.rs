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

/// Called on a native broker thread. The callback must bound time and own/join
/// any transport tasks it starts. `broker` is the actual logical host:port,
/// including metadata-discovered leaders and coordinators. The stream is the
/// peer of the native engine's private socket; it is never an external socket.
pub struct SuppliedTransport {
    connect: Box<dyn Fn(&str, u64, TcpStream) -> io::Result<()> + Send + Sync>,
    pending: Mutex<BTreeMap<c_int, TcpStream>>,
    maximum: usize,
}
impl SuppliedTransport {
    /// Construct a bounded adapter. Authorization and TLS are caller policy.
    pub fn new(
        maximum: usize,
        connect: impl Fn(&str, u64, TcpStream) -> io::Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            connect: Box::new(connect),
            pending: Mutex::new(BTreeMap::new()),
            maximum,
        }
    }
    fn socket(&self) -> io::Result<c_int> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| io::Error::other("transport stopped"))?;
        if pending.len() >= self.maximum {
            return Err(io::Error::other("transport admission limit"));
        }
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
        pending.insert(descriptor, peer);
        Ok(descriptor)
    }
}
unsafe extern "C" fn socket<C: ClientContext>(
    _: c_int,
    _: c_int,
    _: c_int,
    opaque: *mut c_void,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        (&*(opaque as *const C))
            .supplied_transport()
            .unwrap()
            .socket()
            .unwrap_or(-1)
    }))
    .unwrap_or(-1)
}
unsafe extern "C" fn connect<C: ClientContext>(
    fd: c_int,
    id: *const c_char,
    instance: u64,
    opaque: *mut c_void,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| -> io::Result<()> {
        unsafe {
            let adapter = (&*(opaque as *const C)).supplied_transport().unwrap();
            let peer = adapter
                .pending
                .lock()
                .map_err(|_| io::Error::other("transport stopped"))?
                .remove(&fd)
                .ok_or_else(|| io::Error::other("unknown private socket"))?;
            let broker = CStr::from_ptr(id)
                .to_str()
                .map_err(|_| io::Error::other("invalid broker name"))?;
            (adapter.connect)(broker, instance, peer)
        }
    }))
    .ok()
    .and_then(Result::ok)
    .map_or(libc::ECONNREFUSED, |()| 0)
}
unsafe extern "C" fn close<C: ClientContext>(fd: c_int, opaque: *mut c_void) -> c_int {
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        if let Ok(mut pending) = (&*(opaque as *const C))
            .supplied_transport()
            .unwrap()
            .pending
            .lock()
        {
            pending.remove(&fd);
        }
    }));
    #[cfg(unix)]
    unsafe {
        drop(TcpStream::from_raw_fd(fd));
    }
    #[cfg(windows)]
    unsafe {
        drop(TcpStream::from_raw_socket(fd as u64));
    }
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
    #[test]
    fn pending_pairs_are_bounded_and_released() {
        let adapter = SuppliedTransport::new(1, |_, _, _| Ok(()));
        let fd = adapter.socket().unwrap();
        assert!(adapter.socket().is_err());
        drop(adapter.pending.lock().unwrap().remove(&fd));
        #[cfg(unix)]
        unsafe {
            drop(TcpStream::from_raw_fd(fd));
        }
        #[cfg(windows)]
        unsafe {
            drop(TcpStream::from_raw_socket(fd as u64));
        }
        assert!(adapter.pending.lock().unwrap().is_empty());
    }
}
