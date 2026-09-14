//! Caller-owned SASL, invoked synchronously on a native broker thread. Protocol
//! framing, peer success, connection lifetime and retries remain in librdkafka.
use crate::{bindings as sys, client::ClientContext};
use std::{
    ffi::{CStr, c_char, c_int, c_void},
    panic::{AssertUnwindSafe, catch_unwind},
};
use zeroize::Zeroizing;

/// Private response token and local completion state for one SASL step.
pub struct Step {
    /// Token to copy into native SASL framing, wiped after use.
    pub response: Zeroizing<Vec<u8>>,
    /// Whether local authentication is complete; the peer still must accept.
    pub complete: bool,
}
/// Callback failure without credential-bearing diagnostic strings.
#[derive(Debug)]
pub struct Error;
/// A connection-private SASL state machine, owned until transport close.
pub trait Exchange: Send {
    /// None starts the exchange; Some supplies the exact peer challenge.
    fn step(&mut self, challenge: Option<&[u8]>) -> Result<Step, Error>;
}
/// Each factory call receives the actual broker authority and must authorize it.
/// Bound every call by cancellation/deadline; no default credentials are read.
pub struct Provider(pub Box<dyn Fn(&str) -> Result<Box<dyn Exchange>, Error> + Send + Sync>);
struct State(Box<dyn Exchange>);
type Callback = unsafe extern "C" fn(
    c_int,
    *const c_char,
    *const u8,
    usize,
    *mut *mut c_void,
    *mut u8,
    usize,
    *mut usize,
    *mut c_int,
    *mut c_void,
) -> c_int;
unsafe extern "C" {
    fn rd_kafka_conf_set_runtime_sasl_cb(
        conf: *mut sys::rd_kafka_conf_t,
        callback: Option<Callback>,
    );
}
unsafe extern "C" fn callback<C: ClientContext>(
    action: c_int,
    broker: *const c_char,
    input: *const u8,
    input_len: usize,
    state: *mut *mut c_void,
    output: *mut u8,
    capacity: usize,
    output_len: *mut usize,
    complete: *mut c_int,
    opaque: *mut c_void,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| -> Result<(), ()> {
        // SAFETY: the native provider owns this opaque slot and serializes calls
        // for one connection; close clears it exactly once, including failures.
        unsafe {
            if action == 2 {
                if !(*state).is_null() {
                    drop(Box::from_raw((*state).cast::<State>()));
                    *state = std::ptr::null_mut();
                }
                return Ok(());
            }
            if action == 0 {
                if !(*state).is_null() {
                    return Err(());
                }
                let provider = (&*opaque.cast::<C>()).supplied_sasl().ok_or(())?;
                let broker = CStr::from_ptr(broker).to_str().map_err(|_| ())?;
                *state =
                    Box::into_raw(Box::new(State((provider.0)(broker).map_err(|_| ())?))).cast();
            } else if action != 1 {
                return Err(());
            }
            if (*state).is_null() || input_len > 65536 {
                return Err(());
            }
            let challenge = if action == 0 {
                None
            } else if input_len == 0 {
                Some(&[][..])
            } else {
                Some(std::slice::from_raw_parts(input, input_len))
            };
            let step = (&mut *(*state).cast::<State>())
                .0
                .step(challenge)
                .map_err(|_| ())?;
            if step.response.len() > capacity {
                return Err(());
            }
            std::ptr::copy_nonoverlapping(step.response.as_ptr(), output, step.response.len());
            *output_len = step.response.len();
            *complete = c_int::from(step.complete);
            Ok(())
        }
    }))
    .ok()
    .and_then(Result::ok)
    .map_or(-1, |()| 0)
}
pub(crate) unsafe fn install<C: ClientContext>(conf: *mut sys::rd_kafka_conf_t) {
    unsafe {
        rd_kafka_conf_set_runtime_sasl_cb(conf, Some(callback::<C>));
    }
}
