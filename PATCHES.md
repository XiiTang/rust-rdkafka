# Controlled Runtime client

Base rust-rdkafka 0.39.0: `598ac4ba1f714852bdf4e5685fe10cf5a66e947c`.
Native librdkafka fork: `8eb2370511047cae79b8bb075cd095cfda76c2c5` (2.12.1).

`SuppliedTransport` provides bounded private TCP byte bridges and logical broker connect callbacks. It performs no Kafka parsing. Its resolver never resolves an external target; the embedding Runtime owns DNS, proxies, TLS, frozen credentials, target authorization and cancellation. Stable native broker-instance IDs distinguish a new broker from reconnection. Native codecs, compression, routing, producer/consumer groups, transactions and administration are unchanged except the documented native bounds.

The owner explicitly joins/destroys native clients. Supplied consumers omit implicit group leave/commit on Drop. Admin maintenance polls authentication/error events separately from operation queues. Delivery facts expose native persistence status. Configuration and OAuth FFI temporaries are erased, and rejected configuration values are redacted.

macOS arm64 evidence in IMAPipe: independent Kafka 4.3.1 five-codec, binary-header, manual-offset, two group-protocol, idempotence, commit/abort and transaction-offset tests; PLAIN, both SCRAM mechanisms and frozen OAuth; HTTP CONNECT with proxy-side DNS and rejected advertised broker target. Native gzip and Java Snappy expansion-bound tests pass. Library Clippy passes with no warnings. Other operating systems and arbitrary broker releases have not been asserted verified.

This fork does not add automatic credential refresh, arbitrary raw requests, a second Kafka engine or business exactly-once guarantees.

`ClientContext::authorize_sasl` admits every authentication start at the native
provider boundary, including same-socket reauthentication. A refusal or panic
crosses the FFI boundary only as a fixed failure code.
The library suite with cmake-build,tokio,ssl-vendored,libz-static,zstd passes 20
tests on macOS arm64. Boundless retains the frozen-expiry and owned-peer tests.

## Caller-owned SASL for every mechanism

A SASL client requires `ClientContext::supplied_sasl`: the companion librdkafka
fork selects only the caller-owned exchange, for whichever single mechanism
`sasl.mechanism` names (PLAIN, SCRAM, OAUTHBEARER, GSSAPI or any other the
caller implements). The native mechanism engines are unreachable, so the OAuth
token-refresh API (`ENABLE_REFRESH_OAUTH_TOKEN`, `generate_oauth_token`,
`OAuthToken` and the refresh event) is removed. `FutureProducerContext`
forwards `authorize_sasl` and `supplied_sasl` to the wrapped context.
Boundless runs PLAIN, both SCRAM mechanisms, OAUTHBEARER and an external
exchange against Kafka 4.3.1 through this path.

## Bounded commit

`Consumer::commit_within` commits through `rd_kafka_commit_queue` on a queue
of its own and waits at most its timeout for the answer, failing with
`ConsumerCommit(OperationTimedOut)` when the timeout passes first; dropping the
queue discards a later answer. A synchronous `commit` waits with
`RD_POLL_INFINITE`: librdkafka defers a commit while the group has no
coordinator until `session.timeout.ms`, and retries one whose coordinator
connection cannot be opened again for as long as that lasts (Boundless #66).
A commit whose request was transmitted is not sent again: it ends with that
request's error (librdkafka `0bc88f8e`).
Test: `mocking::tests::commit_within_ends_at_its_timeout_without_a_coordinator`.
Boundless asserts the commit and the close after it against a fake broker.

## Supplied-transport socket admission and per-half ownership (W17b)

`SuppliedTransport::new(maximum, admit, connect, failed)`. `admit` returns
`SocketPermits::new(listener, native, peer)`: three opaque guards the caller
reserves together before the pairing listener binds. The listener's guard is
dropped when the socket callback returns; the native half's when librdkafka's
close callback closes its descriptor; the peer's travels in
`TransportPeer<S>` into `connect` and the caller's pump (`try_map` converts the
stream and carries the guard; `stream_mut` borrows it) and is dropped after the
stream is destroyed, independently of the native half and of one direction's
EOF. Setup that fails drops every unused guard. `failed` receives the original
`io::Error` of every socket and connect failure, typed caller causes included,
before the FFI failure return, after bootstrap too. `maximum` still bounds
pairs awaiting handoff.

Before (`207cde1`, unchanged at `aab4bc93`, src/transport.rs:37-66, :69-82,
:84-107, :109-129) the pair existed before any caller admission, a socket
error became `-1` with no cause, every connect error became `ECONNREFUSED`,
and nothing outlived the handoff to account the peer still copying after the
native descriptor closed.

Regression: `transport::tests` (8 tests) cover refusal before socket creation
with the typed cause kept, rollback of all three guards with the original OS
error, the listener's guard returned at socket return so three slots suffice,
the peer's guard kept through native close and one-sided EOF until the pump
drops it, a connect cause after a successful handoff, a failed `try_map`
conversion releasing only the peer's guard with its typed cause kept, the
pending bound, stream-before-guard destruction, and panics in `admit`,
`connect` and `failed` contained with every owned guard returned. They do not
compile against the old two-argument constructor; on this branch
`cargo test --lib --no-default-features --features
cmake-build,tokio,ssl-vendored,libz-static,zstd transport::tests` passes all
eight on macOS arm64 (exit 0). Boundless's callers and error-event delivery
change with its W17b item.

W17b port validation on macOS arm64, consumer Rust 1.97.0: all eight
transport unit tests pass (exit 0), with cmake-build,tokio,ssl-vendored,
libz-static,zstd. Strict library/tests Clippy exits 101 in unchanged code:
two items-after-test-module diagnostics in client.rs/config.rs and seven
large-error closure diagnostics in tests/test_low_producers.rs. The port
changes neither file; no diagnostic names transport.rs and no lint is
suppressed. The native librdkafka gitlink remains 0bc88f8e.

## W17d: delivery-event keeps and unread opaque reclamation

Upstream consumes each opaque in its delivery callback while the native event
still owns the record's copied payload/key/headers. `BorrowedMessage::keep_with_event`
shares an existing Rust owner with that exact event; native event destruction
precedes keep release, including callback unwinding. Consumer/admin event ownership
uses the same backing without changing their behavior. There is no admission,
extra data copy, callback signature or public event handle.

Upstream BaseProducer Drop purges, flushes for 500 ms and warns on timeout; native
unread event destruction never calls IntoOpaque::from_ptr. The outstanding
conversion multiset returns a conversion on delivery/refusal or after client and
queue destruction. It includes null/equal encodings and opaque types without drop
glue. No callback/result is synthesized. Existing purge, flush, callback order,
timeouts, transport and threaded/future producer behavior stay unchanged.

Regression owner: `producer::base_producer::ownership_tests`, including actual
retained event Arcs for success/error and callback unwind, forced unread reports,
refused send and conversion multiplicity. The `native-test-adapter` feature exposes
only a fixture hook after callbacks while the actual report event remains live;
release users do not enable it. The cfg(test) probes mark completion after the
actual C client/queue/event destructors; unread keeps assert both native owners
are gone before reclamation. Native sends exercise duplicate and null encodings,
not a separate fake conversion owner. A refused native topic factory now returns
its typed last error through the existing KafkaResult instead of unwrapping NULL.

macOS arm64 focused run: `cargo test -p rdkafka --lib --no-default-features
--features cmake-build,tokio,ssl-vendored,libz-static,zstd -- ownership_tests
refused_topic_creation_is_a_typed_error` passed all six named tests. The native
gitlink is a53ce7a0e58af252db00399832c36d9640118eb3, merging the current native
`boundless` clarification ed45278c without changing the reviewed W17d native
source. This preserves the transmitted-commit refusal rationale alongside the
W17d entry. Strict Clippy and the consumer build are recorded separately; no
full integration/platform lane is claimed.

## W5-D6: open native statuses and owned selected reply facts

Upstream `598ac4ba` rustifies `rd_kafka_resp_err_t` and maps a finite list of
named codes. Native librdkafka passes through arbitrary signed wire i16 errors;
an unnamed value such as 30000 is therefore already an invalid Rust enum at the
FFI boundary. The canonical bindgen command now generates only this enum as a
transparent integer newtype. Other enums keep their existing representation.
`RDKafkaErrorCode` retains every named variant and adds `UnknownCode(i32)`;
`raw_code()` preserves the integer, Display uses native err2str, and `name()`
exposes the existing native err2name accessor to safe consumers. Unknown's
existing -1 identity is unchanged. No native behavior or retry policy changes.

The native projection is copied into owned `BrokerReply`/`ReplyTarget` facts
before its owner dies. `KafkaError::BrokerReply` preserves the original error,
and complex errors expose their optional reply. Both borrowed-message and
consumer event error routes use the native getter. Native fallback diagnostics,
flags and status remain independent of wire code/explanation. Peer text uses
its decoded byte length, including embedded NUL, without trimming or fallback.

Admin errors retain their resource keys and complex native errors; config errors
are checked before presenting entries as success. Inherent richer partition,
commit and group APIs retain completed results and the independent overall error.
The old Consumer trait signatures share the same implementation. Group broker
errors are a per-call collection, and fatal details copy the existing first-fatal
owner. No client-wide last-error cache, retry, alternate sender or completion
event is added. The native gitlink includes the corresponding W5-D6 custody
patch, preserving W17b transport, W17d populations/opaque ownership and the prior
transmitted-commit policy.

Regression: `tests/test_failure_rows.rs` uses real scripted Kafka frames and the
native mock broker. It covers unnamed status 30000 before conversion, reordered
keyed admin results, native-normalized wire 7, exact and absent explanations,
failed configs, DeleteRecords fanout custody, partial/unanswered offsets, both
Fetch error routes and request scope, complete partial groups, multiple broker
errors, and transaction/first-fatal facts after native owner destruction. Native
`0000`/`RD_UT_TEST=failure_rows` additionally exercises callback order and earlier
EndTxn source selection. Results are recorded with the consumer implementation;
these focused checks do not claim a complete integration/platform lane.

Focused macOS arm64 validation with Rust 1.97.0: the six `test_failure_rows`
tests pass, as do the five existing delivery-event/opaque ownership tests,
fallible topic creation and the updated native Display test. Native `0000`
with `RD_UT_TEST=failure_rows` passes at `3ba92052`. The test-only message pointer
boxes the existing destruction-probe backing to keep that instrumentation out
of the enlarged delivery error's inline storage; production message storage
and native-first destruction are unchanged. Strict library/tests Clippy still
reports the pre-existing client/config items-after-test-module warnings and
seven large-error closures in `test_low_producers`; no lint is suppressed.


### W5-D6 review correction: one conversion per native error event

Upstream master `4566e560` still forwards RD_KAFKA_EVENT_ERROR twice for a
BaseProducer: Client::poll_event calls the common handle_error_event and returns
the event, then BaseProducer::poll forwards it through a second handler. The
second handler also loses the selected reply. The producer now drops the already
forwarded error event rather than invoking the callback again; the common
conversion remains the only producer callback entry. Delivery reporting and
consumer callback/poll APIs are unchanged. No deduplication cache or new owner is
introduced.

The consumer adapter selects returned main/split poll errors as its publication
path and suppresses its own publication from the public ClientContext callback.
Its existing installed-consumer slot distinguishes that path from producer/admin
callback-only publication. Both returned routes use the same conversion, including
canonical fatal handling and exact reply custody.

Regression entry points are the consumer's existing real authentication refusal
and Fetch fixtures. They join producers before counting publications; Fetch
covers main and split queues while reserving all but one observation slot, so
redundant forwarding cannot hide as a first-match receive or false overflow.
The existing native Display test also exercises UnknownCode(i32::MAX/MIN) through
both native name and Display, with the native range-arithmetic repair in the
submodule. Focused and sanitizer results are recorded after those checks.


Review-correction validation on macOS arm64/Rust 1.97.0: all six existing
`test_failure_rows` tests and all six selected native-event ownership/topic
creation tests pass. The existing sys Display test passes both i32 extremes
through native name and Display under UBSan, with recovery disabled and the
Apple Clang runtime explicitly linked. Native source is `44fcd0e7` (its final
change only records validation). Strict library Clippy passes for rdkafka/sys;
strict library/tests Clippy reports exactly the same nine baseline diagnostics
(two client/config module-order warnings and seven large-error producer-test
closures), with no suppression or unrelated repair. Consumer publication and
Produce v9 effect results are recorded with the Boundless review correction.

## W3-D6: retained manual commit completion

Upstream rust-rdkafka `598ac4ba` exposes synchronous/asynchronous commit but no
owned per-call completion queue. The current Boundless convenience method
`commit_within_outcome` submits with the existing public C
`rd_kafka_commit_queue`, then destroys its private queue after an empty bounded
poll. librdkafka may still hold that same deferred commit, so the eventual
completion is lost. The native library already provides the required queue
ownership; no native behavior, retry policy or gitlink changes are needed.

`BaseConsumer::start_commit` on Arc admits once and returns `PendingCommit`
keeping its private queue and consumer. Empty polls retain that owner; an actual
event is extracted once using the same owned W5-D6 extraction as the legacy
bounded method. The queue is destroyed before its pinned consumer. Drop releases
observation and does not cancel the native request. Legacy bounded methods keep
their explicit discarding behavior. Native codes and selected broker replies
retain their existing extraction and ownership.

Regression coverage: `consumer::base_consumer::pending_commit_tests` drives a
real mock-broker answer already queued before the first poll, no-group native
admission refusal, an external consumer Arc dropped while pending, and the
existing native destruction probes for queue-before-client release.
`tests/test_failure_rows.rs::pending_commit_keeps_empty_polls_and_its_actual_keyed_answer`
uses the existing scripted broker and production decoder: repeated empty polls
retain the queue, reversed keys preserve unnamed and named statuses plus the
independent overall result, source event destruction precedes inspection, and
exhausted polling never submits again. A mixed wire result retains both original
keys and native propagated status 30000, independently of each selected peer projection. No synthetic event setter,
new native API, callback mailbox, retry or client-wide error cache is added.

Focused macOS arm64 validation with the consumer's Rust 1.97.0 and features
`cmake-build,tokio,ssl-vendored,libz-static,zstd`: the two new ownership/admission
unit tests and all seven `test_failure_rows` tests pass (exit 0). The initial
expectations for success provenance and propagated native overall status were
corrected to the existing W5-D6 decoder contract. Strict library/tests Clippy
exits 101 at the unchanged input-pin baseline: two items-after-test-module
errors in client.rs/config.rs and seven large-error closure errors in
tests/test_low_producers.rs. None names this patch, and no lint is suppressed.
Final consumer validation is recorded with the implementation; no full platform
or landing verdict is claimed here.
