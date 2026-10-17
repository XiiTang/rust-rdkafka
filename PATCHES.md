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
Test: `mocking::tests::commit_within_ends_at_its_timeout_without_a_coordinator`.
Boundless asserts the commit and the close after it against a fake broker.
