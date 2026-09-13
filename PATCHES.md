# Controlled Runtime client

Base rust-rdkafka 0.39.0: `598ac4ba1f714852bdf4e5685fe10cf5a66e947c`.
Native librdkafka fork: `80e4daa4db47383f20a701046c0acdb713070a4a` (2.12.1).

`SuppliedTransport` provides bounded private TCP byte bridges and logical broker connect callbacks. It performs no Kafka parsing. Its resolver never resolves an external target; the embedding Runtime owns DNS, proxies, TLS, frozen credentials, target authorization and cancellation. Stable native broker-instance IDs distinguish a new broker from reconnection. Native codecs, compression, routing, producer/consumer groups, transactions and administration are unchanged except the documented native bounds.

The owner explicitly joins/destroys native clients. Supplied consumers omit implicit group leave/commit on Drop. Admin maintenance polls authentication/error events separately from operation queues. Delivery facts expose native persistence status. Configuration and OAuth FFI temporaries are erased, and rejected configuration values are redacted.

macOS arm64 evidence in IMAPipe: independent Kafka 4.3.1 five-codec, binary-header, manual-offset, two group-protocol, idempotence, commit/abort and transaction-offset tests; PLAIN, both SCRAM mechanisms and frozen OAuth; HTTP CONNECT with proxy-side DNS and rejected advertised broker target. Native gzip and Java Snappy expansion-bound tests pass. Library Clippy passes with no warnings. Other operating systems and arbitrary broker releases have not been asserted verified.

This fork does not add automatic credential refresh, arbitrary raw requests, a second Kafka engine or business exactly-once guarantees.
