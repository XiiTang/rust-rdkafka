//! Decoder-backed error fidelity. No external broker or synthetic reply setter.
use rdkafka::{
    admin::{
        AdminClient, AdminOptions, AlterConfig, NewPartitions, NewTopic, ResourceSpecifier,
        TopicReplication,
    },
    client::DefaultClientContext,
    consumer::{BaseConsumer, Consumer},
    error::{RDKafkaErrorCode, ReplyTarget},
    topic_partition_list::Offset,
    ClientConfig, TopicPartitionList,
};
use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};
const WAIT: Duration = Duration::from_secs(5);
fn string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as i16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}
fn i16v(out: &mut Vec<u8>, v: i16) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn i32v(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn i64v(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_be_bytes());
}
type Handler = dyn Fn(i16, i16, &[u8], &mut Vec<u8>) + Send + Sync;
struct Peer {
    address: String,
    stop: Arc<AtomicBool>,
    sockets: Arc<Mutex<Vec<TcpStream>>>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Peer {
    fn new(handler: impl Fn(i16, i16, &[u8], &mut Vec<u8>) + Send + Sync + 'static) -> Self {
        Self::with_brokers(None, handler)
    }
    fn with_brokers(
        brokers: Option<Arc<Mutex<Vec<(i32, u16)>>>>,
        handler: impl Fn(i16, i16, &[u8], &mut Vec<u8>) + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let address = format!("127.0.0.1:{port}");
        let brokers = brokers.unwrap_or_else(|| Arc::new(Mutex::new(vec![(1, port)])));
        let stop = Arc::new(AtomicBool::new(false));
        let sockets = Arc::new(Mutex::new(Vec::new()));
        let handler: Arc<Handler> = Arc::new(handler);
        let thread = thread::spawn({
            let stop = stop.clone();
            let sockets = sockets.clone();
            move || {
                let mut workers = Vec::new();
                for incoming in listener.incoming() {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    let mut socket = incoming.unwrap();
                    sockets.lock().unwrap().push(socket.try_clone().unwrap());
                    let handler = handler.clone();
                    let brokers = brokers.clone();
                    workers.push(thread::spawn(move || {
                        let mut size = [0; 4];
                        while socket.read_exact(&mut size).is_ok() {
                            let size = u32::from_be_bytes(size) as usize;
                            assert!(size < 1 << 20);
                            let mut request = vec![0; size];
                            socket.read_exact(&mut request).unwrap();
                            let api = i16::from_be_bytes(request[..2].try_into().unwrap());
                            let version = i16::from_be_bytes(request[2..4].try_into().unwrap());
                            let mut response = request[4..8].to_vec();
                            match api {
                                18 => {
                                    i16v(&mut response, if version == 0 { 0 } else { 35 });
                                    let apis = [
                                        (18, 0),
                                        (3, 1),
                                        (10, 0),
                                        (19, 1),
                                        (20, 0),
                                        (37, 0),
                                        (32, 0),
                                        (33, 0),
                                        (42, 0),
                                        (21, 0),
                                        (8, 2),
                                        (9, 2),
                                        (2, 1),
                                        (16, 0),
                                        (15, 0),
                                    ];
                                    i32v(
                                        &mut response,
                                        if version == 0 { apis.len() as i32 } else { 0 },
                                    );
                                    if version == 0 {
                                        for (api, max) in apis {
                                            i16v(&mut response, api);
                                            i16v(&mut response, 0);
                                            i16v(&mut response, max);
                                        }
                                    }
                                }
                                3 => {
                                    let brokers = brokers.lock().unwrap();
                                    i32v(&mut response, brokers.len() as i32);
                                    for &(id, port) in brokers.iter() {
                                        i32v(&mut response, id);
                                        string(&mut response, "127.0.0.1");
                                        i32v(&mut response, i32::from(port));
                                        if version >= 1 {
                                            i16v(&mut response, -1);
                                        }
                                    }
                                    if version >= 1 {
                                        i32v(&mut response, 1);
                                    }
                                    i32v(&mut response, 1);
                                    i16v(&mut response, 0);
                                    string(&mut response, "t");
                                    if version >= 1 {
                                        response.push(0);
                                    }
                                    i32v(&mut response, 3);
                                    for partition in 0..3 {
                                        i16v(&mut response, 0);
                                        i32v(&mut response, partition);
                                        i32v(&mut response, 1);
                                        i32v(&mut response, 1);
                                        i32v(&mut response, 1);
                                        i32v(&mut response, 1);
                                        i32v(&mut response, 1);
                                    }
                                }
                                10 => {
                                    i16v(&mut response, 0);
                                    i32v(&mut response, 1);
                                    string(&mut response, "127.0.0.1");
                                    i32v(&mut response, i32::from(port));
                                }
                                _ => handler(api, version, &request, &mut response),
                            }
                            if socket
                                .write_all(&(response.len() as i32).to_be_bytes())
                                .is_err()
                            {
                                break;
                            }
                            if socket.write_all(&response).is_err() {
                                break;
                            }
                        }
                    }));
                }
                for worker in workers {
                    worker.join().unwrap();
                }
            }
        });
        Self {
            address,
            stop,
            sockets,
            thread: Some(thread),
        }
    }
    fn config(&self) -> ClientConfig {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", &self.address)
            .set("group.id", "g")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("socket.timeout.ms", "1500");
        c
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        for socket in self.sockets.lock().unwrap().iter() {
            let _ = socket.shutdown(Shutdown::Both);
        }
        let _ = TcpStream::connect(&self.address);
        self.thread.take().unwrap().join().unwrap();
    }
}

#[tokio::test]
async fn admin_results_keep_resource_errors() {
    let peer = Peer::new(|api, _, _, out| match api {
        19 | 20 | 37 => {
            if api == 37 {
                i32v(out, 0);
            }
            i32v(out, 2);
            for (name, code) in [("two", 30000), ("one", 7)] {
                string(out, name);
                i16v(out, code);
                if api != 20 {
                    string(out, " actual\0explanation ");
                }
            }
        }
        32 | 33 => {
            i32v(out, 0);
            i32v(out, 2);
            for name in ["two", "one"] {
                i16v(out, 29);
                string(out, " actual\0explanation ");
                out.push(2);
                string(out, name);
                if api == 32 {
                    i32v(out, 0);
                }
            }
        }
        42 => {
            i32v(out, 0);
            i32v(out, 1);
            string(out, "g");
            i16v(out, 69);
        }
        21 => {
            i32v(out, 0);
            i32v(out, 1);
            string(out, "t");
            i32v(out, 1);
            i32v(out, 0);
            i64v(out, 23);
            i16v(out, 29);
        }
        _ => panic!("unexpected API {api}"),
    });
    let admin: AdminClient<DefaultClientContext> = peer.config().create().unwrap();
    let opts = AdminOptions::new()
        .request_timeout(Some(WAIT))
        .operation_timeout(Some(Duration::ZERO));
    let topics = [
        NewTopic::new("one", 1, TopicReplication::Fixed(1)),
        NewTopic::new("two", 1, TopicReplication::Fixed(1)),
    ];
    let created = admin.create_topics(&topics, &opts).await.unwrap();
    let deleted = admin.delete_topics(&["one", "two"], &opts).await.unwrap();
    let expanded = admin
        .create_partitions(
            &[NewPartitions::new("one", 2), NewPartitions::new("two", 2)],
            &opts,
        )
        .await
        .unwrap();
    for results in [&created, &deleted, &expanded] {
        for result in results {
            let error = result.as_ref().unwrap_err();
            let reply = error.error.broker_reply().unwrap();
            assert_eq!(reply.target, ReplyTarget::Topic(error.resource.clone()));
            assert_eq!(reply.peer, peer.address);
            assert_eq!(reply.code, if error.resource == "one" { 7 } else { 30000 });
            if reply.api_key == 20 {
                assert_eq!(reply.explanation, None);
            } else {
                assert_eq!(reply.explanation.as_deref(), Some(" actual\0explanation "));
            }
            if reply.api_key == 19 || reply.api_key == 37 {
                assert_eq!(
                    error.error.code().raw_code(),
                    if error.resource == "one" { 0 } else { 30000 }
                );
            }
        }
    }
    let described = admin
        .describe_configs(
            &[
                ResourceSpecifier::Topic("one"),
                ResourceSpecifier::Topic("two"),
            ],
            &opts,
        )
        .await
        .unwrap();
    let altered = admin
        .alter_configs(
            &[
                AlterConfig::new(ResourceSpecifier::Topic("one")),
                AlterConfig::new(ResourceSpecifier::Topic("two")),
            ],
            &opts,
        )
        .await
        .unwrap();
    for result in &described {
        let error = result.as_ref().unwrap_err();
        assert!(error.resource.entries.is_empty());
        assert_eq!(error.error.broker_reply().unwrap().code, 29);
    }
    for result in &altered {
        assert_eq!(result.as_ref().unwrap_err().error.code().raw_code(), 29);
    }
    let group = admin
        .delete_groups(&["g"], &opts)
        .await
        .unwrap()
        .pop()
        .unwrap()
        .unwrap_err();
    assert_eq!(group.resource, "g");
    assert_eq!(
        group.error.broker_reply().unwrap().target,
        ReplyTarget::Group("g".into())
    );
    let mut parts = TopicPartitionList::new();
    parts
        .add_partition_offset("t", 0, Offset::Offset(1))
        .unwrap();
    let records = admin.delete_records(&parts, &opts).await.unwrap();
    let copy = records.clone();
    drop(records);
    drop(admin);
    assert_eq!(copy.elements()[0].broker_reply().unwrap().code, 29);
    // All owned reply strings remain valid after NativeEvent/result destruction.
    assert_eq!(
        created[1].as_ref().unwrap_err().error.code(),
        RDKafkaErrorCode::UnknownCode(30000)
    );
    assert_eq!(
        created[1].as_ref().unwrap_err().error.code().to_string(),
        unsafe {
            std::ffi::CStr::from_ptr(rdkafka::bindings::rd_kafka_err2str(
                rdkafka::types::RDKafkaRespErr(30000),
            ))
            .to_string_lossy()
        }
    );
}

#[test]
fn kafka_partial_offsets_survive_native_error() {
    let local: BaseConsumer = ClientConfig::new().create().unwrap();
    let mut unanswered = TopicPartitionList::new();
    unanswered
        .add_partition_offset("t", 0, Offset::Offset(999999))
        .unwrap();
    let absent = local.committed_offsets_with_outcome(unanswered, WAIT);
    assert!(absent.error.is_some());
    assert_eq!(absent.partitions.elements()[0].offset(), Offset::Invalid);
    drop(local);

    let peer = Peer::new(|api, _, _, out| match api {
        9 => {
            i32v(out, 1);
            string(out, "t");
            i32v(out, 2);
            for (part, offset, code) in [(0, 41, 0), (1, -1001, 29)] {
                i32v(out, part);
                i64v(out, offset);
                i16v(out, -1);
                i16v(out, code);
            }
            i16v(out, 30); // terminal overall group error after decoded entries
        }
        2 => {
            i32v(out, 1);
            string(out, "t");
            i32v(out, 2);
            for (part, offset, code) in [(0, 41, 0), (1, -1001, 29)] {
                i32v(out, part);
                i16v(out, code);
                i64v(out, 123);
                i64v(out, offset);
            }
        }
        8 => {
            i32v(out, 1);
            string(out, "t");
            i32v(out, 2);
            for part in 0..2 {
                i32v(out, part);
                i16v(out, 29);
            }
        }
        _ => panic!("unexpected API {api}"),
    });
    let consumer: BaseConsumer = peer.config().create().unwrap();
    consumer.fetch_metadata(None, WAIT).unwrap();
    let mut input = TopicPartitionList::new();
    for partition in 0..3 {
        input
            .add_partition_offset("t", partition, Offset::Offset(999999))
            .unwrap();
    }
    let committed = consumer.committed_offsets_with_outcome(input.clone(), WAIT);
    assert!(committed.error.is_some());
    assert_eq!(
        committed.partitions.elements()[0].offset(),
        Offset::Offset(41)
    );
    assert_eq!(
        committed.partitions.elements()[1]
            .broker_reply()
            .unwrap()
            .code,
        29
    );
    let offsets = consumer.offsets_for_times_with_outcome(input.clone(), WAIT);
    assert!(offsets.error.is_some());
    assert_eq!(
        offsets.partitions.elements()[0].offset(),
        Offset::Offset(41)
    );
    assert_eq!(
        offsets.partitions.elements()[1]
            .broker_reply()
            .unwrap()
            .code,
        29
    );
    assert_eq!(committed.partitions.elements()[2].offset(), Offset::Invalid);
    assert_eq!(offsets.partitions.elements()[2].offset(), Offset::Invalid);
    let mut committing = TopicPartitionList::new();
    for partition in 0..2 {
        committing
            .add_partition_offset("t", partition, Offset::Offset(1))
            .unwrap();
    }
    let commit = consumer.commit_within_outcome(&committing, WAIT).unwrap();
    assert!(commit.error.is_some());
    assert_eq!(commit.partitions.count(), 2);
    assert!(commit.partitions.elements()[..2]
        .iter()
        .all(|p| p.broker_reply().unwrap().code == 29));
}

#[test]
fn kafka_fetch_error_keeps_its_reply() {
    use rdkafka::{
        mocking::MockCluster,
        types::{RDKafkaApiKey, RDKafkaRespErr},
    };
    for (split, version, code) in [(false, 4, 29), (true, 4, 29), (false, 12, 31)] {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("t", 1, 1).unwrap();
        cluster
            .apiversion(RDKafkaApiKey::Fetch, Some(version), Some(version))
            .unwrap();
        let consumer: Arc<BaseConsumer> = Arc::new(
            ClientConfig::new()
                .set("bootstrap.servers", cluster.bootstrap_servers())
                .set("group.id", "fetch-reply")
                .set("enable.auto.commit", "false")
                .create()
                .unwrap(),
        );
        consumer.fetch_metadata(None, WAIT).unwrap();
        cluster.broker_down(1).unwrap();
        let mut parts = TopicPartitionList::new();
        parts
            .add_partition_offset("t", 0, Offset::Offset(0))
            .unwrap();
        consumer.assign(&parts).unwrap();
        let queue = split.then(|| consumer.split_partition_queue("t", 0).unwrap());
        cluster.request_errors(RDKafkaApiKey::Fetch, &[RDKafkaRespErr(code)]);
        cluster.broker_up(1).unwrap();
        let deadline = std::time::Instant::now() + WAIT;
        let error = loop {
            assert!(
                std::time::Instant::now() < deadline,
                "Fetch error did not arrive"
            );
            let event = match &queue {
                Some(queue) => {
                    queue.poll(deadline.saturating_duration_since(std::time::Instant::now()))
                }
                None => {
                    consumer.poll(deadline.saturating_duration_since(std::time::Instant::now()))
                }
            };
            if let Some(Err(error)) = event {
                if error.broker_reply().is_some() {
                    break error;
                }
            }
        };
        drop(queue);
        drop(consumer);
        let reply = error.broker_reply().unwrap();
        assert_eq!(reply.code, code);
        assert_eq!(reply.api_key, 1);
        assert_eq!(
            reply.target,
            if version >= 7 {
                ReplyTarget::Request
            } else {
                ReplyTarget::Partition {
                    topic: "t".into(),
                    partition: 0,
                }
            }
        );
        assert!(cluster.bootstrap_servers().contains(&reply.peer));
    }
}

#[test]
fn kafka_group_list_exports_completed_groups_only() {
    for member_tail in [false, true] {
        let peer = Peer::new(move |api, _, _, out| match api {
            16 => {
                i16v(out, 0);
                i32v(out, 1);
                string(out, "complete");
                string(out, "consumer");
            }
            15 => {
                i32v(out, 2);
                let header = |out: &mut Vec<u8>, name, count| {
                    i16v(out, if name == "complete" { 29 } else { 0 });
                    string(out, name);
                    string(out, "Stable");
                    string(out, "consumer");
                    string(out, "range");
                    i32v(out, count);
                };
                let member = |out: &mut Vec<u8>| {
                    string(out, "m");
                    string(out, "c");
                    string(out, "h");
                    i32v(out, 0);
                    i32v(out, 0);
                };
                header(out, "complete", 1);
                member(out);
                header(out, "unfinished", if member_tail { 2 } else { 100001 });
                if member_tail {
                    member(out);
                    string(out, "truncated");
                }
            }
            _ => panic!("unexpected API {api}"),
        });
        let consumer: BaseConsumer = peer.config().create().unwrap();
        consumer.fetch_metadata(None, WAIT).unwrap();
        let outcome = consumer
            .client()
            .fetch_group_list_with_outcome(None, WAIT)
            .unwrap();
        assert!(outcome.error.is_some());
        assert_eq!(outcome.broker_errors.len(), 1);
        let groups = outcome.groups.as_ref().unwrap().groups();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].name(), "complete");
        assert_eq!(groups[0].members().len(), 1);
        assert_eq!(groups[0].members()[0].client_id(), "c");
        assert_eq!(
            groups[0].broker().port,
            peer.address
                .rsplit(':')
                .next()
                .unwrap()
                .parse::<i32>()
                .unwrap()
        );
        let error = groups[0].error().unwrap();
        drop(outcome);
        drop(consumer);
        assert_eq!(
            error.broker_reply().unwrap().target,
            ReplyTarget::Group("complete".into())
        );
        assert_eq!(error.broker_reply().unwrap().code, 29);
    }
}

#[test]
fn reply_facts_outlive_transaction_and_fatal_owners() {
    use rdkafka::{
        mocking::MockCluster,
        producer::{BaseProducer, Producer},
        types::{RDKafkaApiKey, RDKafkaRespErr},
    };
    let cluster = MockCluster::new(1).unwrap();
    cluster.request_errors(RDKafkaApiKey::InitProducerId, &[RDKafkaRespErr(53)]);
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .set("transactional.id", "reply-fatal")
        .create()
        .unwrap();
    let transaction = producer.init_transactions(WAIT).unwrap_err();
    let fatal = producer.client().fatal_error_details().unwrap();
    let copy = fatal.clone();
    unsafe {
        rdkafka::bindings::rd_kafka_test_fatal_error(
            producer.client().native_ptr(),
            RDKafkaRespErr(100),
            c"later native failure".as_ptr(),
        );
    }
    assert_eq!(
        producer
            .client()
            .fatal_error_details()
            .unwrap()
            .broker_reply(),
        copy.broker_reply()
    );
    drop(fatal);
    drop(producer);
    assert!(copy.is_fatal());
    assert_eq!(copy.code().raw_code(), 53);
    assert_eq!(copy.broker_reply().unwrap().api_key, 22);
    assert_eq!(transaction.broker_reply(), copy.broker_reply());
    assert_eq!(copy.broker_reply().unwrap().code, 53);
}

#[test]
fn kafka_group_list_keeps_each_broker_error() {
    let brokers = Arc::new(Mutex::new(Vec::new()));
    let first = Peer::with_brokers(Some(brokers.clone()), |api, _, _, out| {
        assert_eq!(api, 16);
        i16v(out, 31);
        i32v(out, 0);
    });
    let second = Peer::with_brokers(Some(brokers.clone()), |api, _, _, out| {
        assert_eq!(api, 16);
        i16v(out, 30);
        i32v(out, 0);
    });
    *brokers.lock().unwrap() = vec![
        (
            1,
            first.address.rsplit(':').next().unwrap().parse().unwrap(),
        ),
        (
            2,
            second.address.rsplit(':').next().unwrap().parse().unwrap(),
        ),
    ];
    let consumer: BaseConsumer = first.config().create().unwrap();
    consumer.fetch_metadata(None, WAIT).unwrap();
    let outcome = consumer
        .client()
        .fetch_group_list_with_outcome(None, WAIT)
        .unwrap();
    assert!(outcome.groups.as_ref().unwrap().groups().is_empty());
    assert_eq!(outcome.broker_errors.len(), 2);
    drop(consumer);
    for (id, code, peer) in [(1, 31, &first.address), (2, 30, &second.address)] {
        let error = outcome
            .broker_errors
            .iter()
            .find(|error| error.broker_id == id)
            .unwrap();
        assert_eq!(&error.peer, peer);
        assert_eq!(error.api_key, 16);
        assert_eq!(error.error.broker_reply().unwrap().code, code);
        assert_eq!(error.error.broker_reply().unwrap().broker_id, id);
    }
}
