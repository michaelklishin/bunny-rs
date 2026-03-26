# bunny-rs, a Modern Rust Client for RabbitMQ

bunny-rs is a RabbitMQ client for Rust built on Tokio and async/await.
Its API follows [Bunny](https://github.com/ruby-amqp/bunny) (Ruby) and borrows
from the .NET, Java, and [Bunny Swift](https://github.com/michaelklishin/bunny-swift) clients.

Key features:

 * Automatic [connection recovery](https://www.rabbitmq.com/client-libraries/java-api-guide#recovery) with topology replay
 * Streaming [publisher confirms](https://www.rabbitmq.com/docs/confirms) with response tracking for [data safety](https://www.rabbitmq.com/docs/publishers#data-safety)
 * Type-safe builders for [quorum queues](https://www.rabbitmq.com/docs/quorum-queues), [streams](https://www.rabbitmq.com/docs/streams), delayed queues, JMS queues ([Tanzu RabbitMQ](https://docs.vmware.com/en/VMware-RabbitMQ/index.html)), dead-lettering, TTL, and more
 * TLS 1.2 and 1.3 support with [`rustls`](https://github.com/rustls/rustls)
 * Forward-compatible with RabbitMQ 4.3+ ([deprecated features](https://www.rabbitmq.com/docs/deprecated-features) are not exposed)


## Supported Rust Versions

Rust 1.94 or later.


## Supported RabbitMQ Versions

This client targets all [supported RabbitMQ release series](https://www.rabbitmq.com/release-information)
but all key operations should work with older series, including RabbitMQ `3.13.x`.


## Project Maturity

This is a young project by a long time member of the RabbitMQ Core Team.
Breaking API changes are possible.


## Installation

```toml
[dependencies]
bunny-rs = "0.10"
```


## Quick Start

```rust
use bunny_rs::Connection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::from_uri("amqp://localhost").await?;
    let mut ch = conn.open_channel().await?;

    ch.quorum_queue("hello").await?;
    ch.publish("", "hello", b"Hello, World!").await?;

    conn.close().await?;
    Ok(())
}
```


## Usage

### Publishing

Routes to the queue via the [default exchange](https://www.rabbitmq.com/tutorials/amqp-concepts#exchange-default)
(`""`), using queue name as the routing key:

```rust
ch.publish("", "queue-name", b"Hello!").await?;
```

Via a named exchange with a routing key:

```rust
ch.publish("amq.topic", "orders.eu.new", b"{\"id\": 1}").await?;
```

With message properties:

```rust
use bunny_rs::{BasicProperties, PublishOptions};

let opts = PublishOptions {
    properties: BasicProperties::default()
        .content_type("application/json")
        .persistent(),
    ..Default::default()
};
ch.basic_publish("", "queue-name", &opts, b"{\"order\": 123}").await?;
```

Mandatory publish (get unroutable messages back via `recv_return`):

```rust
ch.basic_publish("amq.direct", "no-such-queue", &PublishOptions::mandatory(), b"hello").await?;

if let Some(returned) = ch.recv_return().await {
    println!("returned: {} {}", returned.reply_code, returned.reply_text);
}
```

### Publisher Confirms (Data Safety)

Without confirms, messages can be silently lost if the broker fails between
accepting and persisting. See [Data Safety](https://www.rabbitmq.com/docs/publishers#data-safety).

**Streaming confirms** (recommended): publish a batch, then wait once.

```rust
ch.confirm_select().await?;

for i in 0..1000 {
    ch.publish("", "orders", format!("order-{i}").as_bytes()).await?;
}
ch.wait_for_confirms().await?;
```

**Per-message confirms**:

```rust
ch.confirm_select().await?;

let confirm = ch.publish("", "orders", b"important").await?;
if let Some(c) = confirm {
    c.wait().await?;
}
```

**Typed `ConfirmChannel`** (every publish returns a confirm, never `None`):

```rust
let mut confirm_ch = ch.into_confirm_mode().await?;

let c = confirm_ch.publish("", "orders", &PublishOptions::default(), b"order-1").await?;
c.wait().await?;
```

**Backpressure**: block publishes when too many confirms are outstanding:

```rust
let mut confirm_ch = ch.into_confirm_mode_with_tracking(1000).await?;
```

### Consuming

With manual acknowledgements:

```rust
ch.basic_qos(250).await?;
ch.consume_with_manual_acks("my-queue", "my-consumer").await?;

while let Some(delivery) = ch.recv_delivery().await? {
    println!("{:?}", delivery.body_str().unwrap_or("<binary>"));
    ch.ack(delivery.delivery_tag).await?;
}
```

With automatic acknowledgements:

```rust
ch.consume_with_auto_acks("my-queue", "my-consumer").await?;

while let Some(delivery) = ch.recv_delivery().await? {
    println!("{:?}", delivery.body_str().unwrap_or("<binary>"));
}
```

Using the `Consumer` trait (runs in a background task):

```rust
use bunny_rs::{Consumer, Delivery, ConnectionError, ConsumeOptions};

struct MyConsumer;

impl Consumer for MyConsumer {
    async fn handle_delivery(&mut self, delivery: Delivery) -> Result<(), ConnectionError> {
        println!("{:?}", delivery.body_str());
        Ok(())
    }
}

let handle = ch.basic_consume_with("my-queue", "", ConsumeOptions::default(), MyConsumer).await?;
// ...later:
handle.cancel();
```

### Acknowledgement Helpers

```rust
// Acknowledge a single delivery
ch.ack(delivery.delivery_tag).await?;
// Acknowledge all deliveries up to and including this tag
ch.ack_multiple(delivery.delivery_tag).await?;

// Reject and requeue
ch.reject(delivery.delivery_tag).await?;
// Reject and discard (dead-letter if configured)
ch.discard(delivery.delivery_tag).await?;
// Negatively acknowledge and requeue multiple deliveries
ch.nack_multiple(delivery.delivery_tag).await?;
```

`basic_ack`, `basic_nack`, and `basic_reject` are available when you need
full control over the `multiple` and `requeue` flags.

### Multiple Consumers per Channel

Use `recv_delivery_for` to receive from a specific consumer:

```rust
let tag_a = ch.consume_with_manual_acks("queue-1", "consumer-a").await?;
let tag_b = ch.consume_with_manual_acks("queue-2", "consumer-b").await?;

let delivery = ch.recv_delivery_for(&tag_a).await?;
```

### Declaring Queues

```rust
// Classic durable
ch.durable_queue("events").await?;
// Replicated quorum
ch.quorum_queue("orders").await?;
// Append-only stream
ch.stream_queue("logs").await?;
// Tanzu RabbitMQ delayed
ch.delayed_queue("retry-tasks", Default::default()).await?;
// Tanzu RabbitMQ JMS
ch.jms_queue("jms-tasks", Default::default()).await?;
// Server-named, exclusive, auto-delete
let q = ch.temporary_queue().await?;
```

With full options:

```rust
use std::time::Duration;
use bunny_rs::QueueDeclareOptions;

ch.queue_declare("with-ttl", QueueDeclareOptions::durable()
    .message_ttl(Duration::from_secs(3600))
    .max_length(100_000)
    .dead_letter_exchange("dlx")
).await?;
```

### Declaring Exchanges

```rust
ch.declare_fanout("logs").await?;
ch.declare_direct("direct_logs").await?;
ch.declare_topic("topic_logs").await?;
ch.declare_headers("match_logs").await?;
```

With options:

```rust
use bunny_rs::ExchangeDeclareOptions;

ch.exchange_declare("durable-logs", "fanout", ExchangeDeclareOptions::durable()).await?;
```

### Bindings

```rust
ch.bind("my-queue", "logs", "routing.key").await?;
```

With arguments (e.g. for headers exchanges):

```rust
use bunny_rs::options::{BindingArguments, HeadersMatch};

let args = BindingArguments::new()
    .match_mode(HeadersMatch::All)
    .header("region", "eu")
    .header("priority", 1);
ch.queue_bind("my-queue", "headers-x", "", args).await?;
```

Exchange-to-exchange:

```rust
use bunny_rs::FieldTable;

ch.exchange_bind("destination", "source", "events.#", FieldTable::new()).await?;
```

### Transactions

```rust
let mut tx = ch.into_tx_mode().await?;

tx.publish("", "queue-name", b"msg-1").await?;
tx.publish("", "queue-name", b"msg-2").await?;
tx.commit().await?;

// or: tx.rollback().await?;
```

### Connecting to Multiple Nodes

For redundancy, pass multiple AMQP URIs. The client tries each endpoint
in order on initial connection, and shuffles them on reconnection to
spread load across cluster nodes.

```rust
let conn = Connection::from_uris(&[
    "amqp://guest:guest@rabbit1:5672/",
    "amqp://guest:guest@rabbit2:5672/",
    "amqp://guest:guest@rabbit3:5672/",
]).await?;
```

Or build the endpoint list manually:

```rust
use bunny_rs::{Connection, ConnectionOptions, Endpoint};

let opts = ConnectionOptions {
    endpoints: vec![
        Endpoint::new("rabbit1", 5672),
        Endpoint::new("rabbit2", 5672),
        Endpoint::new("rabbit3", 5672),
    ],
    ..Default::default()
};
let conn = Connection::open(opts).await?;
```

When a single hostname is used, the client resolves it via DNS and
tries all returned A/AAAA records.

### Connection Recovery

Enabled by default. On connection loss (network failure, server restart,
heartbeat timeout), the library reconnects with exponential backoff and
replays topology: exchanges, queues, bindings, and consumers are re-declared.

```rust
let mut events = conn.events();
tokio::spawn(async move {
    while let Ok(event) = events.recv().await {
        println!("{event:?}");
    }
});
```

Configure recovery intervals and limits:

```rust
use std::time::Duration;
use bunny_rs::{ConnectionOptions, RecoveryConfig};

let opts = ConnectionOptions {
    recovery: RecoveryConfig {
        initial_interval: Duration::from_secs(2),
        max_interval: Duration::from_secs(30),
        max_attempts: Some(10),
        ..Default::default()
    },
    ..Default::default()
};
```

### Connection Blocked Notifications

RabbitMQ [blocks publishing connections](https://www.rabbitmq.com/docs/connections#blocked)
when it runs low on resources. The client tracks this automatically:

```rust
if conn.is_blocked().await {
    println!("blocked: {:?}", conn.blocked_reason().await);
}
```

Blocked/unblocked transitions are also emitted as `ConnectionEvent::Blocked`
and `ConnectionEvent::Unblocked` via `conn.events()`.

### Delivery Metadata

```rust
delivery.body_str()
delivery.content_type()
delivery.message_id()
delivery.correlation_id()
delivery.reply_to()

let props = delivery.properties();
props.get_timestamp()
props.get_headers()
```

All accessors return `Option`; properties are parsed lazily on first access.

### Queue Types and Arguments

Every queue type can be selected via a helper (`quorum()`, `stream()`, etc.)
or explicitly with the `QueueType` enum:

```rust
use bunny_rs::{QueueDeclareOptions, QueueType};

// These are equivalent:
QueueDeclareOptions::quorum()
QueueDeclareOptions::default().queue_type(QueueType::Quorum)
```

**Quorum queues** (replicated, durable):

```rust
use bunny_rs::QueueDeclareOptions;
use bunny_rs::options::{DeadLetterStrategy, QueueLeaderLocator};

ch.queue_declare("tasks", QueueDeclareOptions::quorum()
    .delivery_limit(5)
    .dead_letter_exchange("dlx")
    .dead_letter_strategy(DeadLetterStrategy::AtLeastOnce)
    .quorum_initial_group_size(3)
    .leader_locator(QueueLeaderLocator::Balanced)
    .single_active_consumer()
).await?;
```

**Streams** (append-only log):

```rust
use bunny_rs::QueueDeclareOptions;
use bunny_rs::options::MaxAge;

ch.queue_declare("events", QueueDeclareOptions::stream()
    .max_age(MaxAge::days(7))
    .max_length_bytes(1_000_000_000)
    .stream_max_segment_size_bytes(100_000_000)
    .initial_cluster_size(3)
).await?;
```

**Classic queues** with common arguments:

```rust
use std::time::Duration;
use bunny_rs::QueueDeclareOptions;
use bunny_rs::options::OverflowMode;

ch.queue_declare("bounded", QueueDeclareOptions::durable()
    .message_ttl(Duration::from_secs(3600))
    .expires(Duration::from_secs(86400))
    .max_length(100_000)
    .max_length_bytes(50_000_000)
    .overflow(OverflowMode::RejectPublish)
    .dead_letter_exchange("dlx")
    .dead_letter_routing_key("rejected")
    .max_priority(10)
).await?;
```

For arguments not covered by a builder method, use the escape hatch:

```rust
ch.queue_declare("custom", QueueDeclareOptions::durable()
    .with_argument("x-custom-plugin-arg", 42)
).await?;
```

### Consuming from Streams

```rust
use bunny_rs::ConsumeOptions;
use bunny_rs::options::StreamOffset;

ch.basic_consume("my-stream", "my-consumer",
    ConsumeOptions::default()
        .stream_offset(StreamOffset::First)
).await?;
```

Other offset types:

```rust
use bunny_rs::options::{StreamOffset, MaxAge};

// Most recent available
StreamOffset::Last
// Only new messages
StreamOffset::Next
// Absolute log offset
StreamOffset::Offset(1000)
// POSIX timestamp (seconds)
StreamOffset::Timestamp(1719792000)
// Relative time
StreamOffset::Interval(MaxAge::hours(2))
```

### Tanzu RabbitMQ: Delayed Queues

Requires the `rabbitmq_delayed_queue` plugin ([Tanzu RabbitMQ](https://docs.vmware.com/en/VMware-RabbitMQ/index.html)).

```rust
use std::time::Duration;
use bunny_rs::QueueDeclareOptions;
use bunny_rs::options::DelayedRetryType;

ch.delayed_queue("retry-tasks", QueueDeclareOptions::default()
    .delayed_retry_type(DelayedRetryType::Failed)
    .delayed_retry_min(Duration::from_secs(1))
    .delayed_retry_max(Duration::from_secs(60))
).await?;
```

### Tanzu RabbitMQ: JMS Queues

Requires the `rabbitmq_jms` plugin ([Tanzu RabbitMQ](https://docs.vmware.com/en/VMware-RabbitMQ/index.html)).

```rust
use bunny_rs::QueueDeclareOptions;

ch.jms_queue("jms-orders", QueueDeclareOptions::default()
    .selector_fields(&["priority", "region"])
    .delivery_limit(3)
).await?;
```

With a JMS message selector on the consumer:

```rust
use bunny_rs::ConsumeOptions;

ch.basic_consume("jms-orders", "my-consumer",
    ConsumeOptions::default().jms_selector("priority > 5 AND region = 'EU'")
).await?;
```

### TLS

AMQPS URI (uses platform-native certificate verification):

```rust
let conn = Connection::from_uri("amqps://rabbit.example.com:5671/").await?;
```

Custom CA:

```rust
use std::path::Path;
use bunny_rs::{Connection, ConnectionOptions};
use bunny_rs::transport::tls::TlsOptions;

let tls = TlsOptions::with_ca_pem("rabbit.example.com", Path::new("/path/to/ca.pem"))?;
let opts = ConnectionOptions {
    host: "rabbit.example.com".into(),
    port: 5671,
    tls: Some(tls),
    ..Default::default()
};
let conn = Connection::open(opts).await?;
```

Mutual TLS (client certificate):

```rust
let tls = TlsOptions::mutual(
    "rabbit.example.com",
    Path::new("/path/to/ca.pem"),
    Path::new("/path/to/client_cert.pem"),
    Path::new("/path/to/client_key.pem"),
)?;
```


## Documentation

 * [Getting Started with RabbitMQ](https://www.rabbitmq.com/tutorials)
 * [AMQP 0-9-1 Model Explained](https://www.rabbitmq.com/tutorials/amqp-concepts.html)
 * [Connections](https://www.rabbitmq.com/docs/connections)
 * [Channels](https://www.rabbitmq.com/docs/channels)
 * [Queues](https://www.rabbitmq.com/docs/queues)
 * [Quorum Queues](https://www.rabbitmq.com/docs/quorum-queues)
 * [Streams](https://www.rabbitmq.com/docs/streams)
 * [Publishers](https://www.rabbitmq.com/docs/publishers)
 * [Consumers](https://www.rabbitmq.com/docs/consumers)
 * [Publisher Confirms and Data Safety](https://www.rabbitmq.com/docs/confirms)
 * [TLS](https://www.rabbitmq.com/docs/ssl)
 * [Deprecated Features](https://www.rabbitmq.com/docs/deprecated-features)


## Community and Getting Help

 * [GitHub Discussions](https://github.com/michaelklishin/bunny-rs/discussions)
 * [RabbitMQ Discord](https://rabbitmq.com/discord)
 * [RabbitMQ Mailing List](https://groups.google.com/forum/#!forum/rabbitmq-users)


## License

Dual-licensed under Apache 2.0 and MIT.

Copyright (c) 2025-2026 Michael S. Klishin
