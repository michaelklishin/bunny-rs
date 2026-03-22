# bunny-rs, a Modern Rust Client for RabbitMQ

bunny-rs is a RabbitMQ client for Rust built on Tokio and async/await.
Its API follows [Bunny](https://github.com/ruby-amqp/bunny) (Ruby) and borrows
from the .NET, Java, and [Swift](https://github.com/michaelklishin/bunny-swift) clients.

Key features:

 * Automatic [connection recovery](https://www.rabbitmq.com/client-libraries/java-api-guide#recovery) with topology replay
 * Streaming [publisher confirms](https://www.rabbitmq.com/docs/confirms) for [data safety](https://www.rabbitmq.com/docs/publishers#data-safety)
 * Type-safe builders for [quorum queues](https://www.rabbitmq.com/docs/quorum-queues), [streams](https://www.rabbitmq.com/docs/streams), delayed queues, JMS queues ([Tanzu RabbitMQ](https://docs.vmware.com/en/VMware-RabbitMQ/index.html)), dead-lettering, TTL, and more
 * TLS with [rustls](https://github.com/rustls/rustls) (no OpenSSL dependency)
 * Forward-compatible with RabbitMQ 4.3+ ([deprecated features](https://www.rabbitmq.com/docs/deprecated-features) are not exposed)


## Supported Rust Versions

Rust 1.94 or later.


## Supported RabbitMQ Versions

[Currently supported RabbitMQ release series](https://www.rabbitmq.com/release-information).


## Project Maturity

This is a young project by a long time member of the RabbitMQ Core Team.
Breaking API changes are possible.


## Installation

```toml
[dependencies]
bunny-rs = "0.9"
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

```rust
ch.publish("", "queue-name", b"Hello!").await?;
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

### Consuming with Manual Acknowledgements

```rust
ch.consume_with_manual_acks("my-queue", "my-consumer").await?;

while let Some(delivery) = ch.recv_delivery().await? {
    println!("{:?}", delivery.body_str().unwrap_or("<binary>"));
    ch.basic_ack(delivery.delivery_tag, false).await?;
}
```

### Consuming with Automatic Acknowledgements

```rust
ch.consume_with_auto_acks("my-queue", "my-consumer").await?;

while let Some(delivery) = ch.recv_delivery().await? {
    println!("{:?}", delivery.body_str().unwrap_or("<binary>"));
}
```

### Multiple Consumers per Channel

Use `recv_delivery_for` to receive from a specific consumer:

```rust
let tag_a = ch.consume_with_manual_acks("queue-1", "consumer-a").await?;
let tag_b = ch.consume_with_manual_acks("queue-2", "consumer-b").await?;

let delivery = ch.recv_delivery_for(&tag_a).await?;
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

This is the [standard recovery procedure](https://www.rabbitmq.com/client-libraries/java-api-guide#recovery) used by the Java, .NET, Ruby, and Swift clients.

```rust
let mut events = conn.events();
tokio::spawn(async move {
    while let Ok(event) = events.recv().await {
        println!("{event:?}");
    }
});
```

### Declaring Queues

```rust
ch.durable_queue("events").await?;          // classic durable
ch.quorum_queue("orders").await?;           // replicated quorum
ch.stream_queue("logs").await?;             // append-only stream
ch.delayed_queue("retry-tasks", Default::default()).await?;  // Tanzu RabbitMQ delayed
ch.jms_queue("jms-tasks", Default::default()).await?;        // Tanzu RabbitMQ JMS
let q = ch.temporary_queue().await?;        // server-named, exclusive, auto-delete
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

### Prefetch (QoS)

```rust
ch.basic_qos(250).await?;
```

### Bindings

```rust
ch.bind("my-queue", "logs", "routing.key").await?;
```

With arguments:

```rust
use bunny_rs::FieldTable;

ch.queue_bind("my-queue", "headers-x", "", FieldTable::new()).await?;
```

Exchange-to-exchange:

```rust
ch.exchange_bind("destination", "source", "events.#", FieldTable::new()).await?;
```

### Delivery Metadata

```rust
delivery.body_str()        // Option<&str>
delivery.content_type()    // Option<&str>
delivery.message_id()      // Option<&str>
delivery.correlation_id()  // Option<&str>
delivery.reply_to()        // Option<&str>

let props = delivery.properties();
props.get_timestamp()      // Option<u64>
props.get_headers()        // Option<&FieldTable>
```

### Type-Safe Queue Arguments

```rust
use bunny_rs::QueueDeclareOptions;
use bunny_rs::options::{OverflowMode, DeadLetterStrategy, MaxAge};

// Quorum queue with delivery limits and dead-lettering
ch.queue_declare("tasks", QueueDeclareOptions::quorum()
    .delivery_limit(5)
    .dead_letter_exchange("dlx")
    .dead_letter_strategy(DeadLetterStrategy::AtLeastOnce)
).await?;

// Stream with 7-day retention
ch.queue_declare("events", QueueDeclareOptions::stream()
    .max_age(MaxAge::days(7))
    .initial_cluster_size(3)
).await?;

// Bounded queue with overflow policy
ch.queue_declare("bounded", QueueDeclareOptions::durable()
    .max_length(10_000)
    .overflow(OverflowMode::RejectPublish)
).await?;
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

AMQPS URI:

```rust
let conn = Connection::from_uri("amqps://rabbit.example.com:5671/").await?;
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

 * [GitHub Discussions](https://github.com/rabbitmq/bunny-rs/discussions)
 * [RabbitMQ Discord](https://rabbitmq.com/discord)
 * [RabbitMQ Mailing List](https://groups.google.com/forum/#!forum/rabbitmq-users)


## License

Dual-licensed under Apache 2.0 and MIT.

Copyright (c) 2025-2026 Michael S. Klishin
