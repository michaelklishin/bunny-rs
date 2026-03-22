// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::collections::{HashMap, VecDeque};
use std::mem;
use std::str;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use compact_str::CompactString;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot};

use crate::connection::topology::TopologyRegistry;
use crate::connection::{ConnectionError, WriterCommand, WriterTx};
use crate::consumer::{Consumer, ConsumerHandle};
use crate::options::{
    ConsumeOptions, ExchangeDeclareOptions, ExchangeDeleteOptions, PublishOptions,
    QueueDeclareOptions, QueueDeleteOptions, QueueType,
};
use crate::protocol::constants::*;
use crate::protocol::frame::{ContentHeader, Frame};
use crate::protocol::method::*;
use crate::protocol::properties::{BasicProperties, serialize_basic_properties};
use crate::protocol::types::FieldTable;

/// Consumer tag.
pub type ConsumerTag = CompactString;

/// Resource name (queue, exchange, etc).
pub type ResourceName = String;

/// AMQP reply codes. Soft errors close the channel; hard errors close the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum AmqpReplyCode {
    Success = 200,
    // Soft errors (channel-level)
    ContentTooLarge = 311,
    NoRoute = 312,
    NoConsumers = 313,
    AccessRefused = 403,
    NotFound = 404,
    ResourceLocked = 405,
    PreconditionFailed = 406,
    // Hard errors (connection-level)
    ConnectionForced = 320,
    InvalidPath = 402,
    FrameError = 501,
    SyntaxError = 502,
    CommandInvalid = 503,
    ChannelError = 504,
    UnexpectedFrame = 505,
    ResourceError = 506,
    NotAllowed = 530,
    NotImplemented = 540,
    InternalError = 541,
}

impl AmqpReplyCode {
    pub fn from_code(code: u16) -> Option<Self> {
        match code {
            200 => Some(Self::Success),
            311 => Some(Self::ContentTooLarge),
            312 => Some(Self::NoRoute),
            313 => Some(Self::NoConsumers),
            320 => Some(Self::ConnectionForced),
            402 => Some(Self::InvalidPath),
            403 => Some(Self::AccessRefused),
            404 => Some(Self::NotFound),
            405 => Some(Self::ResourceLocked),
            406 => Some(Self::PreconditionFailed),
            501 => Some(Self::FrameError),
            502 => Some(Self::SyntaxError),
            503 => Some(Self::CommandInvalid),
            504 => Some(Self::ChannelError),
            505 => Some(Self::UnexpectedFrame),
            506 => Some(Self::ResourceError),
            530 => Some(Self::NotAllowed),
            540 => Some(Self::NotImplemented),
            541 => Some(Self::InternalError),
            _ => None,
        }
    }

    /// Soft errors close the channel but not the connection.
    pub fn is_soft_error(self) -> bool {
        matches!(
            self,
            Self::ContentTooLarge
                | Self::NoRoute
                | Self::NoConsumers
                | Self::AccessRefused
                | Self::NotFound
                | Self::ResourceLocked
                | Self::PreconditionFailed
        )
    }

    /// Hard errors close the connection.
    pub fn is_hard_error(self) -> bool {
        !self.is_soft_error() && self != Self::Success
    }
}

/// Channel lifecycle events.
#[derive(Debug, Clone)]
pub enum ChannelEvent {
    /// Channel closed.
    Closed {
        code: u16,
        text: String,
        class_id: u16,
        method_id: u16,
        initiated_by_server: bool,
    },
    /// Recovered after reconnection.
    Recovered,
}

impl ChannelEvent {
    /// Parsed reply code, if recognized.
    pub fn amqp_reply_code(&self) -> Option<AmqpReplyCode> {
        match self {
            Self::Closed { code, .. } => AmqpReplyCode::from_code(*code),
            _ => None,
        }
    }

    /// True if the close is a soft (channel-level) error.
    pub fn is_soft_error(&self) -> bool {
        self.amqp_reply_code().is_some_and(|c| c.is_soft_error())
    }
}

/// Message returned via `basic.return` (unroutable mandatory publish).
#[derive(Debug)]
pub struct ReturnedMessage {
    pub reply_code: u16,
    pub reply_text: CompactString,
    pub exchange: CompactString,
    pub routing_key: CompactString,
    pub body: Bytes,
    properties_raw: Bytes,
    properties_cache: OnceLock<BasicProperties>,
}

impl ReturnedMessage {
    /// Lazily parsed properties.
    pub fn properties(&self) -> &BasicProperties {
        self.properties_cache
            .get_or_init(|| parse_properties_raw(&self.properties_raw))
    }
}

/// Shared channel/dispatcher state.
struct ChannelInner {
    confirm_pending: VecDeque<(u64, oneshot::Sender<bool>, Option<OwnedSemaphorePermit>)>,
    confirm_next_seq: u64,
    confirms_enabled: bool,
    confirm_semaphore: Option<Arc<Semaphore>>,
    consumers: HashMap<CompactString, mpsc::UnboundedSender<RawDelivery>>,
    // basic.get result sender
    get_result_tx: Option<oneshot::Sender<RawDelivery>>,
    return_tx: mpsc::UnboundedSender<ReturnedMessage>,
}

/// AMQP channel. A background dispatcher handles confirms, deliveries, and RPCs.
pub struct Channel {
    id: u16,
    writer_tx: WriterTx,
    frame_max: u32,
    inner: Arc<Mutex<ChannelInner>>,
    topology: Arc<Mutex<TopologyRegistry>>,
    rpc_rx: mpsc::UnboundedReceiver<Method>,
    // Per-consumer delivery receivers
    delivery_rxs: HashMap<CompactString, mpsc::UnboundedReceiver<RawDelivery>>,
    event_tx: broadcast::Sender<ChannelEvent>,
    return_rx: mpsc::UnboundedReceiver<ReturnedMessage>,
}

struct RawDelivery {
    consumer_tag: CompactString,
    delivery_tag: u64,
    redelivered: bool,
    exchange: CompactString,
    routing_key: CompactString,
    header: ContentHeader,
    body: Bytes,
}

/// Handle for a pending publish confirm.
pub struct PublishConfirm {
    rx: oneshot::Receiver<bool>,
}

impl PublishConfirm {
    /// Wait for ack. Errors on nack or channel close.
    pub async fn wait(self) -> Result<(), ConnectionError> {
        match self.rx.await {
            Ok(true) => Ok(()),
            Ok(false) => Err(ConnectionError::PublishNacked),
            Err(_) => Err(ConnectionError::ChannelNotOpen),
        }
    }
}

impl Channel {
    pub(crate) fn new(
        id: u16,
        writer_tx: WriterTx,
        frame_rx: mpsc::UnboundedReceiver<Frame>,
        frame_max: u32,
        topology: Arc<Mutex<TopologyRegistry>>,
    ) -> Self {
        let (return_tx, return_rx) = mpsc::unbounded_channel();
        let inner = Arc::new(Mutex::new(ChannelInner {
            confirm_pending: VecDeque::new(),
            confirm_next_seq: 1,
            confirms_enabled: false,
            confirm_semaphore: None,
            consumers: HashMap::new(),
            get_result_tx: None,
            return_tx,
        }));

        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        let (event_tx, _) = broadcast::channel(16);

        let dispatcher_inner = inner.clone();
        let dispatcher_writer = writer_tx.clone();
        let dispatcher_event_tx = event_tx.clone();
        let ch_id = id;
        tokio::spawn(async move {
            dispatcher_loop(
                ch_id,
                frame_rx,
                rpc_tx,
                dispatcher_inner,
                dispatcher_writer,
                dispatcher_event_tx,
            )
            .await;
        });

        Self {
            id,
            writer_tx,
            frame_max,
            inner,
            topology,
            rpc_rx,
            delivery_rxs: HashMap::new(),
            event_tx,
            return_rx,
        }
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    /// Subscribe to channel lifecycle events.
    pub fn events(&self) -> broadcast::Receiver<ChannelEvent> {
        self.event_tx.subscribe()
    }

    /// Receive the next `basic.return`.
    pub async fn recv_return(&mut self) -> Option<ReturnedMessage> {
        self.return_rx.recv().await
    }

    //
    // RPC helpers
    //

    async fn rpc(&mut self, method: Method) -> Result<Method, ConnectionError> {
        let frame = Frame::Method(self.id, Box::new(method));
        self.writer_tx
            .send(WriterCommand::SendFrame(frame))
            .await
            .map_err(|_| ConnectionError::NotConnected)?;
        self.await_rpc_response().await
    }

    async fn await_rpc_response(&mut self) -> Result<Method, ConnectionError> {
        tokio::time::timeout(Duration::from_secs(10), self.rpc_rx.recv())
            .await
            .map_err(|_| ConnectionError::Timeout)?
            .ok_or(ConnectionError::NotConnected)
    }

    async fn send(&self, method: Method) -> Result<(), ConnectionError> {
        let frame = Frame::Method(self.id, Box::new(method));
        self.writer_tx
            .send(WriterCommand::SendFrame(frame))
            .await
            .map_err(|_| ConnectionError::NotConnected)
    }

    //
    // Exchange operations
    //

    pub async fn exchange_declare(
        &mut self,
        name: &str,
        kind: &str,
        opts: ExchangeDeclareOptions,
    ) -> Result<(), ConnectionError> {
        let opts_copy = opts.clone();
        let method = Method::ExchangeDeclare(Box::new(ExchangeDeclareArgs {
            exchange: name.into(),
            kind: kind.into(),
            passive: false,
            durable: opts.durable,
            auto_delete: opts.auto_delete,
            internal: opts.internal,
            nowait: false,
            arguments: opts.into_arguments(),
        }));
        match self.rpc(method).await? {
            Method::ExchangeDeclareOk => {
                self.topology
                    .lock()
                    .await
                    .record_exchange(name, kind, &opts_copy);
                Ok(())
            }
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    /// Declare a fanout exchange with default options.
    pub async fn declare_fanout(&mut self, name: &str) -> Result<(), ConnectionError> {
        self.exchange_declare(name, "fanout", ExchangeDeclareOptions::default())
            .await
    }

    /// Declare a direct exchange with default options.
    pub async fn declare_direct(&mut self, name: &str) -> Result<(), ConnectionError> {
        self.exchange_declare(name, "direct", ExchangeDeclareOptions::default())
            .await
    }

    /// Declare a topic exchange with default options.
    pub async fn declare_topic(&mut self, name: &str) -> Result<(), ConnectionError> {
        self.exchange_declare(name, "topic", ExchangeDeclareOptions::default())
            .await
    }

    /// Declare a headers exchange with default options.
    pub async fn declare_headers(&mut self, name: &str) -> Result<(), ConnectionError> {
        self.exchange_declare(name, "headers", ExchangeDeclareOptions::default())
            .await
    }

    pub async fn exchange_delete(
        &mut self,
        name: &str,
        opts: ExchangeDeleteOptions,
    ) -> Result<(), ConnectionError> {
        let method = Method::ExchangeDelete(Box::new(ExchangeDeleteArgs {
            exchange: name.into(),
            if_unused: opts.if_unused,
            nowait: false,
        }));
        match self.rpc(method).await? {
            Method::ExchangeDeleteOk => {
                self.topology.lock().await.remove_exchange(name);
                Ok(())
            }
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    pub async fn exchange_bind(
        &mut self,
        destination: &str,
        source: &str,
        routing_key: &str,
        arguments: impl Into<FieldTable>,
    ) -> Result<(), ConnectionError> {
        let arguments = arguments.into();
        let args_copy = arguments.clone();
        let method = Method::ExchangeBind(Box::new(ExchangeBindArgs {
            destination: destination.into(),
            source: source.into(),
            routing_key: routing_key.into(),
            nowait: false,
            arguments,
        }));
        match self.rpc(method).await? {
            Method::ExchangeBindOk => {
                self.topology.lock().await.record_exchange_binding(
                    destination,
                    source,
                    routing_key,
                    &args_copy,
                );
                Ok(())
            }
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    pub async fn exchange_unbind(
        &mut self,
        destination: &str,
        source: &str,
        routing_key: &str,
        arguments: impl Into<FieldTable>,
    ) -> Result<(), ConnectionError> {
        let method = Method::ExchangeUnbind(Box::new(ExchangeUnbindArgs {
            destination: destination.into(),
            source: source.into(),
            routing_key: routing_key.into(),
            nowait: false,
            arguments: arguments.into(),
        }));
        match self.rpc(method).await? {
            Method::ExchangeUnbindOk => {
                self.topology.lock().await.remove_exchange_binding(
                    destination,
                    source,
                    routing_key,
                );
                Ok(())
            }
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    //
    // Queue operations
    //

    pub async fn queue_declare(
        &mut self,
        name: &str,
        opts: QueueDeclareOptions,
    ) -> Result<QueueInfo, ConnectionError> {
        let opts_copy = opts.clone();
        let server_named = name.is_empty();
        let method = Method::QueueDeclare(Box::new(QueueDeclareArgs {
            queue: name.into(),
            passive: false,
            durable: opts.durable,
            exclusive: opts.exclusive,
            auto_delete: opts.auto_delete,
            nowait: false,
            arguments: opts.into_arguments(),
        }));
        match self.rpc(method).await? {
            Method::QueueDeclareOk(args) => {
                if !opts_copy.exclusive {
                    self.topology.lock().await.record_queue(
                        args.queue.as_str(),
                        &opts_copy,
                        server_named,
                    );
                }
                Ok(QueueInfo {
                    name: args.queue.to_string(),
                    message_count: args.message_count,
                    consumer_count: args.consumer_count,
                })
            }
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    pub async fn queue_delete(
        &mut self,
        name: &str,
        opts: QueueDeleteOptions,
    ) -> Result<u32, ConnectionError> {
        let method = Method::QueueDelete(Box::new(QueueDeleteArgs {
            queue: name.into(),
            if_unused: opts.if_unused,
            if_empty: opts.if_empty,
            nowait: false,
        }));
        match self.rpc(method).await? {
            Method::QueueDeleteOk(args) => {
                self.topology.lock().await.remove_queue(name);
                Ok(args.message_count)
            }
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    pub async fn queue_bind(
        &mut self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
        arguments: impl Into<FieldTable>,
    ) -> Result<(), ConnectionError> {
        let arguments = arguments.into();
        let args_copy = arguments.clone();
        let method = Method::QueueBind(Box::new(QueueBindArgs {
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            nowait: false,
            arguments,
        }));
        match self.rpc(method).await? {
            Method::QueueBindOk => {
                self.topology.lock().await.record_queue_binding(
                    queue,
                    exchange,
                    routing_key,
                    &args_copy,
                );
                Ok(())
            }
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    pub async fn queue_unbind(
        &mut self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
        arguments: impl Into<FieldTable>,
    ) -> Result<(), ConnectionError> {
        let method = Method::QueueUnbind(Box::new(QueueUnbindArgs {
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            arguments: arguments.into(),
        }));
        match self.rpc(method).await? {
            Method::QueueUnbindOk => Ok(()),
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    /// Declare a server-named, exclusive, auto-delete queue.
    pub async fn temporary_queue(&mut self) -> Result<QueueInfo, ConnectionError> {
        self.queue_declare("", QueueDeclareOptions::exclusive())
            .await
    }

    /// Declare a durable classic queue.
    pub async fn durable_queue(&mut self, name: &str) -> Result<QueueInfo, ConnectionError> {
        self.queue_declare(name, QueueDeclareOptions::durable())
            .await
    }

    /// Declare a durable quorum queue.
    pub async fn quorum_queue(&mut self, name: &str) -> Result<QueueInfo, ConnectionError> {
        self.queue_declare(name, QueueDeclareOptions::quorum())
            .await
    }

    /// Declare a durable stream.
    pub async fn stream_queue(&mut self, name: &str) -> Result<QueueInfo, ConnectionError> {
        self.queue_declare(name, QueueDeclareOptions::stream())
            .await
    }

    /// Declare a Tanzu RabbitMQ delayed queue.
    ///
    /// The queue type is set appropriately under the hood. Pass `Default::default()` for
    /// a plain delayed queue, or chain delayed-specific builders:
    ///
    /// ```ignore
    /// ch.delayed_queue("tasks", QueueDeclareOptions::default()
    ///     .delayed_retry_type(DelayedRetryType::Failed)
    ///     .delayed_retry_min(Duration::from_secs(1))
    ///     .delayed_retry_max(Duration::from_secs(60))
    /// ).await?;
    /// ```
    pub async fn delayed_queue(
        &mut self,
        name: &str,
        opts: QueueDeclareOptions,
    ) -> Result<QueueInfo, ConnectionError> {
        self.queue_declare(name, opts.queue_type(QueueType::Delayed))
            .await
    }

    /// Declare a Tanzu RabbitMQ JMS queue.
    ///
    /// The queue type is set appropriately under the hood. Pass `Default::default()` for
    /// a plain JMS queue, or chain JMS-specific builders:
    ///
    /// ```ignore
    /// ch.jms_queue("orders", QueueDeclareOptions::default()
    ///     .selector_fields(&["priority", "region"])
    ///     .selector_field_max_bytes(256)
    /// ).await?;
    /// ```
    pub async fn jms_queue(
        &mut self,
        name: &str,
        opts: QueueDeclareOptions,
    ) -> Result<QueueInfo, ConnectionError> {
        self.queue_declare(name, opts.queue_type(QueueType::Jms))
            .await
    }

    /// Passive queue declare (checks existence).
    pub async fn queue_declare_passive(
        &mut self,
        name: &str,
    ) -> Result<QueueInfo, ConnectionError> {
        let method = Method::QueueDeclare(Box::new(QueueDeclareArgs {
            queue: name.into(),
            passive: true,
            durable: false,
            exclusive: false,
            auto_delete: false,
            nowait: false,
            arguments: FieldTable::new(),
        }));
        match self.rpc(method).await? {
            Method::QueueDeclareOk(args) => Ok(QueueInfo {
                name: args.queue.to_string(),
                message_count: args.message_count,
                consumer_count: args.consumer_count,
            }),
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    /// Passive exchange declare (checks existence).
    pub async fn exchange_declare_passive(&mut self, name: &str) -> Result<(), ConnectionError> {
        let method = Method::ExchangeDeclare(Box::new(ExchangeDeclareArgs {
            exchange: name.into(),
            kind: "".into(),
            passive: true,
            durable: false,
            auto_delete: false,
            internal: false,
            nowait: false,
            arguments: FieldTable::new(),
        }));
        match self.rpc(method).await? {
            Method::ExchangeDeclareOk => Ok(()),
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    pub async fn queue_purge(&mut self, name: &str) -> Result<u32, ConnectionError> {
        let method = Method::QueuePurge(Box::new(QueuePurgeArgs {
            queue: name.into(),
            nowait: false,
        }));
        match self.rpc(method).await? {
            Method::QueuePurgeOk(args) => Ok(args.message_count),
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    //
    // QoS
    //

    /// Set per-consumer prefetch.
    pub async fn basic_qos(&mut self, prefetch_count: u16) -> Result<(), ConnectionError> {
        let method = Method::BasicQos(Box::new(BasicQosArgs {
            prefetch_size: 0,
            prefetch_count,
            global: false,
        }));
        match self.rpc(method).await? {
            Method::BasicQosOk => Ok(()),
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    //
    // Publishing
    //

    /// Publish with defaults.
    pub async fn publish(
        &mut self,
        exchange: &str,
        routing_key: &str,
        body: impl AsRef<[u8]>,
    ) -> Result<Option<PublishConfirm>, ConnectionError> {
        self.basic_publish(exchange, routing_key, &PublishOptions::default(), body)
            .await
    }

    pub async fn basic_publish(
        &mut self,
        exchange: &str,
        routing_key: &str,
        opts: &PublishOptions,
        body: impl AsRef<[u8]>,
    ) -> Result<Option<PublishConfirm>, ConnectionError> {
        let body = body.as_ref();
        let method_frame = Frame::Method(
            self.id,
            Box::new(Method::BasicPublish(Box::new(BasicPublishArgs {
                exchange: exchange.into(),
                routing_key: routing_key.into(),
                mandatory: opts.mandatory,
                immediate: false,
            }))),
        );

        let mut props_buf = Vec::new();
        serialize_basic_properties(&opts.properties, &mut props_buf)
            .map_err(ConnectionError::Protocol)?;

        let header_frame = Frame::Header(
            self.id,
            ContentHeader {
                class_id: CLASS_BASIC,
                body_size: body.len() as u64,
                properties_raw: Bytes::from(props_buf),
            },
        );

        let max_body = if self.frame_max == 0 {
            body.len()
        } else {
            (self.frame_max as usize).saturating_sub(FRAME_OVERHEAD)
        };

        let mut frames = vec![method_frame, header_frame];
        if !body.is_empty() {
            if max_body == 0 || body.len() <= max_body {
                frames.push(Frame::Body(self.id, Bytes::copy_from_slice(body)));
            } else {
                for chunk in body.chunks(max_body) {
                    frames.push(Frame::Body(self.id, Bytes::copy_from_slice(chunk)));
                }
            }
        }

        // Acquire backpressure permit (if any) without holding ChannelInner lock
        let permit = {
            let inner = self.inner.lock().await;
            inner.confirm_semaphore.clone()
        };
        let permit = match permit {
            Some(sem) => Some(
                sem.acquire_owned()
                    .await
                    .map_err(|_| ConnectionError::ChannelNotOpen)?,
            ),
            None => None,
        };

        // Register confirm before send
        let confirm = {
            let mut inner = self.inner.lock().await;
            if inner.confirms_enabled {
                let seq_no = inner.confirm_next_seq;
                inner.confirm_next_seq += 1;
                let (tx, rx) = oneshot::channel();
                inner.confirm_pending.push_back((seq_no, tx, permit));
                Some(PublishConfirm { rx })
            } else {
                None
            }
        };

        self.writer_tx
            .send(WriterCommand::SendFrames(frames))
            .await
            .map_err(|_| ConnectionError::NotConnected)?;

        Ok(confirm)
    }

    //
    // Consuming
    //

    /// Bind with no extra arguments.
    pub async fn bind(
        &mut self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
    ) -> Result<(), ConnectionError> {
        self.queue_bind(queue, exchange, routing_key, FieldTable::new())
            .await
    }

    /// Consume with manual acks.
    pub async fn consume_with_manual_acks(
        &mut self,
        queue: &str,
        consumer_tag: &str,
    ) -> Result<ConsumerTag, ConnectionError> {
        self.basic_consume(queue, consumer_tag, ConsumeOptions::default())
            .await
    }

    /// Consume with auto ack.
    pub async fn consume_with_auto_acks(
        &mut self,
        queue: &str,
        consumer_tag: &str,
    ) -> Result<ConsumerTag, ConnectionError> {
        self.basic_consume(queue, consumer_tag, ConsumeOptions::default().no_ack(true))
            .await
    }

    pub async fn basic_consume(
        &mut self,
        queue: &str,
        consumer_tag: &str,
        opts: ConsumeOptions,
    ) -> Result<ConsumerTag, ConnectionError> {
        let opts_copy = opts.clone();

        // Pre-register so deliveries arriving before ConsumeOk are routed
        let (tx, rx) = mpsc::unbounded_channel();
        if !consumer_tag.is_empty() {
            self.inner
                .lock()
                .await
                .consumers
                .insert(consumer_tag.into(), tx.clone());
        }

        let method = Method::BasicConsume(Box::new(BasicConsumeArgs {
            queue: queue.into(),
            consumer_tag: consumer_tag.into(),
            no_local: false,
            no_ack: opts.no_ack,
            exclusive: opts.exclusive,
            nowait: false,
            arguments: opts.into_arguments(),
        }));
        match self.rpc(method).await? {
            Method::BasicConsumeOk(args) => {
                let actual_tag = args.consumer_tag.clone();
                self.topology.lock().await.record_consumer(
                    self.id,
                    queue,
                    actual_tag.as_str(),
                    &opts_copy,
                );
                // If server assigned a different tag, update the map
                {
                    let mut inner = self.inner.lock().await;
                    if consumer_tag.is_empty() || actual_tag.as_str() != consumer_tag {
                        inner.consumers.remove(consumer_tag);
                        inner.consumers.insert(actual_tag.clone(), tx);
                    }
                }
                self.delivery_rxs.insert(actual_tag.clone(), rx);
                Ok(actual_tag)
            }
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    pub async fn basic_cancel(&mut self, consumer_tag: &str) -> Result<(), ConnectionError> {
        let method = Method::BasicCancel(Box::new(BasicCancelArgs {
            consumer_tag: consumer_tag.into(),
            nowait: false,
        }));
        match self.rpc(method).await? {
            Method::BasicCancelOk(_) => {
                self.topology.lock().await.remove_consumer(consumer_tag);
                self.inner.lock().await.consumers.remove(consumer_tag);
                self.delivery_rxs.remove(consumer_tag);
                Ok(())
            }
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    /// Consume via a [`Consumer`] trait object (background task).
    pub async fn basic_consume_with(
        &mut self,
        queue: &str,
        consumer_tag: &str,
        opts: ConsumeOptions,
        mut consumer: impl Consumer,
    ) -> Result<ConsumerHandle, ConnectionError> {
        let tag = self.basic_consume(queue, consumer_tag, opts).await?;
        consumer.handle_consume_ok(&tag);

        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();
        let mut delivery_rx = self
            .delivery_rxs
            .remove(&tag)
            .ok_or(ConnectionError::ChannelNotOpen)?;

        let tag_clone = tag.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut cancel_rx => break,
                    delivery = delivery_rx.recv() => {
                        match delivery {
                            Some(raw) => {
                                if consumer.handle_delivery(Delivery::from(raw)).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        Ok(ConsumerHandle::new(tag_clone, cancel_tx))
    }

    /// Polls a queue for a single message. Returns `None` if empty.
    ///
    /// # Warning
    ///
    /// **Do not use this in a loop.** Polling with `basic_get` is extremely
    /// inefficient compared to [`basic_consume`](Self::basic_consume).
    /// See [RabbitMQ documentation on polling consumers](https://www.rabbitmq.com/docs/consumers#polling).
    pub async fn basic_get(
        &mut self,
        queue: &str,
        no_ack: bool,
    ) -> Result<Option<Delivery>, ConnectionError> {
        // Register a oneshot for the get result before sending the RPC
        let (tx, rx) = oneshot::channel();
        self.inner.lock().await.get_result_tx = Some(tx);

        let method = Method::BasicGet(Box::new(BasicGetArgs {
            queue: queue.into(),
            no_ack,
        }));
        match self.rpc(method).await? {
            Method::BasicGetEmpty => {
                // Clean up unused oneshot
                self.inner.lock().await.get_result_tx = None;
                Ok(None)
            }
            Method::BasicGetOk(_) => {
                // The dispatcher assembles header+body and sends via the oneshot
                let raw = tokio::time::timeout(Duration::from_secs(30), rx)
                    .await
                    .map_err(|_| ConnectionError::Timeout)?
                    .map_err(|_| ConnectionError::NotConnected)?;
                Ok(Some(Delivery::from(raw)))
            }
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    //
    // Transactions
    //

    pub async fn tx_select(&mut self) -> Result<(), ConnectionError> {
        match self.rpc(Method::TxSelect).await? {
            Method::TxSelectOk => Ok(()),
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    pub async fn tx_commit(&mut self) -> Result<(), ConnectionError> {
        match self.rpc(Method::TxCommit).await? {
            Method::TxCommitOk => Ok(()),
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    pub async fn tx_rollback(&mut self) -> Result<(), ConnectionError> {
        match self.rpc(Method::TxRollback).await? {
            Method::TxRollbackOk => Ok(()),
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    //
    // Delivery reception
    //

    /// Next delivery for the given consumer. `None` if closed.
    pub async fn recv_delivery_for(
        &mut self,
        consumer_tag: &str,
    ) -> Result<Option<Delivery>, ConnectionError> {
        let rx = self
            .delivery_rxs
            .get_mut(consumer_tag)
            .ok_or(ConnectionError::ChannelNotOpen)?;
        match rx.recv().await {
            Some(raw) => Ok(Some(Delivery::from(raw))),
            None => Ok(None),
        }
    }

    /// Next delivery from any consumer. Best with a single consumer.
    pub async fn recv_delivery(&mut self) -> Result<Option<Delivery>, ConnectionError> {
        if self.delivery_rxs.is_empty() {
            return Err(ConnectionError::ChannelNotOpen);
        }
        // With one consumer, just read from it directly
        if let Some((_, rx)) = self.delivery_rxs.iter_mut().next() {
            match rx.recv().await {
                Some(raw) => return Ok(Some(Delivery::from(raw))),
                None => return Ok(None),
            }
        }
        Err(ConnectionError::ChannelNotOpen)
    }

    //
    // Acknowledgements
    //

    pub async fn basic_ack(
        &self,
        delivery_tag: u64,
        multiple: bool,
    ) -> Result<(), ConnectionError> {
        self.send(Method::BasicAck {
            delivery_tag,
            multiple,
        })
        .await
    }

    pub async fn basic_nack(
        &self,
        delivery_tag: u64,
        multiple: bool,
        requeue: bool,
    ) -> Result<(), ConnectionError> {
        self.send(Method::BasicNack {
            delivery_tag,
            multiple,
            requeue,
        })
        .await
    }

    pub async fn basic_reject(
        &self,
        delivery_tag: u64,
        requeue: bool,
    ) -> Result<(), ConnectionError> {
        self.send(Method::BasicReject(Box::new(BasicRejectArgs {
            delivery_tag,
            requeue,
        })))
        .await
    }

    //
    // Publisher Confirms
    //

    /// Enable publisher confirms. Idempotent.
    pub async fn confirm_select(&mut self) -> Result<(), ConnectionError> {
        let method = Method::ConfirmSelect(Box::new(ConfirmSelectArgs { nowait: false }));
        match self.rpc(method).await? {
            Method::ConfirmSelectOk => {
                let mut inner = self.inner.lock().await;
                inner.confirms_enabled = true;
                Ok(())
            }
            _ => Err(ConnectionError::UnexpectedMethod),
        }
    }

    /// Enable confirms with backpressure. Publishes block when
    /// `outstanding_limit` unconfirmed messages are in flight.
    pub async fn confirm_select_with_tracking(
        &mut self,
        outstanding_limit: usize,
    ) -> Result<(), ConnectionError> {
        self.confirm_select().await?;
        if outstanding_limit > 0 {
            let mut inner = self.inner.lock().await;
            inner.confirm_semaphore = Some(Arc::new(Semaphore::new(outstanding_limit)));
        }
        Ok(())
    }

    /// Block until all pending confirms resolve.
    pub async fn wait_for_confirms(&self) -> Result<(), ConnectionError> {
        loop {
            let pending = {
                let inner = self.inner.lock().await;
                inner.confirm_pending.len()
            };
            if pending == 0 {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Next publish sequence number.
    pub async fn next_publish_seq_no(&self) -> u64 {
        let inner = self.inner.lock().await;
        if inner.confirms_enabled {
            inner.confirm_next_seq
        } else {
            0
        }
    }

    //
    // Channel close
    //

    pub async fn close(&mut self) -> Result<(), ConnectionError> {
        // Send channel.close; dispatcher handles close-ok
        let method = Method::ChannelClose(Box::new(ChannelCloseArgs {
            reply_code: 200,
            reply_text: "Normal shutdown".into(),
            class_id: 0,
            method_id: 0,
        }));
        let _ = self.send(method).await;
        // Brief wait for close-ok
        let _ = tokio::time::timeout(Duration::from_millis(500), self.rpc_rx.recv()).await;
        Ok(())
    }
}

//
// Background dispatcher task
//

enum ContentTarget {
    Consumer(CompactString),
    PollingConsumerResult,
    Return {
        reply_code: u16,
        reply_text: CompactString,
    },
}

impl ContentTarget {
    fn consumer_tag(&self) -> CompactString {
        match self {
            ContentTarget::Consumer(tag) => tag.clone(),
            ContentTarget::PollingConsumerResult | ContentTarget::Return { .. } => {
                CompactString::default()
            }
        }
    }
}

struct ContentMethodInfo {
    target: ContentTarget,
    delivery_tag: u64,
    redelivered: bool,
    exchange: CompactString,
    routing_key: CompactString,
}

enum ContentState {
    Idle,
    AwaitingHeader(ContentMethodInfo),
    AwaitingBody {
        info: ContentMethodInfo,
        header: ContentHeader,
        body_size: u64,
        accumulated: Vec<u8>,
    },
}

async fn dispatcher_loop(
    channel_id: u16,
    mut frame_rx: mpsc::UnboundedReceiver<Frame>,
    rpc_tx: mpsc::UnboundedSender<Method>,
    inner: Arc<Mutex<ChannelInner>>,
    writer_tx: WriterTx,
    event_tx: broadcast::Sender<ChannelEvent>,
) {
    let mut state = ContentState::Idle;

    while let Some(frame) = frame_rx.recv().await {
        match frame {
            Frame::Method(_, method) => {
                match *method {
                    // Confirms
                    Method::BasicAck {
                        delivery_tag,
                        multiple,
                    } => {
                        let mut inner = inner.lock().await;
                        resolve_confirms(&mut inner.confirm_pending, delivery_tag, multiple, true);
                    }
                    Method::BasicNack {
                        delivery_tag,
                        multiple,
                        ..
                    } => {
                        let mut inner = inner.lock().await;
                        resolve_confirms(&mut inner.confirm_pending, delivery_tag, multiple, false);
                    }

                    // Start content assembly
                    Method::BasicDeliver {
                        consumer_tag,
                        delivery_tag,
                        redelivered,
                        exchange,
                        routing_key,
                    } => {
                        state = ContentState::AwaitingHeader(ContentMethodInfo {
                            target: ContentTarget::Consumer(consumer_tag),
                            delivery_tag,
                            redelivered,
                            exchange,
                            routing_key,
                        });
                    }
                    Method::BasicGetOk(ref args) => {
                        state = ContentState::AwaitingHeader(ContentMethodInfo {
                            target: ContentTarget::PollingConsumerResult,
                            delivery_tag: args.delivery_tag,
                            redelivered: args.redelivered,
                            exchange: args.exchange.clone(),
                            routing_key: args.routing_key.clone(),
                        });
                        let _ = rpc_tx.send(*method);
                    }

                    // basic.return
                    Method::BasicReturn {
                        reply_code,
                        reply_text,
                        exchange,
                        routing_key,
                    } => {
                        state = ContentState::AwaitingHeader(ContentMethodInfo {
                            target: ContentTarget::Return {
                                reply_code,
                                reply_text,
                            },
                            delivery_tag: 0,
                            redelivered: false,
                            exchange,
                            routing_key,
                        });
                    }

                    // Broker-initiated cancel
                    Method::BasicCancel(args) => {
                        let close_ok = Frame::Method(
                            channel_id,
                            Box::new(Method::BasicCancelOk(Box::new(BasicCancelOkArgs {
                                consumer_tag: args.consumer_tag,
                            }))),
                        );
                        let _ = writer_tx.send(WriterCommand::SendFrame(close_ok)).await;
                    }

                    // Channel close from broker
                    Method::ChannelClose(ref args) => {
                        let _ = event_tx.send(ChannelEvent::Closed {
                            code: args.reply_code,
                            text: args.reply_text.to_string(),
                            class_id: args.class_id,
                            method_id: args.method_id,
                            initiated_by_server: true,
                        });
                        let close_ok = Frame::Method(channel_id, Box::new(Method::ChannelCloseOk));
                        let _ = writer_tx.send(WriterCommand::SendFrame(close_ok)).await;
                        let _ = rpc_tx.send(*method);
                        break;
                    }

                    // RPC response
                    other => {
                        let _ = rpc_tx.send(other);
                    }
                }
            }

            Frame::Header(_, header) => {
                if let ContentState::AwaitingHeader(info) = state {
                    let body_size = header.body_size;
                    if body_size == 0 {
                        let consumer_tag = info.target.consumer_tag();
                        let delivery = RawDelivery {
                            consumer_tag,
                            delivery_tag: info.delivery_tag,
                            redelivered: info.redelivered,
                            exchange: info.exchange,
                            routing_key: info.routing_key,
                            header,
                            body: Bytes::new(),
                        };
                        dispatch_delivery(&inner, &info.target, delivery).await;
                        state = ContentState::Idle;
                    } else {
                        state = ContentState::AwaitingBody {
                            info,
                            header,
                            body_size,
                            accumulated: Vec::with_capacity(body_size as usize),
                        };
                    }
                }
            }

            Frame::Body(_, data) => {
                if let ContentState::AwaitingBody {
                    ref mut accumulated,
                    body_size,
                    ..
                } = state
                {
                    accumulated.extend_from_slice(&data);
                    if accumulated.len() as u64 >= body_size {
                        // Content assembly complete
                        if let ContentState::AwaitingBody {
                            info,
                            header,
                            accumulated,
                            ..
                        } = mem::replace(&mut state, ContentState::Idle)
                        {
                            let delivery = RawDelivery {
                                consumer_tag: info.target.consumer_tag(),
                                delivery_tag: info.delivery_tag,
                                redelivered: info.redelivered,
                                exchange: info.exchange,
                                routing_key: info.routing_key,
                                header,
                                body: Bytes::from(accumulated),
                            };
                            dispatch_delivery(&inner, &info.target, delivery).await;
                        }
                    }
                }
            }

            Frame::Heartbeat => {}
        }
    }
}

fn resolve_confirms(
    pending: &mut VecDeque<(u64, oneshot::Sender<bool>, Option<OwnedSemaphorePermit>)>,
    delivery_tag: u64,
    multiple: bool,
    ack: bool,
) {
    if multiple {
        while let Some((seq_no, _, _)) = pending.front() {
            if *seq_no > delivery_tag {
                break;
            }
            if let Some((_, tx, _permit)) = pending.pop_front() {
                let _ = tx.send(ack);
                // _permit is dropped here, freeing a semaphore slot
            }
        }
    } else if let Some(pos) = pending.iter().position(|(s, _, _)| *s == delivery_tag)
        && let Some((_, tx, _permit)) = pending.remove(pos)
    {
        let _ = tx.send(ack);
    }
}

async fn dispatch_delivery(
    inner: &Arc<Mutex<ChannelInner>>,
    target: &ContentTarget,
    delivery: RawDelivery,
) {
    let mut guard = inner.lock().await;
    match target {
        ContentTarget::Consumer(tag) => {
            if let Some(tx) = guard.consumers.get(tag.as_str()) {
                let _ = tx.send(delivery);
            } else {
                tracing::warn!(consumer_tag = %tag, "delivery for unknown consumer, dropping");
            }
        }
        ContentTarget::PollingConsumerResult => {
            if let Some(tx) = guard.get_result_tx.take() {
                let _ = tx.send(delivery);
            }
        }
        ContentTarget::Return {
            reply_code,
            reply_text,
        } => {
            let _ = guard.return_tx.send(ReturnedMessage {
                reply_code: *reply_code,
                reply_text: reply_text.clone(),
                exchange: delivery.exchange,
                routing_key: delivery.routing_key,
                body: delivery.body,
                properties_raw: delivery.header.properties_raw,
                properties_cache: OnceLock::new(),
            });
        }
    }
}

fn parse_properties_raw(raw: &Bytes) -> BasicProperties {
    if raw.is_empty() {
        BasicProperties::default()
    } else {
        crate::protocol::properties::parse_basic_properties(raw)
            .map(|(_, p)| p)
            .unwrap_or_default()
    }
}

impl From<RawDelivery> for Delivery {
    fn from(raw: RawDelivery) -> Self {
        Self {
            consumer_tag: raw.consumer_tag,
            delivery_tag: raw.delivery_tag,
            redelivered: raw.redelivered,
            exchange: raw.exchange,
            routing_key: raw.routing_key,
            body: raw.body,
            properties_raw: raw.header.properties_raw,
            properties_cache: OnceLock::new(),
        }
    }
}

/// Declared queue metadata.
#[derive(Debug, Clone)]
pub struct QueueInfo {
    pub name: String,
    pub message_count: u32,
    pub consumer_count: u32,
}

/// A delivered message. Properties parsed lazily.
#[derive(Debug)]
pub struct Delivery {
    pub consumer_tag: CompactString,
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: CompactString,
    pub routing_key: CompactString,
    pub body: Bytes,
    properties_raw: Bytes,
    properties_cache: OnceLock<BasicProperties>,
}

impl Delivery {
    pub fn properties(&self) -> &BasicProperties {
        self.properties_cache
            .get_or_init(|| parse_properties_raw(&self.properties_raw))
    }

    pub fn body_str(&self) -> Option<&str> {
        str::from_utf8(&self.body).ok()
    }

    pub fn reply_to(&self) -> Option<&str> {
        self.properties().get_reply_to()
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.properties().get_correlation_id()
    }

    pub fn message_id(&self) -> Option<&str> {
        self.properties().get_message_id()
    }

    pub fn content_type(&self) -> Option<&str> {
        self.properties().get_content_type()
    }
}
