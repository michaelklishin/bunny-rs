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
use crate::consumer::{Consumer, ConsumerHandle, DeliveryHandler, SubscribeOptions};
use crate::exchange::Exchange;
use crate::options::{
    ConsumeOptions, ExchangeDeclareOptions, ExchangeDeleteOptions, PublishOptions,
    QueueDeclareOptions, QueueDeleteOptions, QueueType,
};
use crate::protocol::constants::*;
use crate::protocol::frame::{ContentHeader, Frame, encode_publish_direct};
use crate::protocol::method::*;
use crate::protocol::properties::{BasicProperties, parse_basic_properties};
use crate::protocol::types::FieldTable;
use crate::queue::Queue;

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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
    pub fn amqp_reply_code(&self) -> Option<AmqpReplyCode> {
        match self {
            Self::Closed { code, .. } => AmqpReplyCode::from_code(*code),
            _ => None,
        }
    }

    /// True if the close is a soft (channel-level) error.
    #[must_use]
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
pub(crate) struct ChannelInner {
    confirm_pending: VecDeque<(u64, oneshot::Sender<bool>, Option<OwnedSemaphorePermit>)>,
    confirm_next_seq: u64,
    confirms_enabled: bool,
    confirm_semaphore: Option<Arc<Semaphore>>,
    pub(crate) consumers: HashMap<CompactString, mpsc::UnboundedSender<RawDelivery>>,
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
    delivery_rxs: HashMap<CompactString, mpsc::UnboundedReceiver<RawDelivery>>,
    event_tx: broadcast::Sender<ChannelEvent>,
    return_rx: mpsc::UnboundedReceiver<ReturnedMessage>,
    /// Set to `true` after an explicit `close()` call.
    explicitly_closed: bool,
}

pub(crate) struct RawDelivery {
    pub(crate) consumer_tag: CompactString,
    pub(crate) delivery_tag: u64,
    pub(crate) redelivered: bool,
    pub(crate) exchange: CompactString,
    pub(crate) routing_key: CompactString,
    pub(crate) header: ContentHeader,
    pub(crate) body: Bytes,
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
            explicitly_closed: false,
        }
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    pub(crate) fn acker(&self) -> Acker {
        Acker::new(self.id, self.writer_tx.clone())
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

    /// Handle to the default (nameless) exchange.
    pub fn default_exchange(&mut self) -> Exchange<'_> {
        Exchange::new(self, "".into())
    }

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

    /// Handle to a named queue, delegating operations to this channel.
    pub fn queue(&mut self, name: &str) -> Queue<'_> {
        Queue::new(self, name.into())
    }

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
            Method::QueueUnbindOk => {
                self.topology
                    .lock()
                    .await
                    .remove_queue_binding(queue, exchange, routing_key);
                Ok(())
            }
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

    /// Set per-consumer prefetch (`global: false`).
    pub async fn basic_qos(&mut self, prefetch_count: u16) -> Result<(), ConnectionError> {
        self.basic_qos_global(prefetch_count, false).await
    }

    /// Set prefetch with explicit `global` flag.
    ///
    /// When `global` is `false`, the limit applies per new consumer on this channel.
    /// When `global` is `true`, the limit is shared across all consumers on this channel.
    pub async fn basic_qos_global(
        &mut self,
        prefetch_count: u16,
        global: bool,
    ) -> Result<(), ConnectionError> {
        let method = Method::BasicQos(Box::new(BasicQosArgs {
            prefetch_size: 0,
            prefetch_count,
            global,
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

        let max_body = if self.frame_max == 0 {
            body.len()
        } else {
            (self.frame_max as usize).saturating_sub(FRAME_OVERHEAD)
        };

        // Encode directly: avoid intermediary allocations on the hot path.
        let estimate = 128 + body.len() + FRAME_OVERHEAD * 3;
        let mut buf = Vec::with_capacity(estimate);
        encode_publish_direct(
            &mut buf,
            self.id,
            exchange,
            routing_key,
            opts.mandatory,
            &opts.properties,
            body,
            max_body,
        )
        .map_err(ConnectionError::Protocol)?;

        let raw = Bytes::from(buf);

        // Fast path: skip both mutex locks when confirms are not enabled.
        let confirm = {
            let mut inner = self.inner.lock().await;
            if inner.confirms_enabled {
                let permit = match inner.confirm_semaphore.clone() {
                    Some(sem) => {
                        // Must release lock before the potentially-blocking acquire.
                        drop(inner);
                        let permit = sem
                            .acquire_owned()
                            .await
                            .map_err(|_| ConnectionError::ChannelNotOpen)?;
                        inner = self.inner.lock().await;
                        Some(permit)
                    }
                    None => None,
                };
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
            .send(WriterCommand::SendRaw(raw))
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

    /// Internal helper backing
    /// [`Queue::subscribe_with`](crate::queue::Queue::subscribe_with).
    pub(crate) async fn start_consumer_with<F, Fut>(
        &mut self,
        queue: &str,
        opts: SubscribeOptions,
        handler: F,
    ) -> Result<ConsumerHandle, ConnectionError>
    where
        F: FnMut(Delivery) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        let (consume_opts, tag) = self.prepare_subscribe(opts).await?;
        self.basic_consume_with(queue, &tag, consume_opts, ClosureHandler { f: handler })
            .await
    }

    /// Internal helper backing
    /// [`Queue::subscribe`](crate::queue::Queue::subscribe).
    pub(crate) async fn start_consumer(
        &mut self,
        queue: &str,
        opts: SubscribeOptions,
    ) -> Result<Consumer, ConnectionError> {
        let (consume_opts, tag) = self.prepare_subscribe(opts).await?;
        let actual_tag = self.basic_consume(queue, &tag, consume_opts).await?;
        let delivery_rx = self
            .delivery_rxs
            .remove(&actual_tag)
            .ok_or(ConnectionError::ChannelNotOpen)?;
        Ok(Consumer::new(
            actual_tag,
            self.id,
            self.writer_tx.clone(),
            self.inner.clone(),
            delivery_rx,
            self.acker(),
        ))
    }

    /// Apply prefetch and resolve a non-empty consumer tag. A non-empty tag is
    /// required so that `basic_consume` pre-registers the delivery sender
    /// before the RPC, closing a race where the broker's first delivery would
    /// otherwise land before the post-`BasicConsumeOk` insert.
    async fn prepare_subscribe(
        &mut self,
        opts: SubscribeOptions,
    ) -> Result<(ConsumeOptions, CompactString), ConnectionError> {
        if let Some(prefetch) = opts.prefetch {
            self.basic_qos(prefetch).await?;
        }
        let tag = match opts.consumer_tag {
            Some(t) if !t.is_empty() => t,
            _ => generate_consumer_tag(),
        };
        Ok((opts.consume, tag))
    }

    /// Consume via a [`DeliveryHandler`] trait object (background task).
    pub async fn basic_consume_with(
        &mut self,
        queue: &str,
        consumer_tag: &str,
        opts: ConsumeOptions,
        mut consumer: impl DeliveryHandler,
    ) -> Result<ConsumerHandle, ConnectionError> {
        let tag = self.basic_consume(queue, consumer_tag, opts).await?;
        consumer.handle_consume_ok(&tag);

        let (cancel_tx, mut cancel_rx) = oneshot::channel();
        let mut delivery_rx = self
            .delivery_rxs
            .remove(&tag)
            .ok_or(ConnectionError::ChannelNotOpen)?;

        let tag_clone = tag.clone();
        let acker = self.acker();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut cancel_rx => break,
                    delivery = delivery_rx.recv() => {
                        match delivery {
                            Some(raw) => {
                                let delivery = Delivery::from_raw(raw, acker.clone());
                                if consumer.handle_delivery(delivery).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        Ok(ConsumerHandle::new(
            tag_clone,
            cancel_tx,
            self.id,
            self.writer_tx.clone(),
            self.inner.clone(),
        ))
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
                Ok(Some(Delivery::from_raw(raw, self.acker())))
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
    // Acknowledgements
    //

    /// Acknowledge a single delivery.
    pub async fn ack(&self, delivery_tag: u64) -> Result<(), ConnectionError> {
        self.basic_ack(delivery_tag, false).await
    }

    /// Acknowledge all deliveries up to and including `delivery_tag`.
    pub async fn ack_multiple(&self, delivery_tag: u64) -> Result<(), ConnectionError> {
        self.basic_ack(delivery_tag, true).await
    }

    /// Negatively acknowledge all deliveries up to and including `delivery_tag`
    /// and requeue them.
    pub async fn nack_multiple(&self, delivery_tag: u64) -> Result<(), ConnectionError> {
        self.basic_nack(delivery_tag, true, true).await
    }

    /// Reject a single delivery and requeue it.
    pub async fn reject(&self, delivery_tag: u64) -> Result<(), ConnectionError> {
        self.basic_reject(delivery_tag, true).await
    }

    /// Reject a single delivery and discard (dead-letter) it.
    pub async fn discard(&self, delivery_tag: u64) -> Result<(), ConnectionError> {
        self.basic_reject(delivery_tag, false).await
    }

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
        self.explicitly_closed = true;

        // Drain pending confirms so publishers get a clean PublishNacked
        // instead of an opaque ChannelNotOpen error when the sender is dropped.
        {
            let mut inner = self.inner.lock().await;
            while let Some((_seq, tx, _permit)) = inner.confirm_pending.pop_front() {
                let _ = tx.send(false);
            }
        }

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

impl Drop for Channel {
    fn drop(&mut self) {
        if self.explicitly_closed {
            return;
        }
        // Best-effort channel.close so the broker releases resources promptly.
        let frame = Frame::Method(
            self.id,
            Box::new(Method::ChannelClose(Box::new(ChannelCloseArgs {
                reply_code: 200,
                reply_text: "Normal shutdown".into(),
                class_id: 0,
                method_id: 0,
            }))),
        );
        let _ = self.writer_tx.try_send(WriterCommand::SendFrame(frame));
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

                    // Broker-initiated cancel: per AMQP 0-9-1 the client must
                    // not reply. Reclaim the consumer entry so any in-flight
                    // delivery is dropped instead of routed.
                    Method::BasicCancel(args) => {
                        inner
                            .lock()
                            .await
                            .consumers
                            .remove(args.consumer_tag.as_str());
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
                        // Drain pending confirms so publishers don't hang
                        {
                            let mut inner = inner.lock().await;
                            while let Some((_seq, tx, _permit)) = inner.confirm_pending.pop_front()
                            {
                                let _ = tx.send(false);
                            }
                        }
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
                            accumulated: Vec::with_capacity(
                                usize::try_from(body_size).unwrap_or(usize::MAX),
                            ),
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
                    let is_single_frame = accumulated.is_empty() && data.len() as u64 >= body_size;
                    if !is_single_frame {
                        accumulated.extend_from_slice(&data);
                    }
                    if (is_single_frame || accumulated.len() as u64 >= body_size)
                        && let ContentState::AwaitingBody {
                            info,
                            header,
                            accumulated,
                            ..
                        } = mem::replace(&mut state, ContentState::Idle)
                    {
                        // Zero-copy for single-frame bodies; assembled copy otherwise.
                        let body = if is_single_frame {
                            data
                        } else {
                            Bytes::from(accumulated)
                        };
                        let delivery = RawDelivery {
                            consumer_tag: info.target.consumer_tag(),
                            delivery_tag: info.delivery_tag,
                            redelivered: info.redelivered,
                            exchange: info.exchange,
                            routing_key: info.routing_key,
                            header,
                            body,
                        };
                        dispatch_delivery(&inner, &info.target, delivery).await;
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
            // Try the send; if the receiver is gone (the user dropped the
            // Consumer / ConsumerHandle), reclaim the entry now.
            let mut stale = false;
            if let Some(tx) = guard.consumers.get(tag.as_str()) {
                if tx.send(delivery).is_err() {
                    stale = true;
                }
            } else {
                tracing::warn!(consumer_tag = %tag, "delivery for unknown consumer, dropping");
            }
            if stale {
                guard.consumers.remove(tag.as_str());
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

/// Async-closure adapter so closures can stand in for the [`DeliveryHandler`] trait.
struct ClosureHandler<F> {
    f: F,
}

impl<F, Fut> DeliveryHandler for ClosureHandler<F>
where
    F: FnMut(Delivery) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), ConnectionError>> + Send + 'static,
{
    fn handle_delivery(
        &mut self,
        delivery: Delivery,
    ) -> impl std::future::Future<Output = Result<(), ConnectionError>> + Send {
        (self.f)(delivery)
    }
}

/// Generates a client-side consumer tag of the form `bunny-rs-<unix_ms>-<rand>`.
pub(crate) fn generate_consumer_tag() -> CompactString {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let suffix: u32 = fastrand::u32(..);
    compact_str::format_compact!("bunny-rs-{ms}-{suffix}")
}

fn parse_properties_raw(raw: &Bytes) -> BasicProperties {
    if raw.is_empty() {
        BasicProperties::default()
    } else {
        parse_basic_properties(raw)
            .map(|(_, p)| p)
            .unwrap_or_default()
    }
}

impl Delivery {
    pub(crate) fn from_raw(raw: RawDelivery, acker: Acker) -> Self {
        Self {
            consumer_tag: raw.consumer_tag,
            delivery_tag: raw.delivery_tag,
            redelivered: raw.redelivered,
            exchange: raw.exchange,
            routing_key: raw.routing_key,
            body: raw.body,
            properties_raw: raw.header.properties_raw,
            properties_cache: OnceLock::new(),
            acker,
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

impl QueueInfo {
    #[must_use]
    pub fn message_count(&self) -> u32 {
        self.message_count
    }

    #[must_use]
    pub fn consumer_count(&self) -> u32 {
        self.consumer_count
    }
}

/// Cheap, cloneable handle for acknowledging deliveries without holding a
/// `Channel` reference. Restricted to a specific channel by design: delivery
/// tags from other channels must not be passed.
#[derive(Debug, Clone)]
pub struct Acker {
    channel_id: u16,
    writer_tx: WriterTx,
}

impl Acker {
    pub(crate) fn new(channel_id: u16, writer_tx: WriterTx) -> Self {
        Self {
            channel_id,
            writer_tx,
        }
    }

    async fn send(&self, method: Method) -> Result<(), ConnectionError> {
        let frame = Frame::Method(self.channel_id, Box::new(method));
        self.writer_tx
            .send(WriterCommand::SendFrame(frame))
            .await
            .map_err(|_| ConnectionError::NotConnected)
    }

    pub async fn ack(&self, delivery_tag: u64, multiple: bool) -> Result<(), ConnectionError> {
        self.send(Method::BasicAck {
            delivery_tag,
            multiple,
        })
        .await
    }

    pub async fn nack(
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

    pub async fn reject(&self, delivery_tag: u64, requeue: bool) -> Result<(), ConnectionError> {
        self.send(Method::BasicReject(Box::new(BasicRejectArgs {
            delivery_tag,
            requeue,
        })))
        .await
    }
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
    acker: Acker,
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

    /// Acknowledge this single delivery.
    pub async fn ack(&self) -> Result<(), ConnectionError> {
        self.acker.ack(self.delivery_tag, false).await
    }

    /// Acknowledge this delivery and all earlier unacknowledged deliveries
    /// on the same channel.
    pub async fn ack_multiple(&self) -> Result<(), ConnectionError> {
        self.acker.ack(self.delivery_tag, true).await
    }

    /// Negatively acknowledge this delivery and requeue it.
    pub async fn nack(&self) -> Result<(), ConnectionError> {
        self.acker.nack(self.delivery_tag, false, true).await
    }

    /// Negatively acknowledge this delivery and all earlier unacknowledged
    /// deliveries on the same channel; requeue them.
    pub async fn nack_multiple(&self) -> Result<(), ConnectionError> {
        self.acker.nack(self.delivery_tag, true, true).await
    }

    /// Reject this delivery and requeue it.
    pub async fn reject(&self) -> Result<(), ConnectionError> {
        self.acker.reject(self.delivery_tag, true).await
    }

    /// Reject this delivery and discard it (dead-letter if configured).
    pub async fn discard(&self) -> Result<(), ConnectionError> {
        self.acker.reject(self.delivery_tag, false).await
    }

    /// Clone the [`Acker`] for use after the delivery is consumed (for
    /// example, when handing the body off to a worker task).
    pub fn acker(&self) -> Acker {
        self.acker.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::generate_consumer_tag;
    use std::collections::HashSet;

    #[test]
    fn generate_consumer_tag_format() {
        let tag = generate_consumer_tag();
        assert!(tag.starts_with("bunny-rs-"));
        let rest = &tag["bunny-rs-".len()..];
        let (ms, suffix) = rest.split_once('-').unwrap();
        assert!(ms.parse::<u64>().is_ok());
        assert!(suffix.parse::<u32>().is_ok());
    }

    #[test]
    fn generate_consumer_tag_is_unique_under_load() {
        // 1000 invocations, 32 bits of randomness: birthday-paradox collision
        // probability is ~1.16e-4. A flake here is a real entropy regression.
        const N: usize = 1000;
        let mut seen = HashSet::with_capacity(N);
        for _ in 0..N {
            assert!(seen.insert(generate_consumer_tag()));
        }
    }
}
