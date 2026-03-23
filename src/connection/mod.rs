// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

pub mod endpoint;
mod handshake;
pub use handshake::negotiate_heartbeat;
pub(crate) mod recovery;
pub mod topology;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio_util::codec::{Decoder, Encoder};

use crate::credentials::Password;
use crate::errors::ProtocolError;
use crate::protocol::codec::AmqpCodec;
use crate::protocol::constants::*;
use crate::protocol::frame::Frame;
use crate::protocol::method::Method;
use crate::protocol::types::FieldTable;
use crate::transport::Transport;

#[derive(Clone)]
pub struct ConnectionOptions {
    pub host: String,
    pub port: u16,
    /// Multiple endpoints for redundancy. When non-empty, these take precedence
    /// over `host`/`port`. Tried sequentially on initial connection, shuffled
    /// on recovery.
    pub endpoints: Vec<endpoint::Endpoint>,
    pub username: String,
    pub password: Password,
    pub virtual_host: String,
    pub heartbeat: u16,
    pub channel_max: u16,
    pub frame_max: u32,
    pub connection_name: Option<String>,
    #[cfg(feature = "tls")]
    pub tls: Option<crate::transport::tls::TlsOptions>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: 5672,
            endpoints: Vec::new(),
            username: "guest".into(),
            password: Password::from("guest"),
            virtual_host: "/".into(),
            heartbeat: DEFAULT_HEARTBEAT,
            channel_max: DEFAULT_CHANNEL_MAX,
            frame_max: DEFAULT_FRAME_MAX,
            connection_name: None,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
}

impl ConnectionOptions {
    pub fn from_uri(uri: &str) -> Result<Self, ConnectionError> {
        #[allow(unused_mut)]
        let mut uri_buf = uri.to_string();
        let result = Self::parse_uri(&uri_buf);
        #[cfg(feature = "zeroize")]
        zeroize::Zeroize::zeroize(&mut uri_buf);
        result
    }

    /// Build connection options from multiple AMQP URIs.
    ///
    /// Credentials, virtual host, and TLS scheme are taken from the first URI.
    /// Host and port from each URI become the endpoint list.
    ///
    /// ```ignore
    /// let opts = ConnectionOptions::from_uris(&[
    ///     "amqp://guest:guest@rabbit1:5672/",
    ///     "amqp://guest:guest@rabbit2:5672/",
    ///     "amqp://guest:guest@rabbit3:5672/",
    /// ])?;
    /// ```
    pub fn from_uris(uris: &[&str]) -> Result<Self, ConnectionError> {
        if uris.is_empty() {
            return Err(ConnectionError::InvalidUri(
                "at least one URI is required".into(),
            ));
        }

        #[allow(unused_mut)]
        let mut uri_bufs: Vec<String> = uris.iter().map(|s| s.to_string()).collect();

        // Parse the first URI for credentials, vhost, TLS, and first endpoint
        let result = (|| {
            let mut opts = Self::parse_uri(&uri_bufs[0])?;

            if uri_bufs.len() > 1 {
                let first_ep = endpoint::Endpoint::new(opts.host.clone(), opts.port);
                let mut endpoints = vec![first_ep];

                for uri_str in &uri_bufs[1..] {
                    let parsed = url::Url::parse(uri_str)
                        .map_err(|e| ConnectionError::InvalidUri(e.to_string()))?;
                    let is_tls = parsed.scheme() == "amqps";
                    let default_port = if is_tls { 5671 } else { 5672 };
                    let host = parsed.host_str().unwrap_or("localhost").to_string();
                    let port = parsed.port().unwrap_or(default_port);
                    endpoints.push(endpoint::Endpoint::new(host, port));
                }

                opts.endpoints = endpoints;
            }

            Ok(opts)
        })();

        #[cfg(feature = "zeroize")]
        for buf in &mut uri_bufs {
            zeroize::Zeroize::zeroize(buf);
        }

        result
    }

    fn parse_uri(uri: &str) -> Result<Self, ConnectionError> {
        let parsed =
            url::Url::parse(uri).map_err(|e| ConnectionError::InvalidUri(e.to_string()))?;

        let is_tls = parsed.scheme() == "amqps";
        let default_port = if is_tls { 5671 } else { 5672 };

        let host = parsed.host_str().unwrap_or("localhost").to_string();
        let port = parsed.port().unwrap_or(default_port);
        let username = if parsed.username().is_empty() {
            "guest".to_string()
        } else {
            percent_decode(parsed.username())
        };
        let password = Password::from(
            parsed
                .password()
                .map(percent_decode)
                .unwrap_or_else(|| "guest".to_string()),
        );
        let virtual_host = if parsed.path().is_empty() || parsed.path() == "/" {
            "/".to_string()
        } else {
            percent_decode(&parsed.path()[1..])
        };

        #[cfg(feature = "tls")]
        let tls = if is_tls {
            Some(crate::transport::tls::TlsOptions::new(&host).map_err(ConnectionError::Io)?)
        } else {
            None
        };

        Ok(Self {
            host,
            port,
            username,
            password,
            virtual_host,
            #[cfg(feature = "tls")]
            tls,
            ..Default::default()
        })
    }
}

impl ConnectionOptions {
    /// Returns the effective endpoint list. If `endpoints` is non-empty, use it.
    /// Otherwise, synthesize a single endpoint from `host` and `port`.
    pub(crate) fn effective_endpoints(&self) -> Vec<endpoint::Endpoint> {
        if self.endpoints.is_empty() {
            vec![endpoint::Endpoint::new(self.host.clone(), self.port)]
        } else {
            self.endpoints.clone()
        }
    }
}

fn percent_decode(s: &str) -> String {
    url::form_urlencoded::parse(s.as_bytes())
        .map(|(k, _)| k.into_owned())
        .next()
        .unwrap_or_else(|| s.to_string())
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("authentication failed")]
    AuthenticationFailed,

    #[error("invalid AMQP URI: {0}")]
    InvalidUri(String),

    #[error("not connected")]
    NotConnected,

    #[error("channel not open")]
    ChannelNotOpen,

    #[error("operation timed out")]
    Timeout,

    #[error("connection closed by peer: {code} {text}")]
    ConnectionClosed { code: u16, text: String },

    #[error("channel closed by peer: {code} {text}")]
    ChannelClosed {
        code: u16,
        text: String,
        class_id: u16,
        method_id: u16,
    },

    #[error("publish was nacked by broker")]
    PublishNacked,

    #[error("unexpected method received")]
    UnexpectedMethod,
}

pub(crate) type WriterTx = mpsc::Sender<WriterCommand>;

pub(crate) enum WriterCommand {
    SendFrame(Frame),
    SendFrames(Vec<Frame>),
}

/// Lifecycle events emitted by a [`Connection`].
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    RecoveryStarted,
    RecoverySucceeded,
    RecoveryFailed(String),
    Blocked(String),
    Unblocked,
    QueueNameChanged { old: String, new: String },
    ConsumerTagChanged { old: String, new: String },
}

/// Controls which topology entities are replayed during recovery.
/// Entities filtered out are skipped, not removed from the canonical topology.
#[allow(clippy::type_complexity)]
pub struct TopologyRecoveryFilter {
    pub exchange_filter: Option<Box<dyn Fn(&topology::RecordedExchange) -> bool + Send + Sync>>,
    pub queue_filter: Option<Box<dyn Fn(&topology::RecordedQueue) -> bool + Send + Sync>>,
    pub queue_binding_filter:
        Option<Box<dyn Fn(&topology::RecordedQueueBinding) -> bool + Send + Sync>>,
    pub exchange_binding_filter:
        Option<Box<dyn Fn(&topology::RecordedExchangeBinding) -> bool + Send + Sync>>,
    pub consumer_filter: Option<Box<dyn Fn(&topology::RecordedConsumer) -> bool + Send + Sync>>,
}

struct ChannelSlot {
    rpc_tx: mpsc::UnboundedSender<Frame>,
}

struct ConnectionInner {
    is_open: AtomicBool,
    writer_tx: Mutex<WriterTx>,
    channels: Mutex<HashMap<u16, ChannelSlot>>,
    next_channel_id: Mutex<u16>,
    negotiated_frame_max: u32,
    negotiated_channel_max: u16,
    negotiated_heartbeat: u16,
    server_properties: FieldTable,
    blocked_reason: Mutex<Option<String>>,
    topology: Arc<Mutex<topology::TopologyRegistry>>,
    opts: ConnectionOptions,
    resolver: endpoint::AddressResolver,
    recovery_config: recovery::RecoveryConfig,
    recovery_filter: Mutex<Option<TopologyRecoveryFilter>>,
    event_tx: broadcast::Sender<ConnectionEvent>,
    last_sent_activity: AtomicU64,
    /// Incremented on each recovery so that writer and heartbeat loops
    /// from a previous connection notice the change and exit.
    connection_recovery_epoch: AtomicU64,
}

/// AMQP 0-9-1 connection to a RabbitMQ node.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

impl Connection {
    pub async fn open(opts: ConnectionOptions) -> Result<Self, ConnectionError> {
        let resolver = endpoint::AddressResolver::from_endpoints(opts.effective_endpoints());
        // Initial connection: resolve but do NOT shuffle
        let endpoints = resolver.resolve().await;
        let transport = Self::connect_to_first(&endpoints, &opts).await?;
        Self::open_with_transport(transport, resolver, opts).await
    }

    pub async fn from_uri(uri: &str) -> Result<Self, ConnectionError> {
        let opts = ConnectionOptions::from_uri(uri)?;
        Self::open(opts).await
    }

    /// Connect using multiple AMQP URIs for redundancy.
    ///
    /// Credentials, virtual host, and TLS scheme are taken from the first URI.
    /// Host and port from each URI become the endpoint list.
    pub async fn from_uris(uris: &[&str]) -> Result<Self, ConnectionError> {
        let opts = ConnectionOptions::from_uris(uris)?;
        Self::open(opts).await
    }

    /// Try each endpoint sequentially until one succeeds.
    async fn connect_to_first(
        endpoints: &[endpoint::Endpoint],
        opts: &ConnectionOptions,
    ) -> Result<Transport, ConnectionError> {
        let mut last_err = ConnectionError::NotConnected;
        for ep in endpoints {
            match Self::try_connect_endpoint(ep, opts).await {
                Ok(transport) => return Ok(transport),
                Err(e) => {
                    tracing::debug!(host = %ep.host, port = ep.port, error = %e, "endpoint unreachable");
                    last_err = e;
                }
            }
        }
        Err(last_err)
    }

    /// Connect TCP (+ optional TLS) to a single endpoint.
    pub(crate) async fn try_connect_endpoint(
        ep: &endpoint::Endpoint,
        #[allow(unused_variables)] opts: &ConnectionOptions,
    ) -> Result<Transport, ConnectionError> {
        let tcp = TcpStream::connect(ep.addr_string()).await?;
        tcp.set_nodelay(true)?;

        #[cfg(feature = "tls")]
        let transport = if let Some(ref tls) = opts.tls {
            tls.connect(tcp).await?
        } else {
            Transport::Tcp(tcp)
        };
        #[cfg(not(feature = "tls"))]
        let transport = Transport::Tcp(tcp);

        Ok(transport)
    }

    async fn open_with_transport(
        mut transport: Transport,
        resolver: endpoint::AddressResolver,
        opts: ConnectionOptions,
    ) -> Result<Self, ConnectionError> {
        transport.write_all(PROTOCOL_HEADER).await?;
        transport.flush().await?;

        let mut read_buf = BytesMut::with_capacity(8192);
        let mut codec = AmqpCodec::new(opts.frame_max);

        let negotiated =
            handshake::perform(&mut transport, &mut read_buf, &mut codec, &opts).await?;

        let (reader, writer) = tokio::io::split(transport);
        let (writer_tx, writer_rx) = mpsc::channel::<WriterCommand>(128);

        let inner = Arc::new(ConnectionInner {
            is_open: AtomicBool::new(true),
            writer_tx: Mutex::new(writer_tx.clone()),
            channels: Mutex::new(HashMap::new()),
            next_channel_id: Mutex::new(1),
            negotiated_frame_max: negotiated.frame_max,
            negotiated_channel_max: negotiated.channel_max,
            negotiated_heartbeat: negotiated.heartbeat,
            server_properties: negotiated.server_properties,
            blocked_reason: Mutex::new(None),
            topology: Arc::new(Mutex::new(topology::TopologyRegistry::default())),
            opts,
            resolver,
            recovery_config: recovery::RecoveryConfig::default(),
            recovery_filter: Mutex::new(None),
            event_tx: broadcast::channel(16).0,
            last_sent_activity: AtomicU64::new(monotonic_millis()),
            connection_recovery_epoch: AtomicU64::new(0),
        });

        let generation = inner.connection_recovery_epoch.load(Ordering::Relaxed);
        let writer_inner = inner.clone();
        tokio::spawn(async move {
            writer_loop(writer, writer_rx, &writer_inner).await;
            // Skip if recovery has already established a newer connection.
            if writer_inner.connection_recovery_epoch.load(Ordering::Relaxed) == generation {
                writer_inner.is_open.store(false, Ordering::Release);
            }
        });

        let reader_inner = inner.clone();
        tokio::spawn(async move {
            reader_loop(reader, read_buf, codec, reader_inner, generation).await;
        });

        if negotiated.heartbeat > 0 {
            let hb_inner = inner.clone();
            let interval_secs = negotiated.heartbeat as u64;
            tokio::spawn(async move {
                heartbeat_loop(hb_inner, interval_secs, generation).await;
            });
        }

        Ok(Connection { inner })
    }

    pub fn is_open(&self) -> bool {
        self.inner.is_open.load(Ordering::Acquire)
    }

    /// Subscribe to lifecycle events.
    pub fn events(&self) -> broadcast::Receiver<ConnectionEvent> {
        self.inner.event_tx.subscribe()
    }

    /// Server properties from `connection.start`.
    pub fn server_properties(&self) -> &FieldTable {
        &self.inner.server_properties
    }

    /// Negotiated `frame_max` (bytes).
    pub fn frame_max(&self) -> u32 {
        self.inner.negotiated_frame_max
    }

    /// Negotiated `channel_max`.
    pub fn channel_max(&self) -> u16 {
        self.inner.negotiated_channel_max
    }

    /// Negotiated heartbeat interval (seconds).
    pub fn heartbeat(&self) -> u16 {
        self.inner.negotiated_heartbeat
    }

    /// True if the broker has sent `connection.blocked`.
    pub async fn is_blocked(&self) -> bool {
        self.inner.blocked_reason.lock().await.is_some()
    }

    /// Reason from `connection.blocked`, or `None`.
    pub async fn blocked_reason(&self) -> Option<String> {
        self.inner.blocked_reason.lock().await.clone()
    }

    /// Filter topology entities replayed during recovery.
    pub async fn set_topology_recovery_filter(&self, filter: TopologyRecoveryFilter) {
        *self.inner.recovery_filter.lock().await = Some(filter);
    }

    pub async fn open_channel(&self) -> Result<crate::channel::Channel, ConnectionError> {
        if !self.is_open() {
            return Err(ConnectionError::NotConnected);
        }

        let channel_id = {
            let mut next = self.inner.next_channel_id.lock().await;
            if *next == 0 {
                *next = 1;
            }
            let id = *next;
            *next = next.wrapping_add(1);
            id
        };

        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();

        {
            let mut channels = self.inner.channels.lock().await;
            channels.insert(channel_id, ChannelSlot { rpc_tx });
        }

        // Send channel.open
        let open_frame = Frame::Method(channel_id, Box::new(Method::ChannelOpen));
        self.inner
            .writer_tx
            .lock()
            .await
            .send(WriterCommand::SendFrame(open_frame))
            .await
            .map_err(|_| ConnectionError::NotConnected)?;

        // Wait for channel.open-ok
        let mut rpc_rx = rpc_rx;
        let response = tokio::time::timeout(Duration::from_secs(30), rpc_rx.recv())
            .await
            .map_err(|_| ConnectionError::Timeout)?
            .ok_or(ConnectionError::NotConnected)?;

        match response {
            Frame::Method(_, method) if matches!(*method, Method::ChannelOpenOk) => {}
            Frame::Method(_, method) => {
                if let Method::ChannelClose(ref args) = *method {
                    return Err(ConnectionError::ChannelClosed {
                        code: args.reply_code,
                        text: args.reply_text.to_string(),
                        class_id: args.class_id,
                        method_id: args.method_id,
                    });
                }
                return Err(ConnectionError::UnexpectedMethod);
            }
            _ => return Err(ConnectionError::UnexpectedMethod),
        }

        let writer_tx = self.inner.writer_tx.lock().await.clone();
        Ok(crate::channel::Channel::new(
            channel_id,
            writer_tx,
            rpc_rx,
            self.inner.negotiated_frame_max,
            self.inner.topology.clone(),
        ))
    }

    pub async fn close(&self) -> Result<(), ConnectionError> {
        if !self.is_open() {
            return Ok(());
        }

        let close_frame = Frame::Method(
            0,
            Box::new(Method::ConnectionClose(Box::new(
                crate::protocol::method::ConnectionCloseArgs {
                    reply_code: 200,
                    reply_text: "Normal shutdown".into(),
                    class_id: 0,
                    method_id: 0,
                },
            ))),
        );

        let _ = self
            .inner
            .writer_tx
            .lock()
            .await
            .send(WriterCommand::SendFrame(close_frame))
            .await;

        self.inner.is_open.store(false, Ordering::Release);
        Ok(())
    }
}

async fn writer_loop(
    mut writer: tokio::io::WriteHalf<Transport>,
    mut rx: mpsc::Receiver<WriterCommand>,
    inner: &ConnectionInner,
) {
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut codec = AmqpCodec::new_framing(DEFAULT_FRAME_MAX);

    loop {
        let Some(cmd) = rx.recv().await else { break };
        encode_command(&cmd, &mut codec, &mut buf);

        // Coalesce queued writes
        let mut batch_count = 1usize;
        while batch_count < 256 {
            match rx.try_recv() {
                Ok(cmd) => {
                    encode_command(&cmd, &mut codec, &mut buf);
                    batch_count += 1;
                }
                Err(_) => break,
            }
        }

        if writer.write_all(&buf).await.is_err() {
            break;
        }
        if writer.flush().await.is_err() {
            break;
        }
        inner
            .last_sent_activity
            .store(monotonic_millis(), Ordering::Relaxed);
        buf.clear();
    }
}

fn encode_command(cmd: &WriterCommand, codec: &mut AmqpCodec, buf: &mut BytesMut) {
    match cmd {
        WriterCommand::SendFrame(frame) => {
            let _ = codec.encode(frame.clone(), buf);
        }
        WriterCommand::SendFrames(frames) => {
            for frame in frames {
                let _ = codec.encode(frame.clone(), buf);
            }
        }
    }
}

fn reader_loop(
    reader: tokio::io::ReadHalf<Transport>,
    read_buf: BytesMut,
    codec: AmqpCodec,
    inner: Arc<ConnectionInner>,
    generation: u64,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(reader_loop_impl(reader, read_buf, codec, inner, generation))
}

async fn reader_loop_impl(
    reader: tokio::io::ReadHalf<Transport>,
    read_buf: BytesMut,
    codec: AmqpCodec,
    inner: Arc<ConnectionInner>,
    generation: u64,
) {
    read_frames(reader, read_buf, codec, &inner).await;

    // A newer connection was established via recovery; this reader is stale.
    if inner.connection_recovery_epoch.load(Ordering::Relaxed) != generation {
        return;
    }

    // Connection lost. Attempt recovery if enabled.
    inner.is_open.store(false, Ordering::Release);

    if !inner.recovery_config.enabled {
        return;
    }

    tracing::info!("connection lost, starting recovery");
    let _ = inner.event_tx.send(ConnectionEvent::RecoveryStarted);
    // Snapshot topology under lock
    let mut topology_snapshot = {
        let topo = inner.topology.lock().await;
        topology::TopologyRegistry {
            exchanges: topo
                .exchanges
                .iter()
                .map(|e| topology::RecordedExchange {
                    name: e.name.clone(),
                    kind: e.kind.clone(),
                    opts: e.opts.clone(),
                })
                .collect(),
            queues: topo
                .queues
                .iter()
                .map(|q| topology::RecordedQueue {
                    name: q.name.clone(),
                    opts: q.opts.clone(),
                    server_named: q.server_named,
                })
                .collect(),
            queue_bindings: topo
                .queue_bindings
                .iter()
                .map(|b| topology::RecordedQueueBinding {
                    queue: b.queue.clone(),
                    exchange: b.exchange.clone(),
                    routing_key: b.routing_key.clone(),
                    arguments: b.arguments.clone(),
                })
                .collect(),
            exchange_bindings: topo
                .exchange_bindings
                .iter()
                .map(|b| topology::RecordedExchangeBinding {
                    destination: b.destination.clone(),
                    source: b.source.clone(),
                    routing_key: b.routing_key.clone(),
                    arguments: b.arguments.clone(),
                })
                .collect(),
            consumers: topo
                .consumers
                .iter()
                .map(|c| topology::RecordedConsumer {
                    channel_id: c.channel_id,
                    queue: c.queue.clone(),
                    consumer_tag: c.consumer_tag.clone(),
                    opts: c.opts.clone(),
                })
                .collect(),
        }
    };
    // Apply recovery filter to snapshot
    {
        let filter_guard = inner.recovery_filter.lock().await;
        if let Some(ref filter) = *filter_guard {
            if let Some(ref f) = filter.exchange_filter {
                topology_snapshot.exchanges.retain(f);
            }
            if let Some(ref f) = filter.queue_filter {
                topology_snapshot.queues.retain(f);
            }
            if let Some(ref f) = filter.queue_binding_filter {
                topology_snapshot.queue_bindings.retain(f);
            }
            if let Some(ref f) = filter.exchange_binding_filter {
                topology_snapshot.exchange_bindings.retain(f);
            }
            if let Some(ref f) = filter.consumer_filter {
                topology_snapshot.consumers.retain(f);
            }
        }
    }
    let recovered = recovery::attempt_recovery(
        &inner.opts,
        &inner.resolver,
        &mut topology_snapshot,
        &inner.recovery_config,
        &inner.event_tx,
    )
    .await;

    match recovered {
        Some((transport, name_changes)) => {
            tracing::info!("connection recovered");

            // Apply name/tag changes to canonical topology
            if !name_changes.is_empty() {
                let mut topo = inner.topology.lock().await;
                for change in &name_changes {
                    match change {
                        ConnectionEvent::QueueNameChanged { old, new } => {
                            topo.update_queue_name(old, new);
                        }
                        ConnectionEvent::ConsumerTagChanged { old, new } => {
                            topo.update_consumer_tag(old, new);
                        }
                        _ => {}
                    }
                }
            }

            let _ = inner.event_tx.send(ConnectionEvent::RecoverySucceeded);

            let (new_reader, new_writer) = tokio::io::split(transport);
            let (new_writer_tx, new_writer_rx) = mpsc::channel::<WriterCommand>(128);

            // Advance the epoch so loops from the old connection notice and exit.
            let generation = inner.connection_recovery_epoch.fetch_add(1, Ordering::Relaxed) + 1;

            // Swap writer and reset activity tracking for the fresh connection
            *inner.writer_tx.lock().await = new_writer_tx;
            inner
                .last_sent_activity
                .store(monotonic_millis(), Ordering::Relaxed);
            inner.is_open.store(true, Ordering::Release);

            let writer_inner = inner.clone();
            tokio::spawn(async move {
                writer_loop(new_writer, new_writer_rx, &writer_inner).await;
                if writer_inner.connection_recovery_epoch.load(Ordering::Relaxed) == generation {
                    writer_inner.is_open.store(false, Ordering::Release);
                }
            });
            let inner2 = inner.clone();
            let frame_max = inner2.negotiated_frame_max;
            tokio::spawn(Box::pin(async move {
                reader_loop(
                    new_reader,
                    BytesMut::with_capacity(8192),
                    AmqpCodec::new_framing(frame_max),
                    inner2,
                    generation,
                )
                .await;
            }));

            // Restart heartbeat loop for the recovered connection
            if inner.negotiated_heartbeat > 0 {
                let hb_inner = inner.clone();
                let interval_secs = inner.negotiated_heartbeat as u64;
                tokio::spawn(async move {
                    heartbeat_loop(hb_inner, interval_secs, generation).await;
                });
            }
        }
        None => {
            tracing::error!("recovery failed, connection permanently closed");
            let _ = inner
                .event_tx
                .send(ConnectionEvent::RecoveryFailed("exhausted".into()));
        }
    }
}

async fn read_frames(
    mut reader: tokio::io::ReadHalf<Transport>,
    mut read_buf: BytesMut,
    mut codec: AmqpCodec,
    inner: &ConnectionInner,
) {
    // When heartbeats are enabled, time out after 2× the negotiated interval
    // with no data from the peer (allows one missed heartbeat as slack).
    let read_timeout = if inner.negotiated_heartbeat > 0 {
        Some(Duration::from_secs(inner.negotiated_heartbeat as u64 * 2))
    } else {
        None
    };

    loop {
        loop {
            match codec.decode(&mut read_buf) {
                Ok(Some(frame)) => dispatch_frame(inner, frame).await,
                Ok(None) => break,
                Err(_) => return,
            }
        }

        let mut tmp = [0u8; 8192];
        let read_result = if let Some(timeout) = read_timeout {
            match tokio::time::timeout(timeout, reader.read(&mut tmp)).await {
                Ok(result) => result,
                Err(_) => {
                    tracing::warn!(
                        heartbeat = inner.negotiated_heartbeat,
                        "detected missed heartbeats from the server, closing connection"
                    );
                    return;
                }
            }
        } else {
            reader.read(&mut tmp).await
        };

        match read_result {
            Ok(0) => return,
            Ok(n) => read_buf.extend_from_slice(&tmp[..n]),
            Err(_) => return,
        }
    }
}

async fn dispatch_frame(inner: &ConnectionInner, frame: Frame) {
    match &frame {
        Frame::Heartbeat => {
            // Nothing to do: the socket read timeout in read_frames
            // already tracks peer liveness. Each side sends heartbeats
            // on its own timer, so we never echo them back.
            tracing::trace!("heartbeat received");
        }
        Frame::Method(0, method) => match method.as_ref() {
            Method::ConnectionClose(_args) => {
                let close_ok = Frame::Method(0, Box::new(Method::ConnectionCloseOk));
                let _ = inner
                    .writer_tx
                    .lock()
                    .await
                    .send(WriterCommand::SendFrame(close_ok))
                    .await;
                inner.is_open.store(false, Ordering::Release);
            }
            Method::ConnectionCloseOk => {
                inner.is_open.store(false, Ordering::Release);
            }
            Method::ConnectionBlocked(args) => {
                let reason = args.reason.to_string();
                *inner.blocked_reason.lock().await = Some(reason.clone());
                let _ = inner.event_tx.send(ConnectionEvent::Blocked(reason));
            }
            Method::ConnectionUnblocked => {
                *inner.blocked_reason.lock().await = None;
                let _ = inner.event_tx.send(ConnectionEvent::Unblocked);
            }
            _ => {}
        },
        Frame::Method(channel_id, _)
        | Frame::Header(channel_id, _)
        | Frame::Body(channel_id, _) => {
            let channels = inner.channels.lock().await;
            if let Some(slot) = channels.get(channel_id) {
                let _ = slot.rpc_tx.send(frame);
            }
        }
    }
}

async fn heartbeat_loop(inner: Arc<ConnectionInner>, interval_secs: u64, generation: u64) {
    let half = Duration::from_millis(interval_secs * 1000 / 2);
    loop {
        tokio::time::sleep(half).await;

        if !inner.is_open.load(Ordering::Acquire) {
            break;
        }
        // Exit if a newer heartbeat loop has been spawned (after recovery)
        if inner.connection_recovery_epoch.load(Ordering::Relaxed) != generation {
            break;
        }

        // Only send a heartbeat when the connection has been idle
        // (no writes for at least half the heartbeat interval).
        let last = inner.last_sent_activity.load(Ordering::Relaxed);
        let elapsed = monotonic_millis().saturating_sub(last);
        if elapsed >= half.as_millis() as u64 {
            let _ = inner
                .writer_tx
                .lock()
                .await
                .send(WriterCommand::SendFrame(Frame::Heartbeat))
                .await;
        }
    }
}

/// Monotonic milliseconds since process start. Immune to wall-clock
/// adjustments (NTP, leap seconds, manual changes).
fn monotonic_millis() -> u64 {
    static BASE: OnceLock<Instant> = OnceLock::new();
    let base = BASE.get_or_init(Instant::now);
    base.elapsed().as_millis() as u64
}
