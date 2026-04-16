// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::time::Duration;

use crate::errors::BoxError;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::broadcast;
use tokio_util::codec::{Decoder, Encoder};

use crate::connection::endpoint::{AddressResolver, Endpoint};
use crate::connection::handshake;
use crate::connection::topology::TopologyRegistry;
use crate::connection::{Connection, ConnectionEvent, ConnectionOptions};
use crate::protocol::codec::AmqpCodec;
use crate::protocol::constants::*;
use crate::protocol::frame::Frame;
use crate::protocol::method::*;
use crate::transport::Transport;

/// Connection recovery settings.
///
/// When the connection drops, the client will reconnect and replay the
/// recorded topology (exchanges, queues, bindings, consumers).
#[derive(Clone)]
pub struct RecoveryConfig {
    pub enabled: bool,
    /// Replay recorded topology (exchanges, queues, bindings, consumers)
    /// after reconnecting. Default: `true`.
    pub topology_recovery: bool,
    pub initial_interval: Duration,
    pub max_interval: Duration,
    pub backoff_multiplier: f64,
    /// `None` means unlimited attempts.
    pub max_attempts: Option<u32>,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            topology_recovery: true,
            initial_interval: Duration::from_secs(5),
            max_interval: Duration::from_secs(60),
            backoff_multiplier: 2.0,
            max_attempts: None,
        }
    }
}

/// Reconnect and replay topology. Returns transport + name/tag change events.
pub(crate) async fn attempt_recovery(
    opts: &ConnectionOptions,
    resolver: &AddressResolver,
    topology: &mut TopologyRegistry,
    config: &RecoveryConfig,
    event_tx: &broadcast::Sender<ConnectionEvent>,
) -> Option<(Transport, Vec<ConnectionEvent>)> {
    if !config.enabled {
        return None;
    }

    let mut interval = config.initial_interval;
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        tracing::info!(attempt, "attempting connection recovery");
        tokio::time::sleep(interval).await;

        // Refresh credentials before reconnecting (e.g. expired OAuth 2 token).
        let mut recovery_opts;
        let effective_opts = if let Some(ref provider) = opts.credentials_provider {
            recovery_opts = opts.clone();
            match provider.credentials().await {
                Ok(creds) => {
                    recovery_opts.username = creds.username;
                    recovery_opts.password = creds.password;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "credentials refresh failed during recovery");
                }
            }
            &recovery_opts
        } else {
            opts
        };

        match try_reconnect(effective_opts, resolver).await {
            Ok(mut transport) => {
                if !config.topology_recovery {
                    tracing::info!(
                        attempt,
                        "connection recovery succeeded (topology replay skipped)"
                    );
                    return Some((transport, Vec::new()));
                }
                match replay_topology(&mut transport, effective_opts, topology, event_tx).await {
                    Ok(changes) => {
                        tracing::info!(attempt, "connection recovery succeeded");
                        return Some((transport, changes));
                    }
                    Err(e) => {
                        tracing::warn!(attempt, error = %e, "topology replay failed");
                    }
                }
            }
            Err(e) => {
                tracing::warn!(attempt, error = %e, "reconnection failed");
            }
        }

        if let Some(max) = config.max_attempts
            && attempt >= max
        {
            tracing::error!(attempts = attempt, "recovery exhausted");
            return None;
        }

        // exponential backoff
        interval = Duration::from_secs_f64(
            (interval.as_secs_f64() * config.backoff_multiplier)
                .min(config.max_interval.as_secs_f64()),
        );
    }
}

/// Resolve and shuffle endpoints, try each until one connects.
async fn try_reconnect(
    opts: &ConnectionOptions,
    resolver: &AddressResolver,
) -> Result<Transport, BoxError> {
    let endpoints = resolver.resolve_and_shuffle().await;

    let mut last_err: BoxError = "no endpoints".into();
    for ep in &endpoints {
        match try_connect_single(ep, opts).await {
            Ok(transport) => return Ok(transport),
            Err(e) => {
                tracing::debug!(host = %ep.host, port = ep.port, error = %e, "recovery endpoint unreachable");
                last_err = e;
            }
        }
    }
    Err(last_err)
}

async fn try_connect_single(
    ep: &Endpoint,
    opts: &ConnectionOptions,
) -> Result<Transport, BoxError> {
    let mut transport = Connection::try_connect_endpoint(ep, opts).await?;

    transport.write_all(PROTOCOL_HEADER).await?;
    transport.flush().await?;

    let mut read_buf = BytesMut::with_capacity(8192);
    let mut codec = AmqpCodec::new(opts.frame_max);
    let _negotiated = handshake::perform(&mut transport, &mut read_buf, &mut codec, opts).await?;

    Ok(transport)
}

async fn replay_topology(
    transport: &mut Transport,
    opts: &ConnectionOptions,
    topology: &mut TopologyRegistry,
    event_tx: &broadcast::Sender<ConnectionEvent>,
) -> Result<Vec<ConnectionEvent>, BoxError> {
    let mut buf = BytesMut::with_capacity(256);
    let mut codec = AmqpCodec::new_framing(opts.frame_max);
    let mut read_buf = BytesMut::with_capacity(8192);
    let mut changes = Vec::new();

    // Open replay channel
    send_and_expect(
        transport,
        &mut codec,
        &mut buf,
        &mut read_buf,
        1,
        Method::ChannelOpen,
        |m| matches!(m, Method::ChannelOpenOk),
    )
    .await?;

    // Re-declare exchanges
    for ex in &topology.exchanges {
        let method = Method::ExchangeDeclare(Box::new(ExchangeDeclareArgs {
            exchange: ex.name.clone(),
            kind: ex.kind.clone(),
            passive: false,
            durable: ex.opts.durable,
            auto_delete: ex.opts.auto_delete,
            internal: ex.opts.internal,
            nowait: false,
            arguments: ex.opts.arguments().clone(),
        }));
        send_and_expect(
            transport,
            &mut codec,
            &mut buf,
            &mut read_buf,
            1,
            method,
            |m| matches!(m, Method::ExchangeDeclareOk),
        )
        .await?;
    }

    // Re-declare queues
    for q in &mut topology.queues {
        let method = Method::QueueDeclare(Box::new(QueueDeclareArgs {
            queue: if q.server_named {
                "".into()
            } else {
                q.name.clone()
            },
            passive: false,
            durable: q.opts.durable,
            exclusive: q.opts.exclusive,
            auto_delete: q.opts.auto_delete,
            nowait: false,
            arguments: q.opts.arguments().clone(),
        }));
        let response = send_and_receive_matching(
            transport,
            &mut codec,
            &mut buf,
            &mut read_buf,
            1,
            method,
            |m| matches!(m, Method::QueueDeclareOk(_)),
        )
        .await?;
        if let Method::QueueDeclareOk(ref args) = response
            && q.server_named
            && args.queue != q.name
        {
            let old = q.name.to_string();
            let new = args.queue.to_string();
            // Propagate rename through snapshot
            for b in topology.queue_bindings.iter_mut() {
                if b.queue == q.name {
                    b.queue = args.queue.clone();
                }
            }
            for c in topology.consumers.iter_mut() {
                if c.queue == q.name {
                    c.queue = args.queue.clone();
                }
            }
            q.name = args.queue.clone();
            let evt = ConnectionEvent::QueueNameChanged { old, new };
            let _ = event_tx.send(evt.clone());
            changes.push(evt);
        }
    }

    // Re-establish queue bindings
    for b in &topology.queue_bindings {
        let method = Method::QueueBind(Box::new(QueueBindArgs {
            queue: b.queue.clone(),
            exchange: b.exchange.clone(),
            routing_key: b.routing_key.clone(),
            nowait: false,
            arguments: b.arguments.clone(),
        }));
        send_and_expect(
            transport,
            &mut codec,
            &mut buf,
            &mut read_buf,
            1,
            method,
            |m| matches!(m, Method::QueueBindOk),
        )
        .await?;
    }

    // Re-establish exchange bindings
    for b in &topology.exchange_bindings {
        let method = Method::ExchangeBind(Box::new(ExchangeBindArgs {
            destination: b.destination.clone(),
            source: b.source.clone(),
            routing_key: b.routing_key.clone(),
            nowait: false,
            arguments: b.arguments.clone(),
        }));
        send_and_expect(
            transport,
            &mut codec,
            &mut buf,
            &mut read_buf,
            1,
            method,
            |m| matches!(m, Method::ExchangeBindOk),
        )
        .await?;
    }

    // Re-register consumers.
    for c in &mut topology.consumers {
        let method = Method::BasicConsume(Box::new(BasicConsumeArgs {
            queue: c.queue.clone(),
            consumer_tag: c.consumer_tag.clone(),
            no_local: false,
            no_ack: c.opts.no_ack,
            exclusive: c.opts.exclusive,
            nowait: false,
            arguments: c.opts.arguments().clone(),
        }));
        let response = send_and_receive_matching(
            transport,
            &mut codec,
            &mut buf,
            &mut read_buf,
            1,
            method,
            |m| matches!(m, Method::BasicConsumeOk(_)),
        )
        .await?;
        if let Method::BasicConsumeOk(ref args) = response
            && args.consumer_tag != c.consumer_tag
        {
            let old = c.consumer_tag.to_string();
            let new = args.consumer_tag.to_string();
            c.consumer_tag = args.consumer_tag.clone();
            let evt = ConnectionEvent::ConsumerTagChanged { old, new };
            let _ = event_tx.send(evt.clone());
            changes.push(evt);
        }
    }

    // Close replay channel
    let close = Method::ChannelClose(Box::new(ChannelCloseArgs {
        reply_code: 200,
        reply_text: "topology replay done".into(),
        class_id: 0,
        method_id: 0,
    }));
    send_and_expect(
        transport,
        &mut codec,
        &mut buf,
        &mut read_buf,
        1,
        close,
        |m| matches!(m, Method::ChannelCloseOk),
    )
    .await?;

    Ok(changes)
}

/// Send one method, then read frames until a method matching `expect` is
/// observed. Method frames that do not match, and all header/body frames, are
/// discarded. Used for steps where the broker may interleave unrelated frames,
/// for example a `basic.deliver` arriving on the heels of a replayed
/// `basic.consume`.
async fn send_and_receive_matching(
    transport: &mut Transport,
    codec: &mut AmqpCodec,
    write_buf: &mut BytesMut,
    read_buf: &mut BytesMut,
    channel: u16,
    method: Method,
    expect: impl Fn(&Method) -> bool,
) -> Result<Method, BoxError> {
    let frame = Frame::Method(channel, Box::new(method));
    codec.encode(frame, write_buf)?;
    transport.write_all(write_buf).await?;
    transport.flush().await?;
    write_buf.clear();

    loop {
        if let Some(frame) = codec.decode(read_buf)? {
            if let Frame::Method(_, m) = frame {
                if expect(&m) {
                    return Ok(*m);
                }
                tracing::debug!(method = ?m, "discarding non-matching method during recovery");
            }
            continue;
        }
        let mut tmp = [0u8; 8192];
        let n = transport.read(&mut tmp).await?;
        if n == 0 {
            return Err("connection closed during recovery".into());
        }
        read_buf.extend_from_slice(&tmp[..n]);
    }
}

async fn send_and_expect(
    transport: &mut Transport,
    codec: &mut AmqpCodec,
    write_buf: &mut BytesMut,
    read_buf: &mut BytesMut,
    channel: u16,
    method: Method,
    expect: impl Fn(&Method) -> bool,
) -> Result<(), BoxError> {
    send_and_receive_matching(
        transport, codec, write_buf, read_buf, channel, method, expect,
    )
    .await?;
    Ok(())
}
