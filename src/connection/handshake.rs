// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::io;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::{Decoder, Encoder};

use crate::connection::{ConnectionError, ConnectionOptions};
use crate::protocol::codec::AmqpCodec;
use crate::protocol::frame::Frame;
use crate::protocol::method::*;
use crate::protocol::types::{FieldTable, FieldValue};
use crate::transport::Transport;

pub(crate) struct NegotiatedParams {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
    pub server_properties: FieldTable,
}

pub(crate) async fn perform(
    transport: &mut Transport,
    buf: &mut BytesMut,
    codec: &mut AmqpCodec,
    opts: &ConnectionOptions,
) -> Result<NegotiatedParams, ConnectionError> {
    tracing::debug!("awaiting connection.start");
    let start = read_method(transport, buf, codec).await?;
    tracing::debug!("received connection.start");
    let start_args = match start {
        Method::ConnectionStart(args) => args,
        _ => return Err(ConnectionError::UnexpectedMethod),
    };
    let server_properties = start_args.server_properties.clone();

    // connection.start-ok (PLAIN)
    let mut client_properties = FieldTable::new();
    client_properties.insert("product", "bunny-rs");
    client_properties.insert("version", env!("CARGO_PKG_VERSION"));
    client_properties.insert("platform", "Rust");

    let mut capabilities = FieldTable::new();
    capabilities.insert("publisher_confirms", true);
    capabilities.insert("consumer_cancel_notify", true);
    capabilities.insert("exchange_exchange_bindings", true);
    capabilities.insert("basic.nack", true);
    capabilities.insert("connection.blocked", true);
    capabilities.insert("authentication_failure_close", true);
    client_properties.insert("capabilities", FieldValue::Table(capabilities));

    if let Some(ref name) = opts.connection_name {
        client_properties.insert("connection_name", name.as_str());
    }

    // PLAIN: \0username\0password
    let mut response = Vec::new();
    response.push(0);
    response.extend_from_slice(opts.username.as_bytes());
    response.push(0);
    response.extend_from_slice(opts.password.as_str().as_bytes());

    let start_ok = Method::ConnectionStartOk(Box::new(ConnectionStartOkArgs {
        client_properties,
        mechanism: "PLAIN".into(),
        response,
        locale: "en_US".into(),
    }));

    tracing::debug!("sending connection.start-ok");
    send_method(transport, codec, 0, start_ok).await?;
    tracing::debug!("awaiting connection.tune");
    let tune = read_method(transport, buf, codec).await?;
    tracing::debug!("got connection.tune");
    let tune_args = match tune {
        Method::ConnectionTune(args) => args,
        Method::ConnectionClose(_) => {
            return Err(ConnectionError::AuthenticationFailed);
        }
        _ => return Err(ConnectionError::UnexpectedMethod),
    };

    // Negotiate
    let channel_max = if tune_args.channel_max == 0 {
        opts.channel_max
    } else {
        tune_args.channel_max.min(opts.channel_max)
    };

    let frame_max = if tune_args.frame_max == 0 {
        opts.frame_max
    } else {
        tune_args.frame_max.min(opts.frame_max)
    };

    let heartbeat = if tune_args.heartbeat == 0 || opts.heartbeat == 0 {
        0
    } else {
        tune_args.heartbeat.min(opts.heartbeat)
    };

    // Send tune-ok
    let tune_ok = Method::ConnectionTuneOk(Box::new(ConnectionTuneOkArgs {
        channel_max,
        frame_max,
        heartbeat,
    }));
    send_method(transport, codec, 0, tune_ok).await?;

    // Send connection.open
    let open = Method::ConnectionOpen(Box::new(ConnectionOpenArgs {
        vhost: opts.virtual_host.as_str().into(),
    }));
    send_method(transport, codec, 0, open).await?;

    // Read connection.open-ok
    let open_ok = read_method(transport, buf, codec).await?;
    match open_ok {
        Method::ConnectionOpenOk => {}
        Method::ConnectionClose(args) => {
            return Err(ConnectionError::ConnectionClosed {
                code: args.reply_code,
                text: args.reply_text.to_string(),
            });
        }
        _ => return Err(ConnectionError::UnexpectedMethod),
    }

    Ok(NegotiatedParams {
        channel_max,
        frame_max,
        heartbeat,
        server_properties,
    })
}

async fn read_method(
    transport: &mut Transport,
    buf: &mut BytesMut,
    codec: &mut AmqpCodec,
) -> Result<Method, ConnectionError> {
    loop {
        if let Some(frame) = codec.decode(buf)? {
            return match frame {
                Frame::Method(_, method) => Ok(*method),
                Frame::Heartbeat => continue,
                _ => Err(ConnectionError::UnexpectedMethod),
            };
        }

        let mut tmp = [0u8; 8192];
        let n = transport.read(&mut tmp).await?;
        if n == 0 {
            return Err(ConnectionError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed during handshake",
            )));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

async fn send_method(
    transport: &mut Transport,
    codec: &mut AmqpCodec,
    channel: u16,
    method: Method,
) -> Result<(), ConnectionError> {
    let frame = Frame::Method(channel, Box::new(method));
    let mut out = BytesMut::new();
    codec.encode(frame, &mut out)?;
    transport.write_all(&out).await?;
    transport.flush().await?;
    Ok(())
}
