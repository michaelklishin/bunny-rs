// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use compact_str::CompactString;

use crate::errors::{NomError, ProtocolError};
use crate::protocol::constants::*;
use crate::protocol::types::{
    FieldTable, parse_field_table, parse_long_string, parse_short_string, serialize_field_table,
    serialize_long_string, serialize_short_string,
};

use nom::IResult;
use nom::number::complete::{be_u8, be_u16, be_u32, be_u64};

type NomResult<'a, T> = IResult<&'a [u8], T, NomError>;

//
// Hot-path inline variants
//

/// AMQP 0-9-1 method. Hot-path variants are inline; cold-path use `Box<Args>`.
#[derive(Debug, Clone, PartialEq)]
pub enum Method {
    //
    // Connection
    //
    ConnectionStart(Box<ConnectionStartArgs>),
    ConnectionStartOk(Box<ConnectionStartOkArgs>),
    ConnectionSecure(Box<ConnectionSecureArgs>),
    ConnectionSecureOk(Box<ConnectionSecureOkArgs>),
    ConnectionTune(Box<ConnectionTuneArgs>),
    ConnectionTuneOk(Box<ConnectionTuneOkArgs>),
    ConnectionOpen(Box<ConnectionOpenArgs>),
    ConnectionOpenOk,
    ConnectionClose(Box<ConnectionCloseArgs>),
    ConnectionCloseOk,
    ConnectionBlocked(Box<ConnectionBlockedArgs>),
    ConnectionUnblocked,
    ConnectionUpdateSecret(Box<ConnectionUpdateSecretArgs>),
    ConnectionUpdateSecretOk,

    //
    // Channel
    //
    ChannelOpen,
    ChannelOpenOk,
    ChannelFlow(Box<ChannelFlowArgs>),
    ChannelFlowOk(Box<ChannelFlowOkArgs>),
    ChannelClose(Box<ChannelCloseArgs>),
    ChannelCloseOk,

    //
    // Exchange
    //
    ExchangeDeclare(Box<ExchangeDeclareArgs>),
    ExchangeDeclareOk,
    ExchangeDelete(Box<ExchangeDeleteArgs>),
    ExchangeDeleteOk,
    ExchangeBind(Box<ExchangeBindArgs>),
    ExchangeBindOk,
    ExchangeUnbind(Box<ExchangeUnbindArgs>),
    ExchangeUnbindOk,

    //
    // Queue
    //
    QueueDeclare(Box<QueueDeclareArgs>),
    QueueDeclareOk(Box<QueueDeclareOkArgs>),
    QueueBind(Box<QueueBindArgs>),
    QueueBindOk,
    QueuePurge(Box<QueuePurgeArgs>),
    QueuePurgeOk(Box<QueuePurgeOkArgs>),
    QueueDelete(Box<QueueDeleteArgs>),
    QueueDeleteOk(Box<QueueDeleteOkArgs>),
    QueueUnbind(Box<QueueUnbindArgs>),
    QueueUnbindOk,

    //
    // Basic (hot-path inlined)
    //
    BasicQos(Box<BasicQosArgs>),
    BasicQosOk,
    BasicConsume(Box<BasicConsumeArgs>),
    BasicConsumeOk(Box<BasicConsumeOkArgs>),
    BasicCancel(Box<BasicCancelArgs>),
    BasicCancelOk(Box<BasicCancelOkArgs>),
    BasicPublish(Box<BasicPublishArgs>),
    BasicReturn {
        reply_code: u16,
        reply_text: CompactString,
        exchange: CompactString,
        routing_key: CompactString,
    },
    BasicDeliver {
        consumer_tag: CompactString,
        delivery_tag: u64,
        redelivered: bool,
        exchange: CompactString,
        routing_key: CompactString,
    },
    BasicGet(Box<BasicGetArgs>),
    BasicGetOk(Box<BasicGetOkArgs>),
    BasicGetEmpty,
    BasicAck {
        delivery_tag: u64,
        multiple: bool,
    },
    BasicReject(Box<BasicRejectArgs>),
    BasicRecoverAsync(Box<BasicRecoverAsyncArgs>),
    BasicRecover(Box<BasicRecoverArgs>),
    BasicRecoverOk,
    BasicNack {
        delivery_tag: u64,
        multiple: bool,
        requeue: bool,
    },

    //
    // Tx
    //
    TxSelect,
    TxSelectOk,
    TxCommit,
    TxCommitOk,
    TxRollback,
    TxRollbackOk,

    //
    // Confirm
    //
    ConfirmSelect(Box<ConfirmSelectArgs>),
    ConfirmSelectOk,
}

//
// Args structs (cold-path)
//

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionStartArgs {
    pub version_major: u8,
    pub version_minor: u8,
    pub server_properties: FieldTable,
    pub mechanisms: Vec<u8>,
    pub locales: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionStartOkArgs {
    pub client_properties: FieldTable,
    pub mechanism: CompactString,
    pub response: Vec<u8>,
    pub locale: CompactString,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionSecureArgs {
    pub challenge: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionSecureOkArgs {
    pub response: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionTuneArgs {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionTuneOkArgs {
    pub channel_max: u16,
    pub frame_max: u32,
    pub heartbeat: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionOpenArgs {
    pub vhost: CompactString,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionCloseArgs {
    pub reply_code: u16,
    pub reply_text: CompactString,
    pub class_id: u16,
    pub method_id: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionBlockedArgs {
    pub reason: CompactString,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionUpdateSecretArgs {
    pub secret: Vec<u8>,
    pub reason: CompactString,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelFlowArgs {
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelFlowOkArgs {
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChannelCloseArgs {
    pub reply_code: u16,
    pub reply_text: CompactString,
    pub class_id: u16,
    pub method_id: u16,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeDeclareArgs {
    pub exchange: CompactString,
    pub kind: CompactString,
    pub passive: bool,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    pub nowait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeDeleteArgs {
    pub exchange: CompactString,
    pub if_unused: bool,
    pub nowait: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeBindArgs {
    pub destination: CompactString,
    pub source: CompactString,
    pub routing_key: CompactString,
    pub nowait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeUnbindArgs {
    pub destination: CompactString,
    pub source: CompactString,
    pub routing_key: CompactString,
    pub nowait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDeclareArgs {
    pub queue: CompactString,
    pub passive: bool,
    pub durable: bool,
    pub exclusive: bool,
    pub auto_delete: bool,
    pub nowait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDeclareOkArgs {
    pub queue: CompactString,
    pub message_count: u32,
    pub consumer_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueBindArgs {
    pub queue: CompactString,
    pub exchange: CompactString,
    pub routing_key: CompactString,
    pub nowait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueuePurgeArgs {
    pub queue: CompactString,
    pub nowait: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueuePurgeOkArgs {
    pub message_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDeleteArgs {
    pub queue: CompactString,
    pub if_unused: bool,
    pub if_empty: bool,
    pub nowait: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueDeleteOkArgs {
    pub message_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueUnbindArgs {
    pub queue: CompactString,
    pub exchange: CompactString,
    pub routing_key: CompactString,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicQosArgs {
    pub prefetch_size: u32,
    pub prefetch_count: u16,
    pub global: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicConsumeArgs {
    pub queue: CompactString,
    pub consumer_tag: CompactString,
    pub no_local: bool,
    pub no_ack: bool,
    pub exclusive: bool,
    pub nowait: bool,
    pub arguments: FieldTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicConsumeOkArgs {
    pub consumer_tag: CompactString,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicCancelArgs {
    pub consumer_tag: CompactString,
    pub nowait: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicCancelOkArgs {
    pub consumer_tag: CompactString,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicPublishArgs {
    pub exchange: CompactString,
    pub routing_key: CompactString,
    pub mandatory: bool,
    pub immediate: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicGetArgs {
    pub queue: CompactString,
    pub no_ack: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicGetOkArgs {
    pub delivery_tag: u64,
    pub redelivered: bool,
    pub exchange: CompactString,
    pub routing_key: CompactString,
    pub message_count: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicRejectArgs {
    pub delivery_tag: u64,
    pub requeue: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicRecoverAsyncArgs {
    pub requeue: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicRecoverArgs {
    pub requeue: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConfirmSelectArgs {
    pub nowait: bool,
}

//
// Parsing
//

pub fn parse_method(input: &[u8]) -> NomResult<'_, Method> {
    let (input, class_id) = be_u16(input)?;
    let (input, method_id) = be_u16(input)?;

    match (class_id, method_id) {
        // ── Connection ──
        (CLASS_CONNECTION, METHOD_CONNECTION_START) => {
            let (input, version_major) = be_u8(input)?;
            let (input, version_minor) = be_u8(input)?;
            let (input, server_properties) = parse_field_table(input)?;
            let (input, mechanisms_raw) = parse_long_string(input)?;
            let (input, locales_raw) = parse_long_string(input)?;
            Ok((
                input,
                Method::ConnectionStart(Box::new(ConnectionStartArgs {
                    version_major,
                    version_minor,
                    server_properties,
                    mechanisms: mechanisms_raw.to_vec(),
                    locales: locales_raw.to_vec(),
                })),
            ))
        }
        (CLASS_CONNECTION, METHOD_CONNECTION_START_OK) => {
            let (input, client_properties) = parse_field_table(input)?;
            let (input, mechanism) = parse_short_string(input)?;
            let (input, response_raw) = parse_long_string(input)?;
            let (input, locale) = parse_short_string(input)?;
            Ok((
                input,
                Method::ConnectionStartOk(Box::new(ConnectionStartOkArgs {
                    client_properties,
                    mechanism: CompactString::new(mechanism),
                    response: response_raw.to_vec(),
                    locale: CompactString::new(locale),
                })),
            ))
        }
        (CLASS_CONNECTION, METHOD_CONNECTION_SECURE) => {
            let (input, challenge_raw) = parse_long_string(input)?;
            Ok((
                input,
                Method::ConnectionSecure(Box::new(ConnectionSecureArgs {
                    challenge: challenge_raw.to_vec(),
                })),
            ))
        }
        (CLASS_CONNECTION, METHOD_CONNECTION_SECURE_OK) => {
            let (input, response_raw) = parse_long_string(input)?;
            Ok((
                input,
                Method::ConnectionSecureOk(Box::new(ConnectionSecureOkArgs {
                    response: response_raw.to_vec(),
                })),
            ))
        }
        (CLASS_CONNECTION, METHOD_CONNECTION_TUNE) => {
            let (input, channel_max) = be_u16(input)?;
            let (input, frame_max) = be_u32(input)?;
            let (input, heartbeat) = be_u16(input)?;
            Ok((
                input,
                Method::ConnectionTune(Box::new(ConnectionTuneArgs {
                    channel_max,
                    frame_max,
                    heartbeat,
                })),
            ))
        }
        (CLASS_CONNECTION, METHOD_CONNECTION_TUNE_OK) => {
            let (input, channel_max) = be_u16(input)?;
            let (input, frame_max) = be_u32(input)?;
            let (input, heartbeat) = be_u16(input)?;
            Ok((
                input,
                Method::ConnectionTuneOk(Box::new(ConnectionTuneOkArgs {
                    channel_max,
                    frame_max,
                    heartbeat,
                })),
            ))
        }
        (CLASS_CONNECTION, METHOD_CONNECTION_OPEN) => {
            let (input, vhost) = parse_short_string(input)?;
            // reserved-1 (short string) and reserved-2 (bit) -- skip
            let (input, _reserved1) = parse_short_string(input)?;
            let (input, _reserved2) = be_u8(input)?;
            Ok((
                input,
                Method::ConnectionOpen(Box::new(ConnectionOpenArgs {
                    vhost: CompactString::new(vhost),
                })),
            ))
        }
        (CLASS_CONNECTION, METHOD_CONNECTION_OPEN_OK) => {
            // known-hosts: RabbitMQ sends a short string (1-byte length + data)
            let (input, _reserved) = parse_short_string(input)?;
            Ok((input, Method::ConnectionOpenOk))
        }
        (CLASS_CONNECTION, METHOD_CONNECTION_CLOSE) => {
            let (input, reply_code) = be_u16(input)?;
            let (input, reply_text) = parse_short_string(input)?;
            let (input, class_id) = be_u16(input)?;
            let (input, method_id) = be_u16(input)?;
            Ok((
                input,
                Method::ConnectionClose(Box::new(ConnectionCloseArgs {
                    reply_code,
                    reply_text: CompactString::new(reply_text),
                    class_id,
                    method_id,
                })),
            ))
        }
        (CLASS_CONNECTION, METHOD_CONNECTION_CLOSE_OK) => Ok((input, Method::ConnectionCloseOk)),
        (CLASS_CONNECTION, METHOD_CONNECTION_BLOCKED) => {
            let (input, reason) = parse_short_string(input)?;
            Ok((
                input,
                Method::ConnectionBlocked(Box::new(ConnectionBlockedArgs {
                    reason: CompactString::new(reason),
                })),
            ))
        }
        (CLASS_CONNECTION, METHOD_CONNECTION_UNBLOCKED) => Ok((input, Method::ConnectionUnblocked)),
        (CLASS_CONNECTION, METHOD_CONNECTION_UPDATE_SECRET) => {
            let (input, secret_raw) = parse_long_string(input)?;
            let (input, reason) = parse_short_string(input)?;
            Ok((
                input,
                Method::ConnectionUpdateSecret(Box::new(ConnectionUpdateSecretArgs {
                    secret: secret_raw.to_vec(),
                    reason: CompactString::new(reason),
                })),
            ))
        }
        (CLASS_CONNECTION, METHOD_CONNECTION_UPDATE_SECRET_OK) => {
            Ok((input, Method::ConnectionUpdateSecretOk))
        }

        // ── Channel ──
        (CLASS_CHANNEL, METHOD_CHANNEL_OPEN) => {
            // reserved-1 (short string)
            let (input, _reserved) = parse_short_string(input)?;
            Ok((input, Method::ChannelOpen))
        }
        (CLASS_CHANNEL, METHOD_CHANNEL_OPEN_OK) => {
            // reserved-1 (long string)
            let (input, _reserved) = parse_long_string(input)?;
            Ok((input, Method::ChannelOpenOk))
        }
        (CLASS_CHANNEL, METHOD_CHANNEL_FLOW) => {
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::ChannelFlow(Box::new(ChannelFlowArgs {
                    active: bits & 1 != 0,
                })),
            ))
        }
        (CLASS_CHANNEL, METHOD_CHANNEL_FLOW_OK) => {
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::ChannelFlowOk(Box::new(ChannelFlowOkArgs {
                    active: bits & 1 != 0,
                })),
            ))
        }
        (CLASS_CHANNEL, METHOD_CHANNEL_CLOSE) => {
            let (input, reply_code) = be_u16(input)?;
            let (input, reply_text) = parse_short_string(input)?;
            let (input, class_id) = be_u16(input)?;
            let (input, method_id) = be_u16(input)?;
            Ok((
                input,
                Method::ChannelClose(Box::new(ChannelCloseArgs {
                    reply_code,
                    reply_text: CompactString::new(reply_text),
                    class_id,
                    method_id,
                })),
            ))
        }
        (CLASS_CHANNEL, METHOD_CHANNEL_CLOSE_OK) => Ok((input, Method::ChannelCloseOk)),

        // ── Exchange ──
        (CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE) => {
            let (input, _ticket) = be_u16(input)?;
            let (input, exchange) = parse_short_string(input)?;
            let (input, kind) = parse_short_string(input)?;
            let (input, bits) = be_u8(input)?;
            let (input, arguments) = parse_field_table(input)?;
            Ok((
                input,
                Method::ExchangeDeclare(Box::new(ExchangeDeclareArgs {
                    exchange: CompactString::new(exchange),
                    kind: CompactString::new(kind),
                    passive: bits & 1 != 0,
                    durable: bits & 2 != 0,
                    auto_delete: bits & 4 != 0,
                    internal: bits & 8 != 0,
                    nowait: bits & 16 != 0,
                    arguments,
                })),
            ))
        }
        (CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE_OK) => Ok((input, Method::ExchangeDeclareOk)),
        (CLASS_EXCHANGE, METHOD_EXCHANGE_DELETE) => {
            let (input, _ticket) = be_u16(input)?;
            let (input, exchange) = parse_short_string(input)?;
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::ExchangeDelete(Box::new(ExchangeDeleteArgs {
                    exchange: CompactString::new(exchange),
                    if_unused: bits & 1 != 0,
                    nowait: bits & 2 != 0,
                })),
            ))
        }
        (CLASS_EXCHANGE, METHOD_EXCHANGE_DELETE_OK) => Ok((input, Method::ExchangeDeleteOk)),
        (CLASS_EXCHANGE, METHOD_EXCHANGE_BIND) => {
            let (input, _ticket) = be_u16(input)?;
            let (input, destination) = parse_short_string(input)?;
            let (input, source) = parse_short_string(input)?;
            let (input, routing_key) = parse_short_string(input)?;
            let (input, bits) = be_u8(input)?;
            let (input, arguments) = parse_field_table(input)?;
            Ok((
                input,
                Method::ExchangeBind(Box::new(ExchangeBindArgs {
                    destination: CompactString::new(destination),
                    source: CompactString::new(source),
                    routing_key: CompactString::new(routing_key),
                    nowait: bits & 1 != 0,
                    arguments,
                })),
            ))
        }
        (CLASS_EXCHANGE, METHOD_EXCHANGE_BIND_OK) => Ok((input, Method::ExchangeBindOk)),
        (CLASS_EXCHANGE, METHOD_EXCHANGE_UNBIND) => {
            let (input, _ticket) = be_u16(input)?;
            let (input, destination) = parse_short_string(input)?;
            let (input, source) = parse_short_string(input)?;
            let (input, routing_key) = parse_short_string(input)?;
            let (input, bits) = be_u8(input)?;
            let (input, arguments) = parse_field_table(input)?;
            Ok((
                input,
                Method::ExchangeUnbind(Box::new(ExchangeUnbindArgs {
                    destination: CompactString::new(destination),
                    source: CompactString::new(source),
                    routing_key: CompactString::new(routing_key),
                    nowait: bits & 1 != 0,
                    arguments,
                })),
            ))
        }
        (CLASS_EXCHANGE, METHOD_EXCHANGE_UNBIND_OK) => Ok((input, Method::ExchangeUnbindOk)),

        // ── Queue ──
        (CLASS_QUEUE, METHOD_QUEUE_DECLARE) => {
            let (input, _ticket) = be_u16(input)?;
            let (input, queue) = parse_short_string(input)?;
            let (input, bits) = be_u8(input)?;
            let (input, arguments) = parse_field_table(input)?;
            Ok((
                input,
                Method::QueueDeclare(Box::new(QueueDeclareArgs {
                    queue: CompactString::new(queue),
                    passive: bits & 1 != 0,
                    durable: bits & 2 != 0,
                    exclusive: bits & 4 != 0,
                    auto_delete: bits & 8 != 0,
                    nowait: bits & 16 != 0,
                    arguments,
                })),
            ))
        }
        (CLASS_QUEUE, METHOD_QUEUE_DECLARE_OK) => {
            let (input, queue) = parse_short_string(input)?;
            let (input, message_count) = be_u32(input)?;
            let (input, consumer_count) = be_u32(input)?;
            Ok((
                input,
                Method::QueueDeclareOk(Box::new(QueueDeclareOkArgs {
                    queue: CompactString::new(queue),
                    message_count,
                    consumer_count,
                })),
            ))
        }
        (CLASS_QUEUE, METHOD_QUEUE_BIND) => {
            let (input, _ticket) = be_u16(input)?;
            let (input, queue) = parse_short_string(input)?;
            let (input, exchange) = parse_short_string(input)?;
            let (input, routing_key) = parse_short_string(input)?;
            let (input, bits) = be_u8(input)?;
            let (input, arguments) = parse_field_table(input)?;
            Ok((
                input,
                Method::QueueBind(Box::new(QueueBindArgs {
                    queue: CompactString::new(queue),
                    exchange: CompactString::new(exchange),
                    routing_key: CompactString::new(routing_key),
                    nowait: bits & 1 != 0,
                    arguments,
                })),
            ))
        }
        (CLASS_QUEUE, METHOD_QUEUE_BIND_OK) => Ok((input, Method::QueueBindOk)),
        (CLASS_QUEUE, METHOD_QUEUE_PURGE) => {
            let (input, _ticket) = be_u16(input)?;
            let (input, queue) = parse_short_string(input)?;
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::QueuePurge(Box::new(QueuePurgeArgs {
                    queue: CompactString::new(queue),
                    nowait: bits & 1 != 0,
                })),
            ))
        }
        (CLASS_QUEUE, METHOD_QUEUE_PURGE_OK) => {
            let (input, message_count) = be_u32(input)?;
            Ok((
                input,
                Method::QueuePurgeOk(Box::new(QueuePurgeOkArgs { message_count })),
            ))
        }
        (CLASS_QUEUE, METHOD_QUEUE_DELETE) => {
            let (input, _ticket) = be_u16(input)?;
            let (input, queue) = parse_short_string(input)?;
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::QueueDelete(Box::new(QueueDeleteArgs {
                    queue: CompactString::new(queue),
                    if_unused: bits & 1 != 0,
                    if_empty: bits & 2 != 0,
                    nowait: bits & 4 != 0,
                })),
            ))
        }
        (CLASS_QUEUE, METHOD_QUEUE_DELETE_OK) => {
            let (input, message_count) = be_u32(input)?;
            Ok((
                input,
                Method::QueueDeleteOk(Box::new(QueueDeleteOkArgs { message_count })),
            ))
        }
        (CLASS_QUEUE, METHOD_QUEUE_UNBIND) => {
            let (input, _ticket) = be_u16(input)?;
            let (input, queue) = parse_short_string(input)?;
            let (input, exchange) = parse_short_string(input)?;
            let (input, routing_key) = parse_short_string(input)?;
            let (input, arguments) = parse_field_table(input)?;
            Ok((
                input,
                Method::QueueUnbind(Box::new(QueueUnbindArgs {
                    queue: CompactString::new(queue),
                    exchange: CompactString::new(exchange),
                    routing_key: CompactString::new(routing_key),
                    arguments,
                })),
            ))
        }
        (CLASS_QUEUE, METHOD_QUEUE_UNBIND_OK) => Ok((input, Method::QueueUnbindOk)),

        // ── Basic ──
        (CLASS_BASIC, METHOD_BASIC_QOS) => {
            let (input, prefetch_size) = be_u32(input)?;
            let (input, prefetch_count) = be_u16(input)?;
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::BasicQos(Box::new(BasicQosArgs {
                    prefetch_size,
                    prefetch_count,
                    global: bits & 1 != 0,
                })),
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_QOS_OK) => Ok((input, Method::BasicQosOk)),
        (CLASS_BASIC, METHOD_BASIC_CONSUME) => {
            let (input, _ticket) = be_u16(input)?;
            let (input, queue) = parse_short_string(input)?;
            let (input, consumer_tag) = parse_short_string(input)?;
            let (input, bits) = be_u8(input)?;
            let (input, arguments) = parse_field_table(input)?;
            Ok((
                input,
                Method::BasicConsume(Box::new(BasicConsumeArgs {
                    queue: CompactString::new(queue),
                    consumer_tag: CompactString::new(consumer_tag),
                    no_local: bits & 1 != 0,
                    no_ack: bits & 2 != 0,
                    exclusive: bits & 4 != 0,
                    nowait: bits & 8 != 0,
                    arguments,
                })),
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_CONSUME_OK) => {
            let (input, consumer_tag) = parse_short_string(input)?;
            Ok((
                input,
                Method::BasicConsumeOk(Box::new(BasicConsumeOkArgs {
                    consumer_tag: CompactString::new(consumer_tag),
                })),
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_CANCEL) => {
            let (input, consumer_tag) = parse_short_string(input)?;
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::BasicCancel(Box::new(BasicCancelArgs {
                    consumer_tag: CompactString::new(consumer_tag),
                    nowait: bits & 1 != 0,
                })),
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_CANCEL_OK) => {
            let (input, consumer_tag) = parse_short_string(input)?;
            Ok((
                input,
                Method::BasicCancelOk(Box::new(BasicCancelOkArgs {
                    consumer_tag: CompactString::new(consumer_tag),
                })),
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_PUBLISH) => {
            let (input, _ticket) = be_u16(input)?;
            let (input, exchange) = parse_short_string(input)?;
            let (input, routing_key) = parse_short_string(input)?;
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::BasicPublish(Box::new(BasicPublishArgs {
                    exchange: CompactString::new(exchange),
                    routing_key: CompactString::new(routing_key),
                    mandatory: bits & 1 != 0,
                    immediate: bits & 2 != 0,
                })),
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_RETURN) => {
            let (input, reply_code) = be_u16(input)?;
            let (input, reply_text) = parse_short_string(input)?;
            let (input, exchange) = parse_short_string(input)?;
            let (input, routing_key) = parse_short_string(input)?;
            Ok((
                input,
                Method::BasicReturn {
                    reply_code,
                    reply_text: CompactString::new(reply_text),
                    exchange: CompactString::new(exchange),
                    routing_key: CompactString::new(routing_key),
                },
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_DELIVER) => {
            let (input, consumer_tag) = parse_short_string(input)?;
            let (input, delivery_tag) = be_u64(input)?;
            let (input, bits) = be_u8(input)?;
            let (input, exchange) = parse_short_string(input)?;
            let (input, routing_key) = parse_short_string(input)?;
            Ok((
                input,
                Method::BasicDeliver {
                    consumer_tag: CompactString::new(consumer_tag),
                    delivery_tag,
                    redelivered: bits & 1 != 0,
                    exchange: CompactString::new(exchange),
                    routing_key: CompactString::new(routing_key),
                },
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_GET) => {
            let (input, _ticket) = be_u16(input)?;
            let (input, queue) = parse_short_string(input)?;
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::BasicGet(Box::new(BasicGetArgs {
                    queue: CompactString::new(queue),
                    no_ack: bits & 1 != 0,
                })),
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_GET_OK) => {
            let (input, delivery_tag) = be_u64(input)?;
            let (input, bits) = be_u8(input)?;
            let (input, exchange) = parse_short_string(input)?;
            let (input, routing_key) = parse_short_string(input)?;
            let (input, message_count) = be_u32(input)?;
            Ok((
                input,
                Method::BasicGetOk(Box::new(BasicGetOkArgs {
                    delivery_tag,
                    redelivered: bits & 1 != 0,
                    exchange: CompactString::new(exchange),
                    routing_key: CompactString::new(routing_key),
                    message_count,
                })),
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_GET_EMPTY) => {
            // reserved-1 (short string)
            let (input, _reserved) = parse_short_string(input)?;
            Ok((input, Method::BasicGetEmpty))
        }
        (CLASS_BASIC, METHOD_BASIC_ACK) => {
            let (input, delivery_tag) = be_u64(input)?;
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::BasicAck {
                    delivery_tag,
                    multiple: bits & 1 != 0,
                },
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_REJECT) => {
            let (input, delivery_tag) = be_u64(input)?;
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::BasicReject(Box::new(BasicRejectArgs {
                    delivery_tag,
                    requeue: bits & 1 != 0,
                })),
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_RECOVER_ASYNC) => {
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::BasicRecoverAsync(Box::new(BasicRecoverAsyncArgs {
                    requeue: bits & 1 != 0,
                })),
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_RECOVER) => {
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::BasicRecover(Box::new(BasicRecoverArgs {
                    requeue: bits & 1 != 0,
                })),
            ))
        }
        (CLASS_BASIC, METHOD_BASIC_RECOVER_OK) => Ok((input, Method::BasicRecoverOk)),
        (CLASS_BASIC, METHOD_BASIC_NACK) => {
            let (input, delivery_tag) = be_u64(input)?;
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::BasicNack {
                    delivery_tag,
                    multiple: bits & 1 != 0,
                    requeue: bits & 2 != 0,
                },
            ))
        }

        // ── Tx ──
        (CLASS_TX, METHOD_TX_SELECT) => Ok((input, Method::TxSelect)),
        (CLASS_TX, METHOD_TX_SELECT_OK) => Ok((input, Method::TxSelectOk)),
        (CLASS_TX, METHOD_TX_COMMIT) => Ok((input, Method::TxCommit)),
        (CLASS_TX, METHOD_TX_COMMIT_OK) => Ok((input, Method::TxCommitOk)),
        (CLASS_TX, METHOD_TX_ROLLBACK) => Ok((input, Method::TxRollback)),
        (CLASS_TX, METHOD_TX_ROLLBACK_OK) => Ok((input, Method::TxRollbackOk)),

        // ── Confirm ──
        (CLASS_CONFIRM, METHOD_CONFIRM_SELECT) => {
            let (input, bits) = be_u8(input)?;
            Ok((
                input,
                Method::ConfirmSelect(Box::new(ConfirmSelectArgs {
                    nowait: bits & 1 != 0,
                })),
            ))
        }
        (CLASS_CONFIRM, METHOD_CONFIRM_SELECT_OK) => Ok((input, Method::ConfirmSelectOk)),

        _ => Err(ProtocolError::UnknownMethod {
            class_id,
            method_id,
        }
        .nom_failure()),
    }
}

//
// Serialization
//

pub fn serialize_method(method: &Method, buf: &mut Vec<u8>) {
    match method {
        // ── Connection ──
        Method::ConnectionStart(args) => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_START.to_be_bytes());
            buf.push(args.version_major);
            buf.push(args.version_minor);
            serialize_field_table(&args.server_properties, buf);
            serialize_long_string(&args.mechanisms, buf);
            serialize_long_string(&args.locales, buf);
        }
        Method::ConnectionStartOk(args) => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_START_OK.to_be_bytes());
            serialize_field_table(&args.client_properties, buf);
            serialize_short_string(&args.mechanism, buf).expect("mechanism too long");
            serialize_long_string(&args.response, buf);
            serialize_short_string(&args.locale, buf).expect("locale too long");
        }
        Method::ConnectionSecure(args) => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_SECURE.to_be_bytes());
            serialize_long_string(&args.challenge, buf);
        }
        Method::ConnectionSecureOk(args) => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_SECURE_OK.to_be_bytes());
            serialize_long_string(&args.response, buf);
        }
        Method::ConnectionTune(args) => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_TUNE.to_be_bytes());
            buf.extend_from_slice(&args.channel_max.to_be_bytes());
            buf.extend_from_slice(&args.frame_max.to_be_bytes());
            buf.extend_from_slice(&args.heartbeat.to_be_bytes());
        }
        Method::ConnectionTuneOk(args) => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_TUNE_OK.to_be_bytes());
            buf.extend_from_slice(&args.channel_max.to_be_bytes());
            buf.extend_from_slice(&args.frame_max.to_be_bytes());
            buf.extend_from_slice(&args.heartbeat.to_be_bytes());
        }
        Method::ConnectionOpen(args) => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_OPEN.to_be_bytes());
            serialize_short_string(&args.vhost, buf).expect("vhost too long");
            // reserved-1 (short string ""), reserved-2 (bit 0)
            buf.push(0);
            buf.push(0);
        }
        Method::ConnectionOpenOk => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_OPEN_OK.to_be_bytes());
            // known-hosts (short string "")
            buf.push(0);
        }
        Method::ConnectionClose(args) => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_CLOSE.to_be_bytes());
            buf.extend_from_slice(&args.reply_code.to_be_bytes());
            serialize_short_string(&args.reply_text, buf).expect("reply_text too long");
            buf.extend_from_slice(&args.class_id.to_be_bytes());
            buf.extend_from_slice(&args.method_id.to_be_bytes());
        }
        Method::ConnectionCloseOk => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_CLOSE_OK.to_be_bytes());
        }
        Method::ConnectionBlocked(args) => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_BLOCKED.to_be_bytes());
            serialize_short_string(&args.reason, buf).expect("reason too long");
        }
        Method::ConnectionUnblocked => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_UNBLOCKED.to_be_bytes());
        }
        Method::ConnectionUpdateSecret(args) => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_UPDATE_SECRET.to_be_bytes());
            serialize_long_string(&args.secret, buf);
            serialize_short_string(&args.reason, buf).expect("reason too long");
        }
        Method::ConnectionUpdateSecretOk => {
            buf.extend_from_slice(&CLASS_CONNECTION.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONNECTION_UPDATE_SECRET_OK.to_be_bytes());
        }

        // ── Channel ──
        Method::ChannelOpen => {
            buf.extend_from_slice(&CLASS_CHANNEL.to_be_bytes());
            buf.extend_from_slice(&METHOD_CHANNEL_OPEN.to_be_bytes());
            // reserved-1 (short string "")
            buf.push(0);
        }
        Method::ChannelOpenOk => {
            buf.extend_from_slice(&CLASS_CHANNEL.to_be_bytes());
            buf.extend_from_slice(&METHOD_CHANNEL_OPEN_OK.to_be_bytes());
            // reserved-1 (long string "")
            buf.extend_from_slice(&0u32.to_be_bytes());
        }
        Method::ChannelFlow(args) => {
            buf.extend_from_slice(&CLASS_CHANNEL.to_be_bytes());
            buf.extend_from_slice(&METHOD_CHANNEL_FLOW.to_be_bytes());
            buf.push(if args.active { 1 } else { 0 });
        }
        Method::ChannelFlowOk(args) => {
            buf.extend_from_slice(&CLASS_CHANNEL.to_be_bytes());
            buf.extend_from_slice(&METHOD_CHANNEL_FLOW_OK.to_be_bytes());
            buf.push(if args.active { 1 } else { 0 });
        }
        Method::ChannelClose(args) => {
            buf.extend_from_slice(&CLASS_CHANNEL.to_be_bytes());
            buf.extend_from_slice(&METHOD_CHANNEL_CLOSE.to_be_bytes());
            buf.extend_from_slice(&args.reply_code.to_be_bytes());
            serialize_short_string(&args.reply_text, buf).expect("reply_text too long");
            buf.extend_from_slice(&args.class_id.to_be_bytes());
            buf.extend_from_slice(&args.method_id.to_be_bytes());
        }
        Method::ChannelCloseOk => {
            buf.extend_from_slice(&CLASS_CHANNEL.to_be_bytes());
            buf.extend_from_slice(&METHOD_CHANNEL_CLOSE_OK.to_be_bytes());
        }

        // ── Exchange ──
        Method::ExchangeDeclare(args) => {
            buf.extend_from_slice(&CLASS_EXCHANGE.to_be_bytes());
            buf.extend_from_slice(&METHOD_EXCHANGE_DECLARE.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
            serialize_short_string(&args.exchange, buf).expect("exchange too long");
            serialize_short_string(&args.kind, buf).expect("kind too long");
            let bits = pack_bools(&[
                args.passive,
                args.durable,
                args.auto_delete,
                args.internal,
                args.nowait,
            ]);
            buf.push(bits);
            serialize_field_table(&args.arguments, buf);
        }
        Method::ExchangeDeclareOk => {
            buf.extend_from_slice(&CLASS_EXCHANGE.to_be_bytes());
            buf.extend_from_slice(&METHOD_EXCHANGE_DECLARE_OK.to_be_bytes());
        }
        Method::ExchangeDelete(args) => {
            buf.extend_from_slice(&CLASS_EXCHANGE.to_be_bytes());
            buf.extend_from_slice(&METHOD_EXCHANGE_DELETE.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
            serialize_short_string(&args.exchange, buf).expect("exchange too long");
            let bits = pack_bools(&[args.if_unused, args.nowait]);
            buf.push(bits);
        }
        Method::ExchangeDeleteOk => {
            buf.extend_from_slice(&CLASS_EXCHANGE.to_be_bytes());
            buf.extend_from_slice(&METHOD_EXCHANGE_DELETE_OK.to_be_bytes());
        }
        Method::ExchangeBind(args) => {
            buf.extend_from_slice(&CLASS_EXCHANGE.to_be_bytes());
            buf.extend_from_slice(&METHOD_EXCHANGE_BIND.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
            serialize_short_string(&args.destination, buf).expect("destination too long");
            serialize_short_string(&args.source, buf).expect("source too long");
            serialize_short_string(&args.routing_key, buf).expect("routing_key too long");
            buf.push(if args.nowait { 1 } else { 0 });
            serialize_field_table(&args.arguments, buf);
        }
        Method::ExchangeBindOk => {
            buf.extend_from_slice(&CLASS_EXCHANGE.to_be_bytes());
            buf.extend_from_slice(&METHOD_EXCHANGE_BIND_OK.to_be_bytes());
        }
        Method::ExchangeUnbind(args) => {
            buf.extend_from_slice(&CLASS_EXCHANGE.to_be_bytes());
            buf.extend_from_slice(&METHOD_EXCHANGE_UNBIND.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
            serialize_short_string(&args.destination, buf).expect("destination too long");
            serialize_short_string(&args.source, buf).expect("source too long");
            serialize_short_string(&args.routing_key, buf).expect("routing_key too long");
            buf.push(if args.nowait { 1 } else { 0 });
            serialize_field_table(&args.arguments, buf);
        }
        Method::ExchangeUnbindOk => {
            buf.extend_from_slice(&CLASS_EXCHANGE.to_be_bytes());
            buf.extend_from_slice(&METHOD_EXCHANGE_UNBIND_OK.to_be_bytes());
        }

        // ── Queue ──
        Method::QueueDeclare(args) => {
            buf.extend_from_slice(&CLASS_QUEUE.to_be_bytes());
            buf.extend_from_slice(&METHOD_QUEUE_DECLARE.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
            serialize_short_string(&args.queue, buf).expect("queue too long");
            let bits = pack_bools(&[
                args.passive,
                args.durable,
                args.exclusive,
                args.auto_delete,
                args.nowait,
            ]);
            buf.push(bits);
            serialize_field_table(&args.arguments, buf);
        }
        Method::QueueDeclareOk(args) => {
            buf.extend_from_slice(&CLASS_QUEUE.to_be_bytes());
            buf.extend_from_slice(&METHOD_QUEUE_DECLARE_OK.to_be_bytes());
            serialize_short_string(&args.queue, buf).expect("queue too long");
            buf.extend_from_slice(&args.message_count.to_be_bytes());
            buf.extend_from_slice(&args.consumer_count.to_be_bytes());
        }
        Method::QueueBind(args) => {
            buf.extend_from_slice(&CLASS_QUEUE.to_be_bytes());
            buf.extend_from_slice(&METHOD_QUEUE_BIND.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
            serialize_short_string(&args.queue, buf).expect("queue too long");
            serialize_short_string(&args.exchange, buf).expect("exchange too long");
            serialize_short_string(&args.routing_key, buf).expect("routing_key too long");
            buf.push(if args.nowait { 1 } else { 0 });
            serialize_field_table(&args.arguments, buf);
        }
        Method::QueueBindOk => {
            buf.extend_from_slice(&CLASS_QUEUE.to_be_bytes());
            buf.extend_from_slice(&METHOD_QUEUE_BIND_OK.to_be_bytes());
        }
        Method::QueuePurge(args) => {
            buf.extend_from_slice(&CLASS_QUEUE.to_be_bytes());
            buf.extend_from_slice(&METHOD_QUEUE_PURGE.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
            serialize_short_string(&args.queue, buf).expect("queue too long");
            buf.push(if args.nowait { 1 } else { 0 });
        }
        Method::QueuePurgeOk(args) => {
            buf.extend_from_slice(&CLASS_QUEUE.to_be_bytes());
            buf.extend_from_slice(&METHOD_QUEUE_PURGE_OK.to_be_bytes());
            buf.extend_from_slice(&args.message_count.to_be_bytes());
        }
        Method::QueueDelete(args) => {
            buf.extend_from_slice(&CLASS_QUEUE.to_be_bytes());
            buf.extend_from_slice(&METHOD_QUEUE_DELETE.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
            serialize_short_string(&args.queue, buf).expect("queue too long");
            let bits = pack_bools(&[args.if_unused, args.if_empty, args.nowait]);
            buf.push(bits);
        }
        Method::QueueDeleteOk(args) => {
            buf.extend_from_slice(&CLASS_QUEUE.to_be_bytes());
            buf.extend_from_slice(&METHOD_QUEUE_DELETE_OK.to_be_bytes());
            buf.extend_from_slice(&args.message_count.to_be_bytes());
        }
        Method::QueueUnbind(args) => {
            buf.extend_from_slice(&CLASS_QUEUE.to_be_bytes());
            buf.extend_from_slice(&METHOD_QUEUE_UNBIND.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
            serialize_short_string(&args.queue, buf).expect("queue too long");
            serialize_short_string(&args.exchange, buf).expect("exchange too long");
            serialize_short_string(&args.routing_key, buf).expect("routing_key too long");
            serialize_field_table(&args.arguments, buf);
        }
        Method::QueueUnbindOk => {
            buf.extend_from_slice(&CLASS_QUEUE.to_be_bytes());
            buf.extend_from_slice(&METHOD_QUEUE_UNBIND_OK.to_be_bytes());
        }

        // ── Basic ──
        Method::BasicQos(args) => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_QOS.to_be_bytes());
            buf.extend_from_slice(&args.prefetch_size.to_be_bytes());
            buf.extend_from_slice(&args.prefetch_count.to_be_bytes());
            buf.push(if args.global { 1 } else { 0 });
        }
        Method::BasicQosOk => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_QOS_OK.to_be_bytes());
        }
        Method::BasicConsume(args) => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_CONSUME.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
            serialize_short_string(&args.queue, buf).expect("queue too long");
            serialize_short_string(&args.consumer_tag, buf).expect("consumer_tag too long");
            // no_local hardcoded to false in serialization
            let bits = pack_bools(&[false, args.no_ack, args.exclusive, args.nowait]);
            buf.push(bits);
            serialize_field_table(&args.arguments, buf);
        }
        Method::BasicConsumeOk(args) => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_CONSUME_OK.to_be_bytes());
            serialize_short_string(&args.consumer_tag, buf).expect("consumer_tag too long");
        }
        Method::BasicCancel(args) => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_CANCEL.to_be_bytes());
            serialize_short_string(&args.consumer_tag, buf).expect("consumer_tag too long");
            buf.push(if args.nowait { 1 } else { 0 });
        }
        Method::BasicCancelOk(args) => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_CANCEL_OK.to_be_bytes());
            serialize_short_string(&args.consumer_tag, buf).expect("consumer_tag too long");
        }
        Method::BasicPublish(args) => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_PUBLISH.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
            serialize_short_string(&args.exchange, buf).expect("exchange too long");
            serialize_short_string(&args.routing_key, buf).expect("routing_key too long");
            let bits = pack_bools(&[args.mandatory, args.immediate]);
            buf.push(bits);
        }
        Method::BasicReturn {
            reply_code,
            reply_text,
            exchange,
            routing_key,
        } => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_RETURN.to_be_bytes());
            buf.extend_from_slice(&reply_code.to_be_bytes());
            serialize_short_string(reply_text, buf).expect("reply_text too long");
            serialize_short_string(exchange, buf).expect("exchange too long");
            serialize_short_string(routing_key, buf).expect("routing_key too long");
        }
        Method::BasicDeliver {
            consumer_tag,
            delivery_tag,
            redelivered,
            exchange,
            routing_key,
        } => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_DELIVER.to_be_bytes());
            serialize_short_string(consumer_tag, buf).expect("consumer_tag too long");
            buf.extend_from_slice(&delivery_tag.to_be_bytes());
            buf.push(if *redelivered { 1 } else { 0 });
            serialize_short_string(exchange, buf).expect("exchange too long");
            serialize_short_string(routing_key, buf).expect("routing_key too long");
        }
        Method::BasicGet(args) => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_GET.to_be_bytes());
            buf.extend_from_slice(&0u16.to_be_bytes()); // ticket
            serialize_short_string(&args.queue, buf).expect("queue too long");
            buf.push(if args.no_ack { 1 } else { 0 });
        }
        Method::BasicGetOk(args) => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_GET_OK.to_be_bytes());
            buf.extend_from_slice(&args.delivery_tag.to_be_bytes());
            buf.push(if args.redelivered { 1 } else { 0 });
            serialize_short_string(&args.exchange, buf).expect("exchange too long");
            serialize_short_string(&args.routing_key, buf).expect("routing_key too long");
            buf.extend_from_slice(&args.message_count.to_be_bytes());
        }
        Method::BasicGetEmpty => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_GET_EMPTY.to_be_bytes());
            // reserved-1 (short string "")
            buf.push(0);
        }
        Method::BasicAck {
            delivery_tag,
            multiple,
        } => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_ACK.to_be_bytes());
            buf.extend_from_slice(&delivery_tag.to_be_bytes());
            buf.push(if *multiple { 1 } else { 0 });
        }
        Method::BasicReject(args) => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_REJECT.to_be_bytes());
            buf.extend_from_slice(&args.delivery_tag.to_be_bytes());
            buf.push(if args.requeue { 1 } else { 0 });
        }
        Method::BasicRecoverAsync(args) => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_RECOVER_ASYNC.to_be_bytes());
            buf.push(if args.requeue { 1 } else { 0 });
        }
        Method::BasicRecover(args) => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_RECOVER.to_be_bytes());
            buf.push(if args.requeue { 1 } else { 0 });
        }
        Method::BasicRecoverOk => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_RECOVER_OK.to_be_bytes());
        }
        Method::BasicNack {
            delivery_tag,
            multiple,
            requeue,
        } => {
            buf.extend_from_slice(&CLASS_BASIC.to_be_bytes());
            buf.extend_from_slice(&METHOD_BASIC_NACK.to_be_bytes());
            buf.extend_from_slice(&delivery_tag.to_be_bytes());
            let bits = pack_bools(&[*multiple, *requeue]);
            buf.push(bits);
        }

        // ── Tx ──
        Method::TxSelect => {
            buf.extend_from_slice(&CLASS_TX.to_be_bytes());
            buf.extend_from_slice(&METHOD_TX_SELECT.to_be_bytes());
        }
        Method::TxSelectOk => {
            buf.extend_from_slice(&CLASS_TX.to_be_bytes());
            buf.extend_from_slice(&METHOD_TX_SELECT_OK.to_be_bytes());
        }
        Method::TxCommit => {
            buf.extend_from_slice(&CLASS_TX.to_be_bytes());
            buf.extend_from_slice(&METHOD_TX_COMMIT.to_be_bytes());
        }
        Method::TxCommitOk => {
            buf.extend_from_slice(&CLASS_TX.to_be_bytes());
            buf.extend_from_slice(&METHOD_TX_COMMIT_OK.to_be_bytes());
        }
        Method::TxRollback => {
            buf.extend_from_slice(&CLASS_TX.to_be_bytes());
            buf.extend_from_slice(&METHOD_TX_ROLLBACK.to_be_bytes());
        }
        Method::TxRollbackOk => {
            buf.extend_from_slice(&CLASS_TX.to_be_bytes());
            buf.extend_from_slice(&METHOD_TX_ROLLBACK_OK.to_be_bytes());
        }

        // ── Confirm ──
        Method::ConfirmSelect(args) => {
            buf.extend_from_slice(&CLASS_CONFIRM.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONFIRM_SELECT.to_be_bytes());
            buf.push(if args.nowait { 1 } else { 0 });
        }
        Method::ConfirmSelectOk => {
            buf.extend_from_slice(&CLASS_CONFIRM.to_be_bytes());
            buf.extend_from_slice(&METHOD_CONFIRM_SELECT_OK.to_be_bytes());
        }
    }
}

/// Pack up to 8 boolean flags into a single byte, bit 0 = first element.
fn pack_bools(flags: &[bool]) -> u8 {
    let mut byte = 0u8;
    for (i, &flag) in flags.iter().enumerate() {
        if flag {
            byte |= 1 << i;
        }
    }
    byte
}
