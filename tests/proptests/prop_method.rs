// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use proptest::prelude::*;

use bunny_rs::protocol::method::{
    BasicCancelArgs, BasicCancelOkArgs, BasicConsumeArgs, BasicConsumeOkArgs, BasicGetArgs,
    BasicGetOkArgs, BasicPublishArgs, BasicQosArgs, BasicRejectArgs, ChannelCloseArgs,
    ChannelFlowArgs, ChannelFlowOkArgs, ConfirmSelectArgs, ConnectionBlockedArgs,
    ConnectionCloseArgs, ConnectionOpenArgs, ConnectionStartArgs, ConnectionStartOkArgs,
    ConnectionTuneArgs, ConnectionTuneOkArgs, ConnectionUpdateSecretArgs, ExchangeBindArgs,
    ExchangeDeclareArgs, ExchangeDeleteArgs, ExchangeUnbindArgs, Method, QueueBindArgs,
    QueueDeclareArgs, QueueDeclareOkArgs, QueueDeleteArgs, QueuePurgeArgs, QueueUnbindArgs,
    parse_method, serialize_method,
};
use bunny_rs::protocol::types::FieldTable;

fn arb_short_string() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9._\\-]{0,50}"
}

fn arb_method() -> impl Strategy<Value = Method> {
    prop_oneof![
        // Connection
        (
            any::<u8>(),
            any::<u8>(),
            Just(FieldTable::new()),
            prop::collection::vec(any::<u8>(), 0..16),
            prop::collection::vec(any::<u8>(), 0..16),
        )
            .prop_map(|(maj, min, sp, mech, loc)| {
                Method::ConnectionStart(Box::new(ConnectionStartArgs {
                    version_major: maj,
                    version_minor: min,
                    server_properties: sp,
                    mechanisms: mech,
                    locales: loc,
                }))
            }),
        (
            Just(FieldTable::new()),
            arb_short_string(),
            prop::collection::vec(any::<u8>(), 0..16),
            arb_short_string(),
        )
            .prop_map(|(cp, mech, resp, loc)| {
                Method::ConnectionStartOk(Box::new(ConnectionStartOkArgs {
                    client_properties: cp,
                    mechanism: mech.into(),
                    response: resp,
                    locale: loc.into(),
                }))
            }),
        (any::<u16>(), any::<u32>(), any::<u16>()).prop_map(|(cm, fm, hb)| {
            Method::ConnectionTune(Box::new(ConnectionTuneArgs {
                channel_max: cm,
                frame_max: fm,
                heartbeat: hb,
            }))
        }),
        (any::<u16>(), any::<u32>(), any::<u16>()).prop_map(|(cm, fm, hb)| {
            Method::ConnectionTuneOk(Box::new(ConnectionTuneOkArgs {
                channel_max: cm,
                frame_max: fm,
                heartbeat: hb,
            }))
        }),
        arb_short_string().prop_map(|vh| {
            Method::ConnectionOpen(Box::new(ConnectionOpenArgs { vhost: vh.into() }))
        }),
        Just(Method::ConnectionOpenOk),
        (any::<u16>(), arb_short_string(), any::<u16>(), any::<u16>()).prop_map(
            |(rc, rt, ci, mi)| {
                Method::ConnectionClose(Box::new(ConnectionCloseArgs {
                    reply_code: rc,
                    reply_text: rt.into(),
                    class_id: ci,
                    method_id: mi,
                }))
            }
        ),
        Just(Method::ConnectionCloseOk),
        arb_short_string().prop_map(|r| {
            Method::ConnectionBlocked(Box::new(ConnectionBlockedArgs { reason: r.into() }))
        }),
        Just(Method::ConnectionUnblocked),
        (
            prop::collection::vec(any::<u8>(), 0..16),
            arb_short_string(),
        )
            .prop_map(|(s, r)| {
                Method::ConnectionUpdateSecret(Box::new(ConnectionUpdateSecretArgs {
                    secret: s,
                    reason: r.into(),
                }))
            }),
        Just(Method::ConnectionUpdateSecretOk),
        // Channel
        Just(Method::ChannelOpen),
        Just(Method::ChannelOpenOk),
        any::<bool>().prop_map(|a| Method::ChannelFlow(Box::new(ChannelFlowArgs { active: a }))),
        any::<bool>()
            .prop_map(|a| Method::ChannelFlowOk(Box::new(ChannelFlowOkArgs { active: a }))),
        (any::<u16>(), arb_short_string(), any::<u16>(), any::<u16>()).prop_map(
            |(rc, rt, ci, mi)| {
                Method::ChannelClose(Box::new(ChannelCloseArgs {
                    reply_code: rc,
                    reply_text: rt.into(),
                    class_id: ci,
                    method_id: mi,
                }))
            }
        ),
        Just(Method::ChannelCloseOk),
        // Exchange
        (
            arb_short_string(),
            arb_short_string(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(|(ex, kind, p, d, ad, i, nw)| {
                Method::ExchangeDeclare(Box::new(ExchangeDeclareArgs {
                    exchange: ex.into(),
                    kind: kind.into(),
                    passive: p,
                    durable: d,
                    auto_delete: ad,
                    internal: i,
                    nowait: nw,
                    arguments: FieldTable::new(),
                }))
            }),
        Just(Method::ExchangeDeclareOk),
        (arb_short_string(), any::<bool>(), any::<bool>()).prop_map(|(ex, iu, nw)| {
            Method::ExchangeDelete(Box::new(ExchangeDeleteArgs {
                exchange: ex.into(),
                if_unused: iu,
                nowait: nw,
            }))
        }),
        Just(Method::ExchangeDeleteOk),
        (
            arb_short_string(),
            arb_short_string(),
            arb_short_string(),
            any::<bool>(),
        )
            .prop_map(|(d, s, rk, nw)| {
                Method::ExchangeBind(Box::new(ExchangeBindArgs {
                    destination: d.into(),
                    source: s.into(),
                    routing_key: rk.into(),
                    nowait: nw,
                    arguments: FieldTable::new(),
                }))
            }),
        Just(Method::ExchangeBindOk),
        (
            arb_short_string(),
            arb_short_string(),
            arb_short_string(),
            any::<bool>(),
        )
            .prop_map(|(d, s, rk, nw)| {
                Method::ExchangeUnbind(Box::new(ExchangeUnbindArgs {
                    destination: d.into(),
                    source: s.into(),
                    routing_key: rk.into(),
                    nowait: nw,
                    arguments: FieldTable::new(),
                }))
            }),
        Just(Method::ExchangeUnbindOk),
        // Queue
        (
            arb_short_string(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(|(q, p, d, ex, ad, nw)| {
                Method::QueueDeclare(Box::new(QueueDeclareArgs {
                    queue: q.into(),
                    passive: p,
                    durable: d,
                    exclusive: ex,
                    auto_delete: ad,
                    nowait: nw,
                    arguments: FieldTable::new(),
                }))
            }),
        (arb_short_string(), any::<u32>(), any::<u32>()).prop_map(|(q, mc, cc)| {
            Method::QueueDeclareOk(Box::new(QueueDeclareOkArgs {
                queue: q.into(),
                message_count: mc,
                consumer_count: cc,
            }))
        }),
        (
            arb_short_string(),
            arb_short_string(),
            arb_short_string(),
            any::<bool>(),
        )
            .prop_map(|(q, ex, rk, nw)| {
                Method::QueueBind(Box::new(QueueBindArgs {
                    queue: q.into(),
                    exchange: ex.into(),
                    routing_key: rk.into(),
                    nowait: nw,
                    arguments: FieldTable::new(),
                }))
            }),
        Just(Method::QueueBindOk),
        (arb_short_string(), any::<bool>()).prop_map(|(q, nw)| {
            Method::QueuePurge(Box::new(QueuePurgeArgs {
                queue: q.into(),
                nowait: nw,
            }))
        }),
        (
            arb_short_string(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>()
        )
            .prop_map(|(q, iu, ie, nw)| {
                Method::QueueDelete(Box::new(QueueDeleteArgs {
                    queue: q.into(),
                    if_unused: iu,
                    if_empty: ie,
                    nowait: nw,
                }))
            }),
        (arb_short_string(), arb_short_string(), arb_short_string(),).prop_map(|(q, ex, rk)| {
            Method::QueueUnbind(Box::new(QueueUnbindArgs {
                queue: q.into(),
                exchange: ex.into(),
                routing_key: rk.into(),
                arguments: FieldTable::new(),
            }))
        }),
        Just(Method::QueueUnbindOk),
        // Basic
        (any::<u32>(), any::<u16>(), any::<bool>()).prop_map(|(ps, pc, g)| {
            Method::BasicQos(Box::new(BasicQosArgs {
                prefetch_size: ps,
                prefetch_count: pc,
                global: g,
            }))
        }),
        Just(Method::BasicQosOk),
        (
            arb_short_string(),
            arb_short_string(),
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(|(q, ct, na, ex, nw)| {
                Method::BasicConsume(Box::new(BasicConsumeArgs {
                    queue: q.into(),
                    consumer_tag: ct.into(),
                    no_local: false,
                    no_ack: na,
                    exclusive: ex,
                    nowait: nw,
                    arguments: FieldTable::new(),
                }))
            }),
        arb_short_string().prop_map(|ct| {
            Method::BasicConsumeOk(Box::new(BasicConsumeOkArgs {
                consumer_tag: ct.into(),
            }))
        }),
        (arb_short_string(), any::<bool>()).prop_map(|(ct, nw)| {
            Method::BasicCancel(Box::new(BasicCancelArgs {
                consumer_tag: ct.into(),
                nowait: nw,
            }))
        }),
        arb_short_string().prop_map(|ct| {
            Method::BasicCancelOk(Box::new(BasicCancelOkArgs {
                consumer_tag: ct.into(),
            }))
        }),
        (
            arb_short_string(),
            arb_short_string(),
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(|(ex, rk, m, i)| {
                Method::BasicPublish(Box::new(BasicPublishArgs {
                    exchange: ex.into(),
                    routing_key: rk.into(),
                    mandatory: m,
                    immediate: i,
                }))
            }),
        (
            any::<u16>(),
            arb_short_string(),
            arb_short_string(),
            arb_short_string(),
        )
            .prop_map(|(rc, rt, ex, rk)| Method::BasicReturn {
                reply_code: rc,
                reply_text: rt.into(),
                exchange: ex.into(),
                routing_key: rk.into(),
            }),
        (
            arb_short_string(),
            any::<u64>(),
            any::<bool>(),
            arb_short_string(),
            arb_short_string(),
        )
            .prop_map(|(ct, dt, r, ex, rk)| Method::BasicDeliver {
                consumer_tag: ct.into(),
                delivery_tag: dt,
                redelivered: r,
                exchange: ex.into(),
                routing_key: rk.into(),
            }),
        (arb_short_string(), any::<bool>()).prop_map(|(q, na)| {
            Method::BasicGet(Box::new(BasicGetArgs {
                queue: q.into(),
                no_ack: na,
            }))
        }),
        (
            any::<u64>(),
            any::<bool>(),
            arb_short_string(),
            arb_short_string(),
            any::<u32>(),
        )
            .prop_map(|(dt, r, ex, rk, mc)| {
                Method::BasicGetOk(Box::new(BasicGetOkArgs {
                    delivery_tag: dt,
                    redelivered: r,
                    exchange: ex.into(),
                    routing_key: rk.into(),
                    message_count: mc,
                }))
            }),
        Just(Method::BasicGetEmpty),
        (any::<u64>(), any::<bool>()).prop_map(|(dt, m)| Method::BasicAck {
            delivery_tag: dt,
            multiple: m,
        }),
        (any::<u64>(), any::<bool>()).prop_map(|(dt, r)| {
            Method::BasicReject(Box::new(BasicRejectArgs {
                delivery_tag: dt,
                requeue: r,
            }))
        }),
        Just(Method::BasicRecoverOk),
        (any::<u64>(), any::<bool>(), any::<bool>()).prop_map(|(dt, m, r)| Method::BasicNack {
            delivery_tag: dt,
            multiple: m,
            requeue: r,
        }),
        // Tx
        Just(Method::TxSelect),
        Just(Method::TxSelectOk),
        Just(Method::TxCommit),
        Just(Method::TxCommitOk),
        Just(Method::TxRollback),
        Just(Method::TxRollbackOk),
        // Confirm
        any::<bool>()
            .prop_map(|nw| { Method::ConfirmSelect(Box::new(ConfirmSelectArgs { nowait: nw })) }),
        Just(Method::ConfirmSelectOk),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_method_round_trip(method in arb_method()) {
        let mut buf = Vec::new();
        serialize_method(&method, &mut buf).unwrap();
        let (remaining, parsed) = parse_method(&buf).unwrap();
        prop_assert!(remaining.is_empty(), "trailing bytes: {}", remaining.len());

        // basic.consume serialization hardcodes no_local=false, so normalize before comparison
        let expected = match method {
            Method::BasicConsume(ref args) if args.no_local => {
                let mut args = args.clone();
                args.no_local = false;
                Method::BasicConsume(args)
            }
            other => other,
        };
        prop_assert_eq!(parsed, expected);
    }
}
