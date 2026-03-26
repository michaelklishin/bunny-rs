// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use bytes::Bytes;
use proptest::prelude::*;

use bunny_rs::protocol::frame::{ContentHeader, Frame, parse_frame, serialize_frame};
use bunny_rs::protocol::method::{
    BasicPublishArgs, ChannelCloseArgs, ConnectionCloseArgs, ExchangeDeclareArgs, Method,
    QueueDeclareArgs, QueueDeclareOkArgs,
};
use bunny_rs::protocol::types::FieldTable;

fn arb_short_string() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9._\\-]{0,50}"
}

fn arb_method_frame() -> impl Strategy<Value = Frame> {
    let methods = prop_oneof![
        Just(Method::ChannelOpen),
        Just(Method::ChannelOpenOk),
        Just(Method::ChannelCloseOk),
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
            any::<bool>(),
            any::<bool>()
        )
            .prop_map(|(ex, rk, m, i)| {
                Method::BasicPublish(Box::new(BasicPublishArgs {
                    exchange: ex.into(),
                    routing_key: rk.into(),
                    mandatory: m,
                    immediate: i,
                }))
            }),
    ];
    (1u16..=100, methods).prop_map(|(ch, m)| Frame::Method(ch, Box::new(m)))
}

fn arb_frame() -> impl Strategy<Value = Frame> {
    prop_oneof![
        arb_method_frame(),
        Just(Frame::Heartbeat),
        (1u16..=100, prop::collection::vec(any::<u8>(), 0..256))
            .prop_map(|(ch, data)| Frame::Body(ch, Bytes::from(data))),
        (
            1u16..=100,
            any::<u16>(),
            any::<u64>(),
            prop::collection::vec(any::<u8>(), 0..64)
        )
            .prop_map(|(ch, class_id, body_size, props)| {
                Frame::Header(
                    ch,
                    ContentHeader {
                        class_id,
                        body_size,
                        properties_raw: Bytes::from(props),
                    },
                )
            }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_frame_round_trip(frame in arb_frame()) {
        let mut buf = Vec::new();
        serialize_frame(&frame, &mut buf).unwrap();
        let (remaining, parsed) = parse_frame(&buf).unwrap();
        prop_assert!(remaining.is_empty(), "trailing bytes: {}", remaining.len());
        prop_assert_eq!(parsed, frame);
    }
}
