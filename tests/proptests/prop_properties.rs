// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use proptest::prelude::*;

use bunny_rs::protocol::properties::{
    BasicProperties, DeliveryMode, parse_basic_properties, serialize_basic_properties,
};
use bunny_rs::protocol::types::FieldTable;

fn arb_basic_properties() -> impl Strategy<Value = BasicProperties> {
    // proptest tuple max is 12 elements, so split into two groups
    let first = (
        proptest::option::of("[a-z/]{1,30}"),
        proptest::option::of("[a-z]{1,15}"),
        proptest::option::of(Just(FieldTable::new())),
        proptest::option::of(prop_oneof![Just(1u8), Just(2u8)]),
        proptest::option::of(0..10u8),
        proptest::option::of("[a-f0-9]{8,32}"),
        proptest::option::of("[a-z.]{1,30}"),
    );
    let second = (
        proptest::option::of("[0-9]{1,10}"),
        proptest::option::of("[a-f0-9]{8,32}"),
        proptest::option::of(any::<u64>()),
        proptest::option::of("[a-z.]{1,30}"),
        proptest::option::of("[a-z]{1,15}"),
        proptest::option::of("[a-z]{1,15}"),
    );
    (first, second).prop_map(
        |((ct, ce, h, dm, pri, cid, rt), (exp, mid, ts, mt, uid, aid))| {
            let mut p = BasicProperties::default();
            if let Some(v) = ct {
                p = p.content_type(v);
            }
            if let Some(v) = ce {
                p = p.content_encoding(v);
            }
            if let Some(v) = h {
                p = p.headers(v);
            }
            if let Some(v) = dm {
                p = p.delivery_mode(if v == 2 {
                    DeliveryMode::Persistent
                } else {
                    DeliveryMode::Transient
                });
            }
            if let Some(v) = pri {
                p = p.priority(v);
            }
            if let Some(v) = cid {
                p = p.correlation_id(v);
            }
            if let Some(v) = rt {
                p = p.reply_to(v);
            }
            if let Some(v) = exp {
                p = p.expiration(v);
            }
            if let Some(v) = mid {
                p = p.message_id(v);
            }
            if let Some(v) = ts {
                p = p.timestamp(v);
            }
            if let Some(v) = mt {
                p = p.message_type(v);
            }
            if let Some(v) = uid {
                p = p.user_id(v);
            }
            if let Some(v) = aid {
                p = p.app_id(v);
            }
            p
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_basic_properties_round_trip(props in arb_basic_properties()) {
        let mut buf = Vec::new();
        serialize_basic_properties(&props, &mut buf).unwrap();
        let (remaining, parsed) = parse_basic_properties(&buf).unwrap();
        prop_assert!(remaining.is_empty(), "trailing bytes: {}", remaining.len());
        prop_assert_eq!(parsed, props);
    }

    #[test]
    fn prop_empty_properties_round_trip(_dummy in Just(())) {
        let props = BasicProperties::default();
        let mut buf = Vec::new();
        serialize_basic_properties(&props, &mut buf).unwrap();
        let (remaining, parsed) = parse_basic_properties(&buf).unwrap();
        prop_assert!(remaining.is_empty());
        prop_assert_eq!(parsed, props);
    }

    #[test]
    fn prop_properties_parser_never_panics(data in prop::collection::vec(any::<u8>(), 2..128)) {
        let _ = parse_basic_properties(&data);
    }
}
