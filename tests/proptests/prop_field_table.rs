// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use bytes::Bytes;
use proptest::prelude::*;

use bunny_rs::protocol::types::{
    Decimal, FieldTable, FieldValue, parse_field_table, parse_field_value, parse_short_string,
    serialize_field_table, serialize_field_value, serialize_short_string,
};

fn arb_field_value() -> impl Strategy<Value = FieldValue> {
    let leaf = prop_oneof![
        any::<bool>().prop_map(FieldValue::Bool),
        any::<i8>().prop_map(FieldValue::I8),
        any::<u8>().prop_map(FieldValue::U8),
        any::<i16>().prop_map(FieldValue::I16),
        any::<u16>().prop_map(FieldValue::U16),
        any::<i32>().prop_map(FieldValue::I32),
        any::<u32>().prop_map(FieldValue::U32),
        any::<i64>().prop_map(FieldValue::I64),
        any::<f32>()
            .prop_filter("not NaN", |v| !v.is_nan())
            .prop_map(FieldValue::F32),
        any::<f64>()
            .prop_filter("not NaN", |v| !v.is_nan())
            .prop_map(FieldValue::F64),
        (any::<u8>(), any::<u32>())
            .prop_map(|(s, v)| FieldValue::Decimal(Decimal { scale: s, value: v })),
        any::<u64>().prop_map(FieldValue::Timestamp),
        prop::collection::vec(any::<u8>(), 0..64)
            .prop_map(|v| FieldValue::LongString(Bytes::from(v))),
        prop::collection::vec(any::<u8>(), 0..64)
            .prop_map(|v| FieldValue::ByteArray(Bytes::from(v))),
        Just(FieldValue::Void),
    ];
    leaf.prop_recursive(3, 32, 4, |inner| {
        prop_oneof![
            prop::collection::vec(("[a-zA-Z_]{1,20}", inner.clone()), 0..4).prop_map(|entries| {
                let mut t = FieldTable::new();
                for (k, v) in entries {
                    t.insert(k, v);
                }
                FieldValue::Table(t)
            }),
            prop::collection::vec(inner, 0..4).prop_map(FieldValue::Array),
        ]
    })
}

fn arb_field_table() -> impl Strategy<Value = FieldTable> {
    prop::collection::vec(("[a-zA-Z_]{1,20}", arb_field_value()), 0..6).prop_map(|entries| {
        let mut t = FieldTable::new();
        for (k, v) in entries {
            t.insert(k, v);
        }
        t
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_field_value_round_trip(value in arb_field_value()) {
        let mut buf = Vec::new();
        serialize_field_value(&value, &mut buf);
        let (remaining, parsed) = parse_field_value(&buf).unwrap();
        prop_assert!(remaining.is_empty(), "trailing bytes: {}", remaining.len());
        prop_assert_eq!(parsed, value);
    }

    #[test]
    fn prop_field_table_round_trip(table in arb_field_table()) {
        let mut buf = Vec::new();
        serialize_field_table(&table, &mut buf);
        let (remaining, parsed) = parse_field_table(&buf).unwrap();
        prop_assert!(remaining.is_empty(), "trailing bytes: {}", remaining.len());
        prop_assert_eq!(parsed, table);
    }

    #[test]
    fn prop_short_string_round_trip(s in "[\\x20-\\x7E]{0,255}") {
        let mut buf = Vec::new();
        serialize_short_string(&s, &mut buf).unwrap();
        let (remaining, parsed) = parse_short_string(&buf).unwrap();
        prop_assert!(remaining.is_empty());
        prop_assert_eq!(parsed, s.as_str());
    }

    #[test]
    fn prop_short_string_rejects_overlong(s in "[a-z]{256,512}") {
        let mut buf = Vec::new();
        prop_assert!(serialize_short_string(&s, &mut buf).is_err());
    }

    #[test]
    fn prop_short_string_multibyte(s in "\\PC{0,100}") {
        let mut buf = Vec::new();
        if s.len() <= 255 {
            serialize_short_string(&s, &mut buf).unwrap();
            let (_, parsed) = parse_short_string(&buf).unwrap();
            prop_assert_eq!(parsed, s.as_str());
        } else {
            prop_assert!(serialize_short_string(&s, &mut buf).is_err());
        }
    }

    #[test]
    fn prop_parser_never_panics(data in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = parse_field_table(&data);
        let _ = parse_field_value(&data);
        let _ = parse_short_string(&data);
    }
}
