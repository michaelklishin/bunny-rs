// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

//! Each `Delivery` and `Channel` nack shortcut maps to a specific
//! `(multiple, requeue)` pair. The tests in this module cover the wire format: the
//! four pairs must encode to four different byte sequences, and any pair
//! must survive an encode-decode roundtrip unchanged.

use bunny_rs::protocol::method::{Method, parse_method, serialize_method};
use proptest::prelude::*;

fn encode(method: &Method) -> Vec<u8> {
    let mut buf = Vec::new();
    serialize_method(method, &mut buf).unwrap();
    buf
}

#[test]
fn basic_nack_combinations_are_wire_distinct() {
    let tag = 0xDEAD_BEEF_u64;
    let combos = [
        // nack
        (false, true),
        // nack_multiple
        (true, true),
        // nack_discard
        (false, false),
        // nack_discard_multiple
        (true, false),
    ];
    let mut frames: Vec<Vec<u8>> = combos
        .iter()
        .map(|&(multiple, requeue)| {
            encode(&Method::BasicNack {
                delivery_tag: tag,
                multiple,
                requeue,
            })
        })
        .collect();
    frames.sort();
    frames.dedup();
    assert_eq!(frames.len(), 4);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // For any delivery_tag, every (multiple, requeue) pair must survive an
    // encode-decode cycle into the matching `BasicNack` variant. This is
    // the semantic guarantee the `nack`, `nack_multiple`, `nack_discard`,
    // and `nack_discard_multiple` shortcuts depend on.
    #[test]
    fn basic_nack_round_trip_preserves_flags(
        tag in any::<u64>(),
        multiple in any::<bool>(),
        requeue in any::<bool>(),
    ) {
        let buf = encode(&Method::BasicNack { delivery_tag: tag, multiple, requeue });
        let (rest, parsed) = parse_method(&buf).unwrap();
        prop_assert!(rest.is_empty());
        prop_assert_eq!(
            parsed,
            Method::BasicNack { delivery_tag: tag, multiple, requeue }
        );
    }
}
