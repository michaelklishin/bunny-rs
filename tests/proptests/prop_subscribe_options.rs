// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use proptest::prelude::*;

use bunny_rs::SubscribeOptions;
use bunny_rs::options::ConsumeOptions;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Field setters never cross-contaminate: setting one field leaves the
    /// other fields at their previous values.
    #[test]
    fn prop_setters_independent(
        tag in "[a-z]{0,16}",
        prefetch in 0u16..=u16::MAX,
        exclusive in any::<bool>(),
    ) {
        let opts = SubscribeOptions::manual_ack()
            .consumer_tag(tag.as_str())
            .prefetch(prefetch);
        let opts = if exclusive { opts.exclusive() } else { opts };

        prop_assert_eq!(opts.consumer_tag.as_deref(), Some(tag.as_str()));
        prop_assert_eq!(opts.prefetch, Some(prefetch));
        prop_assert_eq!(opts.consume.exclusive, exclusive);
        // manual_ack default
        prop_assert!(!opts.consume.no_ack);
    }

    /// `auto_ack` and `manual_ack` factories produce the documented `no_ack`
    /// value regardless of which other setters follow.
    #[test]
    fn prop_ack_mode_preserved_through_chain(
        tag in "[a-z]{0,16}",
        prefetch in 0u16..=u16::MAX,
    ) {
        let auto = SubscribeOptions::auto_ack()
            .consumer_tag(tag.as_str())
            .prefetch(prefetch)
            .exclusive();
        prop_assert!(auto.consume.no_ack);

        let manual = SubscribeOptions::manual_ack()
            .consumer_tag(tag.as_str())
            .prefetch(prefetch)
            .exclusive();
        prop_assert!(!manual.consume.no_ack);
    }

    /// Pins the documented overwrite behaviour of `with_consume`.
    #[test]
    fn prop_with_consume_overwrites_no_ack(no_ack in any::<bool>()) {
        let opts = SubscribeOptions::auto_ack()
            .with_consume(ConsumeOptions::default().no_ack(no_ack));
        prop_assert_eq!(opts.consume.no_ack, no_ack);
    }
}
