// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use proptest::prelude::*;

use bunny_rs::connection::negotiate_heartbeat;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_both_zero_yields_zero(server in 0u16..=0, client in 0u16..=0) {
        assert_eq!(negotiate_heartbeat(server, client), 0);
    }

    #[test]
    fn prop_one_zero_yields_nonzero(nonzero in 1u16..=u16::MAX) {
        // If server is 0, use client's value
        assert_eq!(negotiate_heartbeat(0, nonzero), nonzero);
        // If client is 0, use server's value
        assert_eq!(negotiate_heartbeat(nonzero, 0), nonzero);
    }

    #[test]
    fn prop_both_nonzero_yields_min(
        server in 1u16..=u16::MAX,
        client in 1u16..=u16::MAX,
    ) {
        let result = negotiate_heartbeat(server, client);
        assert_eq!(result, server.min(client));
    }

    #[test]
    fn prop_result_never_exceeds_either_input(
        server in 0u16..=u16::MAX,
        client in 0u16..=u16::MAX,
    ) {
        let result = negotiate_heartbeat(server, client);
        assert!(result <= server.max(client));
    }

    #[test]
    fn prop_result_is_zero_only_when_both_zero(
        server in 0u16..=u16::MAX,
        client in 0u16..=u16::MAX,
    ) {
        let result = negotiate_heartbeat(server, client);
        if server > 0 || client > 0 {
            assert!(result > 0, "expected nonzero when at least one input is nonzero");
        }
    }

    #[test]
    fn prop_symmetric(
        server in 0u16..=u16::MAX,
        client in 0u16..=u16::MAX,
    ) {
        assert_eq!(
            negotiate_heartbeat(server, client),
            negotiate_heartbeat(client, server),
        );
    }
}
