// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use proptest::prelude::*;

use bunny_rs::AddressResolver;
use bunny_rs::Endpoint;

fn arb_host() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9.\\-]{0,30}"
}

fn arb_port() -> impl Strategy<Value = u16> {
    1u16..=65535u16
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    #[test]
    fn prop_endpoint_display_format(host in arb_host(), port in arb_port()) {
        let ep = Endpoint::new(host.clone(), port);
        let display = format!("{ep}");
        prop_assert_eq!(display, format!("{host}:{port}"));
    }

    #[test]
    fn prop_endpoint_from_host_uses_default_port(host in arb_host()) {
        let ep = Endpoint::from_host(host.clone());
        prop_assert_eq!(ep.host, host);
        prop_assert_eq!(ep.port, 5672);
    }

    #[test]
    fn prop_endpoint_equality_is_reflexive(host in arb_host(), port in arb_port()) {
        let ep = Endpoint::new(host, port);
        prop_assert_eq!(&ep, &ep.clone());
    }

    #[test]
    fn prop_endpoint_equality_detects_port_difference(
        host in arb_host(),
        p1 in arb_port(),
        p2 in arb_port(),
    ) {
        prop_assume!(p1 != p2);
        let a = Endpoint::new(host.clone(), p1);
        let b = Endpoint::new(host, p2);
        prop_assert_ne!(a, b);
    }

    #[test]
    fn prop_endpoint_equality_detects_host_difference(
        h1 in arb_host(),
        h2 in arb_host(),
        port in arb_port(),
    ) {
        prop_assume!(h1 != h2);
        let a = Endpoint::new(h1, port);
        let b = Endpoint::new(h2, port);
        prop_assert_ne!(a, b);
    }

    #[test]
    fn prop_single_endpoint_creates_dns_resolver(host in arb_host(), port in arb_port()) {
        let ep = Endpoint::new(host, port);
        let resolver = AddressResolver::from_endpoints(vec![ep]);
        prop_assert!(matches!(resolver, AddressResolver::Dns(_)));
    }

    #[test]
    fn prop_multiple_endpoints_creates_list_resolver(
        h1 in arb_host(),
        h2 in arb_host(),
        p1 in arb_port(),
        p2 in arb_port(),
    ) {
        let eps = vec![Endpoint::new(h1, p1), Endpoint::new(h2, p2)];
        let resolver = AddressResolver::from_endpoints(eps);
        prop_assert!(matches!(resolver, AddressResolver::List(_)));
    }
}

#[tokio::test]
async fn test_list_resolver_returns_endpoints_as_is() {
    let eps = vec![
        Endpoint::new("rabbit1", 5672),
        Endpoint::new("rabbit2", 5673),
        Endpoint::new("rabbit3", 5674),
    ];
    let resolver = AddressResolver::from_endpoints(eps.clone());
    let resolved = resolver.resolve().await;
    assert_eq!(resolved, eps);
}

#[tokio::test]
async fn test_dns_resolver_falls_back_on_unresolvable_host() {
    let ep = Endpoint::new("this-host-does-not-exist.invalid", 5672);
    let resolver = AddressResolver::from_endpoints(vec![ep.clone()]);
    let resolved = resolver.resolve().await;
    assert_eq!(resolved, vec![ep]);
}

#[tokio::test]
async fn test_dns_resolver_resolves_localhost() {
    let resolver = AddressResolver::from_endpoints(vec![Endpoint::from_host("localhost")]);
    let resolved = resolver.resolve().await;
    assert!(!resolved.is_empty());
    for ep in &resolved {
        assert_eq!(ep.port, 5672);
    }
}

#[tokio::test]
async fn test_shuffle_changes_order_eventually() {
    let eps = vec![
        Endpoint::new("a", 1),
        Endpoint::new("b", 2),
        Endpoint::new("c", 3),
        Endpoint::new("d", 4),
        Endpoint::new("e", 5),
    ];
    let resolver = AddressResolver::List(eps.clone());

    // With 5 elements, the chance of the same order is 1/120.
    // Over 10 tries, the chance of never shuffling is (1/120)^10 ≈ 0.
    let mut saw_different = false;
    for _ in 0..10 {
        let shuffled = resolver.resolve_and_shuffle().await;
        if shuffled != eps {
            saw_different = true;
            break;
        }
    }
    assert!(saw_different, "shuffle never produced a different order");
}

//
// ConnectionOptions::from_uri / from_uris
//

use bunny_rs::ConnectionOptions;

#[test]
fn test_from_uri_parses_host_and_port() {
    let opts = ConnectionOptions::from_uri("amqp://rabbit1:5673/").unwrap();
    assert_eq!(opts.host, "rabbit1");
    assert_eq!(opts.port, 5673);
    assert!(opts.endpoints.is_empty());
}

#[test]
fn test_from_uri_defaults() {
    let opts = ConnectionOptions::from_uri("amqp://localhost").unwrap();
    assert_eq!(opts.host, "localhost");
    assert_eq!(opts.port, 5672);
    assert_eq!(opts.username, "guest");
    assert_eq!(opts.virtual_host, "/");
}

#[test]
fn test_from_uri_with_credentials_and_vhost() {
    let opts = ConnectionOptions::from_uri("amqp://user:pass@host1:5672/staging").unwrap();
    assert_eq!(opts.username, "user");
    assert_eq!(opts.password.as_str(), "pass");
    assert_eq!(opts.virtual_host, "staging");
}

#[test]
fn test_from_uris_single_uri() {
    let opts = ConnectionOptions::from_uris(&["amqp://rabbit1:5672/"]).unwrap();
    assert_eq!(opts.host, "rabbit1");
    assert_eq!(opts.port, 5672);
    assert!(
        opts.endpoints.is_empty(),
        "single URI should not populate endpoints"
    );
}

#[test]
fn test_from_uris_multiple_uris() {
    let opts = ConnectionOptions::from_uris(&[
        "amqp://user:pass@rabbit1:5672/prod",
        "amqp://user:pass@rabbit2:5673/prod",
        "amqp://user:pass@rabbit3:5674/prod",
    ])
    .unwrap();

    // Credentials from first URI
    assert_eq!(opts.username, "user");
    assert_eq!(opts.password.as_str(), "pass");
    assert_eq!(opts.virtual_host, "prod");

    // All three endpoints
    assert_eq!(opts.endpoints.len(), 3);
    assert_eq!(opts.endpoints[0], Endpoint::new("rabbit1", 5672));
    assert_eq!(opts.endpoints[1], Endpoint::new("rabbit2", 5673));
    assert_eq!(opts.endpoints[2], Endpoint::new("rabbit3", 5674));
}

#[test]
fn test_from_uris_empty_is_error() {
    let result = ConnectionOptions::from_uris(&[]);
    assert!(result.is_err());
}

#[test]
fn test_from_uris_invalid_uri_is_error() {
    let result = ConnectionOptions::from_uris(&["not-a-uri"]);
    assert!(result.is_err());
}

#[test]
fn test_from_uri_invalid_uri_is_error() {
    let result = ConnectionOptions::from_uri("not-a-uri");
    assert!(result.is_err());
}
