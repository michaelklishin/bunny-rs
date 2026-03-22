// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{
    ExchangeDeclareOptions, ExchangeDeleteOptions, PublishOptions, QueueDeclareOptions,
    QueueDeleteOptions,
};
use bunny_rs::protocol::properties::BasicProperties;
use bunny_rs::protocol::types::FieldTable;

#[tokio::test]
async fn test_headers_exchange_match_all() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.hx",
        "headers",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("bunny-rs.test.hx.q", QueueDeclareOptions::default())
        .await
        .unwrap();

    let mut binding_args = FieldTable::new();
    binding_args.insert("x-match", "all");
    binding_args.insert("region", "us");
    binding_args.insert("env", "prod");
    ch.queue_bind("bunny-rs.test.hx.q", "bunny-rs.test.hx", "", binding_args)
        .await
        .unwrap();

    // matching headers
    let mut matching_headers = FieldTable::new();
    matching_headers.insert("region", "us");
    matching_headers.insert("env", "prod");

    ch.basic_publish(
        "bunny-rs.test.hx",
        "",
        &PublishOptions {
            properties: BasicProperties::default().headers(matching_headers),
            ..Default::default()
        },
        b"matched",
    )
    .await
    .unwrap();

    // non-matching headers (missing env)
    let mut partial_headers = FieldTable::new();
    partial_headers.insert("region", "us");

    ch.basic_publish(
        "bunny-rs.test.hx",
        "",
        &PublishOptions {
            properties: BasicProperties::default().headers(partial_headers),
            ..Default::default()
        },
        b"unmatched",
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // only the matching message should arrive
    let d = ch.basic_get("bunny-rs.test.hx.q", true).await.unwrap();
    assert!(d.is_some());
    assert_eq!(d.unwrap().body.as_ref(), b"matched");

    let d2 = ch.basic_get("bunny-rs.test.hx.q", true).await.unwrap();
    assert!(d2.is_none());

    ch.queue_delete("bunny-rs.test.hx.q", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.exchange_delete("bunny-rs.test.hx", ExchangeDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_headers_exchange_match_any() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.hx-any",
        "headers",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("bunny-rs.test.hx-any.q", QueueDeclareOptions::default())
        .await
        .unwrap();

    let mut binding_args = FieldTable::new();
    binding_args.insert("x-match", "any");
    binding_args.insert("color", "red");
    binding_args.insert("size", "large");
    ch.queue_bind(
        "bunny-rs.test.hx-any.q",
        "bunny-rs.test.hx-any",
        "",
        binding_args,
    )
    .await
    .unwrap();

    // only one matching header — should still match with "any"
    let mut headers = FieldTable::new();
    headers.insert("color", "red");

    ch.basic_publish(
        "bunny-rs.test.hx-any",
        "",
        &PublishOptions {
            properties: BasicProperties::default().headers(headers),
            ..Default::default()
        },
        b"any-match",
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let d = ch.basic_get("bunny-rs.test.hx-any.q", true).await.unwrap();
    assert!(d.is_some());
    assert_eq!(d.unwrap().body.as_ref(), b"any-match");

    ch.queue_delete("bunny-rs.test.hx-any.q", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.exchange_delete("bunny-rs.test.hx-any", ExchangeDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
