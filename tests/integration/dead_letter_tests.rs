// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::time::Duration;

use crate::test_helpers::connect;
use bunny_rs::SubscribeOptions;
use bunny_rs::options::{
    ExchangeDeclareOptions, ExchangeDeleteOptions, PublishOptions, QueueDeclareOptions,
    QueueDeleteOptions,
};
use bunny_rs::protocol::types::FieldTable;

#[tokio::test]
async fn test_dead_letter_on_reject() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    // DLX and DL queue
    ch.exchange_declare(
        "bunny-rs.test.dlx",
        "fanout",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("bunny-rs.test.dl-q", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_bind(
        "bunny-rs.test.dl-q",
        "bunny-rs.test.dlx",
        "",
        FieldTable::new(),
    )
    .await
    .unwrap();

    // source queue with DLX
    ch.queue_declare(
        "bunny-rs.test.dl-source",
        QueueDeclareOptions::default().dead_letter_exchange("bunny-rs.test.dlx"),
    )
    .await
    .unwrap();
    ch.queue_purge("bunny-rs.test.dl-source").await.unwrap();
    ch.queue_purge("bunny-rs.test.dl-q").await.unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.dl-source",
        &PublishOptions::default(),
        b"dead-letter-me",
    )
    .await
    .unwrap();

    let mut sub = ch
        .queue("bunny-rs.test.dl-source")
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("dl-consumer"))
        .await
        .unwrap();

    let delivery = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.body.as_ref(), b"dead-letter-me");

    // Reject without requeue routes to the DLX
    delivery.discard().await.unwrap();
    sub.cancel().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // message should appear in the DL queue
    let dl = ch.basic_get("bunny-rs.test.dl-q", true).await.unwrap();
    assert!(dl.is_some(), "message should have been dead-lettered");
    assert_eq!(dl.unwrap().body.as_ref(), b"dead-letter-me");

    // cleanup
    ch.queue_delete("bunny-rs.test.dl-source", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.queue_delete("bunny-rs.test.dl-q", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.exchange_delete("bunny-rs.test.dlx", ExchangeDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_dead_letter_on_message_ttl_expiry() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.exchange_declare(
        "bunny-rs.test.ttl-dlx",
        "fanout",
        ExchangeDeclareOptions::default(),
    )
    .await
    .unwrap();
    ch.queue_declare("bunny-rs.test.ttl-dl-q", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_bind(
        "bunny-rs.test.ttl-dl-q",
        "bunny-rs.test.ttl-dlx",
        "",
        FieldTable::new(),
    )
    .await
    .unwrap();

    // queue with short TTL and DLX
    ch.queue_declare(
        "bunny-rs.test.ttl-source",
        QueueDeclareOptions::default()
            .message_ttl(Duration::from_millis(200))
            .dead_letter_exchange("bunny-rs.test.ttl-dlx"),
    )
    .await
    .unwrap();
    ch.queue_purge("bunny-rs.test.ttl-source").await.unwrap();
    ch.queue_purge("bunny-rs.test.ttl-dl-q").await.unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.ttl-source",
        &PublishOptions::default(),
        b"expire-me",
    )
    .await
    .unwrap();

    // wait for TTL to expire
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let dl = ch.basic_get("bunny-rs.test.ttl-dl-q", true).await.unwrap();
    assert!(
        dl.is_some(),
        "message should have expired and been dead-lettered"
    );
    assert_eq!(dl.unwrap().body.as_ref(), b"expire-me");

    ch.queue_delete("bunny-rs.test.ttl-source", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.queue_delete("bunny-rs.test.ttl-dl-q", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.exchange_delete("bunny-rs.test.ttl-dlx", ExchangeDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
