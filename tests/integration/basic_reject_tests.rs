// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::time::Duration;

use crate::test_helpers::connect;
use bunny_rs::SubscribeOptions;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};

async fn next_delivery(sub: &mut bunny_rs::Consumer) -> bunny_rs::Delivery {
    tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("consumer stalled")
        .expect("consumer closed")
}

#[tokio::test]
async fn test_reject_with_requeue() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare(
        "bunny-rs.test.reject-requeue",
        QueueDeclareOptions::default(),
    )
    .await
    .unwrap();
    ch.queue_purge("bunny-rs.test.reject-requeue")
        .await
        .unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.reject-requeue",
        &PublishOptions::default(),
        b"reject-me",
    )
    .await
    .unwrap();

    let mut sub = ch
        .queue("bunny-rs.test.reject-requeue")
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("reject-consumer"))
        .await
        .unwrap();

    let delivery = next_delivery(&mut sub).await;
    assert_eq!(delivery.body.as_ref(), b"reject-me");
    assert!(!delivery.redelivered);
    delivery.reject().await.unwrap();

    let delivery2 = next_delivery(&mut sub).await;
    assert_eq!(delivery2.body.as_ref(), b"reject-me");
    assert!(delivery2.redelivered);
    delivery2.ack().await.unwrap();

    sub.cancel().await.unwrap();
    ch.queue_delete(
        "bunny-rs.test.reject-requeue",
        QueueDeleteOptions::default(),
    )
    .await
    .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_reject_without_requeue() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare(
        "bunny-rs.test.reject-discard",
        QueueDeclareOptions::default(),
    )
    .await
    .unwrap();
    ch.queue_purge("bunny-rs.test.reject-discard")
        .await
        .unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.reject-discard",
        &PublishOptions::default(),
        b"discard-me",
    )
    .await
    .unwrap();

    let mut sub = ch
        .queue("bunny-rs.test.reject-discard")
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("discard-consumer"))
        .await
        .unwrap();

    let delivery = next_delivery(&mut sub).await;
    delivery.discard().await.unwrap();
    sub.cancel().await.unwrap();

    // Queue should be empty: the message was discarded.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let empty = ch
        .basic_get("bunny-rs.test.reject-discard", true)
        .await
        .unwrap();
    assert!(empty.is_none());

    ch.queue_delete(
        "bunny-rs.test.reject-discard",
        QueueDeleteOptions::default(),
    )
    .await
    .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
