// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{ConsumeOptions, PublishOptions, QueueDeclareOptions, QueueDeleteOptions};

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

    ch.basic_consume(
        "bunny-rs.test.reject-requeue",
        "reject-consumer",
        ConsumeOptions::default(),
    )
    .await
    .unwrap();

    let delivery = ch.recv_delivery().await.unwrap().unwrap();
    assert_eq!(delivery.body.as_ref(), b"reject-me");
    assert!(!delivery.redelivered);

    ch.basic_reject(delivery.delivery_tag, true).await.unwrap();

    // should be redelivered
    let delivery2 = ch.recv_delivery().await.unwrap().unwrap();
    assert_eq!(delivery2.body.as_ref(), b"reject-me");
    assert!(delivery2.redelivered);
    ch.basic_ack(delivery2.delivery_tag, false).await.unwrap();

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

    ch.basic_consume(
        "bunny-rs.test.reject-discard",
        "discard-consumer",
        ConsumeOptions::default(),
    )
    .await
    .unwrap();

    let delivery = ch.recv_delivery().await.unwrap().unwrap();
    ch.basic_reject(delivery.delivery_tag, false).await.unwrap();

    // queue should be empty — message was discarded
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
