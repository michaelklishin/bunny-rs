// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{ConsumeOptions, PublishOptions, QueueDeclareOptions, QueueDeleteOptions};

#[tokio::test]
async fn test_publish_256kb_payload() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.large-256k", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.large-256k").await.unwrap();

    // 256 KB > default frame_max (128 KB), forces multi-frame body splitting
    let body = vec![0xABu8; 256 * 1024];
    ch.basic_publish(
        "",
        "bunny-rs.test.large-256k",
        &PublishOptions::default(),
        &body,
    )
    .await
    .unwrap();

    ch.basic_consume(
        "bunny-rs.test.large-256k",
        "large-consumer",
        ConsumeOptions::default(),
    )
    .await
    .unwrap();

    let delivery = ch.recv_delivery().await.unwrap().unwrap();
    assert_eq!(delivery.body.len(), 256 * 1024);
    assert!(delivery.body.iter().all(|&b| b == 0xAB));
    ch.basic_ack(delivery.delivery_tag, false).await.unwrap();

    ch.queue_delete("bunny-rs.test.large-256k", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_publish_1mb_payload() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.large-1m", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.large-1m").await.unwrap();

    let body = vec![0xCDu8; 1024 * 1024];
    ch.basic_publish(
        "",
        "bunny-rs.test.large-1m",
        &PublishOptions::default(),
        &body,
    )
    .await
    .unwrap();

    ch.basic_consume(
        "bunny-rs.test.large-1m",
        "1m-consumer",
        ConsumeOptions::default(),
    )
    .await
    .unwrap();

    let delivery = ch.recv_delivery().await.unwrap().unwrap();
    assert_eq!(delivery.body.len(), 1024 * 1024);
    assert!(delivery.body.iter().all(|&b| b == 0xCD));
    ch.basic_ack(delivery.delivery_tag, false).await.unwrap();

    ch.queue_delete("bunny-rs.test.large-1m", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_publish_exactly_frame_max_minus_overhead() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.exact-frame", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.exact-frame").await.unwrap();

    // exactly frame_max - 8 bytes = single body frame at max size
    let body = vec![0x42u8; 131072 - 8];
    ch.basic_publish(
        "",
        "bunny-rs.test.exact-frame",
        &PublishOptions::default(),
        &body,
    )
    .await
    .unwrap();

    ch.basic_consume(
        "bunny-rs.test.exact-frame",
        "exact-consumer",
        ConsumeOptions::default(),
    )
    .await
    .unwrap();

    let delivery = ch.recv_delivery().await.unwrap().unwrap();
    assert_eq!(delivery.body.len(), 131072 - 8);
    ch.basic_ack(delivery.delivery_tag, false).await.unwrap();

    ch.queue_delete("bunny-rs.test.exact-frame", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
