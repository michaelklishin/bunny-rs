// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{ConsumeOptions, QueueDeclareOptions, QueueDeleteOptions};

#[tokio::test]
async fn test_basic_cancel() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.cancel", QueueDeclareOptions::default())
        .await
        .unwrap();

    let tag = ch
        .basic_consume(
            "bunny-rs.test.cancel",
            "cancel-me",
            ConsumeOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(tag.as_str(), "cancel-me");

    ch.basic_cancel("cancel-me").await.unwrap();

    ch.queue_delete("bunny-rs.test.cancel", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_cancel_with_server_generated_tag() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.cancel-gen", QueueDeclareOptions::default())
        .await
        .unwrap();

    // empty tag = server generates one
    let tag = ch
        .basic_consume("bunny-rs.test.cancel-gen", "", ConsumeOptions::default())
        .await
        .unwrap();
    assert!(!tag.is_empty());

    ch.basic_cancel(&tag).await.unwrap();

    ch.queue_delete("bunny-rs.test.cancel-gen", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_multiple_consumers_on_same_queue() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare(
        "bunny-rs.test.multi-consumer",
        QueueDeclareOptions::default(),
    )
    .await
    .unwrap();
    ch.queue_purge("bunny-rs.test.multi-consumer")
        .await
        .unwrap();

    let tag1 = ch
        .basic_consume(
            "bunny-rs.test.multi-consumer",
            "c1",
            ConsumeOptions::default(),
        )
        .await
        .unwrap();
    let tag2 = ch
        .basic_consume(
            "bunny-rs.test.multi-consumer",
            "c2",
            ConsumeOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(tag1.as_str(), "c1");
    assert_eq!(tag2.as_str(), "c2");

    ch.basic_cancel("c1").await.unwrap();
    ch.basic_cancel("c2").await.unwrap();

    ch.queue_delete(
        "bunny-rs.test.multi-consumer",
        QueueDeleteOptions::default(),
    )
    .await
    .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
