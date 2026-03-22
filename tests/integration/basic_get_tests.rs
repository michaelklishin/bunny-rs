// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};
use bunny_rs::protocol::properties::BasicProperties;

#[tokio::test]
async fn test_basic_get_empty_queue() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.get-empty", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.get-empty").await.unwrap();

    let result = ch.basic_get("bunny-rs.test.get-empty", true).await.unwrap();
    assert!(result.is_none());

    ch.queue_delete("bunny-rs.test.get-empty", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_basic_get_with_message() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.get-msg", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.get-msg").await.unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.get-msg",
        &PublishOptions {
            properties: BasicProperties::default()
                .content_type("text/plain")
                .message_id("get-test-1"),
            ..Default::default()
        },
        b"polled message",
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let delivery = ch
        .basic_get("bunny-rs.test.get-msg", false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.body.as_ref(), b"polled message");
    assert_eq!(delivery.properties().get_content_type(), Some("text/plain"));
    assert_eq!(delivery.properties().get_message_id(), Some("get-test-1"));
    assert!(!delivery.redelivered);

    ch.basic_ack(delivery.delivery_tag, false).await.unwrap();

    // queue should now be empty
    let empty = ch.basic_get("bunny-rs.test.get-msg", true).await.unwrap();
    assert!(empty.is_none());

    ch.queue_delete("bunny-rs.test.get-msg", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_basic_get_auto_ack() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.get-autoack", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.get-autoack").await.unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.get-autoack",
        &PublishOptions::default(),
        b"auto-acked",
    )
    .await
    .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // get with auto-ack — no need to ack explicitly
    let delivery = ch
        .basic_get("bunny-rs.test.get-autoack", true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.body.as_ref(), b"auto-acked");

    ch.queue_delete("bunny-rs.test.get-autoack", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
