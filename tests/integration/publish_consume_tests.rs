// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{ConsumeOptions, PublishOptions, QueueDeclareOptions, QueueDeleteOptions};
use bunny_rs::protocol::properties::BasicProperties;

#[tokio::test]
async fn test_publish_to_default_exchange() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    let _q = ch
        .queue_declare("bunny-rs.test.publish", QueueDeclareOptions::default())
        .await
        .unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.publish",
        &PublishOptions {
            properties: BasicProperties::default().content_type("text/plain"),
            ..Default::default()
        },
        b"hello bunny-rs",
    )
    .await
    .unwrap();

    ch.queue_delete("bunny-rs.test.publish", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_publish_and_consume() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    let _q = ch
        .queue_declare("bunny-rs.test.pub-consume", QueueDeclareOptions::default())
        .await
        .unwrap();

    // Purge any leftover messages
    ch.queue_purge("bunny-rs.test.pub-consume").await.unwrap();

    // Publish a message
    ch.basic_publish(
        "",
        "bunny-rs.test.pub-consume",
        &PublishOptions {
            properties: BasicProperties::default()
                .content_type("application/json")
                .message_id("test-msg-1"),
            ..Default::default()
        },
        b"{\"key\": \"value\"}",
    )
    .await
    .unwrap();

    // Start consuming
    let tag = ch
        .basic_consume(
            "bunny-rs.test.pub-consume",
            "test-consumer",
            ConsumeOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(tag.as_str(), "test-consumer");

    // Receive the delivery
    let delivery = ch.recv_delivery().await.unwrap().unwrap();
    assert_eq!(delivery.body.as_ref(), b"{\"key\": \"value\"}");
    assert_eq!(delivery.exchange.as_str(), "");
    assert_eq!(delivery.routing_key.as_str(), "bunny-rs.test.pub-consume");
    assert_eq!(
        delivery.properties().get_content_type(),
        Some("application/json")
    );
    assert_eq!(delivery.properties().get_message_id(), Some("test-msg-1"));
    assert!(!delivery.redelivered);

    // Acknowledge
    ch.basic_ack(delivery.delivery_tag, false).await.unwrap();

    // Cleanup
    ch.queue_delete("bunny-rs.test.pub-consume", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_publish_and_nack_with_requeue() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.nack", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.nack").await.unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.nack",
        &PublishOptions::default(),
        b"nack-me",
    )
    .await
    .unwrap();

    ch.basic_consume(
        "bunny-rs.test.nack",
        "nack-consumer",
        ConsumeOptions::default(),
    )
    .await
    .unwrap();

    let delivery = ch.recv_delivery().await.unwrap().unwrap();
    assert_eq!(delivery.body.as_ref(), b"nack-me");

    // Nack with requeue
    ch.basic_nack(delivery.delivery_tag, false, true)
        .await
        .unwrap();

    // Receive the redelivered message
    let delivery2 = ch.recv_delivery().await.unwrap().unwrap();
    assert_eq!(delivery2.body.as_ref(), b"nack-me");
    assert!(delivery2.redelivered);

    ch.basic_ack(delivery2.delivery_tag, false).await.unwrap();

    ch.queue_delete("bunny-rs.test.nack", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_publish_multiple_messages() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.multi", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.multi").await.unwrap();

    for i in 0..5 {
        let body = format!("message-{i}");
        ch.basic_publish(
            "",
            "bunny-rs.test.multi",
            &PublishOptions::default(),
            body.as_bytes(),
        )
        .await
        .unwrap();
    }

    ch.basic_consume(
        "bunny-rs.test.multi",
        "multi-consumer",
        ConsumeOptions::default(),
    )
    .await
    .unwrap();

    for i in 0..5 {
        let delivery = ch.recv_delivery().await.unwrap().unwrap();
        let expected = format!("message-{i}");
        assert_eq!(delivery.body.as_ref(), expected.as_bytes());
        ch.basic_ack(delivery.delivery_tag, false).await.unwrap();
    }

    ch.queue_delete("bunny-rs.test.multi", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_publish_empty_body() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.empty-body", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.empty-body").await.unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.empty-body",
        &PublishOptions::default(),
        b"",
    )
    .await
    .unwrap();

    ch.basic_consume("bunny-rs.test.empty-body", "empty-consumer", {
        let mut opts = ConsumeOptions::default();
        opts.no_ack = true;
        opts
    })
    .await
    .unwrap();

    let delivery = ch.recv_delivery().await.unwrap().unwrap();
    assert!(delivery.body.is_empty());

    ch.queue_delete("bunny-rs.test.empty-body", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
