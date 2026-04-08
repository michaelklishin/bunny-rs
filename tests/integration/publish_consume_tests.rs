// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::time::Duration;

use crate::test_helpers::connect;
use bunny_rs::SubscribeOptions;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};
use bunny_rs::protocol::properties::BasicProperties;

async fn next_delivery(sub: &mut bunny_rs::Consumer) -> bunny_rs::Delivery {
    tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("consumer stalled")
        .expect("consumer closed")
}

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
    ch.queue_purge("bunny-rs.test.pub-consume").await.unwrap();

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

    let mut sub = ch
        .queue("bunny-rs.test.pub-consume")
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("test-consumer"))
        .await
        .unwrap();
    assert_eq!(sub.consumer_tag(), "test-consumer");

    let delivery = next_delivery(&mut sub).await;
    assert_eq!(delivery.body.as_ref(), b"{\"key\": \"value\"}");
    assert_eq!(delivery.exchange.as_str(), "");
    assert_eq!(delivery.routing_key.as_str(), "bunny-rs.test.pub-consume");
    assert_eq!(
        delivery.properties().get_content_type(),
        Some("application/json")
    );
    assert_eq!(delivery.properties().get_message_id(), Some("test-msg-1"));
    assert!(!delivery.redelivered);

    delivery.ack().await.unwrap();
    sub.cancel().await.unwrap();

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

    let mut sub = ch
        .queue("bunny-rs.test.nack")
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("nack-consumer"))
        .await
        .unwrap();

    let delivery = next_delivery(&mut sub).await;
    assert_eq!(delivery.body.as_ref(), b"nack-me");
    delivery.nack().await.unwrap();

    let delivery2 = next_delivery(&mut sub).await;
    assert_eq!(delivery2.body.as_ref(), b"nack-me");
    assert!(delivery2.redelivered);
    delivery2.ack().await.unwrap();

    sub.cancel().await.unwrap();
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

    let mut sub = ch
        .queue("bunny-rs.test.multi")
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("multi-consumer"))
        .await
        .unwrap();

    for i in 0..5 {
        let delivery = next_delivery(&mut sub).await;
        let expected = format!("message-{i}");
        assert_eq!(delivery.body.as_ref(), expected.as_bytes());
        delivery.ack().await.unwrap();
    }

    sub.cancel().await.unwrap();
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

    let mut sub = ch
        .queue("bunny-rs.test.empty-body")
        .subscribe(SubscribeOptions::auto_ack().consumer_tag("empty-consumer"))
        .await
        .unwrap();

    let delivery = next_delivery(&mut sub).await;
    assert!(delivery.body.is_empty());

    sub.cancel().await.unwrap();
    ch.queue_delete("bunny-rs.test.empty-body", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
