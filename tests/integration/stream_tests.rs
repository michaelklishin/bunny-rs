// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::time::Duration;

use crate::test_helpers::connect;
use bunny_rs::SubscribeOptions;
use bunny_rs::options::{
    ConsumeOptions, PublishOptions, QueueDeclareOptions, QueueDeleteOptions, StreamOffset,
};

#[tokio::test]
async fn test_stream_queue_declare() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    let info = ch
        .queue_declare(
            "bunny-rs.test.stream-declare",
            QueueDeclareOptions::stream(),
        )
        .await
        .unwrap();
    assert_eq!(info.name, "bunny-rs.test.stream-declare");

    ch.queue_delete(
        "bunny-rs.test.stream-declare",
        QueueDeleteOptions::default(),
    )
    .await
    .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_stream_publish_and_consume() {
    let conn = connect().await;
    let mut pub_ch = conn.open_channel().await.unwrap();
    let mut con_ch = conn.open_channel().await.unwrap();

    pub_ch
        .queue_declare(
            "bunny-rs.test.stream-pub-con",
            QueueDeclareOptions::stream(),
        )
        .await
        .unwrap();

    // Publish a few messages
    for i in 0..5u8 {
        pub_ch
            .basic_publish(
                "",
                "bunny-rs.test.stream-pub-con",
                &PublishOptions::default(),
                &[i],
            )
            .await
            .unwrap();
    }

    // Consume from the beginning of the stream
    let mut sub = con_ch
        .queue("bunny-rs.test.stream-pub-con")
        .subscribe(
            SubscribeOptions::manual_ack()
                .consumer_tag("stream-consumer")
                .prefetch(10)
                .with_consume(ConsumeOptions::default().stream_offset(StreamOffset::First)),
        )
        .await
        .unwrap();

    // Should receive at least the 5 messages we published
    let mut received = 0u32;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while received < 5 {
        match tokio::time::timeout_at(deadline, sub.recv()).await {
            Ok(Some(d)) => {
                d.ack().await.unwrap();
                received += 1;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    assert!(received >= 5, "expected >= 5 deliveries, got {received}");
    sub.cancel().await.unwrap();

    pub_ch
        .queue_delete(
            "bunny-rs.test.stream-pub-con",
            QueueDeleteOptions::default(),
        )
        .await
        .unwrap();
    pub_ch.close().await.unwrap();
    con_ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_stream_consume_from_offset() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.stream-offset", QueueDeclareOptions::stream())
        .await
        .unwrap();

    // Publish messages
    for i in 0..10u8 {
        ch.basic_publish(
            "",
            "bunny-rs.test.stream-offset",
            &PublishOptions::default(),
            &[i],
        )
        .await
        .unwrap();
    }

    // Consume with StreamOffset::Last: should only get recent messages.
    let mut con_ch = conn.open_channel().await.unwrap();
    let mut sub = con_ch
        .queue("bunny-rs.test.stream-offset")
        .subscribe(
            SubscribeOptions::manual_ack()
                .prefetch(10)
                .with_consume(ConsumeOptions::default().stream_offset(StreamOffset::Last)),
        )
        .await
        .unwrap();

    // Publish one more message after subscribing
    ch.basic_publish(
        "",
        "bunny-rs.test.stream-offset",
        &PublishOptions::default(),
        b"after-subscribe",
    )
    .await
    .unwrap();

    let delivery = tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("timed out waiting for stream delivery")
        .expect("no delivery");
    assert!(!delivery.body.is_empty());
    delivery.ack().await.unwrap();
    sub.cancel().await.unwrap();

    ch.queue_delete("bunny-rs.test.stream-offset", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    con_ch.close().await.unwrap();
    conn.close().await.unwrap();
}
