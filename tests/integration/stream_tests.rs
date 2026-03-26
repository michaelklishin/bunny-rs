// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{
    ConsumeOptions, PublishOptions, QueueDeclareOptions, QueueDeleteOptions, StreamOffset,
};

#[tokio::test]
async fn test_stream_queue_declare() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    let info = ch
        .queue_declare("bunny-rs.test.stream-declare", QueueDeclareOptions::stream())
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
    con_ch.basic_qos(10).await.unwrap();
    con_ch
        .basic_consume(
            "bunny-rs.test.stream-pub-con",
            "stream-consumer",
            ConsumeOptions::default().stream_offset(StreamOffset::First),
        )
        .await
        .unwrap();

    // Should receive at least the 5 messages we published
    let mut received = 0u32;
    let timeout = std::time::Duration::from_secs(5);
    let deadline = tokio::time::Instant::now() + timeout;

    while received < 5 {
        match tokio::time::timeout_at(deadline, con_ch.recv_delivery()).await {
            Ok(Ok(Some(_))) => received += 1,
            Ok(Ok(None)) => break,
            Ok(Err(e)) => panic!("delivery error: {e}"),
            Err(_) => break,
        }
    }

    assert!(received >= 5, "expected >= 5 deliveries, got {received}");

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

    ch.queue_declare(
        "bunny-rs.test.stream-offset",
        QueueDeclareOptions::stream(),
    )
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

    // Consume with StreamOffset::Last — should only get recent messages
    let mut con_ch = conn.open_channel().await.unwrap();
    con_ch.basic_qos(10).await.unwrap();
    con_ch
        .basic_consume(
            "bunny-rs.test.stream-offset",
            "",
            ConsumeOptions::default().stream_offset(StreamOffset::Last),
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

    // Should receive at least one delivery (either from the tail of the
    // existing log or the newly published message).
    let delivery = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        con_ch.recv_delivery(),
    )
    .await
    .expect("timed out waiting for stream delivery")
    .unwrap()
    .expect("no delivery");

    assert!(!delivery.body.is_empty());

    ch.queue_delete(
        "bunny-rs.test.stream-offset",
        QueueDeleteOptions::default(),
    )
    .await
    .unwrap();
    ch.close().await.unwrap();
    con_ch.close().await.unwrap();
    conn.close().await.unwrap();
}
