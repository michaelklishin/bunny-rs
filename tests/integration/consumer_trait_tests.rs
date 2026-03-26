// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::test_helpers::connect;
use bunny_rs::connection::ConnectionError;
use bunny_rs::consumer::Consumer;
use bunny_rs::options::{ConsumeOptions, PublishOptions, QueueDeclareOptions, QueueDeleteOptions};
use bunny_rs::Delivery;

struct CountingConsumer {
    count: Arc<AtomicU64>,
}

impl Consumer for CountingConsumer {
    async fn handle_delivery(&mut self, _delivery: Delivery) -> Result<(), ConnectionError> {
        self.count.fetch_add(1, Ordering::Release);
        Ok(())
    }
}

#[tokio::test]
async fn test_basic_consume_with_trait() {
    let conn = connect().await;
    let mut pub_ch = conn.open_channel().await.unwrap();
    let mut con_ch = conn.open_channel().await.unwrap();

    pub_ch
        .queue_declare("bunny-rs.test.consumer-trait", QueueDeclareOptions::default())
        .await
        .unwrap();
    con_ch.basic_qos(10).await.unwrap();

    let count = Arc::new(AtomicU64::new(0));
    let consumer = CountingConsumer {
        count: count.clone(),
    };

    let handle = con_ch
        .basic_consume_with(
            "bunny-rs.test.consumer-trait",
            "trait-consumer",
            ConsumeOptions::default(),
            consumer,
        )
        .await
        .unwrap();

    assert_eq!(handle.consumer_tag(), "trait-consumer");

    // Publish messages
    for i in 0..10u8 {
        pub_ch
            .basic_publish(
                "",
                "bunny-rs.test.consumer-trait",
                &PublishOptions::default(),
                &[i],
            )
            .await
            .unwrap();
    }

    // Wait for deliveries to be processed
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while count.load(Ordering::Acquire) < 10 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("timed out waiting for consumer to receive all messages");

    assert_eq!(count.load(Ordering::Acquire), 10);

    handle.cancel();

    pub_ch
        .queue_delete(
            "bunny-rs.test.consumer-trait",
            QueueDeleteOptions::default(),
        )
        .await
        .unwrap();
    pub_ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_consumer_handle_drop_cancels() {
    let conn = connect().await;
    let mut pub_ch = conn.open_channel().await.unwrap();
    let mut con_ch = conn.open_channel().await.unwrap();

    pub_ch
        .queue_declare(
            "bunny-rs.test.consumer-drop",
            QueueDeclareOptions::default(),
        )
        .await
        .unwrap();
    con_ch.basic_qos(10).await.unwrap();

    let count = Arc::new(AtomicU64::new(0));
    let consumer = CountingConsumer {
        count: count.clone(),
    };

    let handle = con_ch
        .basic_consume_with(
            "bunny-rs.test.consumer-drop",
            "",
            ConsumeOptions::default(),
            consumer,
        )
        .await
        .unwrap();

    // Drop handle — should fire basic.cancel via Drop
    drop(handle);

    // Brief wait for the cancel to propagate
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Publish a message — it should not reach the dropped consumer
    pub_ch
        .basic_publish(
            "",
            "bunny-rs.test.consumer-drop",
            &PublishOptions::default(),
            b"orphan",
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // The counter should still be 0 — no deliveries after drop
    assert_eq!(count.load(Ordering::Acquire), 0);

    pub_ch
        .queue_delete(
            "bunny-rs.test.consumer-drop",
            QueueDeleteOptions::default(),
        )
        .await
        .unwrap();
    pub_ch.close().await.unwrap();
    conn.close().await.unwrap();
}
