// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

//! Closure-based [`Queue::subscribe_with`] consumer API.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};
use bunny_rs::{ConnectionError, SubscribeOptions};

use crate::test_helpers::connect;

async fn declare_purge(ch: &mut bunny_rs::Channel, name: &str) {
    ch.queue_declare(name, QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge(name).await.unwrap();
}

#[tokio::test]
async fn subscribe_with_closure_acks_each_delivery() {
    let conn = connect().await;
    let mut pub_ch = conn.open_channel().await.unwrap();
    let mut con_ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe-with.acks";
    declare_purge(&mut pub_ch, q).await;

    let count = Arc::new(AtomicU64::new(0));
    let count_clone = count.clone();
    let handle = con_ch
        .queue(q)
        .subscribe_with(SubscribeOptions::manual_ack(), move |delivery| {
            let c = count_clone.clone();
            async move {
                c.fetch_add(1, Ordering::Release);
                delivery.ack().await
            }
        })
        .await
        .unwrap();
    assert!(!handle.consumer_tag().is_empty());

    for i in 0..6u8 {
        pub_ch
            .basic_publish("", q, &PublishOptions::default(), &[i])
            .await
            .unwrap();
    }

    tokio::time::timeout(Duration::from_secs(5), async {
        while count.load(Ordering::Acquire) < 6 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("closure consumer did not see all 6 messages");

    handle.cancel();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(pub_ch.queue_purge(q).await.unwrap(), 0);

    pub_ch
        .queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    pub_ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn subscribe_with_explicit_consumer_tag_round_trips() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe-with.tag";
    declare_purge(&mut ch, q).await;

    let handle = ch
        .queue(q)
        .subscribe_with(
            SubscribeOptions::manual_ack().consumer_tag("closure-tag"),
            move |d| async move { d.ack().await },
        )
        .await
        .unwrap();
    assert_eq!(handle.consumer_tag(), "closure-tag");

    handle.cancel();
    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn subscribe_with_prefetch_limits_in_flight_messages() {
    let conn = connect().await;
    let mut pub_ch = conn.open_channel().await.unwrap();
    let mut con_ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe-with.prefetch";
    declare_purge(&mut pub_ch, q).await;

    for _ in 0..10u8 {
        pub_ch
            .basic_publish("", q, &PublishOptions::default(), b"x")
            .await
            .unwrap();
    }

    let count = Arc::new(AtomicU64::new(0));
    let c = count.clone();
    let handle = con_ch
        .queue(q)
        .subscribe_with(
            SubscribeOptions::manual_ack().prefetch(1),
            move |delivery| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::Release);
                    delivery.ack().await
                }
            },
        )
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while count.load(Ordering::Acquire) < 10 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("closure consumer did not consume 10 messages with prefetch=1");

    handle.cancel();
    pub_ch
        .queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    pub_ch.close().await.unwrap();
    conn.close().await.unwrap();
}

/// Dropping the returned handle (without explicit `cancel`) must also stop the
/// consumer.
#[tokio::test]
async fn subscribe_with_handle_drop_cancels_consumer() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe-with.drop";
    declare_purge(&mut ch, q).await;

    {
        let _handle = ch
            .queue(q)
            .subscribe_with(
                SubscribeOptions::manual_ack().consumer_tag("drop-handle"),
                move |d| async move { d.ack().await },
            )
            .await
            .unwrap();
        let info = ch
            .queue_declare(q, QueueDeclareOptions::default())
            .await
            .unwrap();
        assert_eq!(info.consumer_count, 1);
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    let info = ch
        .queue_declare(q, QueueDeclareOptions::default())
        .await
        .unwrap();
    assert_eq!(info.consumer_count, 0);

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

/// A handler returning `Err` must terminate the background consumer task and
/// stop processing further deliveries.
#[tokio::test]
async fn subscribe_with_handler_error_stops_consumer() {
    let conn = connect().await;
    let mut pub_ch = conn.open_channel().await.unwrap();
    let mut con_ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.subscribe-with.handler-err";
    declare_purge(&mut pub_ch, q).await;

    let seen = Arc::new(AtomicU64::new(0));
    let seen_clone = seen.clone();
    let handle = con_ch
        .queue(q)
        .subscribe_with(
            SubscribeOptions::manual_ack().consumer_tag("err-stops"),
            move |delivery| {
                let s = seen_clone.clone();
                async move {
                    s.fetch_add(1, Ordering::Release);
                    delivery.ack().await?;
                    Err(ConnectionError::PublishNacked)
                }
            },
        )
        .await
        .unwrap();

    pub_ch
        .basic_publish("", q, &PublishOptions::default(), b"first")
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while seen.load(Ordering::Acquire) < 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("first delivery never observed");

    // The background task already exited when the handler returned Err;
    // dropping the handle is what removes the broker-side consumer.
    drop(handle);
    tokio::time::sleep(Duration::from_millis(150)).await;
    let info = pub_ch
        .queue_declare(q, QueueDeclareOptions::default())
        .await
        .unwrap();
    assert_eq!(info.consumer_count, 0);

    // After the handler errored, no further deliveries should reach it.
    let before = seen.load(Ordering::Acquire);
    pub_ch
        .basic_publish("", q, &PublishOptions::default(), b"second")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(seen.load(Ordering::Acquire), before);

    pub_ch.queue_purge(q).await.unwrap();
    pub_ch
        .queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    pub_ch.close().await.unwrap();
    conn.close().await.unwrap();
}
