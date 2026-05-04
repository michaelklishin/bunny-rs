// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::time::Duration;

use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};
use bunny_rs::{Channel, Delivery};

use crate::test_helpers::connect;

async fn declare_purge(ch: &mut Channel, name: &str) {
    ch.queue_declare(name, QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge(name).await.unwrap();
}

async fn poll_for_delivery(ch: &mut Channel, queue: &str) -> Option<Delivery> {
    for _ in 0..50 {
        if let Some(d) = ch.basic_get(queue, false).await.unwrap() {
            return Some(d);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

#[tokio::test]
async fn delivery_nack_discard_drops_single_message() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.nack-discard.single";
    declare_purge(&mut ch, q).await;

    ch.basic_publish("", q, &PublishOptions::default(), b"drop-me")
        .await
        .unwrap();

    let delivery = poll_for_delivery(&mut ch, q).await.unwrap();
    assert_eq!(delivery.body.as_ref(), b"drop-me");
    delivery.nack_discard().await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    let leftover = ch.basic_get(q, true).await.unwrap();
    assert!(
        leftover.is_none(),
        "discarded message must not be redelivered"
    );

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn delivery_nack_discard_multiple_drops_a_batch() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.nack-discard.multiple";
    declare_purge(&mut ch, q).await;

    for i in 0..4u8 {
        ch.basic_publish("", q, &PublishOptions::default(), &[i])
            .await
            .unwrap();
    }

    let mut last = None;
    for _ in 0..4 {
        last = Some(poll_for_delivery(&mut ch, q).await.unwrap());
    }
    last.unwrap().nack_discard_multiple().await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    let leftover = ch.basic_get(q, true).await.unwrap();
    assert!(
        leftover.is_none(),
        "all four messages should have been discarded"
    );

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn delivery_nack_multiple_requeues_a_batch() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.nack-multiple.requeue";
    declare_purge(&mut ch, q).await;

    for i in 0..3u8 {
        ch.basic_publish("", q, &PublishOptions::default(), &[i])
            .await
            .unwrap();
    }

    let mut last = None;
    for _ in 0..3 {
        last = Some(poll_for_delivery(&mut ch, q).await.unwrap());
    }
    last.unwrap().nack_multiple().await.unwrap();

    for _ in 0..3 {
        let d = poll_for_delivery(&mut ch, q).await.unwrap();
        assert!(
            d.redelivered,
            "every requeued delivery must be marked redelivered"
        );
        d.ack().await.unwrap();
    }

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn channel_nack_requeues_single_delivery() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.channel-nack.requeue";
    declare_purge(&mut ch, q).await;

    ch.basic_publish("", q, &PublishOptions::default(), b"requeue-me")
        .await
        .unwrap();

    let first = poll_for_delivery(&mut ch, q).await.unwrap();
    let tag = first.delivery_tag;
    drop(first);
    ch.nack(tag).await.unwrap();

    let second = poll_for_delivery(&mut ch, q).await.unwrap();
    assert!(second.redelivered);
    second.ack().await.unwrap();

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn channel_nack_discard_drops_single_delivery() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.channel-nack-discard.single";
    declare_purge(&mut ch, q).await;

    ch.basic_publish("", q, &PublishOptions::default(), b"drop-me")
        .await
        .unwrap();

    let d = poll_for_delivery(&mut ch, q).await.unwrap();
    let tag = d.delivery_tag;
    drop(d);
    ch.nack_discard(tag).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(ch.basic_get(q, true).await.unwrap().is_none());

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn channel_nack_discard_multiple_drops_a_batch() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.channel-nack-discard.multiple";
    declare_purge(&mut ch, q).await;

    for i in 0..3u8 {
        ch.basic_publish("", q, &PublishOptions::default(), &[i])
            .await
            .unwrap();
    }

    let mut last = None;
    for _ in 0..3 {
        last = Some(poll_for_delivery(&mut ch, q).await.unwrap());
    }
    ch.nack_discard_multiple(last.unwrap().delivery_tag)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(ch.basic_get(q, true).await.unwrap().is_none());

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
