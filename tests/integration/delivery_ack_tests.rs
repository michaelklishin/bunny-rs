// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

//! Self-acknowledging `Delivery` API.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bunny_rs::DeliveryHandler;
use bunny_rs::options::{ConsumeOptions, PublishOptions, QueueDeclareOptions, QueueDeleteOptions};

use crate::test_helpers::connect;

async fn declare_purge(ch: &mut bunny_rs::Channel, name: &str) {
    ch.queue_declare(name, QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge(name).await.unwrap();
}

#[tokio::test]
async fn delivery_ack_via_basic_get() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.delivery-ack.get";
    declare_purge(&mut ch, q).await;

    ch.basic_publish("", q, &PublishOptions::default(), b"hello")
        .await
        .unwrap();

    let delivery = ch.basic_get(q, false).await.unwrap().unwrap();
    assert_eq!(delivery.body.as_ref(), b"hello");
    delivery.ack().await.unwrap();

    let empty = ch.basic_get(q, true).await.unwrap();
    assert!(empty.is_none());

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn delivery_nack_requeues() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.delivery-ack.nack";
    declare_purge(&mut ch, q).await;

    ch.basic_publish("", q, &PublishOptions::default(), b"please-requeue")
        .await
        .unwrap();

    let first = ch.basic_get(q, false).await.unwrap().unwrap();
    assert_eq!(first.body.as_ref(), b"please-requeue");
    assert!(!first.redelivered);
    first.nack().await.unwrap();

    let second = loop {
        if let Some(d) = ch.basic_get(q, false).await.unwrap() {
            break d;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(second.body.as_ref(), b"please-requeue");
    assert!(second.redelivered);
    second.ack().await.unwrap();

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn delivery_reject_requeues_and_discard_drops() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.delivery-ack.reject-discard";
    declare_purge(&mut ch, q).await;

    // reject(requeue=true): the message must come back as redelivered.
    ch.basic_publish("", q, &PublishOptions::default(), b"reject-me")
        .await
        .unwrap();
    let first = ch.basic_get(q, false).await.unwrap().unwrap();
    first.reject().await.unwrap();
    let requeued = loop {
        if let Some(d) = ch.basic_get(q, false).await.unwrap() {
            break d;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(requeued.redelivered);
    requeued.ack().await.unwrap();

    // discard: with no DLX configured the message must not come back.
    ch.basic_publish("", q, &PublishOptions::default(), b"discard-me")
        .await
        .unwrap();
    let to_drop = ch.basic_get(q, false).await.unwrap().unwrap();
    to_drop.discard().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = ch.basic_get(q, true).await.unwrap();
    assert!(after.is_none(), "discarded message must not be redelivered");

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn ack_multiple_via_delivery() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.delivery-ack.multiple";
    declare_purge(&mut ch, q).await;

    for i in 0..5u8 {
        ch.basic_publish("", q, &PublishOptions::default(), &[i])
            .await
            .unwrap();
    }

    let mut tags = Vec::new();
    for _ in 0..5 {
        let d = loop {
            if let Some(d) = ch.basic_get(q, false).await.unwrap() {
                break d;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        tags.push(d.delivery_tag);
        if tags.len() == 5 {
            d.ack_multiple().await.unwrap();
        }
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    let empty = ch.basic_get(q, true).await.unwrap();
    assert!(empty.is_none());

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

/// `DeliveryHandler` impls should be able to use the new self-acknowledging
/// `Delivery` API instead of stashing delivery tags.
#[tokio::test]
async fn delivery_handler_uses_delivery_ack() {
    struct AckingHandler {
        seen: Arc<AtomicU64>,
    }

    impl DeliveryHandler for AckingHandler {
        async fn handle_delivery(
            &mut self,
            delivery: bunny_rs::Delivery,
        ) -> Result<(), bunny_rs::ConnectionError> {
            self.seen.fetch_add(1, Ordering::Release);
            delivery.ack().await
        }
    }

    let conn = connect().await;
    let mut pub_ch = conn.open_channel().await.unwrap();
    let mut con_ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.delivery-ack.trait";
    declare_purge(&mut pub_ch, q).await;
    con_ch.basic_qos(10).await.unwrap();

    let seen = Arc::new(AtomicU64::new(0));
    let handle = con_ch
        .basic_consume_with(
            q,
            "delivery-ack-trait",
            ConsumeOptions::default(),
            AckingHandler { seen: seen.clone() },
        )
        .await
        .unwrap();

    for i in 0..7u8 {
        pub_ch
            .basic_publish("", q, &PublishOptions::default(), &[i])
            .await
            .unwrap();
    }

    tokio::time::timeout(Duration::from_secs(5), async {
        while seen.load(Ordering::Acquire) < 7 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("consumer did not see all 7 messages");

    handle.cancel();

    tokio::time::sleep(Duration::from_millis(100)).await;
    let leftover = pub_ch.queue_purge(q).await.unwrap();
    assert_eq!(leftover, 0, "all deliveries should have been acked");

    pub_ch
        .queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    pub_ch.close().await.unwrap();
    conn.close().await.unwrap();
}

/// Cloning an `Acker` and acking from a separate task: the pattern used to
/// hand a delivery body off to a worker pool.
#[tokio::test]
async fn cloned_acker_acks_from_another_task() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    let q = "bunny-rs.test.delivery-ack.cloned-acker";
    declare_purge(&mut ch, q).await;

    ch.basic_publish("", q, &PublishOptions::default(), b"deferred")
        .await
        .unwrap();

    let delivery = ch.basic_get(q, false).await.unwrap().unwrap();
    let tag = delivery.delivery_tag;
    let acker = delivery.acker();

    // The cloned Acker must outlive the Delivery.
    drop(delivery);

    let join = tokio::spawn(async move { acker.ack(tag, false).await });
    join.await.unwrap().unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;
    let empty = ch.basic_get(q, true).await.unwrap();
    assert!(empty.is_none());

    ch.queue_delete(q, QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
