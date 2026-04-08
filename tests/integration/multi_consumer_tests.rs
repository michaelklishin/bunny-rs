// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use std::time::Duration;

use crate::test_helpers::connect;
use bunny_rs::SubscribeOptions;
use bunny_rs::options::{QueueDeclareOptions, QueueDeleteOptions};

async fn next_delivery(sub: &mut bunny_rs::Consumer) -> bunny_rs::Delivery {
    tokio::time::timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("consumer stalled")
        .expect("consumer closed")
}

#[tokio::test]
async fn test_two_consumers_same_queue_round_robin() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.mc-rr", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.mc-rr").await.unwrap();
    ch.basic_qos(1).await.unwrap();

    let mut sub_a = ch
        .queue("bunny-rs.test.mc-rr")
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("consumer-a"))
        .await
        .unwrap();
    let mut sub_b = ch
        .queue("bunny-rs.test.mc-rr")
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("consumer-b"))
        .await
        .unwrap();

    ch.publish("", "bunny-rs.test.mc-rr", b"msg-1")
        .await
        .unwrap();
    ch.publish("", "bunny-rs.test.mc-rr", b"msg-2")
        .await
        .unwrap();

    // Each consumer should get one message (round-robin)
    let d_a = next_delivery(&mut sub_a).await;
    d_a.ack().await.unwrap();
    let d_b = next_delivery(&mut sub_b).await;
    d_b.ack().await.unwrap();

    assert_ne!(d_a.body.as_ref(), d_b.body.as_ref());

    sub_a.cancel().await.unwrap();
    sub_b.cancel().await.unwrap();
    ch.queue_delete("bunny-rs.test.mc-rr", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_two_consumers_different_queues() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.mc-q1", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_declare("bunny-rs.test.mc-q2", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.mc-q1").await.unwrap();
    ch.queue_purge("bunny-rs.test.mc-q2").await.unwrap();

    let mut sub1 = ch
        .queue("bunny-rs.test.mc-q1")
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("c-q1"))
        .await
        .unwrap();
    let mut sub2 = ch
        .queue("bunny-rs.test.mc-q2")
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("c-q2"))
        .await
        .unwrap();

    ch.publish("", "bunny-rs.test.mc-q1", b"for-q1")
        .await
        .unwrap();
    ch.publish("", "bunny-rs.test.mc-q2", b"for-q2")
        .await
        .unwrap();

    let d1 = next_delivery(&mut sub1).await;
    assert_eq!(d1.body.as_ref(), b"for-q1");
    d1.ack().await.unwrap();

    let d2 = next_delivery(&mut sub2).await;
    assert_eq!(d2.body.as_ref(), b"for-q2");
    d2.ack().await.unwrap();

    sub1.cancel().await.unwrap();
    sub2.cancel().await.unwrap();
    ch.queue_delete("bunny-rs.test.mc-q1", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.queue_delete("bunny-rs.test.mc-q2", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_cancel_one_consumer_other_continues() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.mc-cancel", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.mc-cancel").await.unwrap();

    let mut sub_stays = ch
        .queue("bunny-rs.test.mc-cancel")
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("stays"))
        .await
        .unwrap();
    let sub_goes = ch
        .queue("bunny-rs.test.mc-cancel")
        .subscribe(SubscribeOptions::manual_ack().consumer_tag("goes"))
        .await
        .unwrap();

    sub_goes.cancel().await.unwrap();

    ch.publish("", "bunny-rs.test.mc-cancel", b"after-cancel")
        .await
        .unwrap();

    let d = next_delivery(&mut sub_stays).await;
    assert_eq!(d.body.as_ref(), b"after-cancel");
    d.ack().await.unwrap();

    sub_stays.cancel().await.unwrap();
    ch.queue_delete("bunny-rs.test.mc-cancel", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
