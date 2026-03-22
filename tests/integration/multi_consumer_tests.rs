// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{QueueDeclareOptions, QueueDeleteOptions};

#[tokio::test]
async fn test_two_consumers_same_queue_round_robin() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.mc-rr", QueueDeclareOptions::default())
        .await
        .unwrap();
    ch.queue_purge("bunny-rs.test.mc-rr").await.unwrap();
    ch.basic_qos(1).await.unwrap();

    let tag1 = ch
        .consume_with_manual_acks("bunny-rs.test.mc-rr", "consumer-a")
        .await
        .unwrap();
    let tag2 = ch
        .consume_with_manual_acks("bunny-rs.test.mc-rr", "consumer-b")
        .await
        .unwrap();

    // publish 2 messages
    ch.publish("", "bunny-rs.test.mc-rr", b"msg-1")
        .await
        .unwrap();
    ch.publish("", "bunny-rs.test.mc-rr", b"msg-2")
        .await
        .unwrap();

    // each consumer should get one message (round-robin)
    let d1 = ch.recv_delivery_for(&tag1).await.unwrap().unwrap();
    ch.basic_ack(d1.delivery_tag, false).await.unwrap();

    let d2 = ch.recv_delivery_for(&tag2).await.unwrap().unwrap();
    ch.basic_ack(d2.delivery_tag, false).await.unwrap();

    // both messages received, different bodies
    assert_ne!(d1.body.as_ref(), d2.body.as_ref());

    ch.basic_cancel(&tag1).await.unwrap();
    ch.basic_cancel(&tag2).await.unwrap();
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

    let tag1 = ch
        .consume_with_manual_acks("bunny-rs.test.mc-q1", "c-q1")
        .await
        .unwrap();
    let tag2 = ch
        .consume_with_manual_acks("bunny-rs.test.mc-q2", "c-q2")
        .await
        .unwrap();

    ch.publish("", "bunny-rs.test.mc-q1", b"for-q1")
        .await
        .unwrap();
    ch.publish("", "bunny-rs.test.mc-q2", b"for-q2")
        .await
        .unwrap();

    let d1 = ch.recv_delivery_for(&tag1).await.unwrap().unwrap();
    assert_eq!(d1.body.as_ref(), b"for-q1");
    ch.basic_ack(d1.delivery_tag, false).await.unwrap();

    let d2 = ch.recv_delivery_for(&tag2).await.unwrap().unwrap();
    assert_eq!(d2.body.as_ref(), b"for-q2");
    ch.basic_ack(d2.delivery_tag, false).await.unwrap();

    ch.basic_cancel(&tag1).await.unwrap();
    ch.basic_cancel(&tag2).await.unwrap();
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

    let tag1 = ch
        .consume_with_manual_acks("bunny-rs.test.mc-cancel", "stays")
        .await
        .unwrap();
    let tag2 = ch
        .consume_with_manual_acks("bunny-rs.test.mc-cancel", "goes")
        .await
        .unwrap();

    ch.basic_cancel(&tag2).await.unwrap();

    // publish after cancel — only "stays" should receive
    ch.publish("", "bunny-rs.test.mc-cancel", b"after-cancel")
        .await
        .unwrap();

    let d = ch.recv_delivery_for(&tag1).await.unwrap().unwrap();
    assert_eq!(d.body.as_ref(), b"after-cancel");
    ch.basic_ack(d.delivery_tag, false).await.unwrap();

    ch.basic_cancel(&tag1).await.unwrap();
    ch.queue_delete("bunny-rs.test.mc-cancel", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
