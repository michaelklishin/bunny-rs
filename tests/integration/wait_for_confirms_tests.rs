// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_wait_for_confirms_returns_immediately_when_empty() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    ch.confirm_select().await.unwrap();

    let start = Instant::now();
    ch.wait_for_confirms().await.unwrap();
    assert!(
        start.elapsed() < Duration::from_millis(50),
        "should return immediately with no pending confirms"
    );

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_wait_for_confirms_wakes_on_ack() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    ch.confirm_select().await.unwrap();

    ch.queue_declare("bunny-rs.test.wait-wake", QueueDeclareOptions::default())
        .await
        .unwrap();

    for i in 0..20 {
        let body = format!("msg-{i}");
        ch.basic_publish(
            "",
            "bunny-rs.test.wait-wake",
            &PublishOptions::default(),
            body.as_bytes(),
        )
        .await
        .unwrap();
    }

    let start = Instant::now();
    ch.wait_for_confirms().await.unwrap();
    let elapsed = start.elapsed();

    // With Notify the wait should resolve in under 50ms (typically < 5ms).
    // The old 10ms-polling approach would add at least one sleep cycle.
    assert!(
        elapsed < Duration::from_millis(200),
        "wait_for_confirms took {elapsed:?}, expected fast wakeup"
    );

    ch.queue_delete("bunny-rs.test.wait-wake", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_wait_for_confirms_concurrent_with_publish() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();
    ch.confirm_select().await.unwrap();

    ch.queue_declare(
        "bunny-rs.test.wait-concurrent",
        QueueDeclareOptions::default(),
    )
    .await
    .unwrap();

    // Publish a batch, then wait
    for i in 0..50 {
        let body = format!("msg-{i}");
        ch.basic_publish(
            "",
            "bunny-rs.test.wait-concurrent",
            &PublishOptions::default(),
            body.as_bytes(),
        )
        .await
        .unwrap();
    }

    ch.wait_for_confirms().await.unwrap();

    // Sequence number should reflect all 50 publishes
    assert_eq!(ch.next_publish_seq_no(), 51);

    ch.queue_delete(
        "bunny-rs.test.wait-concurrent",
        QueueDeleteOptions::default(),
    )
    .await
    .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_next_publish_seq_no_is_sync() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    // Before confirms enabled, returns 0
    assert_eq!(ch.next_publish_seq_no(), 0);

    ch.confirm_select().await.unwrap();
    assert_eq!(ch.next_publish_seq_no(), 1);

    ch.queue_declare("bunny-rs.test.seq-sync", QueueDeclareOptions::default())
        .await
        .unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.seq-sync",
        &PublishOptions::default(),
        b"a",
    )
    .await
    .unwrap();

    // Sequence number is available without awaiting
    assert_eq!(ch.next_publish_seq_no(), 2);

    ch.basic_publish(
        "",
        "bunny-rs.test.seq-sync",
        &PublishOptions::default(),
        b"b",
    )
    .await
    .unwrap();
    assert_eq!(ch.next_publish_seq_no(), 3);

    ch.queue_delete("bunny-rs.test.seq-sync", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
