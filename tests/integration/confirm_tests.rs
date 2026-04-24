// Copyright (c) Michael S. Klishin and Contributors.
// Licensed under the Apache License 2.0 and MIT licenses.
// See LICENSE-APACHE and LICENSE-MIT in the repository root for details.

use crate::test_helpers::connect;
use bunny_rs::options::{PublishOptions, QueueDeclareOptions, QueueDeleteOptions};

#[tokio::test]
async fn test_confirm_select_is_idempotent() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.confirm_select().await.unwrap();
    ch.confirm_select().await.unwrap();

    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_publish_with_confirm_ack() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.confirm_select().await.unwrap();

    ch.queue_declare("bunny-rs.test.confirm-ack", QueueDeclareOptions::default())
        .await
        .unwrap();

    let confirm = ch
        .basic_publish(
            "",
            "bunny-rs.test.confirm-ack",
            &PublishOptions::default(),
            b"confirmed",
        )
        .await
        .unwrap();

    // confirms are enabled, so we should get a handle
    let confirm = confirm.expect("should have confirm handle");
    confirm.wait().await.unwrap();

    ch.queue_delete("bunny-rs.test.confirm-ack", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_publish_without_confirms_returns_none() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.queue_declare("bunny-rs.test.no-confirm", QueueDeclareOptions::default())
        .await
        .unwrap();

    let confirm = ch
        .basic_publish(
            "",
            "bunny-rs.test.no-confirm",
            &PublishOptions::default(),
            b"unconfirmed",
        )
        .await
        .unwrap();

    assert!(confirm.is_none());

    ch.queue_delete("bunny-rs.test.no-confirm", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_publish_seq_no_increments() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.confirm_select().await.unwrap();
    assert_eq!(ch.next_publish_seq_no(), 1);

    ch.queue_declare("bunny-rs.test.seq-no", QueueDeclareOptions::default())
        .await
        .unwrap();

    ch.basic_publish(
        "",
        "bunny-rs.test.seq-no",
        &PublishOptions::default(),
        b"msg1",
    )
    .await
    .unwrap();
    assert_eq!(ch.next_publish_seq_no(), 2);

    ch.basic_publish(
        "",
        "bunny-rs.test.seq-no",
        &PublishOptions::default(),
        b"msg2",
    )
    .await
    .unwrap();
    assert_eq!(ch.next_publish_seq_no(), 3);

    ch.queue_delete("bunny-rs.test.seq-no", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_wait_for_confirms_batch() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.confirm_select().await.unwrap();

    ch.queue_declare(
        "bunny-rs.test.confirm-batch",
        QueueDeclareOptions::default(),
    )
    .await
    .unwrap();

    for i in 0..10 {
        let body = format!("batch-{i}");
        ch.basic_publish(
            "",
            "bunny-rs.test.confirm-batch",
            &PublishOptions::default(),
            body.as_bytes(),
        )
        .await
        .unwrap();
    }

    ch.wait_for_confirms().await.unwrap();

    ch.queue_delete("bunny-rs.test.confirm-batch", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_confirm_multiple_individual_waits() {
    let conn = connect().await;
    let mut ch = conn.open_channel().await.unwrap();

    ch.confirm_select().await.unwrap();

    ch.queue_declare(
        "bunny-rs.test.confirm-multi",
        QueueDeclareOptions::default(),
    )
    .await
    .unwrap();

    let mut confirms = Vec::new();
    for i in 0..5 {
        let body = format!("msg-{i}");
        let c = ch
            .basic_publish(
                "",
                "bunny-rs.test.confirm-multi",
                &PublishOptions::default(),
                body.as_bytes(),
            )
            .await
            .unwrap()
            .unwrap();
        confirms.push(c);
    }

    // Wait for all individually
    for c in confirms {
        c.wait().await.unwrap();
    }

    ch.queue_delete("bunny-rs.test.confirm-multi", QueueDeleteOptions::default())
        .await
        .unwrap();
    ch.close().await.unwrap();
    conn.close().await.unwrap();
}
